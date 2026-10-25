//! Async segment reader working on top of ranged reads.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::bloom::{Sbbf, value_set_contains};
use crate::columns;
use crate::encoding::{
    bytes::{BytesColumn, decode_bytes},
    decompress, int,
};
use crate::error::{Error, Result, corrupt};
use crate::footer::{
    BlockMeta, ChunkMeta, Footer, IndexKind, TRAILER_LEN, decode_footer_and_trailer, decode_trailer,
};
use crate::record::{Header, Record, TimestampType};
use crate::varint::Cursor;

/// Chunks closer than this are fetched with a single ranged read.
const COALESCE_GAP: u64 = 64 << 10;

#[async_trait]
pub trait RangeReader: Send + Sync {
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes>;
}

/// In-memory reader, mostly for tests and freshly written segments.
pub struct BytesReader(pub Bytes);

#[async_trait]
impl RangeReader for BytesReader {
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.end > self.0.len() as u64 || range.start > range.end {
            return Err(Error::Read(format!(
                "range {range:?} out of bounds for {} bytes",
                self.0.len()
            )));
        }
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum HeaderProjection {
    #[default]
    None,
    All,
    /// Only the value columns of these key ids (plus the shape column).
    Keys(Vec<u32>),
}

/// Columns to decode. Offsets are always decoded.
#[derive(Debug, Clone, Default)]
pub struct Projection {
    pub timestamp: bool,
    pub timestamp_type: bool,
    pub key: bool,
    pub value_size: bool,
    pub value_schema_id: bool,
    pub headers: HeaderProjection,
}

impl Projection {
    pub fn all() -> Self {
        Self {
            timestamp: true,
            timestamp_type: true,
            key: true,
            value_size: true,
            value_schema_id: true,
            headers: HeaderProjection::All,
        }
    }

    fn column_ids(&self, footer: &Footer) -> Vec<u32> {
        let mut ids = vec![columns::OFFSET];
        if self.timestamp {
            ids.push(columns::TIMESTAMP);
        }
        if self.timestamp_type {
            ids.push(columns::TIMESTAMP_TYPE);
        }
        if self.key {
            ids.push(columns::KEY);
        }
        if self.value_size {
            ids.push(columns::VALUE_SIZE);
        }
        if self.value_schema_id {
            ids.push(columns::VALUE_SCHEMA_ID);
        }
        match &self.headers {
            HeaderProjection::None => {}
            HeaderProjection::All => {
                ids.push(columns::HEADER_SHAPE);
                ids.extend((0..footer.header_keys.len() as u32).map(|k| columns::HEADER_BASE + k));
            }
            HeaderProjection::Keys(keys) => {
                ids.push(columns::HEADER_SHAPE);
                ids.extend(keys.iter().map(|k| columns::HEADER_BASE + k));
            }
        }
        ids
    }
}

/// Index entries of one column for all blocks of a segment.
pub struct ColumnIndex {
    entries: HashMap<u32, (IndexKind, Bytes)>,
}

impl ColumnIndex {
    /// `false` only when the block certainly does not contain the hashed value.
    pub fn might_contain(&self, block_idx: u32, hash: u64) -> Result<bool> {
        match self.entries.get(&block_idx) {
            None | Some((IndexKind::None, _)) => Ok(true),
            Some((IndexKind::ValueSet, data)) => value_set_contains(data, hash),
            Some((IndexKind::Bloom, data)) => Ok(Sbbf::from_bytes(data)?.contains(hash)),
        }
    }
}

pub struct SegmentReader {
    reader: Arc<dyn RangeReader>,
    footer: Arc<Footer>,
}

impl SegmentReader {
    /// Opens a segment whose footer location is known from the manifest.
    pub async fn open(
        reader: Arc<dyn RangeReader>,
        footer_offset: u64,
        footer_len: u64,
    ) -> Result<Self> {
        let data = reader
            .read_range(footer_offset..footer_offset + footer_len + TRAILER_LEN)
            .await?;
        let footer = Arc::new(decode_footer_and_trailer(&data)?);
        Ok(Self { reader, footer })
    }

    /// Opens a segment knowing only its size (two ranged reads).
    pub async fn open_with_size(reader: Arc<dyn RangeReader>, size: u64) -> Result<Self> {
        if size < TRAILER_LEN + crate::footer::HEADER_LEN {
            return corrupt("file too small");
        }
        let trailer = reader.read_range(size - TRAILER_LEN..size).await?;
        let (len, _) = decode_trailer(&trailer)?;
        let footer_offset = size - TRAILER_LEN - len as u64;
        Self::open(reader, footer_offset, len as u64).await
    }

    pub fn from_footer(reader: Arc<dyn RangeReader>, footer: Arc<Footer>) -> Self {
        Self { reader, footer }
    }

    pub fn footer(&self) -> &Arc<Footer> {
        &self.footer
    }

    /// Reads the index region of one column (a single ranged read).
    pub async fn column_index(&self, column_id: u32) -> Result<Option<ColumnIndex>> {
        let Some(meta) = self.footer.column_index(column_id) else {
            return Ok(None);
        };
        let data = self
            .reader
            .read_range(meta.offset..meta.offset + meta.length)
            .await?;
        let mut entries = HashMap::with_capacity(meta.entries.len());
        for e in &meta.entries {
            let end = e.rel_offset + e.length;
            if end > data.len() as u64 {
                return corrupt("index entry out of range");
            }
            entries.insert(
                e.block_idx,
                (e.kind, data.slice(e.rel_offset as usize..end as usize)),
            );
        }
        Ok(Some(ColumnIndex { entries }))
    }

    pub async fn read_block(&self, block_idx: usize, proj: &Projection) -> Result<DecodedBlock> {
        let Some(block) = self.footer.blocks.get(block_idx) else {
            return corrupt(format!("block {block_idx} out of range"));
        };
        let wanted = proj.column_ids(&self.footer);
        let mut chunks: Vec<&ChunkMeta> = block
            .chunks
            .iter()
            .filter(|c| wanted.contains(&c.column_id))
            .collect();
        chunks.sort_by_key(|c| c.offset);
        let raw = self.fetch_chunks(&chunks).await?;
        DecodedBlock::decode(self.footer.clone(), block_idx, block, proj, &raw)
    }

    async fn fetch_chunks(&self, chunks: &[&ChunkMeta]) -> Result<HashMap<u32, Vec<u8>>> {
        let mut groups: Vec<(Range<u64>, Vec<&ChunkMeta>)> = Vec::new();
        for c in chunks {
            let r = c.offset..c.offset + c.length;
            match groups.last_mut() {
                Some((g, members)) if r.start <= g.end + COALESCE_GAP => {
                    g.end = g.end.max(r.end);
                    members.push(c);
                }
                _ => groups.push((r, vec![c])),
            }
        }
        let mut out = HashMap::with_capacity(chunks.len());
        for (range, members) in groups {
            let data = self.reader.read_range(range.clone()).await?;
            for c in members {
                let start = (c.offset - range.start) as usize;
                let bytes = &data[start..start + c.length as usize];
                if crc32c::crc32c(bytes) != c.crc32c {
                    return corrupt(format!("checksum mismatch in column {}", c.column_id));
                }
                let payload = decompress(c.compression, bytes, c.uncompressed_len as usize)?;
                out.insert(c.column_id, payload);
            }
        }
        Ok(out)
    }
}

/// A header of a decoded row: key id and value.
pub type HeaderRef<'a> = (u32, Option<&'a [u8]>);

/// Decoded columns of one block. Columns that were not projected are empty.
pub struct DecodedBlock {
    pub footer: Arc<Footer>,
    pub block_idx: usize,
    pub topic_id: u32,
    pub partition: i32,
    pub row_count: usize,
    pub offsets: Vec<i64>,
    pub timestamps: Vec<i64>,
    pub timestamp_types: Vec<i64>,
    pub keys: Option<BytesColumn>,
    pub value_sizes: Vec<Option<i64>>,
    pub value_schema_ids: Vec<Option<i64>>,
    /// Shape id per row (empty when headers are not projected).
    pub shapes: Vec<u32>,
    /// Header value columns by key id, in occurrence order.
    pub header_cols: HashMap<u32, BytesColumn>,
}

impl DecodedBlock {
    fn decode(
        footer: Arc<Footer>,
        block_idx: usize,
        block: &BlockMeta,
        proj: &Projection,
        raw: &HashMap<u32, Vec<u8>>,
    ) -> Result<Self> {
        let n = block.row_count as usize;
        let meta = |col: u32| block.chunk(col);
        let ints = |col: u32| -> Result<Option<Vec<i64>>> {
            match (meta(col), raw.get(&col)) {
                (Some(m), Some(p)) => {
                    let mut cur = Cursor::new(p);
                    Ok(Some(int::decode_ints(
                        m.encoding,
                        &mut cur,
                        m.value_count as usize,
                    )?))
                }
                _ => Ok(None),
            }
        };
        let nullable = |col: u32| -> Result<Vec<Option<i64>>> {
            match (meta(col), raw.get(&col)) {
                (Some(m), Some(p)) => int::decode_nullable_ints(
                    m.encoding,
                    p,
                    m.value_count as usize,
                    m.null_count as usize,
                ),
                _ => Ok(vec![None; n]),
            }
        };
        let bytes_col = |col: u32| -> Result<Option<BytesColumn>> {
            match (meta(col), raw.get(&col)) {
                (Some(m), Some(p)) => Ok(Some(decode_bytes(
                    m.encoding,
                    p,
                    m.value_count as usize,
                    m.null_count as usize,
                )?)),
                _ => Ok(None),
            }
        };

        let offsets = match (meta(columns::OFFSET), raw.get(&columns::OFFSET)) {
            (Some(_), Some(p)) => {
                let mut cur = Cursor::new(p);
                int::decode_offsets(&mut cur, block.first_offset, n)?
            }
            _ => return corrupt("block without offset column"),
        };
        let timestamps = if proj.timestamp {
            ints(columns::TIMESTAMP)?.unwrap_or_default()
        } else {
            Vec::new()
        };
        let timestamp_types = if proj.timestamp_type {
            ints(columns::TIMESTAMP_TYPE)?.unwrap_or_else(|| vec![0; n])
        } else {
            Vec::new()
        };
        let keys = if proj.key {
            Some(
                bytes_col(columns::KEY)?
                    .unwrap_or_else(|| BytesColumn::from_values(std::iter::repeat_n(None, n))),
            )
        } else {
            None
        };
        let value_sizes = if proj.value_size {
            nullable(columns::VALUE_SIZE)?
        } else {
            Vec::new()
        };
        let value_schema_ids = if proj.value_schema_id {
            nullable(columns::VALUE_SCHEMA_ID)?
        } else {
            Vec::new()
        };
        let mut shapes = Vec::new();
        let mut header_cols = HashMap::new();
        if proj.headers != HeaderProjection::None {
            shapes = ints(columns::HEADER_SHAPE)?
                .unwrap_or_else(|| vec![0; n])
                .into_iter()
                .map(|s| s as u32)
                .collect();
            if let Some(bad) = shapes.iter().find(|s| **s as usize >= footer.shapes.len()) {
                return corrupt(format!("shape id {bad} out of range"));
            }
            for (col, _) in raw.iter().filter(|(c, _)| **c >= columns::HEADER_BASE) {
                if let Some(c) = bytes_col(*col)? {
                    header_cols.insert(col - columns::HEADER_BASE, c);
                }
            }
        }
        for (name, len) in [
            ("timestamp", timestamps.len()),
            ("timestamp_type", timestamp_types.len()),
            ("value_size", value_sizes.len()),
            ("value_schema_id", value_schema_ids.len()),
            ("header_shape", shapes.len()),
        ] {
            if len != 0 && len != n {
                return corrupt(format!("column {name} has {len} values, expected {n}"));
            }
        }
        Ok(Self {
            topic_id: block.topic_id,
            partition: block.partition,
            row_count: n,
            footer,
            block_idx,
            offsets,
            timestamps,
            timestamp_types,
            keys,
            value_sizes,
            value_schema_ids,
            shapes,
            header_cols,
        })
    }

    pub fn topic(&self) -> &str {
        &self.footer.topics[self.topic_id as usize]
    }

    fn shape(&self, row: usize) -> &[u32] {
        &self.footer.shapes[self.shapes[row] as usize]
    }

    /// Rows where any occurrence of header `key_id` satisfies `pred`.
    pub fn rows_where_header(
        &self,
        key_id: u32,
        mut pred: impl FnMut(Option<&[u8]>) -> bool,
    ) -> Vec<usize> {
        let Some(col) = self.header_cols.get(&key_id) else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        let mut pos = 0usize;
        for row in 0..self.shapes.len() {
            let occurrences = self.shape(row).iter().filter(|k| **k == key_id).count();
            if (pos..pos + occurrences).any(|p| p < col.len() && pred(col.get(p))) {
                rows.push(row);
            }
            pos += occurrences;
        }
        rows
    }

    /// Headers of every row; only projected header keys are included.
    pub fn header_rows(&self) -> Vec<Vec<HeaderRef<'_>>> {
        let mut cursors: HashMap<u32, usize> = HashMap::new();
        let mut out = Vec::with_capacity(self.shapes.len());
        for row in 0..self.shapes.len() {
            let mut headers = Vec::with_capacity(self.shape(row).len());
            for k in self.shape(row) {
                let pos = cursors.entry(*k).or_insert(0);
                if let Some(col) = self.header_cols.get(k)
                    && *pos < col.len()
                {
                    headers.push((*k, col.get(*pos)));
                }
                *pos += 1;
            }
            out.push(headers);
        }
        out
    }

    /// Full records; requires a block decoded with `Projection::all()`.
    pub fn to_records(&self) -> Vec<Record> {
        let headers = self.header_rows();
        (0..self.row_count)
            .map(|i| Record {
                offset: self.offsets[i],
                timestamp: self.timestamps.get(i).copied().unwrap_or_default(),
                timestamp_type: TimestampType::from_code(
                    self.timestamp_types.get(i).copied().unwrap_or_default(),
                ),
                key: self
                    .keys
                    .as_ref()
                    .and_then(|k| k.get(i))
                    .map(<[u8]>::to_vec),
                value_size: self.value_sizes.get(i).copied().flatten().map(|v| v as u32),
                value_schema_id: self
                    .value_schema_ids
                    .get(i)
                    .copied()
                    .flatten()
                    .map(|v| v as i32),
                headers: headers
                    .get(i)
                    .map(|hs| {
                        hs.iter()
                            .map(|(k, v)| Header {
                                key: self.footer.header_keys[*k as usize].clone(),
                                value: v.map(<[u8]>::to_vec),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect()
    }
}
