//! Header, footer and trailer serialization (docs/format-kseg.md §2, §4).

use crate::error::{Result, corrupt};
use crate::varint::{Cursor, put_str, put_uvarint, put_zvarint};

pub const MAGIC: &[u8; 4] = b"KSEG";
pub const FORMAT_VERSION: u16 = 1;
pub const HEADER_LEN: u64 = 8;
pub const TRAILER_LEN: u64 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SegmentType {
    Topic = 0,
    Mixed = 1,
}

impl SegmentType {
    pub fn from_code(c: u8) -> Result<Self> {
        match c {
            0 => Ok(Self::Topic),
            1 => Ok(Self::Mixed),
            other => corrupt(format!("unknown segment type {other}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Mixed => "mixed",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkMeta {
    pub column_id: u32,
    pub offset: u64,
    pub length: u64,
    pub uncompressed_len: u64,
    pub encoding: u8,
    pub compression: u8,
    pub value_count: u64,
    pub null_count: u64,
    pub crc32c: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockMeta {
    pub topic_id: u32,
    pub partition: i32,
    pub row_count: u32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub ts_min: i64,
    pub ts_max: i64,
    pub chunks: Vec<ChunkMeta>,
}

impl BlockMeta {
    pub fn chunk(&self, column_id: u32) -> Option<&ChunkMeta> {
        self.chunks.iter().find(|c| c.column_id == column_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    None = 0,
    ValueSet = 1,
    Bloom = 2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockIndexEntry {
    pub block_idx: u32,
    pub kind: IndexKind,
    pub rel_offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnIndexMeta {
    pub column_id: u32,
    pub offset: u64,
    pub length: u64,
    pub entries: Vec<BlockIndexEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub segment_id: [u8; 16],
    pub segment_type: SegmentType,
    pub level: u8,
    pub created_at_ms: i64,
    pub writer: String,
    pub topics: Vec<String>,
    pub header_keys: Vec<String>,
    pub shapes: Vec<Vec<u32>>,
    pub blocks: Vec<BlockMeta>,
    pub index_section: (u64, u64),
    pub column_indexes: Vec<ColumnIndexMeta>,
}

impl Footer {
    pub fn topic_id(&self, topic: &str) -> Option<u32> {
        self.topics
            .iter()
            .position(|t| t == topic)
            .map(|i| i as u32)
    }

    pub fn header_key_id(&self, key: &str) -> Option<u32> {
        self.header_keys
            .iter()
            .position(|k| k == key)
            .map(|i| i as u32)
    }

    pub fn column_index(&self, column_id: u32) -> Option<&ColumnIndexMeta> {
        self.column_indexes
            .iter()
            .find(|c| c.column_id == column_id)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.segment_id);
        b.push(self.segment_type as u8);
        b.push(self.level);
        put_zvarint(&mut b, self.created_at_ms);
        put_str(&mut b, &self.writer);
        put_uvarint(&mut b, self.topics.len() as u64);
        for t in &self.topics {
            put_str(&mut b, t);
        }
        put_uvarint(&mut b, self.header_keys.len() as u64);
        for k in &self.header_keys {
            put_str(&mut b, k);
        }
        put_uvarint(&mut b, self.shapes.len() as u64);
        for s in &self.shapes {
            put_uvarint(&mut b, s.len() as u64);
            for k in s {
                put_uvarint(&mut b, *k as u64);
            }
        }
        put_uvarint(&mut b, self.blocks.len() as u64);
        for blk in &self.blocks {
            put_uvarint(&mut b, blk.topic_id as u64);
            put_zvarint(&mut b, blk.partition as i64);
            put_uvarint(&mut b, blk.row_count as u64);
            put_zvarint(&mut b, blk.first_offset);
            put_zvarint(&mut b, blk.last_offset);
            put_zvarint(&mut b, blk.ts_min);
            put_zvarint(&mut b, blk.ts_max);
            put_uvarint(&mut b, blk.chunks.len() as u64);
            for c in &blk.chunks {
                put_uvarint(&mut b, c.column_id as u64);
                put_uvarint(&mut b, c.offset);
                put_uvarint(&mut b, c.length);
                put_uvarint(&mut b, c.uncompressed_len);
                b.push(c.encoding);
                b.push(c.compression);
                put_uvarint(&mut b, c.value_count);
                put_uvarint(&mut b, c.null_count);
                b.extend_from_slice(&c.crc32c.to_le_bytes());
            }
        }
        put_uvarint(&mut b, self.index_section.0);
        put_uvarint(&mut b, self.index_section.1);
        put_uvarint(&mut b, self.column_indexes.len() as u64);
        for ci in &self.column_indexes {
            put_uvarint(&mut b, ci.column_id as u64);
            put_uvarint(&mut b, ci.offset);
            put_uvarint(&mut b, ci.length);
            put_uvarint(&mut b, ci.entries.len() as u64);
            for e in &ci.entries {
                put_uvarint(&mut b, e.block_idx as u64);
                b.push(e.kind as u8);
                put_uvarint(&mut b, e.rel_offset);
                put_uvarint(&mut b, e.length);
            }
        }
        b
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut c = Cursor::new(data);
        let segment_id: [u8; 16] = c.take(16)?.try_into().unwrap();
        let segment_type = SegmentType::from_code(c.u8()?)?;
        let level = c.u8()?;
        let created_at_ms = c.zvarint()?;
        let writer = c.string()?;
        let max = data.len();
        let n = c.len(max)?;
        let topics = (0..n).map(|_| c.string()).collect::<Result<Vec<_>>>()?;
        let n = c.len(max)?;
        let header_keys = (0..n).map(|_| c.string()).collect::<Result<Vec<_>>>()?;
        let n = c.len(max)?;
        let mut shapes = Vec::with_capacity(n);
        for _ in 0..n {
            let len = c.len(max)?;
            let mut s = Vec::with_capacity(len);
            for _ in 0..len {
                let k = c.len(header_keys.len().saturating_sub(1))?;
                s.push(k as u32);
            }
            shapes.push(s);
        }
        let n = c.len(max)?;
        let mut blocks = Vec::with_capacity(n);
        for _ in 0..n {
            let topic_id = c.len(topics.len().saturating_sub(1))? as u32;
            let partition = c.zvarint()? as i32;
            let row_count = c.uvarint()? as u32;
            let first_offset = c.zvarint()?;
            let last_offset = c.zvarint()?;
            let ts_min = c.zvarint()?;
            let ts_max = c.zvarint()?;
            let nc = c.len(max)?;
            let mut chunks = Vec::with_capacity(nc);
            for _ in 0..nc {
                chunks.push(ChunkMeta {
                    column_id: c.uvarint()? as u32,
                    offset: c.uvarint()?,
                    length: c.uvarint()?,
                    uncompressed_len: c.uvarint()?,
                    encoding: c.u8()?,
                    compression: c.u8()?,
                    value_count: c.uvarint()?,
                    null_count: c.uvarint()?,
                    crc32c: c.u32_le()?,
                });
            }
            blocks.push(BlockMeta {
                topic_id,
                partition,
                row_count,
                first_offset,
                last_offset,
                ts_min,
                ts_max,
                chunks,
            });
        }
        let index_section = (c.uvarint()?, c.uvarint()?);
        let n = c.len(max)?;
        let mut column_indexes = Vec::with_capacity(n);
        for _ in 0..n {
            let column_id = c.uvarint()? as u32;
            let offset = c.uvarint()?;
            let length = c.uvarint()?;
            let ne = c.len(max)?;
            let mut entries = Vec::with_capacity(ne);
            for _ in 0..ne {
                let block_idx = c.uvarint()? as u32;
                let kind = match c.u8()? {
                    0 => IndexKind::None,
                    1 => IndexKind::ValueSet,
                    2 => IndexKind::Bloom,
                    other => return corrupt(format!("unknown index kind {other}")),
                };
                entries.push(BlockIndexEntry {
                    block_idx,
                    kind,
                    rel_offset: c.uvarint()?,
                    length: c.uvarint()?,
                });
            }
            column_indexes.push(ColumnIndexMeta {
                column_id,
                offset,
                length,
                entries,
            });
        }
        if c.remaining() != 0 {
            return corrupt("trailing bytes in footer");
        }
        Ok(Self {
            segment_id,
            segment_type,
            level,
            created_at_ms,
            writer,
            topics,
            header_keys,
            shapes,
            blocks,
            index_section,
            column_indexes,
        })
    }
}

pub fn encode_header(segment_type: SegmentType) -> [u8; 8] {
    let mut h = [0u8; 8];
    h[..4].copy_from_slice(MAGIC);
    h[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    h[6] = segment_type as u8;
    h
}

pub fn encode_trailer(footer_len: u32, footer_crc: u32) -> [u8; 16] {
    let mut t = [0u8; 16];
    t[..4].copy_from_slice(&footer_len.to_le_bytes());
    t[4..8].copy_from_slice(&footer_crc.to_le_bytes());
    t[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    t[12..].copy_from_slice(MAGIC);
    t
}

/// Parses the trailer; returns (footer_len, footer_crc).
pub fn decode_trailer(t: &[u8]) -> Result<(u32, u32)> {
    if t.len() != TRAILER_LEN as usize || &t[12..] != MAGIC {
        return corrupt("bad trailer magic");
    }
    let version = u16::from_le_bytes(t[8..10].try_into().unwrap());
    if version != FORMAT_VERSION {
        return Err(crate::Error::Unsupported(format!(
            "segment format version {version}"
        )));
    }
    Ok((
        u32::from_le_bytes(t[..4].try_into().unwrap()),
        u32::from_le_bytes(t[4..8].try_into().unwrap()),
    ))
}

/// Decodes the compressed footer bytes followed by the trailer.
pub fn decode_footer_and_trailer(data: &[u8]) -> Result<Footer> {
    if data.len() < TRAILER_LEN as usize {
        return corrupt("footer region too short");
    }
    let (body, trailer) = data.split_at(data.len() - TRAILER_LEN as usize);
    let (len, crc) = decode_trailer(trailer)?;
    if len as usize != body.len() {
        return corrupt("footer length mismatch");
    }
    if crc32c::crc32c(body) != crc {
        return corrupt("footer checksum mismatch");
    }
    let raw = zstd::stream::decode_all(body)?;
    Footer::decode(&raw)
}
