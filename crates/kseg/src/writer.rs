//! Builds a complete KSEG file in memory.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use uuid::Uuid;

use crate::bloom::{Sbbf, hash_value, value_set_bytes};
use crate::columns;
use crate::encoding::{bytes::encode_bytes, code, compress, int};
use crate::error::{Error, Result};
use crate::footer::{
    BlockIndexEntry, BlockMeta, ChunkMeta, ColumnIndexMeta, Footer, IndexKind, SegmentType,
    encode_header, encode_trailer,
};
use crate::record::Record;

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub max_block_rows: usize,
    pub max_block_bytes: usize,
    pub bloom_bits_per_value: u32,
    pub value_set_max: usize,
    pub zstd_level: i32,
    pub index_key: bool,
    pub exclude_header_keys: HashSet<String>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            max_block_rows: 65_536,
            max_block_bytes: 8 << 20,
            bloom_bits_per_value: 10,
            value_set_max: 64,
            zstd_level: 3,
            index_key: true,
            exclude_header_keys: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSummary {
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicSummary {
    pub topic: String,
    pub ts_min: i64,
    pub ts_max: i64,
    pub rows: u64,
    pub partitions: Vec<PartitionSummary>,
}

/// What the commit log needs to know about a written segment.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentSummary {
    pub segment_id: Uuid,
    pub segment_type: SegmentType,
    pub level: u8,
    pub created_at_ms: i64,
    pub size: u64,
    pub rows: u64,
    pub footer_offset: u64,
    pub footer_len: u64,
    pub header_keys: Vec<String>,
    pub topics: Vec<TopicSummary>,
}

pub struct SegmentWriter {
    opts: WriterOptions,
    segment_type: SegmentType,
    level: u8,
    parts: BTreeMap<(String, i32), Vec<Record>>,
    rows: usize,
}

impl SegmentWriter {
    pub fn new(segment_type: SegmentType, level: u8, opts: WriterOptions) -> Self {
        Self {
            opts,
            segment_type,
            level,
            parts: BTreeMap::new(),
            rows: 0,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn topics(&self) -> BTreeSet<&str> {
        self.parts.keys().map(|(t, _)| t.as_str()).collect()
    }

    /// Appends records of one partition. Offsets must be strictly increasing,
    /// also across calls for the same partition.
    pub fn add(&mut self, topic: &str, partition: i32, records: Vec<Record>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let entry = self.parts.entry((topic.to_owned(), partition)).or_default();
        let mut prev = entry.last().map(|r| r.offset);
        for r in &records {
            if let Some(p) = prev
                && r.offset <= p
            {
                return Err(Error::InvalidInput(format!(
                    "offsets must be strictly increasing in {topic}/{partition}: {} after {p}",
                    r.offset
                )));
            }
            prev = Some(r.offset);
        }
        self.rows += records.len();
        entry.extend(records);
        Ok(())
    }

    pub fn finish(
        self,
        segment_id: Uuid,
        created_at_ms: i64,
        writer: &str,
    ) -> Result<(Vec<u8>, SegmentSummary)> {
        if self.parts.is_empty() {
            return Err(Error::InvalidInput("segment has no records".into()));
        }
        let topics: Vec<String> = self
            .parts
            .keys()
            .map(|(t, _)| t.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if self.segment_type == SegmentType::Topic && topics.len() != 1 {
            return Err(Error::InvalidInput(format!(
                "topic segment must contain exactly one topic, got {}",
                topics.len()
            )));
        }
        let topic_ids: HashMap<&str, u32> = topics
            .iter()
            .enumerate()
            .map(|(i, t)| (t.as_str(), i as u32))
            .collect();
        let header_keys: Vec<String> = self
            .parts
            .values()
            .flatten()
            .flat_map(|r| r.headers.iter().map(|h| h.key.as_str()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(str::to_owned)
            .collect();
        let key_ids: HashMap<&str, u32> = header_keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i as u32))
            .collect();
        let indexed_header_cols: HashSet<u32> = header_keys
            .iter()
            .enumerate()
            .filter(|(_, k)| !self.opts.exclude_header_keys.contains(*k))
            .map(|(i, _)| columns::HEADER_BASE + i as u32)
            .collect();

        let mut ctx = BlockCtx {
            opts: &self.opts,
            key_ids: &key_ids,
            indexed_header_cols: &indexed_header_cols,
            shapes: vec![Vec::new()],
            shape_ids: HashMap::from([(Vec::new(), 0u32)]),
            index_data: BTreeMap::new(),
            out: encode_header(self.segment_type).to_vec(),
        };
        let mut blocks = Vec::new();
        let mut topic_summaries: BTreeMap<String, TopicSummary> = BTreeMap::new();

        for ((topic, partition), records) in &self.parts {
            let topic_id = topic_ids[topic.as_str()];
            let mut start = 0;
            while start < records.len() {
                let mut end = start;
                let mut bytes = 0usize;
                while end < records.len()
                    && end - start < self.opts.max_block_rows
                    && (end == start || bytes < self.opts.max_block_bytes)
                {
                    bytes += records[end].metadata_size();
                    end += 1;
                }
                let block_idx = blocks.len() as u32;
                blocks.push(ctx.write_block(
                    &records[start..end],
                    topic_id,
                    *partition,
                    block_idx,
                )?);
                start = end;
            }
            let ts_min = records.iter().map(|r| r.timestamp).min().unwrap();
            let ts_max = records.iter().map(|r| r.timestamp).max().unwrap();
            let ts = topic_summaries
                .entry(topic.clone())
                .or_insert_with(|| TopicSummary {
                    topic: topic.clone(),
                    ts_min,
                    ts_max,
                    rows: 0,
                    partitions: Vec::new(),
                });
            ts.ts_min = ts.ts_min.min(ts_min);
            ts.ts_max = ts.ts_max.max(ts_max);
            ts.rows += records.len() as u64;
            ts.partitions.push(PartitionSummary {
                partition: *partition,
                first_offset: records[0].offset,
                last_offset: records.last().unwrap().offset,
                rows: records.len() as u64,
            });
        }

        let BlockCtx {
            shapes,
            index_data,
            mut out,
            ..
        } = ctx;

        let index_start = out.len() as u64;
        let mut column_indexes = Vec::new();
        for (column_id, entries) in index_data {
            let col_offset = out.len() as u64;
            let mut metas = Vec::with_capacity(entries.len());
            for (block_idx, kind, data) in entries {
                metas.push(BlockIndexEntry {
                    block_idx,
                    kind,
                    rel_offset: out.len() as u64 - col_offset,
                    length: data.len() as u64,
                });
                out.extend_from_slice(&data);
            }
            column_indexes.push(ColumnIndexMeta {
                column_id,
                offset: col_offset,
                length: out.len() as u64 - col_offset,
                entries: metas,
            });
        }
        let index_section = (index_start, out.len() as u64 - index_start);

        let footer = Footer {
            segment_id: *segment_id.as_bytes(),
            segment_type: self.segment_type,
            level: self.level,
            created_at_ms,
            writer: writer.to_owned(),
            topics,
            header_keys: header_keys.clone(),
            shapes,
            blocks,
            index_section,
            column_indexes,
        };
        let footer_bytes = zstd::bulk::compress(&footer.encode(), 3)?;
        let footer_offset = out.len() as u64;
        let footer_len = footer_bytes.len() as u64;
        let crc = crc32c::crc32c(&footer_bytes);
        out.extend_from_slice(&footer_bytes);
        out.extend_from_slice(&encode_trailer(footer_len as u32, crc));

        let summary = SegmentSummary {
            segment_id,
            segment_type: self.segment_type,
            level: self.level,
            created_at_ms,
            size: out.len() as u64,
            rows: self.rows as u64,
            footer_offset,
            footer_len,
            header_keys,
            topics: topic_summaries.into_values().collect(),
        };
        Ok((out, summary))
    }
}

type IndexEntries = Vec<(u32, IndexKind, Vec<u8>)>;

struct BlockCtx<'a> {
    opts: &'a WriterOptions,
    key_ids: &'a HashMap<&'a str, u32>,
    indexed_header_cols: &'a HashSet<u32>,
    shapes: Vec<Vec<u32>>,
    shape_ids: HashMap<Vec<u32>, u32>,
    index_data: BTreeMap<u32, IndexEntries>,
    out: Vec<u8>,
}

impl BlockCtx<'_> {
    fn write_block(
        &mut self,
        recs: &[Record],
        topic_id: u32,
        partition: i32,
        block_idx: u32,
    ) -> Result<BlockMeta> {
        let n = recs.len();
        let mut chunks = Vec::new();

        let offsets: Vec<i64> = recs.iter().map(|r| r.offset).collect();
        chunks.push(self.push_chunk(
            columns::OFFSET,
            code::OFFSET_GAPS,
            int::encode_offsets(&offsets),
            n,
            0,
        )?);

        let ts: Vec<i64> = recs.iter().map(|r| r.timestamp).collect();
        let (c, p) = int::encode_ints(&ts);
        chunks.push(self.push_chunk(columns::TIMESTAMP, c, p, n, 0)?);

        let tst: Vec<i64> = recs.iter().map(|r| r.timestamp_type as i64).collect();
        let (c, p) = int::encode_ints(&tst);
        chunks.push(self.push_chunk(columns::TIMESTAMP_TYPE, c, p, n, 0)?);

        let keys: Vec<Option<&[u8]>> = recs.iter().map(|r| r.key.as_deref()).collect();
        if keys.iter().any(Option::is_some) {
            let (c, p, nulls) = encode_bytes(&keys);
            chunks.push(self.push_chunk(columns::KEY, c, p, n, nulls)?);
            if self.opts.index_key {
                self.build_index(columns::KEY, block_idx, keys.iter().flatten().copied());
            }
        }

        let sizes: Vec<Option<i64>> = recs.iter().map(|r| r.value_size.map(i64::from)).collect();
        if sizes.iter().any(Option::is_some) {
            let (c, p, nulls) = int::encode_nullable_ints(&sizes);
            chunks.push(self.push_chunk(columns::VALUE_SIZE, c, p, n, nulls)?);
        }

        let schema_ids: Vec<Option<i64>> = recs
            .iter()
            .map(|r| r.value_schema_id.map(i64::from))
            .collect();
        if schema_ids.iter().any(Option::is_some) {
            let (c, p, nulls) = int::encode_nullable_ints(&schema_ids);
            chunks.push(self.push_chunk(columns::VALUE_SCHEMA_ID, c, p, n, nulls)?);
        }

        // headers: shape column + one value column per header key
        let mut shape_col = Vec::with_capacity(n);
        let mut values: BTreeMap<u32, Vec<Option<&[u8]>>> = BTreeMap::new();
        for r in recs {
            let shape: Vec<u32> = r
                .headers
                .iter()
                .map(|h| self.key_ids[h.key.as_str()])
                .collect();
            for (h, k) in r.headers.iter().zip(&shape) {
                values.entry(*k).or_default().push(h.value.as_deref());
            }
            let next = self.shapes.len() as u32;
            let id = match self.shape_ids.get(&shape) {
                Some(id) => *id,
                None => {
                    self.shapes.push(shape.clone());
                    self.shape_ids.insert(shape, next);
                    next
                }
            };
            shape_col.push(id as i64);
        }
        if shape_col.iter().any(|s| *s != 0) {
            let (c, p) = int::encode_ints(&shape_col);
            chunks.push(self.push_chunk(columns::HEADER_SHAPE, c, p, n, 0)?);
        }
        for (key_id, vals) in values {
            let column_id = columns::HEADER_BASE + key_id;
            let (c, p, nulls) = encode_bytes(&vals);
            chunks.push(self.push_chunk(column_id, c, p, vals.len(), nulls)?);
            if self.indexed_header_cols.contains(&column_id) {
                self.build_index(column_id, block_idx, vals.iter().flatten().copied());
            }
        }

        Ok(BlockMeta {
            topic_id,
            partition,
            row_count: n as u32,
            first_offset: offsets[0],
            last_offset: *offsets.last().unwrap(),
            ts_min: *ts.iter().min().unwrap(),
            ts_max: *ts.iter().max().unwrap(),
            chunks,
        })
    }

    fn push_chunk(
        &mut self,
        column_id: u32,
        encoding: u8,
        payload: Vec<u8>,
        value_count: usize,
        null_count: usize,
    ) -> Result<ChunkMeta> {
        let uncompressed_len = payload.len() as u64;
        let (compression, data) = compress(payload, self.opts.zstd_level)?;
        let meta = ChunkMeta {
            column_id,
            offset: self.out.len() as u64,
            length: data.len() as u64,
            uncompressed_len,
            encoding,
            compression,
            value_count: value_count as u64,
            null_count: null_count as u64,
            crc32c: crc32c::crc32c(&data),
        };
        self.out.extend_from_slice(&data);
        Ok(meta)
    }

    fn build_index<'v>(
        &mut self,
        column_id: u32,
        block_idx: u32,
        values: impl Iterator<Item = &'v [u8]>,
    ) {
        let hashes: HashSet<u64> = values.map(hash_value).collect();
        if hashes.is_empty() {
            return;
        }
        let entry = if hashes.len() <= self.opts.value_set_max {
            let mut sorted: Vec<u64> = hashes.into_iter().collect();
            sorted.sort_unstable();
            (block_idx, IndexKind::ValueSet, value_set_bytes(&sorted))
        } else {
            let mut bloom = Sbbf::with_ndv(hashes.len(), self.opts.bloom_bits_per_value);
            for h in hashes {
                bloom.insert(h);
            }
            (block_idx, IndexKind::Bloom, bloom.to_bytes())
        };
        self.index_data.entry(column_id).or_default().push(entry);
    }
}
