//! Commit log actions (docs/commit-protocol.md §3).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const READER_VERSION: u32 = 1;
pub const WRITER_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Protocol(Protocol),
    Table(TableMeta),
    CommitInfo(CommitInfo),
    Add(AddSegment),
    Remove(RemoveSegment),
    OffsetRange(OffsetRange),
    OffsetReset(OffsetReset),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Protocol {
    pub min_reader_version: u32,
    pub min_writer_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMeta {
    pub id: Uuid,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Operation {
    Create,
    Ingest,
    Compact,
    Retention,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitInfo {
    pub txn_id: Uuid,
    pub writer: String,
    pub timestamp: i64,
    pub operation: Operation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentKind {
    Topic,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataFormat {
    Parquet,
    Avro,
}

impl From<model::FileFormat> for DataFormat {
    fn from(f: model::FileFormat) -> Self {
        match f {
            model::FileFormat::Parquet => Self::Parquet,
            model::FileFormat::Avro => Self::Avro,
        }
    }
}

impl From<DataFormat> for model::FileFormat {
    fn from(f: DataFormat) -> Self {
        match f {
            DataFormat::Parquet => Self::Parquet,
            DataFormat::Avro => Self::Avro,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionEntry {
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicEntry {
    pub topic: String,
    pub ts_min: i64,
    pub ts_max: i64,
    pub rows: u64,
    pub partitions: Vec<PartitionEntry>,
}

/// Segment manifest: everything needed for pruning without opening the file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddSegment {
    pub segment_id: Uuid,
    /// Path relative to the table root.
    pub path: String,
    pub format: DataFormat,
    /// 0 = written by ingest, 1 = compacted multi-topic, 2 = compacted single topic.
    pub level: u8,
    pub segment_type: SegmentKind,
    pub size: u64,
    pub rows: u64,
    pub created_at: i64,
    /// Every header key present in the file.
    #[serde(default)]
    pub header_keys: Vec<String>,
    /// Header keys with a `__h:<key>` pivot column (Parquet only).
    #[serde(default)]
    pub pivot_keys: Vec<String>,
    pub topics: Vec<TopicEntry>,
}

/// What a writer knows about a data file before it is uploaded.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub format: model::FileFormat,
    pub level: u8,
    pub stats: model::FileStats,
    pub pivot_keys: Vec<String>,
}

impl AddSegment {
    pub fn new(
        segment_id: Uuid,
        path: String,
        size: u64,
        created_at: i64,
        m: &SegmentMeta,
    ) -> Self {
        Self {
            segment_id,
            path,
            format: m.format.into(),
            level: m.level,
            segment_type: if m.stats.topics.len() == 1 {
                SegmentKind::Topic
            } else {
                SegmentKind::Mixed
            },
            size,
            rows: m.stats.rows,
            created_at,
            header_keys: m.stats.header_keys.clone(),
            pivot_keys: m.pivot_keys.clone(),
            topics: m
                .stats
                .topics
                .iter()
                .map(|t| TopicEntry {
                    topic: t.topic.clone(),
                    ts_min: t.ts_min,
                    ts_max: t.ts_max,
                    rows: t.rows,
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| PartitionEntry {
                            partition: p.partition,
                            first_offset: p.first_offset,
                            last_offset: p.last_offset,
                            rows: p.rows,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    pub fn topic(&self, topic: &str) -> Option<&TopicEntry> {
        self.topics.iter().find(|t| t.topic == topic)
    }

    pub fn ts_range(&self) -> (i64, i64) {
        let min = self.topics.iter().map(|t| t.ts_min).min().unwrap_or(0);
        let max = self.topics.iter().map(|t| t.ts_max).max().unwrap_or(0);
        (min, max)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoveSegment {
    pub segment_id: Uuid,
    pub deletion_timestamp: i64,
}

/// Advances `committed(topic, partition)` from `start` to `end` (exclusive).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OffsetRange {
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_uuid: Option<String>,
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

/// Deliberate jump of the committed offset (data that can no longer be indexed).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OffsetReset {
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_uuid: Option<String>,
    pub partition: i32,
    /// Expected committed offset; `None` when the partition is not tracked yet.
    pub start: Option<i64>,
    pub end: i64,
    pub reason: String,
}
