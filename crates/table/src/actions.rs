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

impl From<kseg::SegmentType> for SegmentKind {
    fn from(t: kseg::SegmentType) -> Self {
        match t {
            kseg::SegmentType::Topic => Self::Topic,
            kseg::SegmentType::Mixed => Self::Mixed,
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
    pub segment_type: SegmentKind,
    pub level: u8,
    pub size: u64,
    pub rows: u64,
    pub footer_offset: u64,
    pub footer_len: u64,
    pub format_version: u16,
    pub created_at: i64,
    #[serde(default)]
    pub header_keys: Vec<String>,
    pub topics: Vec<TopicEntry>,
}

impl AddSegment {
    pub fn from_summary(path: String, s: &kseg::SegmentSummary) -> Self {
        Self {
            segment_id: s.segment_id,
            path,
            segment_type: s.segment_type.into(),
            level: s.level,
            size: s.size,
            rows: s.rows,
            footer_offset: s.footer_offset,
            footer_len: s.footer_len,
            format_version: kseg::footer::FORMAT_VERSION,
            created_at: s.created_at_ms,
            header_keys: s.header_keys.clone(),
            topics: s
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
