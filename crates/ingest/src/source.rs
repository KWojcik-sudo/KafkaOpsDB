//! Where records come from: the Kafka source abstraction and an in-memory implementation.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use model::Record;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMeta {
    pub partition: i32,
    pub log_start: i64,
    pub high_watermark: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: Vec<PartitionMeta>,
}

/// Records with offsets in `[start, end)` of one partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FetchRange {
    pub topic: String,
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone)]
pub struct FetchedPartition {
    pub topic: String,
    pub partition: i32,
    pub start: i64,
    /// Consumer position after the fetch; `<= end` of the requested range. Smaller than
    /// `end` when the fetch timed out (e.g. an open transaction holds back the LSO).
    pub next_offset: i64,
    pub records: Vec<Record>,
}

#[async_trait]
pub trait KafkaSource: Send + Sync {
    async fn list_topics(&self) -> Result<Vec<TopicMeta>>;

    /// Reads the requested ranges. Records outside a range are dropped. A range whose
    /// start is below the partition's log start fails with `Error::OffsetOutOfRange`.
    async fn fetch(&self, ranges: &[FetchRange]) -> Result<Vec<FetchedPartition>>;
}

#[derive(Default)]
struct MemPartition {
    log_start: i64,
    next: i64,
    records: Vec<Record>,
}

/// In-memory "cluster" used by tests and examples.
#[derive(Default)]
pub struct MemorySource {
    topics: Mutex<BTreeMap<String, BTreeMap<i32, MemPartition>>>,
    pub fail_fetch: AtomicBool,
}

impl MemorySource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_topic(&self, topic: &str, partitions: i32) {
        let mut t = self.topics.lock().unwrap();
        let parts = t.entry(topic.to_owned()).or_default();
        for p in 0..partitions {
            parts.entry(p).or_default();
        }
    }

    pub fn delete_topic(&self, topic: &str) {
        self.topics.lock().unwrap().remove(topic);
    }

    /// Appends a record and returns its offset. `skip` leaves a gap before it
    /// (transaction markers, compaction).
    pub fn produce(&self, topic: &str, partition: i32, mut record: Record, skip: i64) -> i64 {
        let mut t = self.topics.lock().unwrap();
        let p = t
            .entry(topic.to_owned())
            .or_default()
            .entry(partition)
            .or_default();
        p.next += skip;
        record.offset = p.next;
        p.next += 1;
        p.records.push(record);
        p.next - 1
    }

    /// Simulates retention deleting everything below `log_start`.
    pub fn truncate(&self, topic: &str, partition: i32, log_start: i64) {
        let mut t = self.topics.lock().unwrap();
        if let Some(p) = t.get_mut(topic).and_then(|t| t.get_mut(&partition)) {
            p.log_start = log_start.min(p.next);
            p.records.retain(|r| r.offset >= log_start);
        }
    }

    pub fn high_watermark(&self, topic: &str, partition: i32) -> i64 {
        let t = self.topics.lock().unwrap();
        t.get(topic)
            .and_then(|t| t.get(&partition))
            .map_or(0, |p| p.next)
    }
}

#[async_trait]
impl KafkaSource for MemorySource {
    async fn list_topics(&self) -> Result<Vec<TopicMeta>> {
        let t = self.topics.lock().unwrap();
        Ok(t.iter()
            .map(|(name, parts)| TopicMeta {
                name: name.clone(),
                partitions: parts
                    .iter()
                    .map(|(id, p)| PartitionMeta {
                        partition: *id,
                        log_start: p.log_start,
                        high_watermark: p.next,
                    })
                    .collect(),
            })
            .collect())
    }

    async fn fetch(&self, ranges: &[FetchRange]) -> Result<Vec<FetchedPartition>> {
        if self.fail_fetch.load(Ordering::SeqCst) {
            return Err(Error::Kafka("injected fetch failure".into()));
        }
        let t = self.topics.lock().unwrap();
        ranges
            .iter()
            .map(|r| {
                let p = t
                    .get(&r.topic)
                    .and_then(|t| t.get(&r.partition))
                    .ok_or_else(|| {
                        Error::Kafka(format!("unknown partition {}/{}", r.topic, r.partition))
                    })?;
                if r.start < p.log_start {
                    return Err(Error::OffsetOutOfRange {
                        topic: r.topic.clone(),
                        partition: r.partition,
                        requested: r.start,
                    });
                }
                let records: Vec<Record> = p
                    .records
                    .iter()
                    .filter(|rec| rec.offset >= r.start && rec.offset < r.end)
                    .cloned()
                    .collect();
                Ok(FetchedPartition {
                    topic: r.topic.clone(),
                    partition: r.partition,
                    start: r.start,
                    next_offset: r.end.min(p.next),
                    records,
                })
            })
            .collect()
    }
}
