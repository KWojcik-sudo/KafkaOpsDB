//! Executes one ingest task: fetch → segments → upload → single atomic commit.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use kseg::{SegmentType, SegmentWriter, WriterOptions};
use table::{Action, OffsetRange, Operation, SegmentKind, Table, now_ms};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::planner::IngestTask;
use crate::source::{FetchedPartition, KafkaSource};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// A topic with at least this much metadata in a task gets its own `topic` segment.
    pub topic_segment_min_bytes: u64,
    pub writer: WriterOptions,
    pub node: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            topic_segment_min_bytes: 16 << 20,
            writer: WriterOptions::default(),
            node: "kafkaopsdb".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    Committed {
        version: u64,
    },
    /// Nothing could be read (e.g. an open transaction holds back the LSO).
    NoProgress,
    /// Someone else already committed (part of) these ranges; the work was discarded.
    Conflict(String),
    Failed(String),
}

#[derive(Debug, Clone, Default)]
pub struct TaskStats {
    pub records: u64,
    pub metadata_bytes: u64,
    /// (topic, records, metadata bytes)
    pub per_topic: Vec<(String, u64, u64)>,
    pub segments: Vec<(SegmentKind, u64)>,
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub task: IngestTask,
    pub outcome: TaskOutcome,
    pub stats: TaskStats,
    pub duration: Duration,
}

pub async fn execute_task(
    table: &Table,
    source: &dyn KafkaSource,
    cfg: &WorkerConfig,
    task: IngestTask,
) -> TaskResult {
    let started = Instant::now();
    let mut stats = TaskStats::default();
    let outcome = match run(table, source, cfg, &task, &mut stats).await {
        Ok(Some(version)) => TaskOutcome::Committed { version },
        Ok(None) => TaskOutcome::NoProgress,
        Err(Error::Table(e)) if e.is_conflict() => TaskOutcome::Conflict(e.to_string()),
        Err(e) => TaskOutcome::Failed(e.to_string()),
    };
    match &outcome {
        TaskOutcome::Conflict(reason) => {
            warn!(task = %task.id, %reason, "task conflicted, work discarded")
        }
        TaskOutcome::Failed(reason) => warn!(task = %task.id, %reason, "task failed"),
        _ => debug!(task = %task.id, ?outcome, records = stats.records, "task done"),
    }
    TaskResult {
        task,
        outcome,
        stats,
        duration: started.elapsed(),
    }
}

async fn run(
    table: &Table,
    source: &dyn KafkaSource,
    cfg: &WorkerConfig,
    task: &IngestTask,
    stats: &mut TaskStats,
) -> Result<Option<u64>> {
    let fetched = source.fetch(&task.ranges).await?;

    let mut by_topic: BTreeMap<String, Vec<&FetchedPartition>> = BTreeMap::new();
    for f in &fetched {
        by_topic.entry(f.topic.clone()).or_default().push(f);
    }
    let mut writers = Vec::new();
    let mut mixed = SegmentWriter::new(SegmentType::Mixed, 0, cfg.writer.clone());
    for (topic, parts) in &by_topic {
        let records: u64 = parts.iter().map(|p| p.records.len() as u64).sum();
        let bytes: u64 = parts
            .iter()
            .flat_map(|p| &p.records)
            .map(|r| r.metadata_size() as u64)
            .sum();
        stats.records += records;
        stats.metadata_bytes += bytes;
        stats.per_topic.push((topic.clone(), records, bytes));
        if records == 0 {
            continue;
        }
        let target = if bytes >= cfg.topic_segment_min_bytes {
            writers.push(SegmentWriter::new(
                SegmentType::Topic,
                0,
                cfg.writer.clone(),
            ));
            writers.last_mut().unwrap()
        } else {
            &mut mixed
        };
        for p in parts {
            target.add(topic, p.partition, p.records.clone())?;
        }
    }
    if !mixed.is_empty() {
        writers.push(mixed);
    }

    let node = cfg.node.clone();
    let encoded = tokio::task::spawn_blocking(move || {
        writers
            .into_iter()
            .map(|w| w.finish(Uuid::now_v7(), now_ms(), &node))
            .collect::<kseg::Result<Vec<_>>>()
    })
    .await
    .map_err(|e| Error::Join(e.to_string()))??;

    let mut actions = Vec::new();
    for (data, summary) in encoded {
        let add = table.put_segment(data, &summary).await?;
        stats.segments.push((add.segment_type, add.size));
        actions.push(Action::Add(add));
    }
    let mut progressed = false;
    for f in &fetched {
        if f.next_offset > f.start {
            progressed = true;
            actions.push(Action::OffsetRange(OffsetRange {
                topic: f.topic.clone(),
                topic_uuid: None,
                partition: f.partition,
                start: f.start,
                end: f.next_offset,
            }));
        }
    }
    if !progressed {
        return Ok(None);
    }
    let version = table
        .commit(Operation::Ingest, Some(task.id.clone()), actions)
        .await?;
    Ok(Some(version))
}
