//! Executes one ingest task: fetch → one L0 file → upload → single atomic commit.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use model::{FileFormat, FileStats, ParquetOptions, PivotConfig};
use table::{Action, DataFormat, OffsetRange, Operation, SegmentMeta, Table};
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::planner::IngestTask;
use crate::source::{FetchedPartition, KafkaSource};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// L0 files with fewer rows are written as Avro, larger ones as Parquet (Snappy).
    pub l0_parquet_min_rows: usize,
    pub pivots: PivotConfig,
    pub node: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            l0_parquet_min_rows: 25_000,
            pivots: PivotConfig::default(),
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
    /// Format and size of the written L0 file(s).
    pub segments: Vec<(DataFormat, u64)>,
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

    let mut by_topic: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for f in &fetched {
        let t = by_topic.entry(f.topic.clone()).or_default();
        t.0 += f.records.len() as u64;
        t.1 += f
            .records
            .iter()
            .map(|r| r.metadata_size() as u64)
            .sum::<u64>();
    }
    for (topic, (records, bytes)) in by_topic {
        stats.records += records;
        stats.metadata_bytes += bytes;
        stats.per_topic.push((topic, records, bytes));
    }

    let mut actions = Vec::new();
    if stats.records > 0 {
        let parts: Vec<FetchedPartition> = fetched
            .iter()
            .filter(|f| !f.records.is_empty())
            .cloned()
            .collect();
        let min_rows = cfg.l0_parquet_min_rows;
        let pivots = cfg.pivots.clone();
        let (data, meta) =
            tokio::task::spawn_blocking(move || encode_l0(&parts, min_rows, &pivots))
                .await
                .map_err(|e| Error::Join(e.to_string()))??;
        let add = table.put_segment(data, &meta).await?;
        stats.segments.push((add.format, add.size));
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

/// Encodes fetched records as one L0 file sorted by (topic, partition, offset).
fn encode_l0(
    parts: &[FetchedPartition],
    parquet_min_rows: usize,
    pivots: &PivotConfig,
) -> Result<(Vec<u8>, SegmentMeta)> {
    let batch = model::records_to_batch(
        parts
            .iter()
            .map(|p| (p.topic.as_str(), p.partition, p.records.as_slice())),
    )?;
    let batch = model::sort_batch(&batch)?;
    let stats = FileStats::from_batch(&batch)?;
    let (format, data, pivot_keys) = if batch.num_rows() < parquet_min_rows {
        (FileFormat::Avro, model::write_avro(&batch)?, Vec::new())
    } else {
        let keys = model::choose_pivot_keys(&batch, pivots)?;
        let data =
            model::write_parquet(&model::with_pivots(&batch, &keys)?, &ParquetOptions::l0())?;
        (FileFormat::Parquet, data, keys)
    };
    Ok((
        data,
        SegmentMeta {
            format,
            level: 0,
            stats,
            pivot_keys,
        },
    ))
}
