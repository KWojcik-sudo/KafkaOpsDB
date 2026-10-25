//! Compaction tasks: L0 → L1, L1 → L2 promotion, L1/L2 merges and retention
//! (docs/storage-format.md §2). The planner is part of the master role; tasks run on workers.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{AsArray, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::TimestampMillisecondType;
use futures::StreamExt;
use model::{FileFormat, FileStats, ParquetOptions, PivotConfig};
use regex::Regex;
use table::{Action, AddSegment, Operation, RemoveSegment, SegmentMeta, Snapshot, Table, now_ms};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct RetentionRule {
    pub topics: Regex,
    pub retention: Duration,
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub l0_min_files: usize,
    pub l0_max_age: Duration,
    pub l0_max_bytes: u64,
    /// Upper bound of the (compressed) input of one task.
    pub max_input_bytes: u64,
    pub l1_target_bytes: u64,
    pub l2_promote_min_bytes: u64,
    pub l2_target_bytes: u64,
    /// Minimum number of small files that triggers an L1 or L2 merge.
    pub merge_min_files: usize,
    pub retention: Vec<RetentionRule>,
    pub pivots: PivotConfig,
    pub parallelism: usize,
    pub tick: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_min_files: 8,
            l0_max_age: Duration::from_secs(300),
            l0_max_bytes: 256 << 20,
            max_input_bytes: 512 << 20,
            l1_target_bytes: 256 << 20,
            l2_promote_min_bytes: 128 << 20,
            l2_target_bytes: 512 << 20,
            merge_min_files: 4,
            retention: Vec::new(),
            pivots: PivotConfig::default(),
            parallelism: 1,
            tick: Duration::from_secs(10),
        }
    }
}

impl CompactionConfig {
    fn retention_of(&self, topic: &str) -> Option<Duration> {
        self.retention
            .iter()
            .find(|r| r.topics.is_match(topic))
            .map(|r| r.retention)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionKind {
    CompactL0,
    MergeL1,
    PromoteL2 {
        topic: String,
    },
    MergeL2 {
        topic: String,
    },
    /// Drops rows older than the topic retention; outputs keep the input level.
    Retention {
        level: u8,
    },
}

#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub id: String,
    pub kind: CompactionKind,
    pub inputs: Vec<AddSegment>,
}

impl CompactionTask {
    fn new(kind: CompactionKind, inputs: Vec<AddSegment>) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            kind,
            inputs,
        }
    }

    pub fn input_bytes(&self) -> u64 {
        self.inputs.iter().map(|s| s.size).sum()
    }
}

/// Estimated bytes of `topic` inside a multi-topic file.
fn topic_bytes(seg: &AddSegment, topic: &str) -> u64 {
    match seg.topic(topic) {
        Some(t) if seg.rows > 0 => (seg.size as u128 * t.rows as u128 / seg.rows as u128) as u64,
        _ => 0,
    }
}

pub struct CompactionPlanner {
    cfg: CompactionConfig,
    in_flight: HashSet<Uuid>,
}

impl CompactionPlanner {
    pub fn new(cfg: CompactionConfig) -> Self {
        Self {
            cfg,
            in_flight: HashSet::new(),
        }
    }

    /// Plans tasks with pairwise disjoint inputs that are not already being compacted.
    pub fn plan(&mut self, snap: &Snapshot, now: i64) -> Vec<CompactionTask> {
        let mut tasks = Vec::new();
        let mut taken: HashSet<Uuid> = self.in_flight.clone();
        let free = |taken: &HashSet<Uuid>| {
            let mut v: Vec<&AddSegment> = snap
                .segments
                .values()
                .filter(|s| !taken.contains(&s.segment_id))
                .collect();
            v.sort_by_key(|s| (s.created_at, s.segment_id));
            v
        };
        let take = |inputs: Vec<&AddSegment>, max: u64| -> Vec<AddSegment> {
            let mut total = 0;
            let mut out = Vec::new();
            for s in inputs {
                if !out.is_empty() && total + s.size > max {
                    break;
                }
                total += s.size;
                out.push(s.clone());
            }
            out
        };
        let push =
            |tasks: &mut Vec<CompactionTask>, taken: &mut HashSet<Uuid>, t: CompactionTask| {
                for s in &t.inputs {
                    taken.insert(s.segment_id);
                }
                tasks.push(t);
            };

        // retention first: expired data should not be compacted further
        for level in 0..=2u8 {
            let expired: Vec<&AddSegment> = free(&taken)
                .into_iter()
                .filter(|s| s.level == level)
                .filter(|s| {
                    s.topics.iter().any(|t| {
                        self.cfg
                            .retention_of(&t.topic)
                            .is_some_and(|r| t.ts_min < now - r.as_millis() as i64)
                    })
                })
                .collect();
            if !expired.is_empty() {
                let inputs = take(expired, self.cfg.max_input_bytes);
                push(
                    &mut tasks,
                    &mut taken,
                    CompactionTask::new(CompactionKind::Retention { level }, inputs),
                );
            }
        }

        let l0: Vec<&AddSegment> = free(&taken).into_iter().filter(|s| s.level == 0).collect();
        if !l0.is_empty() {
            let total: u64 = l0.iter().map(|s| s.size).sum();
            let oldest_age = now - l0[0].created_at;
            if l0.len() >= self.cfg.l0_min_files
                || total >= self.cfg.l0_max_bytes
                || oldest_age >= self.cfg.l0_max_age.as_millis() as i64
            {
                let inputs = take(l0, self.cfg.max_input_bytes);
                push(
                    &mut tasks,
                    &mut taken,
                    CompactionTask::new(CompactionKind::CompactL0, inputs),
                );
            }
        }

        let mut l1_bytes: BTreeMap<&str, u64> = BTreeMap::new();
        let l1: Vec<&AddSegment> = free(&taken).into_iter().filter(|s| s.level == 1).collect();
        for s in &l1 {
            for t in &s.topics {
                *l1_bytes.entry(t.topic.as_str()).or_default() += topic_bytes(s, &t.topic);
            }
        }
        let mut hot: Vec<(&str, u64)> = l1_bytes
            .into_iter()
            .filter(|(_, b)| *b >= self.cfg.l2_promote_min_bytes)
            .collect();
        hot.sort_by_key(|h| std::cmp::Reverse(h.1));
        for (topic, _) in hot {
            let inputs: Vec<&AddSegment> = free(&taken)
                .into_iter()
                .filter(|s| s.level == 1 && s.topic(topic).is_some())
                .collect();
            if inputs.is_empty() {
                continue;
            }
            let inputs = take(inputs, self.cfg.max_input_bytes);
            push(
                &mut tasks,
                &mut taken,
                CompactionTask::new(
                    CompactionKind::PromoteL2 {
                        topic: topic.to_owned(),
                    },
                    inputs,
                ),
            );
        }

        let small_l1: Vec<&AddSegment> = free(&taken)
            .into_iter()
            .filter(|s| s.level == 1 && s.size < self.cfg.l1_target_bytes / 2)
            .collect();
        if small_l1.len() >= self.cfg.merge_min_files {
            let inputs = take(small_l1, self.cfg.max_input_bytes);
            if inputs.len() >= 2 {
                push(
                    &mut tasks,
                    &mut taken,
                    CompactionTask::new(CompactionKind::MergeL1, inputs),
                );
            }
        }

        let mut small_l2: BTreeMap<String, Vec<&AddSegment>> = BTreeMap::new();
        for s in free(&taken) {
            if s.level == 2 && s.size < self.cfg.l2_target_bytes / 2 && s.topics.len() == 1 {
                small_l2
                    .entry(s.topics[0].topic.clone())
                    .or_default()
                    .push(s);
            }
        }
        for (topic, files) in small_l2 {
            if files.len() >= self.cfg.merge_min_files {
                let inputs = take(files, self.cfg.max_input_bytes);
                if inputs.len() >= 2 {
                    push(
                        &mut tasks,
                        &mut taken,
                        CompactionTask::new(CompactionKind::MergeL2 { topic }, inputs),
                    );
                }
            }
        }

        for t in &tasks {
            for s in &t.inputs {
                self.in_flight.insert(s.segment_id);
            }
        }
        tasks
    }

    pub fn finished(&mut self, task: &CompactionTask) {
        for s in &task.inputs {
            self.in_flight.remove(&s.segment_id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionOutcome {
    Committed { version: u64, outputs: usize },
    Conflict(String),
    Failed(String),
}

/// Reads, rewrites and atomically swaps the task's input files.
pub async fn execute_compaction(
    table: &Table,
    cfg: &CompactionConfig,
    task: &CompactionTask,
) -> CompactionOutcome {
    match run_compaction(table, cfg, task).await {
        Ok((version, outputs)) => CompactionOutcome::Committed { version, outputs },
        Err(Error::Table(e)) if e.is_conflict() => CompactionOutcome::Conflict(e.to_string()),
        Err(e) => CompactionOutcome::Failed(e.to_string()),
    }
}

async fn run_compaction(
    table: &Table,
    cfg: &CompactionConfig,
    task: &CompactionTask,
) -> Result<(u64, usize)> {
    let mut inputs: Vec<(FileFormat, bytes::Bytes)> = Vec::with_capacity(task.inputs.len());
    let mut reads = futures::stream::iter(task.inputs.clone())
        .map(|s| async move {
            let data = table.read_segment(&s).await?;
            Ok::<_, Error>((FileFormat::from(s.format), data))
        })
        .buffered(4);
    while let Some(r) = reads.next().await {
        inputs.push(r?);
    }
    let input_bytes = task.input_bytes();
    let input_rows: u64 = task.inputs.iter().map(|s| s.rows).sum();
    let kind = task.kind.clone();
    let cfg2 = cfg.clone();
    let now = now_ms();
    let outputs = tokio::task::spawn_blocking(move || {
        rewrite(&kind, &cfg2, inputs, input_bytes, input_rows, now)
    })
    .await
    .map_err(|e| Error::Join(e.to_string()))??;

    let mut actions: Vec<Action> = task
        .inputs
        .iter()
        .map(|s| {
            Action::Remove(RemoveSegment {
                segment_id: s.segment_id,
                deletion_timestamp: now_ms(),
            })
        })
        .collect();
    let n = outputs.len();
    for (data, meta) in outputs {
        actions.push(Action::Add(table.put_segment(data, &meta).await?));
    }
    let version = table
        .commit(Operation::Compact, Some(task.id.clone()), actions)
        .await?;
    Ok((version, n))
}

type Output = (Vec<u8>, SegmentMeta);

fn rewrite(
    kind: &CompactionKind,
    cfg: &CompactionConfig,
    inputs: Vec<(FileFormat, bytes::Bytes)>,
    input_bytes: u64,
    input_rows: u64,
    now: i64,
) -> Result<Vec<Output>> {
    let mut batches = Vec::new();
    for (format, data) in inputs {
        batches.extend(model::read_file(format, data)?);
    }
    let all = model::sort_batch(&model::concat_base(&batches)?)?;
    let bytes_per_row = (input_bytes as f64 / input_rows.max(1) as f64).max(1.0);
    let rows_for = |target: u64| ((target as f64 / bytes_per_row) as usize).max(1);

    let mut out = Vec::new();
    match kind {
        CompactionKind::CompactL0 | CompactionKind::MergeL1 => {
            write_level(&all, 1, rows_for(cfg.l1_target_bytes), cfg, &mut out)?;
        }
        CompactionKind::MergeL2 { .. } => {
            write_level(&all, 2, rows_for(cfg.l2_target_bytes), cfg, &mut out)?;
        }
        CompactionKind::PromoteL2 { topic } => {
            let mut rest = Vec::new();
            for (t, b) in model::split_by_topic(&all)? {
                if &t == topic {
                    write_level(&b, 2, rows_for(cfg.l2_target_bytes), cfg, &mut out)?;
                } else {
                    rest.push(b);
                }
            }
            if !rest.is_empty() {
                let rest = model::concat_base(&rest)?;
                write_level(&rest, 1, rows_for(cfg.l1_target_bytes), cfg, &mut out)?;
            }
        }
        CompactionKind::Retention { level } => {
            let kept = drop_expired(&all, cfg, now)?;
            if kept.num_rows() > 0 {
                if *level == 2 {
                    for (_, b) in model::split_by_topic(&kept)? {
                        write_level(&b, 2, rows_for(cfg.l2_target_bytes), cfg, &mut out)?;
                    }
                } else {
                    write_level(
                        &kept,
                        (*level).max(1),
                        rows_for(cfg.l1_target_bytes),
                        cfg,
                        &mut out,
                    )?;
                }
            }
        }
    }
    Ok(out)
}

fn drop_expired(batch: &RecordBatch, cfg: &CompactionConfig, now: i64) -> Result<RecordBatch> {
    let topic = batch.column(0).as_string::<i32>();
    let ts = batch
        .column_by_name(model::schema::TIMESTAMP)
        .ok_or_else(|| Error::Config("missing timestamp".into()))?
        .as_primitive::<TimestampMillisecondType>();
    let keep: BooleanArray = (0..batch.num_rows())
        .map(|i| {
            Some(match cfg.retention_of(topic.value(i)) {
                Some(r) => ts.value(i) >= now - r.as_millis() as i64,
                None => true,
            })
        })
        .collect();
    Ok(filter_record_batch(batch, &keep).map_err(model::Error::from)?)
}

/// Writes a sorted batch as Parquet files of at most `rows_per_file` rows.
fn write_level(
    batch: &RecordBatch,
    level: u8,
    rows_per_file: usize,
    cfg: &CompactionConfig,
    out: &mut Vec<Output>,
) -> Result<()> {
    let mut start = 0;
    while start < batch.num_rows() {
        let len = rows_per_file.min(batch.num_rows() - start);
        let chunk = batch.slice(start, len);
        let keys = model::choose_pivot_keys(&chunk, &cfg.pivots)?;
        let data = model::write_parquet(
            &model::with_pivots(&chunk, &keys)?,
            &ParquetOptions::compacted(),
        )?;
        out.push((
            data,
            SegmentMeta {
                format: FileFormat::Parquet,
                level,
                stats: FileStats::from_batch(&chunk)?,
                pivot_keys: keys,
            },
        ));
        start += len;
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct CompactionStats {
    pub tasks: AtomicU64,
    pub conflicts: AtomicU64,
    pub failures: AtomicU64,
    pub input_bytes: AtomicU64,
    pub output_files: AtomicU64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    pub tasks: usize,
    pub committed: usize,
    pub conflicts: usize,
    pub failed: usize,
}

/// Plans and runs compaction tasks in-process (single node: master + worker).
pub struct CompactionService {
    table: Arc<Table>,
    cfg: CompactionConfig,
    planner: Mutex<CompactionPlanner>,
    pub stats: CompactionStats,
}

impl CompactionService {
    pub fn new(table: Arc<Table>, cfg: CompactionConfig) -> Arc<Self> {
        Arc::new(Self {
            planner: Mutex::new(CompactionPlanner::new(cfg.clone())),
            table,
            cfg,
            stats: CompactionStats::default(),
        })
    }

    pub async fn step(&self) -> Result<CompactionReport> {
        let snap = self.table.refresh().await?;
        let tasks = self.planner.lock().unwrap().plan(&snap, now_ms());
        let mut report = CompactionReport {
            tasks: tasks.len(),
            ..Default::default()
        };
        let results: Vec<_> = futures::stream::iter(tasks)
            .map(|t| async move {
                let outcome = execute_compaction(&self.table, &self.cfg, &t).await;
                (t, outcome)
            })
            .buffer_unordered(self.cfg.parallelism.max(1))
            .collect()
            .await;
        for (task, outcome) in results {
            self.planner.lock().unwrap().finished(&task);
            let s = &self.stats;
            s.tasks.fetch_add(1, Ordering::Relaxed);
            match outcome {
                CompactionOutcome::Committed { version, outputs } => {
                    report.committed += 1;
                    s.input_bytes
                        .fetch_add(task.input_bytes(), Ordering::Relaxed);
                    s.output_files.fetch_add(outputs as u64, Ordering::Relaxed);
                    info!(task = %task.id, kind = ?task.kind, inputs = task.inputs.len(), outputs, version, "compacted");
                }
                CompactionOutcome::Conflict(reason) => {
                    report.conflicts += 1;
                    s.conflicts.fetch_add(1, Ordering::Relaxed);
                    warn!(task = %task.id, %reason, "compaction conflicted");
                }
                CompactionOutcome::Failed(reason) => {
                    report.failed += 1;
                    s.failures.fetch_add(1, Ordering::Relaxed);
                    warn!(task = %task.id, %reason, "compaction failed");
                }
            }
        }
        Ok(report)
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            let busy = match self.step().await {
                Ok(r) => r.tasks > 0,
                Err(e) => {
                    warn!(error = %e, "compaction round failed");
                    false
                }
            };
            if cancel.is_cancelled() {
                break;
            }
            if !busy {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(self.cfg.tick) => {}
                }
            }
        }
    }
}
