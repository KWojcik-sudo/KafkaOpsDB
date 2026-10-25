//! The indexing loop: planner (master role) + worker pool, connected in-process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use table::{Action, Operation, Table};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::Result;
use crate::planner::{Planner, PlannerConfig};
use crate::source::{KafkaSource, TopicMeta};
use crate::worker::{TaskOutcome, WorkerConfig, execute_task};

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub planner: PlannerConfig,
    pub worker: WorkerConfig,
    pub parallelism: usize,
    /// Sleep between planning rounds when there was nothing to do.
    pub tick: Duration,
    /// How long topic metadata (high watermarks) is cached.
    pub metadata_refresh: Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            planner: PlannerConfig::default(),
            worker: WorkerConfig::default(),
            parallelism: 4,
            tick: Duration::from_secs(1),
            metadata_refresh: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Default)]
pub struct IngestStats {
    pub records: AtomicU64,
    pub metadata_bytes: AtomicU64,
    pub commits: AtomicU64,
    pub conflicts: AtomicU64,
    pub failures: AtomicU64,
    pub resets: AtomicU64,
    pub segments_topic: AtomicU64,
    pub segments_mixed: AtomicU64,
    pub segment_bytes: AtomicU64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StepReport {
    pub tasks: usize,
    pub committed: usize,
    pub conflicts: usize,
    pub failed: usize,
    pub resets: usize,
    pub records: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed: Option<i64>,
    pub log_start: i64,
    pub high_watermark: i64,
    pub lag: i64,
}

pub struct IngestService {
    table: Arc<Table>,
    source: Arc<dyn KafkaSource>,
    cfg: ServiceConfig,
    planner: Mutex<Planner>,
    metadata: tokio::sync::Mutex<Option<(Instant, Arc<Vec<TopicMeta>>)>>,
    last_metadata: Mutex<Arc<Vec<TopicMeta>>>,
    pub stats: IngestStats,
}

impl IngestService {
    pub fn new(table: Arc<Table>, source: Arc<dyn KafkaSource>, cfg: ServiceConfig) -> Arc<Self> {
        Arc::new(Self {
            planner: Mutex::new(Planner::new(cfg.planner.clone())),
            table,
            source,
            cfg,
            metadata: tokio::sync::Mutex::new(None),
            last_metadata: Mutex::new(Arc::new(Vec::new())),
            stats: IngestStats::default(),
        })
    }

    async fn metadata(&self) -> Result<Arc<Vec<TopicMeta>>> {
        let mut cached = self.metadata.lock().await;
        if let Some((at, m)) = cached.as_ref()
            && at.elapsed() < self.cfg.metadata_refresh
        {
            return Ok(m.clone());
        }
        let m = Arc::new(self.source.list_topics().await?);
        *cached = Some((Instant::now(), m.clone()));
        *self.last_metadata.lock().unwrap() = m.clone();
        Ok(m)
    }

    async fn invalidate_metadata(&self) {
        *self.metadata.lock().await = None;
    }

    /// One planning round: commit offset resets, run all planned tasks, wait for them.
    pub async fn step(&self) -> Result<StepReport> {
        let snap = self.table.refresh().await?;
        let topics = self.metadata().await?;
        let plan = self
            .planner
            .lock()
            .unwrap()
            .plan(&snap, &topics, Instant::now());
        let mut report = StepReport::default();

        for r in plan.resets {
            warn!(topic = %r.topic, partition = r.partition, start = ?r.start, end = r.end, reason = %r.reason, "offset reset");
            match self
                .table
                .commit(Operation::Reset, None, vec![Action::OffsetReset(r)])
                .await
            {
                Ok(_) => {
                    report.resets += 1;
                    self.stats.resets.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => warn!(error = %e, "offset reset not committed"),
            }
        }

        report.tasks = plan.tasks.len();
        let results: Vec<_> = futures::stream::iter(plan.tasks)
            .map(|task| execute_task(&self.table, self.source.as_ref(), &self.cfg.worker, task))
            .buffer_unordered(self.cfg.parallelism.max(1))
            .collect()
            .await;

        let mut any_failed = false;
        for r in &results {
            self.planner
                .lock()
                .unwrap()
                .task_finished(&r.task, &r.stats.per_topic, Instant::now());
            match &r.outcome {
                TaskOutcome::Committed { .. } => {
                    report.committed += 1;
                    report.records += r.stats.records;
                    let s = &self.stats;
                    s.commits.fetch_add(1, Ordering::Relaxed);
                    s.records.fetch_add(r.stats.records, Ordering::Relaxed);
                    s.metadata_bytes
                        .fetch_add(r.stats.metadata_bytes, Ordering::Relaxed);
                    for (kind, size) in &r.stats.segments {
                        match kind {
                            table::SegmentKind::Topic => {
                                s.segments_topic.fetch_add(1, Ordering::Relaxed)
                            }
                            table::SegmentKind::Mixed => {
                                s.segments_mixed.fetch_add(1, Ordering::Relaxed)
                            }
                        };
                        s.segment_bytes.fetch_add(*size, Ordering::Relaxed);
                    }
                }
                TaskOutcome::NoProgress => {}
                TaskOutcome::Conflict(_) => {
                    report.conflicts += 1;
                    self.stats.conflicts.fetch_add(1, Ordering::Relaxed);
                }
                TaskOutcome::Failed(_) => {
                    report.failed += 1;
                    any_failed = true;
                    self.stats.failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if any_failed || report.resets > 0 {
            self.invalidate_metadata().await;
        }
        Ok(report)
    }

    /// Runs until cancelled. A round that did work is immediately followed by the next one.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        info!("ingest service started");
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let busy = match self.step().await {
                Ok(r) => r.tasks > 0 || r.resets > 0,
                Err(e) => {
                    warn!(error = %e, "ingest round failed");
                    self.invalidate_metadata().await;
                    false
                }
            };
            if !busy {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(self.cfg.tick) => {}
                }
            }
        }
        info!("ingest service stopped");
    }

    /// Lag per partition based on the last fetched metadata.
    pub fn lag(&self) -> Vec<PartitionLag> {
        let topics = self.last_metadata.lock().unwrap().clone();
        let snap = self.table.snapshot();
        let planner = self.planner.lock().unwrap();
        topics
            .iter()
            .filter(|t| planner.topic_selected(&t.name))
            .flat_map(|t| {
                let snap = &snap;
                t.partitions.iter().map(move |p| {
                    let committed = snap.committed(&t.name, p.partition);
                    PartitionLag {
                        topic: t.name.clone(),
                        partition: p.partition,
                        committed,
                        log_start: p.log_start,
                        high_watermark: p.high_watermark,
                        lag: p.high_watermark - committed.unwrap_or(p.log_start),
                    }
                })
            })
            .collect()
    }
}
