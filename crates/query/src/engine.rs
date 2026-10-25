//! SQL entry point: rewrite, plan with DataFusion over a pinned snapshot, execute.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use table::Table;
use tracing::{debug, warn};

use crate::cache::FileCache;
use crate::error::{Error, Result};
use crate::scan::{ScanMetrics, TopicsTable};
use crate::sql::rewrite;
use crate::udf::{header_all_udf, header_udf};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// A query refreshes the snapshot when it is older than this.
    pub refresh_interval: Duration,
    pub target_partitions: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(1),
            target_partitions: std::thread::available_parallelism().map_or(4, |n| n.get()),
        }
    }
}

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub snapshot_version: u64,
    pub elapsed: Duration,
}

impl QueryResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

pub struct QueryEngine {
    table: Arc<Table>,
    cache: Arc<FileCache>,
    cfg: EngineConfig,
    last_refresh: Mutex<Option<Instant>>,
    pub metrics: Arc<ScanMetrics>,
}

impl QueryEngine {
    pub fn new(table: Arc<Table>, cache: Arc<FileCache>, cfg: EngineConfig) -> Self {
        Self {
            table,
            cache,
            cfg,
            last_refresh: Mutex::new(None),
            metrics: Arc::new(ScanMetrics::default()),
        }
    }

    pub fn cache(&self) -> &Arc<FileCache> {
        &self.cache
    }

    async fn snapshot(&self, force: bool) -> Result<Arc<table::Snapshot>> {
        let stale = force
            || self
                .last_refresh
                .lock()
                .unwrap()
                .is_none_or(|t| t.elapsed() >= self.cfg.refresh_interval);
        if stale {
            let snap = self.table.refresh().await?;
            *self.last_refresh.lock().unwrap() = Some(Instant::now());
            Ok(snap)
        } else {
            Ok(self.table.snapshot())
        }
    }

    async fn run(&self, sql: &str, snap: Arc<table::Snapshot>) -> Result<QueryResult> {
        let started = Instant::now();
        let config = SessionConfig::new()
            .with_target_partitions(self.cfg.target_partitions.max(1))
            .with_information_schema(false);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udf(header_udf());
        ctx.register_udf(header_all_udf());
        let version = snap.version;
        ctx.register_table(
            "topics",
            Arc::new(TopicsTable::new(
                snap,
                self.table.root().clone(),
                self.cache.clone(),
                self.metrics.clone(),
            )),
        )?;
        let df = ctx.sql(sql).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await?;
        Ok(QueryResult {
            schema,
            batches,
            snapshot_version: version,
            elapsed: started.elapsed(),
        })
    }

    /// Executes one read-only SQL statement against the latest snapshot.
    pub async fn sql(&self, sql: &str) -> Result<QueryResult> {
        let rewritten = rewrite(sql)?;
        debug!(%rewritten, "query");
        let snap = self.snapshot(false).await?;
        match self.run(&rewritten, snap).await {
            // a file of an old snapshot can disappear through vacuum: retry once on a fresh one
            Err(Error::DataFusion(e)) if e.to_string().contains("not found") => {
                warn!(error = %e, "file vanished during query, retrying on a fresh snapshot");
                let snap = self.snapshot(true).await?;
                self.run(&rewritten, snap).await
            }
            other => other,
        }
    }
}
