//! Single-node mode: indexer (planner + workers), compaction, vacuum and the query engine
//! in one process.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ingest::kafka::{KafkaConfig, RdKafkaSource};
use ingest::{
    CompactionService, IngestService, InitialPosition, PlannerConfig, RetentionRule, ServiceConfig,
    WorkerConfig,
};
use query::{CacheConfig, EngineConfig, FileCache, QueryEngine};
use regex::Regex;
use table::{Table, TableConfig};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::Config;

pub struct App {
    pub node_id: String,
    pub table: Arc<Table>,
    pub engine: Arc<QueryEngine>,
    pub ingest: Option<Arc<IngestService>>,
    pub compaction: Option<Arc<CompactionService>>,
    pub started: Instant,
}

fn regexes(patterns: &[String]) -> Result<Vec<Regex>, String> {
    patterns
        .iter()
        .map(|p| Regex::new(p).map_err(|e| format!("invalid regex {p:?}: {e}")))
        .collect()
}

fn pivots(cfg: &Config) -> model::PivotConfig {
    model::PivotConfig {
        always: cfg.pivot.always.clone(),
        min_fraction: cfg.pivot.min_fraction,
        max_keys: cfg.pivot.max_keys,
    }
}

pub fn ingest_config(cfg: &Config) -> Result<ServiceConfig, String> {
    let i = &cfg.ingest;
    let initial_position = match i.initial_position.as_str() {
        "earliest" => InitialPosition::Earliest,
        "latest" => InitialPosition::Latest,
        other => {
            return Err(format!(
                "ingest.initial_position must be earliest or latest, got {other:?}"
            ));
        }
    };
    Ok(ServiceConfig {
        planner: PlannerConfig {
            include: regexes(&i.include)?,
            exclude: regexes(&i.exclude)?,
            flush_min_bytes: i.flush_min_bytes.0,
            max_flush_age: i.max_flush_age.0,
            task_max_bytes: i.task_max_bytes.0,
            max_records_per_partition: i.max_records_per_partition,
            initial_position,
            ..Default::default()
        },
        worker: WorkerConfig {
            l0_parquet_min_rows: i.l0_parquet_min_rows,
            pivots: pivots(cfg),
            node: cfg.node_id.clone(),
        },
        parallelism: i.parallelism,
        tick: i.tick.0,
        metadata_refresh: i.metadata_refresh.0,
    })
}

pub fn compaction_config(cfg: &Config) -> Result<ingest::CompactionConfig, String> {
    let c = &cfg.compaction;
    Ok(ingest::CompactionConfig {
        l0_min_files: c.l0_min_files,
        l0_max_age: c.l0_max_age.0,
        l0_max_bytes: c.l0_max_bytes.0,
        max_input_bytes: c.max_input_bytes.0,
        l1_target_bytes: c.l1_target_bytes.0,
        l2_promote_min_bytes: c.l2_promote_min_bytes.0,
        l2_target_bytes: c.l2_target_bytes.0,
        merge_min_files: c.merge_min_files,
        retention: cfg
            .retention
            .iter()
            .map(|r| {
                Ok(RetentionRule {
                    topics: Regex::new(&r.topics)
                        .map_err(|e| format!("invalid retention regex: {e}"))?,
                    retention: r.retention.0,
                })
            })
            .collect::<Result<_, String>>()?,
        pivots: pivots(cfg),
        parallelism: c.parallelism,
        tick: c.tick.0,
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
}

pub async fn run(cfg: Config) -> Result<(), String> {
    let options: HashMap<String, String> = cfg.storage.options.clone().into_iter().collect();
    let (store, root) = table::open_store(&cfg.storage.url, &options).map_err(|e| e.to_string())?;
    table::self_test(store.as_ref(), &root)
        .await
        .map_err(|e| format!("storage self-test failed: {e}"))?;
    let table = Table::open_or_create(
        store.clone(),
        root,
        cfg.node_id.clone(),
        TableConfig {
            checkpoint_interval: cfg.storage.checkpoint_interval.max(1),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    info!(url = %cfg.storage.url, version = table.snapshot().version, "table opened");

    let cancel = CancellationToken::new();
    let mut tasks = Vec::new();

    let ingest = if cfg.ingest.enabled {
        let source = RdKafkaSource::new(KafkaConfig {
            bootstrap_servers: cfg.kafka.bootstrap_servers.clone(),
            properties: cfg.kafka.properties.clone(),
            metadata_timeout: cfg.kafka.metadata_timeout.0,
            fetch_timeout: cfg.kafka.fetch_timeout.0,
        })
        .map_err(|e| e.to_string())?;
        let svc = IngestService::new(table.clone(), Arc::new(source), ingest_config(&cfg)?);
        tasks.push(tokio::spawn(svc.clone().run(cancel.clone())));
        Some(svc)
    } else {
        None
    };
    let compaction = if cfg.compaction.enabled {
        let svc = CompactionService::new(table.clone(), compaction_config(&cfg)?);
        tasks.push(tokio::spawn(svc.clone().run(cancel.clone())));
        Some(svc)
    } else {
        None
    };
    if cfg.vacuum.enabled {
        let table = table.clone();
        let cancel = cancel.clone();
        let vcfg = table::VacuumConfig {
            tombstone_grace: cfg.vacuum.tombstone_grace.0,
            orphan_grace: cfg.vacuum.orphan_grace.0,
            log_retention: cfg.vacuum.log_retention.0,
        };
        let interval = cfg.vacuum.interval.0;
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
                if let Err(e) = table.vacuum(&vcfg).await {
                    warn!(error = %e, "vacuum failed");
                }
            }
        }));
    }

    let cache = FileCache::new(
        store,
        &CacheConfig {
            memory_bytes: cfg.query.cache_memory_bytes.0,
            disk_dir: cfg.query.cache_dir.clone(),
            disk_bytes: cfg.query.cache_disk_bytes.0,
        },
    )
    .map_err(|e| e.to_string())?;
    let mut engine_cfg = EngineConfig {
        refresh_interval: cfg.query.refresh_interval.0,
        ..Default::default()
    };
    if let Some(n) = cfg.query.target_partitions {
        engine_cfg.target_partitions = n;
    }
    let app = Arc::new(App {
        node_id: cfg.node_id.clone(),
        engine: Arc::new(QueryEngine::new(table.clone(), cache, engine_cfg)),
        table,
        ingest,
        compaction,
        started: Instant::now(),
    });

    let listener = tokio::net::TcpListener::bind(&cfg.query.listen)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", cfg.query.listen))?;
    info!(listen = %cfg.query.listen, "http api ready");
    let server_cancel = cancel.clone();
    axum::serve(listener, crate::http::router(app))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            info!("shutting down: finishing in-flight tasks");
            server_cancel.cancel();
        })
        .await
        .map_err(|e| e.to_string())?;

    cancel.cancel();
    let all = futures_join(tasks);
    if tokio::time::timeout(Duration::from_secs(120), all)
        .await
        .is_err()
    {
        warn!("background tasks did not stop in time");
    }
    info!("stopped");
    Ok(())
}

async fn futures_join(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for t in tasks {
        let _ = t.await;
    }
}
