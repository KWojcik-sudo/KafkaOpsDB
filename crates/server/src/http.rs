//! HTTP API: `POST /v1/query`, `GET /v1/status`, `GET /metrics`, `GET /health`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::standalone::App;

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/v1/query", post(query))
        .route("/v1/status", get(status))
        .route("/metrics", get(metrics))
        .route("/health", get(|| async { "ok" }))
        .with_state(app)
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default)]
    format: Option<String>,
}

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(json!({ "error": msg.into() }))).into_response()
}

async fn query(State(app): State<Arc<App>>, Json(req): Json<QueryRequest>) -> Response {
    let result = match app.engine.sql(&req.sql).await {
        Ok(r) => r,
        Err(e) => return error(StatusCode::BAD_REQUEST, e.to_string()),
    };
    match req.format.as_deref().unwrap_or("json") {
        "json" => {
            let mut body = query::format::to_json(&result.schema, &result.batches);
            body["row_count"] = json!(result.num_rows());
            body["elapsed_ms"] = json!(result.elapsed.as_secs_f64() * 1000.0);
            body["snapshot_version"] = json!(result.snapshot_version);
            Json(body).into_response()
        }
        "ndjson" => (
            [(header::CONTENT_TYPE, "application/x-ndjson")],
            query::format::to_ndjson(&result.batches),
        )
            .into_response(),
        "table" => (
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            query::format::to_table(&result.schema, &result.batches),
        )
            .into_response(),
        other => error(StatusCode::BAD_REQUEST, format!("unknown format {other:?}")),
    }
}

fn files_summary(app: &App) -> Vec<Value> {
    let snap = app.table.snapshot();
    let mut by: BTreeMap<(u8, String), (u64, u64, u64)> = BTreeMap::new();
    for s in snap.segments.values() {
        let e = by
            .entry((s.level, format!("{:?}", s.format).to_lowercase()))
            .or_default();
        e.0 += 1;
        e.1 += s.size;
        e.2 += s.rows;
    }
    by.into_iter()
        .map(|((level, format), (files, bytes, rows))| {
            json!({ "level": level, "format": format, "files": files, "bytes": bytes, "rows": rows })
        })
        .collect()
}

async fn status(State(app): State<Arc<App>>) -> Json<Value> {
    let snap = app.table.snapshot();
    let lag = app.ingest.as_ref().map(|i| i.lag()).unwrap_or_default();
    let total_lag: i64 = lag.iter().map(|l| l.lag.max(0)).sum();
    Json(json!({
        "node_id": app.node_id,
        "uptime_seconds": app.started.elapsed().as_secs(),
        "table": {
            "version": snap.version,
            "files": files_summary(&app),
            "tombstones": snap.tombstones.len(),
            "tracked_partitions": snap.offsets.len(),
        },
        "ingest": {
            "enabled": app.ingest.is_some(),
            "total_lag": total_lag,
            "partitions": lag,
        },
    }))
}

async fn metrics(State(app): State<Arc<App>>) -> impl IntoResponse {
    let mut out = String::new();
    let mut metric = |name: &str, help: &str, kind: &str, samples: Vec<(String, f64)>| {
        out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
        for (labels, v) in samples {
            out.push_str(&format!("{name}{labels} {v}\n"));
        }
    };
    let one = |v: u64| vec![(String::new(), v as f64)];
    let snap = app.table.snapshot();
    metric(
        "kafkaopsdb_table_version",
        "Latest commit log version",
        "gauge",
        one(snap.version),
    );
    let mut files = Vec::new();
    let mut bytes = Vec::new();
    for f in files_summary(&app) {
        let labels = format!(
            "{{level=\"{}\",format=\"{}\"}}",
            f["level"],
            f["format"].as_str().unwrap_or("")
        );
        files.push((labels.clone(), f["files"].as_f64().unwrap_or(0.0)));
        bytes.push((labels, f["bytes"].as_f64().unwrap_or(0.0)));
    }
    metric("kafkaopsdb_files", "Live data files", "gauge", files);
    metric(
        "kafkaopsdb_file_bytes",
        "Bytes in live data files",
        "gauge",
        bytes,
    );

    if let Some(i) = &app.ingest {
        let s = &i.stats;
        let c = |a: &std::sync::atomic::AtomicU64| one(a.load(Ordering::Relaxed));
        metric(
            "kafkaopsdb_ingest_records_total",
            "Indexed records",
            "counter",
            c(&s.records),
        );
        metric(
            "kafkaopsdb_ingest_commits_total",
            "Ingest commits",
            "counter",
            c(&s.commits),
        );
        metric(
            "kafkaopsdb_ingest_conflicts_total",
            "Ingest tasks discarded after a commit conflict",
            "counter",
            c(&s.conflicts),
        );
        metric(
            "kafkaopsdb_ingest_failures_total",
            "Failed ingest tasks",
            "counter",
            c(&s.failures),
        );
        metric(
            "kafkaopsdb_ingest_offset_resets_total",
            "Offset resets (data lost to Kafka retention or recreated topics)",
            "counter",
            c(&s.resets),
        );
        metric(
            "kafkaopsdb_ingest_l0_files_total",
            "L0 files written",
            "counter",
            vec![
                (
                    "{format=\"avro\"}".into(),
                    s.l0_avro_files.load(Ordering::Relaxed) as f64,
                ),
                (
                    "{format=\"parquet\"}".into(),
                    s.l0_parquet_files.load(Ordering::Relaxed) as f64,
                ),
            ],
        );
        metric(
            "kafkaopsdb_ingest_lag",
            "Records not yet indexed per partition",
            "gauge",
            i.lag()
                .iter()
                .map(|l| {
                    (
                        format!("{{topic=\"{}\",partition=\"{}\"}}", l.topic, l.partition),
                        l.lag as f64,
                    )
                })
                .collect(),
        );
    }
    if let Some(cmp) = &app.compaction {
        let s = &cmp.stats;
        let c = |a: &std::sync::atomic::AtomicU64| one(a.load(Ordering::Relaxed));
        metric(
            "kafkaopsdb_compaction_tasks_total",
            "Compaction tasks run",
            "counter",
            c(&s.tasks),
        );
        metric(
            "kafkaopsdb_compaction_conflicts_total",
            "Compaction tasks that lost a race",
            "counter",
            c(&s.conflicts),
        );
        metric(
            "kafkaopsdb_compaction_failures_total",
            "Failed compaction tasks",
            "counter",
            c(&s.failures),
        );
        metric(
            "kafkaopsdb_compaction_input_bytes_total",
            "Bytes compacted",
            "counter",
            c(&s.input_bytes),
        );
    }
    let cs = &app.engine.cache().stats;
    metric(
        "kafkaopsdb_cache_requests_total",
        "Query cache lookups",
        "counter",
        vec![
            (
                "{result=\"memory_hit\"}".into(),
                cs.memory_hits.load(Ordering::Relaxed) as f64,
            ),
            (
                "{result=\"disk_hit\"}".into(),
                cs.disk_hits.load(Ordering::Relaxed) as f64,
            ),
            (
                "{result=\"miss\"}".into(),
                cs.misses.load(Ordering::Relaxed) as f64,
            ),
        ],
    );
    let m = &app.engine.metrics;
    metric(
        "kafkaopsdb_query_pruning_total",
        "Files and row groups considered and pruned by queries",
        "counter",
        vec![
            (
                "{what=\"files_total\"}".into(),
                m.files_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "{what=\"files_pruned\"}".into(),
                m.files_pruned.load(Ordering::Relaxed) as f64,
            ),
            (
                "{what=\"row_groups_total\"}".into(),
                m.row_groups_total.load(Ordering::Relaxed) as f64,
            ),
            (
                "{what=\"row_groups_pruned_stats\"}".into(),
                m.row_groups_pruned_stats.load(Ordering::Relaxed) as f64,
            ),
            (
                "{what=\"row_groups_pruned_bloom\"}".into(),
                m.row_groups_pruned_bloom.load(Ordering::Relaxed) as f64,
            ),
        ],
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out)
}
