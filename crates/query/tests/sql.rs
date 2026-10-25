use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use ingest::{
    CompactionConfig, CompactionService, IngestService, MemorySource, PlannerConfig, ServiceConfig,
    WorkerConfig,
};
use model::{Header, Record};
use object_store::memory::InMemory;
use object_store::path::Path;
use query::{CacheConfig, EngineConfig, FileCache, QueryEngine, format};
use table::{DataFormat, Table, TableConfig};

const TS0: i64 = 1_761_379_200_000;

fn corr(n: i64) -> String {
    format!("{n:08}-corr")
}

fn record(n: i64, i: i64) -> Record {
    let mut headers = vec![
        Header::new("correlation-id", corr(n)),
        Header::new("source", "svc"),
    ];
    if i % 10 == 0 {
        headers.push(Header::new("retry", "1"));
        headers.push(Header::new("retry", "2"));
    }
    Record {
        timestamp: TS0 + i,
        key: Some(format!("key-{i}").into_bytes()),
        value_size: Some(42),
        headers,
        ..Default::default()
    }
}

/// orders: n = i, payments: n = 6i, audit: n = 60i, so `corr(60)` is in all three topics
/// and `corr(1)` only in orders.
fn produce(source: &MemorySource, round: i64) {
    let base = round * 1_000_000;
    for i in 0..3000 {
        source.produce("orders.v1", (i % 2) as i32, record(base + i, i), 0);
    }
    for i in 0..500 {
        source.produce("payments", 0, record(base + i * 6, i), 0);
    }
    for i in 0..50 {
        source.produce("audit", 0, record(base + i * 60, i), 0);
    }
}

struct Fixture {
    table: Arc<Table>,
    engine: QueryEngine,
}

async fn fixture() -> Fixture {
    let source = Arc::new(MemorySource::new());
    source.create_topic("orders.v1", 2);
    source.create_topic("payments", 1);
    source.create_topic("audit", 1);
    let store = Arc::new(InMemory::new());
    let table = Table::open_or_create(
        store.clone(),
        Path::from("t"),
        "test",
        TableConfig::default(),
    )
    .await
    .unwrap();
    let ingest = IngestService::new(
        table.clone(),
        source.clone(),
        ServiceConfig {
            planner: PlannerConfig {
                flush_min_bytes: 128 << 10,
                max_flush_age: Duration::ZERO,
                ..Default::default()
            },
            worker: WorkerConfig {
                l0_parquet_min_rows: 1000,
                ..Default::default()
            },
            parallelism: 2,
            tick: Duration::from_millis(10),
            metadata_refresh: Duration::ZERO,
        },
    );
    let compaction = CompactionService::new(
        table.clone(),
        CompactionConfig {
            l0_min_files: 1,
            l2_promote_min_bytes: 8 << 10,
            merge_min_files: usize::MAX,
            ..Default::default()
        },
    );
    produce(&source, 0);
    while ingest.step().await.unwrap().tasks > 0 {}
    while compaction.step().await.unwrap().tasks > 0 {}
    // fresh data stays in L0, so the table holds every level and format at once
    produce(&source, 1);
    while ingest.step().await.unwrap().tasks > 0 {}

    let snap = table.snapshot();
    let kinds: BTreeSet<(u8, DataFormat)> = snap
        .segments
        .values()
        .map(|s| (s.level, s.format))
        .collect();
    assert!(kinds.contains(&(0, DataFormat::Avro)), "{kinds:?}");
    assert!(kinds.contains(&(0, DataFormat::Parquet)), "{kinds:?}");
    assert!(kinds.contains(&(1, DataFormat::Parquet)), "{kinds:?}");
    assert!(kinds.contains(&(2, DataFormat::Parquet)), "{kinds:?}");

    let dir = std::env::temp_dir().join(format!("kafkaopsdb-cache-{}", uuid::Uuid::now_v7()));
    let cache = FileCache::new(
        store,
        &CacheConfig {
            disk_dir: Some(dir),
            ..Default::default()
        },
    )
    .unwrap();
    let engine = QueryEngine::new(table.clone(), cache, EngineConfig::default());
    Fixture { table, engine }
}

async fn rows(engine: &QueryEngine, sql: &str) -> Vec<serde_json::Value> {
    let r = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    format::to_json_rows(&r.batches)
}

async fn count(engine: &QueryEngine, sql: &str) -> i64 {
    let r = engine.sql(sql).await.unwrap();
    r.batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_across_all_levels() {
    let Fixture { table, engine } = fixture().await;
    let e = &engine;

    // the motivating query, across all topics
    let found = rows(
        e,
        "SELECT topic, partition, offset FROM topics WHERE headers.`correlation-id` = '00000060-corr' ORDER BY topic",
    )
    .await;
    let topics: Vec<&str> = found.iter().map(|r| r["topic"].as_str().unwrap()).collect();
    assert_eq!(topics, vec!["audit", "orders.v1", "payments"]);
    assert_eq!(found[2]["offset"], 10);

    // and narrowed to one topic
    let found = rows(
        e,
        "SELECT * FROM topics WHERE topic = 'payments' AND headers.`correlation-id` = '00000060-corr'",
    )
    .await;
    assert_eq!(found.len(), 1);
    let row = &found[0];
    assert_eq!(row["key"], "key-10");
    assert_eq!(row["value_size"], 42);
    assert_eq!(row["timestamp_type"], "CreateTime");
    assert_eq!(
        row["headers"],
        serde_json::json!([
            {"key": "correlation-id", "value": "00000060-corr"},
            {"key": "source", "value": "svc"},
            {"key": "retry", "value": "1"},
            {"key": "retry", "value": "2"}
        ])
    );

    // fresh round-1 data (still in L0) is visible too
    let found = rows(e, "SELECT offset FROM topics WHERE headers.`correlation-id` = '01000060-corr' AND topic = 'audit'").await;
    assert_eq!(found, vec![serde_json::json!({"offset": 51})]);

    // aggregates
    let counts = rows(
        e,
        "SELECT topic, COUNT(*) AS n FROM topics GROUP BY topic ORDER BY topic",
    )
    .await;
    let counts: BTreeMap<String, i64> = counts
        .iter()
        .map(|r| {
            (
                r["topic"].as_str().unwrap().to_owned(),
                r["n"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        counts,
        BTreeMap::from([
            ("audit".into(), 100),
            ("orders.v1".into(), 6000),
            ("payments".into(), 1000)
        ])
    );
    assert_eq!(
        count(e, "SELECT COUNT(*) FROM topics WHERE topic LIKE 'pay%'").await,
        1000
    );
    assert_eq!(
        count(e, "SELECT COUNT(*) FROM topics WHERE topic = 'orders.v1' AND partition = 0 AND offset BETWEEN 10 AND 19").await,
        10
    );
    assert_eq!(
        count(e, "SELECT COUNT(*) FROM topics WHERE timestamp >= TIMESTAMP '2025-10-25 08:00:02.990' AND topic = 'orders.v1'").await,
        2 * 10
    );
    assert_eq!(
        count(e, "SELECT COUNT(*) FROM topics WHERE key = 'key-7'").await,
        2 * 3
    );
    assert_eq!(
        count(
            e,
            "SELECT COUNT(*) FROM topics WHERE headers.`no-such-header` = 'x'"
        )
        .await,
        0
    );

    // duplicate headers
    let found = rows(
        e,
        "SELECT header_all(headers, 'retry') AS r FROM topics WHERE topic = 'audit' AND offset = 0",
    )
    .await;
    assert_eq!(found[0]["r"], serde_json::json!(["1", "2"]));

    // SELECT * shape and LIMIT
    let r = e.sql("SELECT * FROM topics LIMIT 5").await.unwrap();
    assert_eq!(r.num_rows(), 5);
    let names: Vec<&str> = r
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestamp_type",
            "key",
            "value_size",
            "value_schema_id",
            "headers"
        ]
    );
    assert!(format::to_table(&r.schema, &r.batches).contains("5 row(s)"));
    assert!(r.batches[0].column(5).as_binary::<i32>().len() == r.batches[0].num_rows());

    assert!(e.sql("DELETE FROM topics").await.is_err());
    assert_eq!(table.snapshot().version, r.snapshot_version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pruning_skips_files_and_row_groups() {
    let Fixture { engine, .. } = fixture().await;
    let m = &engine.metrics;
    let snapshot = |v: &std::sync::atomic::AtomicU64| v.load(Ordering::Relaxed);

    // a header key that no file has: every file is pruned from the manifest
    let read_before = snapshot(&m.files_read);
    engine
        .sql("SELECT * FROM topics WHERE headers.`no-such-header` = 'x'")
        .await
        .unwrap();
    assert_eq!(snapshot(&m.files_read), read_before);

    // topic filter prunes files of other topics (the L2 file of orders, ...)
    let pruned_before = snapshot(&m.files_pruned);
    engine
        .sql("SELECT * FROM topics WHERE topic = 'audit'")
        .await
        .unwrap();
    assert!(snapshot(&m.files_pruned) > pruned_before);

    // a value only orders has: bloom filters drop row groups of the other Parquet files
    let bloom_before = snapshot(&m.row_groups_pruned_bloom);
    let found = engine
        .sql("SELECT topic FROM topics WHERE headers.`correlation-id` = '00000001-corr'")
        .await
        .unwrap();
    assert_eq!(found.num_rows(), 1);
    assert!(snapshot(&m.row_groups_pruned_bloom) > bloom_before);

    // the second run is served from the cache
    let misses = engine.cache().stats.misses.load(Ordering::Relaxed);
    engine
        .sql("SELECT topic FROM topics WHERE headers.`correlation-id` = '00000001-corr'")
        .await
        .unwrap();
    assert_eq!(engine.cache().stats.misses.load(Ordering::Relaxed), misses);
}
