//! Helpers shared by the ingest and compaction tests.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ingest::{IngestService, PlannerConfig, ServiceConfig, WorkerConfig};
use model::{Header, Record};
use object_store::memory::InMemory;
use object_store::path::Path;
use table::{Table, TableConfig};

pub fn record(i: i64, topic: &str) -> Record {
    Record {
        timestamp: 1_761_379_200_000 + i,
        key: Some(format!("key-{i}").into_bytes()),
        value_size: Some(100),
        headers: vec![
            Header::new("correlation-id", format!("{topic}-corr-{i}")),
            Header::new("source", "svc"),
        ],
        ..Default::default()
    }
}

pub async fn new_table() -> Arc<Table> {
    Table::open_or_create(
        Arc::new(InMemory::new()),
        Path::from("t"),
        "test",
        TableConfig::default(),
    )
    .await
    .unwrap()
}

pub fn config(min_bytes: u64, max_flush_age: Duration) -> ServiceConfig {
    ServiceConfig {
        planner: PlannerConfig {
            flush_min_bytes: min_bytes,
            max_flush_age,
            task_max_bytes: 4 << 20,
            ..Default::default()
        },
        worker: WorkerConfig::default(),
        parallelism: 4,
        tick: Duration::from_millis(10),
        metadata_refresh: Duration::ZERO,
    }
}

/// All indexed (topic, partition) -> offsets, read back from the segments.
pub async fn indexed(table: &Table) -> BTreeMap<(String, i32), Vec<i64>> {
    let snap = table.refresh().await.unwrap();
    let mut out: BTreeMap<(String, i32), Vec<i64>> = BTreeMap::new();
    for seg in snap.segments.values() {
        let data = table.read_segment(seg).await.unwrap();
        for batch in model::read_file(seg.format.into(), data).unwrap() {
            for (topic, partition, r) in model::batch_to_records(&batch).unwrap() {
                out.entry((topic, partition)).or_default().push(r.offset);
            }
        }
    }
    for v in out.values_mut() {
        v.sort_unstable();
    }
    out
}

pub async fn run_until_idle(svc: &IngestService) {
    for _ in 0..100 {
        let r = svc.step().await.unwrap();
        if r.tasks == 0 && r.resets == 0 {
            return;
        }
    }
    panic!("ingest did not become idle");
}
