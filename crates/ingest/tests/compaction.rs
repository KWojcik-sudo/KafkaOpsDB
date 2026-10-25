use std::sync::Arc;
use std::time::Duration;

use ingest::{
    CompactionConfig, CompactionKind, CompactionOutcome, CompactionPlanner, CompactionService,
    CompactionTask, IngestService, MemorySource, RetentionRule, execute_compaction,
};
use model::Record;
use regex::Regex;
use table::{DataFormat, SegmentKind, Table, now_ms};

mod common;
use common::*;

fn compaction(l0_min_files: usize) -> CompactionConfig {
    CompactionConfig {
        l0_min_files,
        l0_max_age: Duration::from_secs(3600),
        l0_max_bytes: u64::MAX,
        l2_promote_min_bytes: u64::MAX,
        merge_min_files: usize::MAX,
        ..Default::default()
    }
}

async fn compact_until_idle(svc: &CompactionService) -> usize {
    let mut tasks = 0;
    for _ in 0..50 {
        let r = svc.step().await.unwrap();
        assert_eq!(r.failed, 0);
        if r.tasks == 0 {
            return tasks;
        }
        tasks += r.tasks;
    }
    panic!("compaction did not become idle");
}

fn levels(table: &Table) -> Vec<(u8, Vec<String>)> {
    let mut v: Vec<(u8, Vec<String>)> = table
        .snapshot()
        .segments
        .values()
        .map(|s| (s.level, s.topics.iter().map(|t| t.topic.clone()).collect()))
        .collect();
    v.sort();
    v
}

/// Produces `n` records per topic and indexes them into one or more L0 files.
async fn ingest_round(
    source: &MemorySource,
    svc: &IngestService,
    topics: &[(&str, i64)],
    round: i64,
) {
    for (topic, n) in topics {
        for i in 0..*n {
            source.produce(topic, (i % 2) as i32, record(round * 100_000 + i, topic), 0);
        }
    }
    run_until_idle(svc).await;
}

async fn setup(
    topics: &[(&str, i64)],
    rounds: i64,
) -> (Arc<MemorySource>, Arc<IngestService>, Arc<Table>) {
    let source = Arc::new(MemorySource::new());
    for (t, _) in topics {
        source.create_topic(t, 2);
    }
    let table = new_table().await;
    let svc = IngestService::new(
        table.clone(),
        source.clone(),
        config(1 << 40, Duration::ZERO),
    );
    for round in 0..rounds {
        ingest_round(&source, &svc, topics, round).await;
    }
    (source, svc, table)
}

#[tokio::test]
async fn l0_compacts_into_sorted_l1() {
    let (_, _, table) = setup(&[("orders", 300), ("audit", 50), ("payments", 120)], 4).await;
    let before = indexed(&table).await;
    assert_eq!(table.snapshot().segments.len(), 4);
    assert!(
        table
            .snapshot()
            .segments
            .values()
            .all(|s| s.level == 0 && s.format == DataFormat::Avro)
    );

    let svc = CompactionService::new(table.clone(), compaction(3));
    assert_eq!(compact_until_idle(&svc).await, 1);
    let snap = table.snapshot();
    assert!(
        snap.segments
            .values()
            .all(|s| s.level == 1 && s.format == DataFormat::Parquet)
    );
    assert_eq!(
        indexed(&table).await,
        before,
        "compaction must keep every record exactly once"
    );

    // L1 files are sorted by (topic, partition, offset) and carry pivots
    for seg in snap.segments.values() {
        assert!(seg.path.starts_with("data/l1/"));
        assert_eq!(seg.pivot_keys, vec!["correlation-id", "source"]);
        let data = table.read_segment(seg).await.unwrap();
        let mut rows = Vec::new();
        for b in model::read_file(seg.format.into(), data).unwrap() {
            rows.extend(
                model::batch_to_records(&b)
                    .unwrap()
                    .into_iter()
                    .map(|(t, p, r)| (t, p, r.offset)),
            );
        }
        let mut sorted = rows.clone();
        sorted.sort();
        assert_eq!(rows, sorted);
    }
    // old files become tombstones for vacuum
    assert_eq!(snap.tombstones.len(), 4);
}

#[tokio::test]
async fn hot_topics_are_promoted_to_l2_and_merged() {
    let topics = [("hot", 4000), ("cold-a", 10), ("cold-b", 10)];
    let (source, ingest, table) = setup(&topics, 2).await;
    let cfg = CompactionConfig {
        l2_promote_min_bytes: 20 << 10,
        merge_min_files: 2,
        ..compaction(2)
    };
    let svc = CompactionService::new(table.clone(), cfg);
    compact_until_idle(&svc).await;
    let before = indexed(&table).await;

    let snap = table.snapshot();
    for seg in snap.segments.values() {
        if seg.topic("hot").is_some() {
            assert_eq!(seg.level, 2, "{:?}", levels(&table));
            assert_eq!(seg.segment_type, SegmentKind::Topic);
            assert!(seg.path.starts_with("data/l2/topic=hot/"));
        } else {
            assert_eq!(
                seg.level,
                1,
                "quiet topics stay in L1: {:?}",
                levels(&table)
            );
        }
    }

    // more rounds produce more L2 files for the hot topic, which get merged again
    for round in 2..5 {
        ingest_round(&source, &ingest, &topics, round).await;
        compact_until_idle(&svc).await;
    }
    let snap = table.snapshot();
    let hot_l2 = snap
        .segments
        .values()
        .filter(|s| s.level == 2 && s.topic("hot").is_some())
        .count();
    assert_eq!(
        hot_l2,
        1,
        "small L2 files of one topic are merged: {:?}",
        levels(&table)
    );
    let after = indexed(&table).await;
    assert_eq!(after[&("hot".to_owned(), 0)].len(), 5 * 2000);
    for (k, v) in &before {
        assert!(after[k].starts_with(v));
    }
}

#[tokio::test]
async fn retention_drops_only_expired_rows() {
    let source = Arc::new(MemorySource::new());
    source.create_topic("logs", 1);
    source.create_topic("keep", 1);
    let now = now_ms();
    for i in 0..100 {
        let ts = if i < 50 { now - 2 * 3600 * 1000 } else { now };
        for topic in ["logs", "keep"] {
            let r = Record {
                timestamp: ts,
                ..record(i, topic)
            };
            source.produce(topic, 0, r, 0);
        }
    }
    let table = new_table().await;
    let ingest = IngestService::new(
        table.clone(),
        source.clone(),
        config(1 << 40, Duration::ZERO),
    );
    run_until_idle(&ingest).await;

    let cfg = CompactionConfig {
        retention: vec![RetentionRule {
            topics: Regex::new("^logs$").unwrap(),
            retention: Duration::from_secs(3600),
        }],
        ..compaction(usize::MAX)
    };
    let svc = CompactionService::new(table.clone(), cfg);
    assert_eq!(compact_until_idle(&svc).await, 1);
    let idx = indexed(&table).await;
    assert_eq!(idx[&("logs".to_owned(), 0)], (50..100).collect::<Vec<_>>());
    assert_eq!(idx[&("keep".to_owned(), 0)].len(), 100);
    // offsets are untouched by compaction
    assert_eq!(table.snapshot().committed("logs", 0), Some(100));
}

#[tokio::test]
async fn tasks_have_disjoint_inputs_and_lost_races_conflict() {
    let (_, _, table) = setup(&[("a", 100), ("b", 100)], 3).await;
    let cfg = compaction(2);
    let mut planner = CompactionPlanner::new(cfg.clone());
    let snap = table.snapshot();
    let first = planner.plan(&snap, now_ms());
    assert_eq!(first.len(), 1);
    assert!(
        planner.plan(&snap, now_ms()).is_empty(),
        "inputs are in flight"
    );

    let task = &first[0];
    assert_eq!(task.kind, CompactionKind::CompactL0);
    let twin = CompactionTask {
        id: "twin".into(),
        kind: CompactionKind::MergeL1,
        inputs: task.inputs.clone(),
    };
    assert!(matches!(
        execute_compaction(&table, &cfg, task).await,
        CompactionOutcome::Committed { .. }
    ));
    assert!(matches!(
        execute_compaction(&table, &cfg, &twin).await,
        CompactionOutcome::Conflict(_)
    ));
    planner.finished(task);
    let before = indexed(&table).await;
    assert_eq!(before.values().map(Vec::len).sum::<usize>(), 600);
}
