use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ingest::{
    FetchRange, IngestService, IngestTask, InitialPosition, KafkaSource, MemorySource,
    PlannerConfig, ServiceConfig, TaskOutcome, WorkerConfig, execute_task,
};
use kseg::{Header, Projection, Record};
use object_store::memory::InMemory;
use object_store::path::Path;
use table::{SegmentKind, Table, TableConfig};

fn record(i: i64, topic: &str) -> Record {
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

async fn new_table() -> Arc<Table> {
    Table::open_or_create(
        Arc::new(InMemory::new()),
        Path::from("t"),
        "test",
        TableConfig::default(),
    )
    .await
    .unwrap()
}

fn config(min_bytes: u64, max_flush_age: Duration) -> ServiceConfig {
    ServiceConfig {
        planner: PlannerConfig {
            topic_segment_min_bytes: min_bytes,
            max_flush_age,
            task_max_bytes: 4 << 20,
            ..Default::default()
        },
        worker: WorkerConfig {
            topic_segment_min_bytes: min_bytes,
            ..Default::default()
        },
        parallelism: 4,
        tick: Duration::from_millis(10),
        metadata_refresh: Duration::ZERO,
    }
}

/// All indexed (topic, partition) -> offsets, read back from the segments.
async fn indexed(table: &Table) -> BTreeMap<(String, i32), Vec<i64>> {
    let snap = table.refresh().await.unwrap();
    let mut out: BTreeMap<(String, i32), Vec<i64>> = BTreeMap::new();
    for seg in snap.segments.values() {
        let reader = table.open_segment(seg).await.unwrap();
        for b in 0..reader.footer().blocks.len() {
            let block = reader.read_block(b, &Projection::default()).await.unwrap();
            out.entry((block.topic().to_owned(), block.partition))
                .or_default()
                .extend(&block.offsets);
        }
    }
    for v in out.values_mut() {
        v.sort_unstable();
    }
    out
}

async fn run_until_idle(svc: &IngestService) {
    for _ in 0..100 {
        let r = svc.step().await.unwrap();
        if r.tasks == 0 && r.resets == 0 {
            return;
        }
    }
    panic!("ingest did not become idle");
}

#[tokio::test]
async fn indexes_everything_exactly_once() {
    let source = Arc::new(MemorySource::new());
    let mut expected: BTreeMap<(String, i32), Vec<i64>> = BTreeMap::new();
    for topic in ["orders.v1", "payments", "audit"] {
        source.create_topic(topic, 3);
        for i in 0..3000 {
            let p = (i % 3) as i32;
            // a gap every 100 records, like transaction markers
            let skip = if i % 100 == 99 { 1 } else { 0 };
            let off = source.produce(topic, p, record(i, topic), skip);
            expected.entry((topic.into(), p)).or_default().push(off);
        }
    }
    let table = new_table().await;
    let svc = IngestService::new(
        table.clone(),
        source.clone(),
        config(64 << 10, Duration::ZERO),
    );
    run_until_idle(&svc).await;

    assert_eq!(indexed(&table).await, expected);
    let snap = table.snapshot();
    for (topic, p) in expected.keys() {
        assert_eq!(
            snap.committed(topic, *p),
            Some(source.high_watermark(topic, *p))
        );
    }
    assert!(svc.stats.segments_topic.load(Ordering::Relaxed) > 0);
    assert!(svc.lag().iter().all(|l| l.lag == 0));

    // more data arrives; only the new records are indexed
    for i in 3000..3100 {
        let off = source.produce("audit", 0, record(i, "audit"), 0);
        expected.get_mut(&("audit".into(), 0)).unwrap().push(off);
    }
    run_until_idle(&svc).await;
    assert_eq!(indexed(&table).await, expected);
}

#[tokio::test]
async fn crash_before_commit_and_duplicate_workers() {
    let source = Arc::new(MemorySource::new());
    source.create_topic("t", 1);
    for i in 0..500 {
        source.produce("t", 0, record(i, "t"), 0);
    }
    let table = new_table().await;
    let task = IngestTask {
        id: "task-a".into(),
        ranges: vec![FetchRange {
            topic: "t".into(),
            partition: 0,
            start: 0,
            end: 500,
        }],
        estimated_bytes: 0,
    };

    // a worker that dies after uploading but before committing leaves only an orphan
    let fetched = source.fetch(&task.ranges).await.unwrap();
    let mut w = kseg::SegmentWriter::new(kseg::SegmentType::Mixed, 0, Default::default());
    w.add("t", 0, fetched[0].records.clone()).unwrap();
    let (data, summary) = w
        .finish(uuid::Uuid::now_v7(), table::now_ms(), "dead")
        .unwrap();
    table.put_segment(data, &summary).await.unwrap();
    assert!(indexed(&table).await.is_empty());

    // two workers race on the same task: exactly one commit wins
    let cfg = WorkerConfig::default();
    let (a, b) = tokio::join!(
        execute_task(&table, source.as_ref(), &cfg, task.clone()),
        execute_task(&table, source.as_ref(), &cfg, task.clone()),
    );
    let outcomes = [a.outcome, b.outcome];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, TaskOutcome::Committed { .. }))
            .count(),
        1,
        "{outcomes:?}"
    );
    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, TaskOutcome::Conflict(_)))
    );
    let idx = indexed(&table).await;
    assert_eq!(idx[&("t".to_owned(), 0)], (0..500).collect::<Vec<_>>());
}

#[tokio::test]
async fn quiet_topics_share_one_mixed_segment() {
    let source = Arc::new(MemorySource::new());
    for t in 0..50 {
        let topic = format!("quiet-{t:02}");
        source.create_topic(&topic, 1);
        for i in 0..10 {
            source.produce(&topic, 0, record(i, &topic), 0);
        }
    }
    let table = new_table().await;
    // nothing is due before max_flush_age
    let svc = IngestService::new(
        table.clone(),
        source.clone(),
        config(16 << 20, Duration::from_millis(200)),
    );
    let first = svc.step().await.unwrap();
    assert_eq!(first.tasks, 0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let r = svc.step().await.unwrap();
    assert_eq!(r.tasks, 1, "all quiet topics go into one task");
    assert_eq!(r.records, 500);
    let snap = table.snapshot();
    assert_eq!(snap.segments.len(), 1);
    let seg = snap.segments.values().next().unwrap();
    assert_eq!(seg.segment_type, SegmentKind::Mixed);
    assert_eq!(seg.topics.len(), 50);
    assert_eq!(svc.stats.segments_mixed.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn hot_topic_gets_topic_segments_and_quiet_ones_mixed() {
    let source = Arc::new(MemorySource::new());
    source.create_topic("hot", 2);
    source.create_topic("cold", 1);
    for i in 0..4000 {
        source.produce("hot", (i % 2) as i32, record(i, "hot"), 0);
    }
    for i in 0..5 {
        source.produce("cold", 0, record(i, "cold"), 0);
    }
    let table = new_table().await;
    let svc = IngestService::new(
        table.clone(),
        source.clone(),
        config(100 << 10, Duration::ZERO),
    );
    run_until_idle(&svc).await;
    let snap = table.snapshot();
    for seg in snap.segments.values() {
        match seg.segment_type {
            SegmentKind::Topic => assert_eq!(seg.topics[0].topic, "hot"),
            SegmentKind::Mixed => assert!(seg.topic("cold").is_some()),
        }
    }
    assert!(
        snap.segments
            .values()
            .any(|s| s.segment_type == SegmentKind::Topic)
    );
    assert!(
        snap.segments
            .values()
            .any(|s| s.segment_type == SegmentKind::Mixed)
    );
}

#[tokio::test]
async fn retention_outrunning_the_indexer_is_recorded_as_reset() {
    let source = Arc::new(MemorySource::new());
    source.create_topic("t", 1);
    for i in 0..100 {
        source.produce("t", 0, record(i, "t"), 0);
    }
    let table = new_table().await;
    let svc = IngestService::new(
        table.clone(),
        source.clone(),
        config(1 << 30, Duration::ZERO),
    );
    run_until_idle(&svc).await;
    assert_eq!(table.snapshot().committed("t", 0), Some(100));

    for i in 100..300 {
        source.produce("t", 0, record(i, "t"), 0);
    }
    source.truncate("t", 0, 250);
    run_until_idle(&svc).await;
    let snap = table.snapshot();
    assert_eq!(snap.committed("t", 0), Some(300));
    assert_eq!(svc.stats.resets.load(Ordering::Relaxed), 1);
    let offsets = &indexed(&table).await[&("t".to_owned(), 0)];
    assert_eq!(
        offsets.len(),
        150,
        "records 100..250 were lost to retention"
    );

    // a failing source does not commit anything
    source.fail_fetch.store(true, Ordering::SeqCst);
    source.produce("t", 0, record(300, "t"), 0);
    let r = svc.step().await.unwrap();
    assert_eq!(r.failed, 1);
    assert_eq!(table.snapshot().committed("t", 0), Some(300));
}

#[tokio::test]
async fn initial_position_latest_skips_history() {
    let source = Arc::new(MemorySource::new());
    source.create_topic("t", 1);
    for i in 0..100 {
        source.produce("t", 0, record(i, "t"), 0);
    }
    let table = new_table().await;
    let mut cfg = config(1 << 30, Duration::ZERO);
    cfg.planner.initial_position = InitialPosition::Latest;
    let svc = IngestService::new(table.clone(), source.clone(), cfg);
    run_until_idle(&svc).await;
    assert_eq!(table.snapshot().committed("t", 0), Some(100));
    assert!(indexed(&table).await.is_empty());
    source.produce("t", 0, record(100, "t"), 0);
    run_until_idle(&svc).await;
    assert_eq!(indexed(&table).await[&("t".to_owned(), 0)], vec![100]);
}
