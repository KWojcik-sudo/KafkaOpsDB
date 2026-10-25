use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use model::{FileFormat, FileStats, Header, ParquetOptions, Record};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use table::{
    Action, OffsetRange, OffsetReset, Operation, RemoveSegment, SegmentMeta, Table, TableConfig,
    VacuumConfig, now_ms, open_store, self_test,
};

fn range(topic: &str, partition: i32, start: i64, end: i64) -> Action {
    Action::OffsetRange(OffsetRange {
        topic: topic.into(),
        topic_uuid: None,
        partition,
        start,
        end,
    })
}

async fn table_on(store: Arc<dyn ObjectStore>, writer: &str, cfg: TableConfig) -> Arc<Table> {
    Table::open_or_create(store, Path::from("tbl"), writer, cfg)
        .await
        .unwrap()
}

#[tokio::test]
async fn create_commit_reopen() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let t = table_on(store.clone(), "a", TableConfig::default()).await;
    assert_eq!(t.snapshot().version, 0);
    let v = t
        .commit(
            Operation::Ingest,
            Some("task-1".into()),
            vec![range("orders", 0, 0, 100)],
        )
        .await
        .unwrap();
    assert_eq!(v, 1);
    let v = t
        .commit(Operation::Ingest, None, vec![range("orders", 0, 100, 150)])
        .await
        .unwrap();
    assert_eq!(v, 2);

    let reopened = table_on(store, "b", TableConfig::default()).await;
    let snap = reopened.snapshot();
    assert_eq!(snap.version, 2);
    assert_eq!(snap.committed("orders", 0), Some(150));
    assert_eq!(snap.version_of_task("task-1"), Some(1));
    // the same task can never be committed twice
    let err = reopened
        .commit(
            Operation::Ingest,
            Some("task-1".into()),
            vec![range("orders", 0, 150, 160)],
        )
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "{err}");
}

#[tokio::test]
async fn stale_start_is_rejected() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = table_on(store.clone(), "a", TableConfig::default()).await;
    let zombie = table_on(store.clone(), "zombie", TableConfig::default()).await;
    a.commit(Operation::Ingest, None, vec![range("t", 0, 0, 10)])
        .await
        .unwrap();
    // the zombie still believes partition 0 is new and re-commits [0, 10)
    let err = zombie
        .commit(Operation::Ingest, None, vec![range("t", 0, 0, 10)])
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "{err}");
    // and an overlapping range from a stale snapshot is rejected as well
    let err = zombie
        .commit(Operation::Ingest, None, vec![range("t", 0, 5, 20)])
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "{err}");
    // resets carry their own precondition
    let err = zombie
        .commit(
            Operation::Reset,
            None,
            vec![Action::OffsetReset(OffsetReset {
                topic: "t".into(),
                topic_uuid: None,
                partition: 0,
                start: None,
                end: 1000,
                reason: "test".into(),
            })],
        )
        .await
        .unwrap_err();
    assert!(err.is_conflict(), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_produce_contiguous_ranges() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cfg = TableConfig {
        checkpoint_interval: 7,
        max_commit_attempts: 50,
        ..Default::default()
    };
    table_on(store.clone(), "init", cfg.clone()).await;
    let writers = 8;
    let commits_per_writer = 15;
    let mut handles = Vec::new();
    for w in 0..writers {
        let store = store.clone();
        let cfg = cfg.clone();
        handles.push(tokio::spawn(async move {
            let t = table_on(store, &format!("w{w}"), cfg).await;
            let mut done = 0;
            let mut conflicts = 0;
            while done < commits_per_writer {
                let snap = t.refresh().await.unwrap();
                // all writers fight over the same two partitions
                let p = done % 2;
                let start = snap.committed("orders", p).unwrap_or(0);
                match t
                    .commit(
                        Operation::Ingest,
                        None,
                        vec![range("orders", p, start, start + 10)],
                    )
                    .await
                {
                    Ok(_) => done += 1,
                    Err(e) if e.is_conflict() => conflicts += 1,
                    Err(e) => panic!("{e}"),
                }
            }
            conflicts
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let t = table_on(store.clone(), "check", cfg).await;
    let snap = t.snapshot();
    let total = (writers * commits_per_writer) as i64 * 10;
    assert_eq!(
        snap.committed("orders", 0).unwrap() + snap.committed("orders", 1).unwrap(),
        total
    );
    assert_eq!(snap.version, (writers * commits_per_writer) as u64);

    // replaying the full log (ignoring checkpoints) yields the same state
    let mut replay = table::Snapshot::default();
    for v in 0..=snap.version {
        let data = store
            .get(&Path::from(format!("tbl/_log/{v:020}.json")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        replay
            .apply(v, &table::decode_actions(&data).unwrap())
            .unwrap();
    }
    assert_eq!(replay.offsets, snap.offsets);
    assert_eq!(replay.version, snap.version);
}

/// Store whose next commit write succeeds but reports a failure to the caller.
#[derive(Debug)]
struct LyingStore {
    inner: InMemory,
    lie_next_commit: AtomicBool,
}

impl std::fmt::Display for LyingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LyingStore")
    }
}

#[async_trait]
impl ObjectStore for LyingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let r = self.inner.put_opts(location, payload, opts).await?;
        if location.as_ref().contains("_log/0")
            && location.as_ref().ends_with(".json")
            && self.lie_next_commit.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "lying",
                source: "simulated timeout".into(),
            });
        }
        Ok(r)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn ambiguous_write_is_resolved_by_txn_id() {
    let lying = Arc::new(LyingStore {
        inner: InMemory::new(),
        lie_next_commit: AtomicBool::new(false),
    });
    let store: Arc<dyn ObjectStore> = lying.clone();
    let t = table_on(store, "a", TableConfig::default()).await;
    lying.lie_next_commit.store(true, Ordering::SeqCst);
    let v = t
        .commit(Operation::Ingest, None, vec![range("t", 0, 0, 10)])
        .await
        .unwrap();
    assert_eq!(
        v, 1,
        "the write went through, so the commit must report success"
    );
    assert_eq!(
        t.refresh().await.unwrap().version,
        1,
        "no duplicate version"
    );
    assert_eq!(t.snapshot().committed("t", 0), Some(10));
}

fn sample_segment(topic: &str, offsets: std::ops::Range<i64>) -> (Vec<u8>, SegmentMeta) {
    let records: Vec<Record> = offsets
        .map(|o| Record {
            offset: o,
            timestamp: 1_761_379_200_000 + o,
            headers: vec![Header::new("correlation-id", format!("c-{o}"))],
            ..Default::default()
        })
        .collect();
    let batch = model::records_to_batch([(topic, 0, records.as_slice())]).unwrap();
    let meta = SegmentMeta {
        format: FileFormat::Parquet,
        level: 2,
        stats: FileStats::from_batch(&batch).unwrap(),
        pivot_keys: Vec::new(),
    };
    (
        model::write_parquet(&batch, &ParquetOptions::l0()).unwrap(),
        meta,
    )
}

#[tokio::test]
async fn checkpoints_segments_and_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let url = url::Url::from_directory_path(dir.path()).unwrap();
    let (store, root) = open_store(url.as_str(), &HashMap::new()).unwrap();
    self_test(store.as_ref(), &root).await.unwrap();
    let cfg = TableConfig {
        checkpoint_interval: 3,
        ..Default::default()
    };
    let t = Table::open_or_create(store.clone(), root.clone(), "a", cfg.clone())
        .await
        .unwrap();

    let mut segment_ids = Vec::new();
    for i in 0..5i64 {
        let (data, summary) = sample_segment("orders", i * 10..i * 10 + 10);
        let add = t.put_segment(data, &summary).await.unwrap();
        assert!(
            add.path.starts_with("data/l2/topic=orders/date="),
            "{}",
            add.path
        );
        assert_eq!(t.read_segment(&add).await.unwrap().len() as u64, add.size);
        segment_ids.push(add.segment_id);
        t.commit(
            Operation::Ingest,
            None,
            vec![Action::Add(add), range("orders", 0, i * 10, i * 10 + 10)],
        )
        .await
        .unwrap();
    }
    // compaction-like commit: remove two segments
    t.commit(
        Operation::Compact,
        None,
        segment_ids[..2]
            .iter()
            .map(|id| {
                Action::Remove(RemoveSegment {
                    segment_id: *id,
                    deletion_timestamp: now_ms() - 10_000,
                })
            })
            .collect(),
    )
    .await
    .unwrap();
    // removing an already removed segment conflicts
    let err = t
        .commit(
            Operation::Compact,
            None,
            vec![Action::Remove(RemoveSegment {
                segment_id: segment_ids[0],
                deletion_timestamp: now_ms(),
            })],
        )
        .await
        .unwrap_err();
    assert!(err.is_conflict());

    let snap = t.snapshot();
    assert_eq!(snap.version, 6);
    assert_eq!(snap.segments.len(), 3);
    assert!(
        dir.path()
            .join("_log/00000000000000000006.checkpoint.json.zst")
            .exists()
    );

    // reopening starts from the checkpoint and reaches the same state
    let reopened = Table::open_or_create(store.clone(), root.clone(), "b", cfg)
        .await
        .unwrap();
    let r = reopened.snapshot();
    assert_eq!(r.version, snap.version);
    assert_eq!(r.segments, snap.segments);
    assert_eq!(r.offsets, snap.offsets);

    // an orphan (uploaded, never committed) and the removed segments get collected
    let (data, summary) = sample_segment("orders", 100..110);
    let orphan = t.put_segment(data, &summary).await.unwrap();
    let report = t
        .vacuum(&VacuumConfig {
            tombstone_grace: Duration::from_secs(1),
            orphan_grace: Duration::ZERO,
            log_retention: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(report.removed_segments, 2);
    assert_eq!(report.orphans, 1);
    assert!(report.log_files >= 5, "{report:?}");
    assert!(!dir.path().join(&orphan.path).exists());
    for s in snap.segments.values() {
        assert!(dir.path().join(&s.path).exists(), "live segment deleted");
    }
    // after log cleanup the table still opens from the checkpoint
    let again = Table::open_or_create(store, root, "c", TableConfig::default())
        .await
        .unwrap();
    assert_eq!(again.snapshot().offsets, snap.offsets);
}

#[tokio::test]
async fn self_test_detects_missing_conditional_put() {
    let store = InMemory::new();
    self_test(&store, &Path::from("x")).await.unwrap();
}
