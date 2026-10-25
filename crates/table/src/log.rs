//! The table: commit log on object storage (docs/commit-protocol.md).

use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, WriteMultipart};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::actions::{
    Action, AddSegment, CommitInfo, Operation, Protocol, READER_VERSION, TableMeta, WRITER_VERSION,
};
use crate::error::{Error, Result};
use crate::snapshot::{CheckpointData, Snapshot};
use crate::storage::join;

const MULTIPART_THRESHOLD: usize = 16 << 20;
const MULTIPART_CHUNK: usize = 8 << 20;

#[derive(Debug, Clone)]
pub struct TableConfig {
    /// A checkpoint is written after every `checkpoint_interval`-th version.
    pub checkpoint_interval: u64,
    pub max_commit_attempts: u32,
    /// Tombstones older than this are dropped from checkpoints.
    pub tombstone_retention: Duration,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: 100,
            max_commit_attempts: 10,
            tombstone_retention: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct LastCheckpoint {
    version: u64,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct Table {
    store: Arc<dyn ObjectStore>,
    root: Path,
    writer: String,
    cfg: TableConfig,
    state: RwLock<Arc<Snapshot>>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("root", &self.root)
            .field("writer", &self.writer)
            .field("version", &self.snapshot().version)
            .finish()
    }
}

impl Table {
    /// Opens the table at `root`, creating it (log version 0) if it does not exist.
    pub async fn open_or_create(
        store: Arc<dyn ObjectStore>,
        root: Path,
        writer: impl Into<String>,
        cfg: TableConfig,
    ) -> Result<Arc<Self>> {
        let table = Arc::new(Self {
            store,
            root,
            writer: writer.into(),
            cfg,
            state: RwLock::new(Arc::new(Snapshot::default())),
            refresh_lock: tokio::sync::Mutex::new(()),
        });
        if !table.load().await? {
            let actions = vec![
                Action::CommitInfo(CommitInfo {
                    txn_id: Uuid::now_v7(),
                    writer: table.writer.clone(),
                    timestamp: now_ms(),
                    operation: Operation::Create,
                    read_version: None,
                    task_id: None,
                }),
                Action::Protocol(Protocol {
                    min_reader_version: READER_VERSION,
                    min_writer_version: WRITER_VERSION,
                }),
                Action::Table(TableMeta {
                    id: Uuid::now_v7(),
                    created_at: now_ms(),
                }),
            ];
            match table
                .put_create(&table.log_path(0), encode_actions(&actions)?)
                .await
            {
                Ok(()) | Err(Error::Storage(object_store::Error::AlreadyExists { .. })) => {}
                Err(e) => return Err(e),
            }
            if !table.load().await? {
                return Err(Error::Corrupt(
                    "table creation did not become visible".into(),
                ));
            }
        }
        Ok(table)
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn writer(&self) -> &str {
        &self.writer
    }

    pub fn config(&self) -> &TableConfig {
        &self.cfg
    }

    /// The latest snapshot known to this handle (no I/O).
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.state.read().unwrap().clone()
    }

    /// Absolute object path of a table-relative path.
    pub fn path_of(&self, rel: &str) -> Path {
        join(&self.root, rel)
    }

    fn log_dir(&self) -> Path {
        self.root.clone().join("_log")
    }

    fn log_path(&self, version: u64) -> Path {
        self.log_dir().clone().join(format!("{version:020}.json"))
    }

    fn checkpoint_path(&self, version: u64) -> Path {
        self.log_dir()
            .clone()
            .join(format!("{version:020}.checkpoint.json.zst"))
    }

    fn last_checkpoint_path(&self) -> Path {
        self.log_dir().clone().join("_last_checkpoint")
    }

    async fn put_create(&self, path: &Path, body: Bytes) -> Result<()> {
        self.store
            .put_opts(
                path,
                body.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Loads the latest state. Returns false when the table does not exist yet.
    async fn load(&self) -> Result<bool> {
        let base = match self.read_latest_checkpoint().await? {
            Some(s) => s,
            None => match self.read_version(0).await? {
                Some(actions) => {
                    let mut s = Snapshot::default();
                    s.apply(0, &actions)?;
                    s
                }
                None => return Ok(false),
            },
        };
        let snap = self.catch_up(Arc::new(base)).await?;
        self.install(snap);
        Ok(true)
    }

    async fn read_latest_checkpoint(&self) -> Result<Option<Snapshot>> {
        if let Ok(r) = self.store.get(&self.last_checkpoint_path()).await
            && let Ok(b) = r.bytes().await
            && let Ok(lc) = serde_json::from_slice::<LastCheckpoint>(&b)
        {
            match self.read_checkpoint(lc.version).await {
                Ok(s) => return Ok(Some(s)),
                Err(e) => {
                    warn!(version = lc.version, error = %e, "unreadable checkpoint, falling back to listing")
                }
            }
        }
        let mut versions: Vec<u64> = self
            .list_log()
            .await?
            .into_iter()
            .filter_map(|(v, kind)| (kind == LogFileKind::Checkpoint).then_some(v))
            .collect();
        versions.sort_unstable_by(|a, b| b.cmp(a));
        for v in versions {
            match self.read_checkpoint(v).await {
                Ok(s) => return Ok(Some(s)),
                Err(e) => warn!(version = v, error = %e, "unreadable checkpoint"),
            }
        }
        Ok(None)
    }

    async fn read_checkpoint(&self, version: u64) -> Result<Snapshot> {
        let data = self
            .store
            .get(&self.checkpoint_path(version))
            .await?
            .bytes()
            .await?;
        let raw = zstd::stream::decode_all(&data[..])?;
        let cp: CheckpointData = serde_json::from_slice(&raw)?;
        if cp.version != version {
            return Err(Error::Corrupt(format!(
                "checkpoint {version} contains version {}",
                cp.version
            )));
        }
        Ok(Snapshot::from_checkpoint(cp))
    }

    pub(crate) async fn list_log(&self) -> Result<Vec<(u64, LogFileKind)>> {
        let metas: Vec<_> = self.store.list(Some(&self.log_dir())).try_collect().await?;
        Ok(metas
            .iter()
            .filter_map(|m| m.location.filename().and_then(parse_log_name))
            .collect())
    }

    async fn read_version(&self, version: u64) -> Result<Option<Vec<Action>>> {
        match self.store.get(&self.log_path(version)).await {
            Ok(r) => Ok(Some(decode_actions(&r.bytes().await?)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Applies all versions after `snap`, probing until the first missing one.
    async fn catch_up(&self, snap: Arc<Snapshot>) -> Result<Arc<Snapshot>> {
        let Some(first) = self.read_version(snap.version + 1).await? else {
            return Ok(snap);
        };
        let mut next = (*snap).clone();
        let mut version = snap.version + 1;
        let mut actions = first;
        loop {
            next.apply(version, &actions)?;
            version += 1;
            match self.read_version(version).await? {
                Some(a) => actions = a,
                None => break,
            }
        }
        Ok(Arc::new(next))
    }

    fn install(&self, snap: Arc<Snapshot>) {
        let mut state = self.state.write().unwrap();
        if snap.version > state.version || state.table.is_none() {
            *state = snap;
        }
    }

    /// Reads new log versions and returns the latest snapshot.
    pub async fn refresh(&self) -> Result<Arc<Snapshot>> {
        let _guard = self.refresh_lock.lock().await;
        let snap = self.catch_up(self.snapshot()).await?;
        self.install(snap);
        Ok(self.snapshot())
    }

    /// Atomically commits `actions` as the next log version.
    ///
    /// Returns the committed version, or `Error::Conflict` when the preconditions encoded
    /// in the actions (offset range starts, removed segments, task id) no longer hold.
    /// Any other error means the outcome is unknown; callers must re-derive their work
    /// from a refreshed snapshot rather than blindly retrying.
    pub async fn commit(
        &self,
        operation: Operation,
        task_id: Option<String>,
        actions: Vec<Action>,
    ) -> Result<u64> {
        let txn_id = Uuid::now_v7();
        let mut snap = self.snapshot();
        if snap.validate(task_id.as_deref(), &actions).is_err() {
            snap = self.refresh().await?;
            snap.validate(task_id.as_deref(), &actions)?;
        }
        let mut last_error = String::new();
        for attempt in 1..=self.cfg.max_commit_attempts {
            let version = snap.version + 1;
            let mut all = Vec::with_capacity(actions.len() + 1);
            all.push(Action::CommitInfo(CommitInfo {
                txn_id,
                writer: self.writer.clone(),
                timestamp: now_ms(),
                operation,
                read_version: Some(snap.version),
                task_id: task_id.clone(),
            }));
            all.extend(actions.iter().cloned());
            let body = encode_actions(&all)?;
            match self.put_create(&self.log_path(version), body).await {
                Ok(()) => {
                    let mut next = (*snap).clone();
                    next.apply(version, &all)?;
                    let next = Arc::new(next);
                    self.install(next.clone());
                    debug!(version, ?operation, "committed");
                    if version.is_multiple_of(self.cfg.checkpoint_interval)
                        && let Err(e) = self.write_checkpoint(&next).await
                    {
                        warn!(version, error = %e, "checkpoint failed");
                    }
                    return Ok(version);
                }
                Err(e) => {
                    let lost_race =
                        matches!(e, Error::Storage(object_store::Error::AlreadyExists { .. }));
                    if !lost_race {
                        // the write may or may not have happened; the txn id tells
                        warn!(version, attempt, error = %e, "commit write failed");
                        last_error = e.to_string();
                        tokio::time::sleep(backoff(attempt)).await;
                    }
                    snap = match self.refresh().await {
                        Ok(s) => s,
                        Err(e) => {
                            last_error = e.to_string();
                            continue;
                        }
                    };
                    if let Some(v) = snap.version_of_txn(&txn_id) {
                        return Ok(v);
                    }
                    snap.validate(task_id.as_deref(), &actions)?;
                    if lost_race {
                        last_error = format!("lost race for version {version}");
                    }
                }
            }
        }
        Err(Error::CommitRetriesExhausted {
            attempts: self.cfg.max_commit_attempts,
            last: last_error,
        })
    }

    async fn write_checkpoint(&self, snap: &Snapshot) -> Result<()> {
        let mut s = snap.clone();
        s.prune_tombstones(now_ms() - self.cfg.tombstone_retention.as_millis() as i64);
        let json = serde_json::to_vec(&s.to_checkpoint())?;
        let data = zstd::bulk::compress(&json, 3)?;
        match self
            .put_create(&self.checkpoint_path(snap.version), data.into())
            .await
        {
            Ok(()) | Err(Error::Storage(object_store::Error::AlreadyExists { .. })) => {}
            Err(e) => return Err(e),
        }
        let pointer = serde_json::to_vec(&LastCheckpoint {
            version: snap.version,
        })?;
        self.store
            .put(&self.last_checkpoint_path(), pointer.into())
            .await?;
        Ok(())
    }

    /// Uploads a segment file and returns its manifest entry (not yet committed).
    pub async fn put_segment(
        &self,
        data: Vec<u8>,
        summary: &kseg::SegmentSummary,
    ) -> Result<AddSegment> {
        let rel = segment_rel_path(summary);
        let path = self.path_of(&rel);
        if data.len() > MULTIPART_THRESHOLD {
            let upload = self.store.put_multipart(&path).await?;
            let mut w = WriteMultipart::new_with_chunk_size(upload, MULTIPART_CHUNK);
            w.write(&data);
            w.finish().await?;
        } else {
            self.store.put(&path, data.into()).await?;
        }
        Ok(AddSegment::from_summary(rel, summary))
    }
}

fn backoff(attempt: u32) -> Duration {
    let base = 50u64 * (1 << attempt.min(6));
    Duration::from_millis(base + rand::rng().random_range(0..base))
}

pub fn segment_rel_path(s: &kseg::SegmentSummary) -> String {
    let date = chrono::DateTime::from_timestamp_millis(s.created_at_ms)
        .unwrap_or_default()
        .format("%Y-%m-%d");
    match s.segment_type {
        kseg::SegmentType::Topic => format!(
            "data/topic={}/date={date}/{}.kseg",
            s.topics[0].topic, s.segment_id
        ),
        kseg::SegmentType::Mixed => format!("data/mixed/date={date}/{}.kseg", s.segment_id),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogFileKind {
    Commit,
    Checkpoint,
}

pub(crate) fn parse_log_name(name: &str) -> Option<(u64, LogFileKind)> {
    let (num, rest) = name.split_once('.')?;
    if num.len() != 20 {
        return None;
    }
    let v = num.parse().ok()?;
    match rest {
        "json" => Some((v, LogFileKind::Commit)),
        "checkpoint.json.zst" => Some((v, LogFileKind::Checkpoint)),
        _ => None,
    }
}

pub fn encode_actions(actions: &[Action]) -> Result<Bytes> {
    let mut out = Vec::new();
    for a in actions {
        serde_json::to_writer(&mut out, a)?;
        out.push(b'\n');
    }
    Ok(out.into())
}

pub fn decode_actions(data: &[u8]) -> Result<Vec<Action>> {
    data.split(|b| *b == b'\n')
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
        .map(|l| serde_json::from_slice(l).map_err(Error::from))
        .collect()
}
