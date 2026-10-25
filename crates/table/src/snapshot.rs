//! Table state at one log version, built by folding commit actions.

use std::collections::{BTreeMap, HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::actions::{
    Action, AddSegment, CommitInfo, Operation, Protocol, READER_VERSION, RemoveSegment, TableMeta,
    WRITER_VERSION,
};
use crate::error::{Error, Result};

/// How many recent commits are remembered for txn/task deduplication.
pub const RECENT_COMMITS: usize = 10_000;

pub type TopicPartition = (String, i32);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tombstone {
    pub segment_id: Uuid,
    pub path: String,
    pub deletion_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecentCommit {
    pub version: u64,
    pub txn_id: Uuid,
    pub task_id: Option<String>,
    pub operation: Operation,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Snapshot {
    pub version: u64,
    pub protocol: Option<Protocol>,
    pub table: Option<TableMeta>,
    pub segments: BTreeMap<Uuid, AddSegment>,
    pub tombstones: BTreeMap<Uuid, Tombstone>,
    /// Next offset to read per (topic, partition).
    pub offsets: BTreeMap<TopicPartition, i64>,
    pub topic_uuids: BTreeMap<String, String>,
    recent: VecDeque<RecentCommit>,
    txns: HashMap<Uuid, u64>,
    tasks: HashMap<String, u64>,
    initialized: bool,
}

impl Snapshot {
    pub fn committed(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets.get(&(topic.to_owned(), partition)).copied()
    }

    pub fn version_of_txn(&self, txn: &Uuid) -> Option<u64> {
        self.txns.get(txn).copied()
    }

    pub fn version_of_task(&self, task_id: &str) -> Option<u64> {
        self.tasks.get(task_id).copied()
    }

    pub fn recent_commits(&self) -> impl Iterator<Item = &RecentCommit> {
        self.recent.iter()
    }

    pub fn segments_of_topic<'a>(&'a self, topic: &'a str) -> impl Iterator<Item = &'a AddSegment> {
        self.segments
            .values()
            .filter(move |s| s.topic(topic).is_some())
    }

    /// Checks commit preconditions against this state (docs/commit-protocol.md §5).
    pub fn validate(&self, task_id: Option<&str>, actions: &[Action]) -> Result<()> {
        if let Some(task) = task_id
            && let Some(v) = self.version_of_task(task)
        {
            return Err(Error::Conflict(format!(
                "task {task} already committed in version {v}"
            )));
        }
        let mut offsets: HashMap<TopicPartition, Option<i64>> = HashMap::new();
        let mut removed = std::collections::HashSet::new();
        for a in actions {
            match a {
                Action::OffsetRange(r) => {
                    if r.end <= r.start {
                        return Err(Error::Conflict(format!(
                            "empty offset range {}/{} [{}, {})",
                            r.topic, r.partition, r.start, r.end
                        )));
                    }
                    let key = (r.topic.clone(), r.partition);
                    let current = *offsets
                        .entry(key.clone())
                        .or_insert_with(|| self.offsets.get(&key).copied());
                    if let Some(c) = current
                        && c != r.start
                    {
                        return Err(Error::Conflict(format!(
                            "{}/{}: committed offset is {c}, commit starts at {}",
                            r.topic, r.partition, r.start
                        )));
                    }
                    offsets.insert(key, Some(r.end));
                }
                Action::OffsetReset(r) => {
                    let key = (r.topic.clone(), r.partition);
                    let current = *offsets
                        .entry(key.clone())
                        .or_insert_with(|| self.offsets.get(&key).copied());
                    if current != r.start {
                        return Err(Error::Conflict(format!(
                            "{}/{}: committed offset is {current:?}, reset expects {:?}",
                            r.topic, r.partition, r.start
                        )));
                    }
                    offsets.insert(key, Some(r.end));
                }
                Action::Remove(r) => {
                    if !self.segments.contains_key(&r.segment_id) || !removed.insert(r.segment_id) {
                        return Err(Error::Conflict(format!(
                            "segment {} is no longer live",
                            r.segment_id
                        )));
                    }
                }
                Action::Add(a) => {
                    if self.segments.contains_key(&a.segment_id) {
                        return Err(Error::Conflict(format!(
                            "segment {} already exists",
                            a.segment_id
                        )));
                    }
                }
                Action::Protocol(p) => {
                    if p.min_writer_version > WRITER_VERSION {
                        return Err(Error::Protocol(format!(
                            "writer version {} required",
                            p.min_writer_version
                        )));
                    }
                }
                Action::Table(_) | Action::CommitInfo(_) => {}
            }
        }
        Ok(())
    }

    /// Applies the actions of log version `version`, which must directly follow this one
    /// (or be version 0 on an empty snapshot).
    pub fn apply(&mut self, version: u64, actions: &[Action]) -> Result<()> {
        let expected = if self.initialized {
            self.version + 1
        } else {
            0
        };
        if version != expected {
            return Err(Error::Corrupt(format!(
                "log version {version} applied on top of {}",
                self.version
            )));
        }
        let mut info: Option<&CommitInfo> = None;
        for a in actions {
            match a {
                Action::Protocol(p) => {
                    if p.min_reader_version > READER_VERSION {
                        return Err(Error::Protocol(format!(
                            "reader version {} required",
                            p.min_reader_version
                        )));
                    }
                    self.protocol = Some(p.clone());
                }
                Action::Table(t) => self.table = Some(t.clone()),
                Action::CommitInfo(c) => info = Some(c),
                Action::Add(s) => {
                    self.segments.insert(s.segment_id, s.clone());
                }
                Action::Remove(RemoveSegment {
                    segment_id,
                    deletion_timestamp,
                }) => {
                    if let Some(s) = self.segments.remove(segment_id) {
                        self.tombstones.insert(
                            *segment_id,
                            Tombstone {
                                segment_id: *segment_id,
                                path: s.path,
                                deletion_timestamp: *deletion_timestamp,
                            },
                        );
                    }
                }
                Action::OffsetRange(r) => {
                    let key = (r.topic.clone(), r.partition);
                    if let Some(c) = self.offsets.get(&key)
                        && *c != r.start
                    {
                        return Err(Error::Corrupt(format!(
                            "version {version}: {}/{} range starts at {} but committed is {c}",
                            r.topic, r.partition, r.start
                        )));
                    }
                    self.offsets.insert(key, r.end);
                    if let Some(u) = &r.topic_uuid {
                        self.topic_uuids.insert(r.topic.clone(), u.clone());
                    }
                }
                Action::OffsetReset(r) => {
                    self.offsets.insert((r.topic.clone(), r.partition), r.end);
                    if let Some(u) = &r.topic_uuid {
                        self.topic_uuids.insert(r.topic.clone(), u.clone());
                    }
                }
            }
        }
        if let Some(c) = info {
            self.remember(RecentCommit {
                version,
                txn_id: c.txn_id,
                task_id: c.task_id.clone(),
                operation: c.operation,
                timestamp: c.timestamp,
            });
        }
        self.version = version;
        self.initialized = true;
        Ok(())
    }

    fn remember(&mut self, c: RecentCommit) {
        self.txns.insert(c.txn_id, c.version);
        if let Some(t) = &c.task_id {
            self.tasks.insert(t.clone(), c.version);
        }
        self.recent.push_back(c);
        while self.recent.len() > RECENT_COMMITS {
            if let Some(old) = self.recent.pop_front() {
                self.txns.remove(&old.txn_id);
                if let Some(t) = old.task_id {
                    self.tasks.remove(&t);
                }
            }
        }
    }

    /// Drops tombstones older than `cutoff_ms` (their files are long gone).
    pub fn prune_tombstones(&mut self, cutoff_ms: i64) {
        self.tombstones
            .retain(|_, t| t.deletion_timestamp >= cutoff_ms);
    }

    pub fn to_checkpoint(&self) -> CheckpointData {
        CheckpointData {
            version: self.version,
            protocol: self.protocol.clone(),
            table: self.table.clone(),
            segments: self.segments.values().cloned().collect(),
            tombstones: self.tombstones.values().cloned().collect(),
            offsets: self
                .offsets
                .iter()
                .map(|((topic, partition), next)| OffsetEntry {
                    topic: topic.clone(),
                    partition: *partition,
                    next_offset: *next,
                })
                .collect(),
            topic_uuids: self.topic_uuids.clone(),
            recent: self.recent.iter().cloned().collect(),
        }
    }

    pub fn from_checkpoint(c: CheckpointData) -> Self {
        let mut s = Snapshot {
            version: c.version,
            protocol: c.protocol,
            table: c.table,
            segments: c.segments.into_iter().map(|a| (a.segment_id, a)).collect(),
            tombstones: c
                .tombstones
                .into_iter()
                .map(|t| (t.segment_id, t))
                .collect(),
            offsets: c
                .offsets
                .into_iter()
                .map(|o| ((o.topic, o.partition), o.next_offset))
                .collect(),
            topic_uuids: c.topic_uuids,
            initialized: true,
            ..Default::default()
        };
        for r in c.recent {
            s.remember(r);
        }
        s
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetEntry {
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    pub version: u64,
    pub protocol: Option<Protocol>,
    pub table: Option<TableMeta>,
    pub segments: Vec<AddSegment>,
    pub tombstones: Vec<Tombstone>,
    pub offsets: Vec<OffsetEntry>,
    #[serde(default)]
    pub topic_uuids: BTreeMap<String, String>,
    #[serde(default)]
    pub recent: Vec<RecentCommit>,
}
