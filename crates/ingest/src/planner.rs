//! Decides what to index next (docs/architecture.md §6).
//!
//! A topic becomes *due* when its estimated unindexed metadata reaches
//! `flush_min_bytes` (such topics get tasks, and so L0 files, of their own) or when it has been
//! waiting for `max_flush_age` (small topics are then packed together into one
//! multi-topic task). Due topics are taken whole, so data of a topic stays together.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use regex::Regex;
use table::{OffsetReset, Snapshot};
use uuid::Uuid;

use crate::source::{FetchRange, TopicMeta};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialPosition {
    Earliest,
    Latest,
}

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub include: Vec<Regex>,
    pub exclude: Vec<Regex>,
    pub flush_min_bytes: u64,
    pub max_flush_age: Duration,
    /// Upper bound of the estimated metadata held by one task (the per-worker buffer).
    pub task_max_bytes: u64,
    pub max_records_per_partition: i64,
    pub initial_position: InitialPosition,
    /// Metadata bytes per record assumed before a topic has been observed.
    pub default_record_bytes: f64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            include: vec![Regex::new(".*").unwrap()],
            exclude: vec![
                Regex::new("^__").unwrap(),
                Regex::new("^_schemas$").unwrap(),
            ],
            flush_min_bytes: 16 << 20,
            max_flush_age: Duration::from_secs(30),
            task_max_bytes: 256 << 20,
            max_records_per_partition: 2_000_000,
            initial_position: InitialPosition::Earliest,
            default_record_bytes: 256.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestTask {
    pub id: String,
    pub ranges: Vec<FetchRange>,
    pub estimated_bytes: u64,
}

#[derive(Debug, Default)]
pub struct Plan {
    pub tasks: Vec<IngestTask>,
    pub resets: Vec<OffsetReset>,
}

struct Candidate {
    topic: String,
    avg: f64,
    /// (partition, start, high watermark)
    lagging: Vec<(i32, i64, i64)>,
    estimated: f64,
}

pub struct Planner {
    cfg: PlannerConfig,
    avg_record_bytes: HashMap<String, f64>,
    waiting_since: HashMap<(String, i32), Instant>,
    in_flight: HashSet<(String, i32)>,
}

impl Planner {
    pub fn new(cfg: PlannerConfig) -> Self {
        Self {
            cfg,
            avg_record_bytes: HashMap::new(),
            waiting_since: HashMap::new(),
            in_flight: HashSet::new(),
        }
    }

    pub fn config(&self) -> &PlannerConfig {
        &self.cfg
    }

    pub fn topic_selected(&self, topic: &str) -> bool {
        self.cfg.include.iter().any(|r| r.is_match(topic))
            && !self.cfg.exclude.iter().any(|r| r.is_match(topic))
    }

    pub fn plan(&mut self, snap: &Snapshot, topics: &[TopicMeta], now: Instant) -> Plan {
        let mut plan = Plan::default();
        let mut candidates = Vec::new();
        for t in topics {
            if !self.topic_selected(&t.name) {
                continue;
            }
            let avg = self
                .avg_record_bytes
                .get(&t.name)
                .copied()
                .unwrap_or(self.cfg.default_record_bytes);
            let mut lagging = Vec::new();
            let mut oldest = now;
            for p in &t.partitions {
                let tp = (t.name.clone(), p.partition);
                if self.in_flight.contains(&tp) {
                    continue;
                }
                let reset = |start: Option<i64>, end: i64, reason: &str| OffsetReset {
                    topic: t.name.clone(),
                    topic_uuid: None,
                    partition: p.partition,
                    start,
                    end,
                    reason: reason.into(),
                };
                let start = match snap.committed(&t.name, p.partition) {
                    None => match self.cfg.initial_position {
                        InitialPosition::Earliest => p.log_start,
                        InitialPosition::Latest => {
                            plan.resets.push(reset(
                                None,
                                p.high_watermark,
                                "initial_position_latest",
                            ));
                            continue;
                        }
                    },
                    Some(c) if c < p.log_start => {
                        plan.resets
                            .push(reset(Some(c), p.log_start, "log_start_offset_advanced"));
                        continue;
                    }
                    Some(c) if c > p.high_watermark => {
                        // topic re-created or truncated: start over from its log start
                        plan.resets
                            .push(reset(Some(c), p.log_start, "high_watermark_regressed"));
                        continue;
                    }
                    Some(c) => c,
                };
                if p.high_watermark <= start {
                    self.waiting_since.remove(&tp);
                    continue;
                }
                let since = *self.waiting_since.entry(tp).or_insert(now);
                oldest = oldest.min(since);
                lagging.push((p.partition, start, p.high_watermark));
            }
            if lagging.is_empty() {
                continue;
            }
            let lag: i64 = lagging.iter().map(|(_, s, hw)| hw - s).sum();
            let estimated = lag as f64 * avg;
            let due = estimated >= self.cfg.flush_min_bytes as f64
                || now.duration_since(oldest) >= self.cfg.max_flush_age;
            if due {
                candidates.push(Candidate {
                    topic: t.name.clone(),
                    avg,
                    lagging,
                    estimated,
                });
            }
        }

        candidates.sort_by(|a, b| b.estimated.total_cmp(&a.estimated));
        let budget = self.cfg.task_max_bytes as f64;
        let mut current: Option<IngestTask> = None;
        for c in candidates {
            let hot = c.estimated >= self.cfg.flush_min_bytes as f64;
            let per_partition_cap =
                ((budget / c.avg) as i64).clamp(1, self.cfg.max_records_per_partition);
            let ranges: Vec<(FetchRange, f64)> = c
                .lagging
                .iter()
                .map(|(p, start, hw)| {
                    let end = (*hw).min(start + per_partition_cap);
                    (
                        FetchRange {
                            topic: c.topic.clone(),
                            partition: *p,
                            start: *start,
                            end,
                        },
                        (end - start) as f64 * c.avg,
                    )
                })
                .collect();
            if hot {
                // hot topics get tasks of their own, split by partitions to fit the budget
                let mut task = new_task();
                for (r, est) in ranges {
                    if !task.ranges.is_empty() && (task.estimated_bytes as f64 + est) > budget {
                        plan.tasks.push(std::mem::replace(&mut task, new_task()));
                    }
                    task.estimated_bytes += est as u64;
                    task.ranges.push(r);
                }
                plan.tasks.push(task);
            } else {
                let est: f64 = ranges.iter().map(|(_, e)| e).sum();
                let task = current.get_or_insert_with(new_task);
                if !task.ranges.is_empty() && (task.estimated_bytes as f64 + est) > budget {
                    plan.tasks.push(std::mem::replace(task, new_task()));
                }
                let task = current.as_mut().unwrap();
                task.estimated_bytes += est as u64;
                task.ranges.extend(ranges.into_iter().map(|(r, _)| r));
            }
        }
        plan.tasks.extend(current);
        for t in &plan.tasks {
            for r in &t.ranges {
                self.in_flight.insert((r.topic.clone(), r.partition));
            }
        }
        plan
    }

    /// Releases the task's partitions. `observed` holds (topic, records, metadata bytes)
    /// actually read, used to refine size estimates.
    pub fn task_finished(
        &mut self,
        task: &IngestTask,
        observed: &[(String, u64, u64)],
        now: Instant,
    ) {
        for r in &task.ranges {
            let tp = (r.topic.clone(), r.partition);
            self.in_flight.remove(&tp);
            // whatever is still lagging arrived while the task ran
            if self.waiting_since.contains_key(&tp) {
                self.waiting_since.insert(tp, now);
            }
        }
        for (topic, records, bytes) in observed {
            if *records == 0 {
                continue;
            }
            let sample = *bytes as f64 / *records as f64;
            let avg = self.avg_record_bytes.entry(topic.clone()).or_insert(sample);
            *avg = 0.7 * *avg + 0.3 * sample;
        }
    }
}

fn new_task() -> IngestTask {
    IngestTask {
        id: Uuid::now_v7().to_string(),
        ranges: Vec::new(),
        estimated_bytes: 0,
    }
}
