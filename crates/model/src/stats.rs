//! Per-file statistics that go into the commit log manifest.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Int32Type, Int64Type, TimestampMillisecondType};

use crate::batch::row_headers;
use crate::error::{Error, Result};
use crate::schema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStats {
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicStats {
    pub topic: String,
    pub ts_min: i64,
    pub ts_max: i64,
    pub rows: u64,
    pub partitions: Vec<PartitionStats>,
}

struct TopicAcc {
    ts_min: i64,
    ts_max: i64,
    rows: u64,
    partitions: BTreeMap<i32, PartitionStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileStats {
    pub rows: u64,
    pub topics: Vec<TopicStats>,
    /// Every header key present in the file.
    pub header_keys: Vec<String>,
}

impl FileStats {
    pub fn from_batch(batch: &RecordBatch) -> Result<Self> {
        let col = |n: &str| {
            batch
                .column_by_name(n)
                .ok_or_else(|| Error::Invalid(format!("missing column {n}")))
        };
        let topic = col(schema::TOPIC)?.as_string::<i32>();
        let partition = col(schema::PARTITION)?.as_primitive::<Int32Type>();
        let offset = col(schema::OFFSET)?.as_primitive::<Int64Type>();
        let ts = col(schema::TIMESTAMP)?.as_primitive::<TimestampMillisecondType>();
        let headers = col(schema::HEADERS)?.as_list::<i32>();

        let mut topics: BTreeMap<&str, TopicAcc> = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for i in 0..batch.num_rows() {
            let t = topics.entry(topic.value(i)).or_insert_with(|| TopicAcc {
                ts_min: i64::MAX,
                ts_max: i64::MIN,
                rows: 0,
                partitions: BTreeMap::new(),
            });
            t.ts_min = t.ts_min.min(ts.value(i));
            t.ts_max = t.ts_max.max(ts.value(i));
            t.rows += 1;
            let p = partition.value(i);
            let o = offset.value(i);
            let ps = t.partitions.entry(p).or_insert(PartitionStats {
                partition: p,
                first_offset: o,
                last_offset: o,
                rows: 0,
            });
            ps.first_offset = ps.first_offset.min(o);
            ps.last_offset = ps.last_offset.max(o);
            ps.rows += 1;
            for (k, _) in row_headers(headers, i) {
                if !keys.contains(k) {
                    keys.insert(k.to_owned());
                }
            }
        }
        Ok(Self {
            rows: batch.num_rows() as u64,
            topics: topics
                .into_iter()
                .map(|(t, acc)| TopicStats {
                    topic: t.to_owned(),
                    ts_min: acc.ts_min,
                    ts_max: acc.ts_max,
                    rows: acc.rows,
                    partitions: acc.partitions.into_values().collect(),
                })
                .collect(),
            header_keys: keys.into_iter().collect(),
        })
    }
}
