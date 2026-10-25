//! Kafka source on librdkafka.
//!
//! Offsets are managed by the table, so consumers only `assign()` explicit ranges and never
//! join a group or commit. Messages are polled through the native API because the
//! partition-EOF event carries the consumer's fetch position: under `read_committed` that is
//! past transaction markers and aborted records (and never beyond the LSO), which is exactly
//! the offset up to which a range is complete.

use std::collections::{BTreeMap, HashMap};
use std::ffi::CStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kseg::{Header, Record, TimestampType, confluent_schema_id};
use rdkafka::bindings as rd;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::source::{FetchRange, FetchedPartition, KafkaSource, PartitionMeta, TopicMeta};

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    /// Passed verbatim to librdkafka (security.protocol, sasl.*, ssl.* ...).
    pub properties: BTreeMap<String, String>,
    pub metadata_timeout: Duration,
    pub fetch_timeout: Duration,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".into(),
            properties: BTreeMap::new(),
            metadata_timeout: Duration::from_secs(10),
            fetch_timeout: Duration::from_secs(30),
        }
    }
}

pub struct RdKafkaSource {
    cfg: KafkaConfig,
    metadata: Arc<BaseConsumer>,
}

impl RdKafkaSource {
    pub fn new(cfg: KafkaConfig) -> Result<Self> {
        let metadata = Arc::new(
            client_config(&cfg)
                .create::<BaseConsumer>()
                .map_err(kafka_err)?,
        );
        Ok(Self { cfg, metadata })
    }
}

fn kafka_err(e: rdkafka::error::KafkaError) -> Error {
    Error::Kafka(e.to_string())
}

fn client_config(cfg: &KafkaConfig) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.bootstrap_servers)
        .set("group.id", "kafkaopsdb-indexer")
        .set("client.id", "kafkaopsdb")
        .set("isolation.level", "read_committed")
        .set("auto.offset.reset", "error")
        .set("fetch.max.bytes", "52428800")
        .set("queued.max.messages.kbytes", "262144");
    for (k, v) in &cfg.properties {
        c.set(k, v);
    }
    // offsets live in the table: never commit, always report partition EOF
    c.set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true");
    c
}

#[async_trait]
impl KafkaSource for RdKafkaSource {
    async fn list_topics(&self) -> Result<Vec<TopicMeta>> {
        let consumer = self.metadata.clone();
        let timeout = self.cfg.metadata_timeout;
        tokio::task::spawn_blocking(move || {
            let md = consumer.fetch_metadata(None, timeout).map_err(kafka_err)?;
            let mut out = Vec::new();
            for t in md.topics() {
                if t.error().is_some() {
                    continue;
                }
                let mut partitions = Vec::with_capacity(t.partitions().len());
                for p in t.partitions() {
                    let (low, high) = consumer
                        .fetch_watermarks(t.name(), p.id(), timeout)
                        .map_err(kafka_err)?;
                    partitions.push(PartitionMeta {
                        partition: p.id(),
                        log_start: low,
                        high_watermark: high,
                    });
                }
                partitions.sort_by_key(|p| p.partition);
                out.push(TopicMeta {
                    name: t.name().to_owned(),
                    partitions,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Join(e.to_string()))?
    }

    async fn fetch(&self, ranges: &[FetchRange]) -> Result<Vec<FetchedPartition>> {
        let cfg = self.cfg.clone();
        let ranges = ranges.to_vec();
        tokio::task::spawn_blocking(move || {
            // a fresh consumer per fetch: no stale messages or EOF events from earlier assignments
            let consumer = client_config(&cfg)
                .create::<BaseConsumer>()
                .map_err(kafka_err)?;
            let result = fetch_blocking(&consumer, &ranges, cfg.fetch_timeout);
            let _ = consumer.unassign();
            result
        })
        .await
        .map_err(|e| Error::Join(e.to_string()))?
    }
}

struct Progress {
    start: i64,
    end: i64,
    next: i64,
    done: bool,
    records: Vec<Record>,
}

struct MessageGuard(*mut rd::rd_kafka_message_t);

impl Drop for MessageGuard {
    fn drop(&mut self) {
        unsafe { rd::rd_kafka_message_destroy(self.0) }
    }
}

fn fetch_blocking(
    consumer: &BaseConsumer,
    ranges: &[FetchRange],
    timeout: Duration,
) -> Result<Vec<FetchedPartition>> {
    let mut progress: HashMap<(String, i32), Progress> = HashMap::new();
    let mut tpl = TopicPartitionList::new();
    for r in ranges {
        let done = r.start >= r.end;
        progress.insert(
            (r.topic.clone(), r.partition),
            Progress {
                start: r.start,
                end: r.end,
                next: r.start,
                done,
                records: Vec::new(),
            },
        );
        if !done {
            tpl.add_partition_offset(&r.topic, r.partition, Offset::Offset(r.start))
                .map_err(kafka_err)?;
        }
    }
    consumer.assign(&tpl).map_err(kafka_err)?;

    let rk = consumer.client().native_ptr();
    let deadline = Instant::now() + timeout;
    let mut pending = progress.values().filter(|p| !p.done).count();
    while pending > 0 && Instant::now() < deadline {
        let msg = unsafe { rd::rd_kafka_consumer_poll(rk, 100) };
        if msg.is_null() {
            continue;
        }
        let guard = MessageGuard(msg);
        let m = unsafe { &*guard.0 };
        let topic = if m.rkt.is_null() {
            None
        } else {
            unsafe { CStr::from_ptr(rd::rd_kafka_topic_name(m.rkt)) }
                .to_str()
                .ok()
                .map(str::to_owned)
        };
        let key = topic.clone().map(|t| (t, m.partition));
        let entry = key.as_ref().and_then(|k| progress.get_mut(k));
        use rd::rd_kafka_resp_err_t as E;
        match (m.err, entry) {
            (E::RD_KAFKA_RESP_ERR_NO_ERROR, Some(p)) if !p.done => {
                if m.offset < p.next {
                    continue;
                }
                if m.offset >= p.end {
                    p.next = p.end;
                } else {
                    p.records.push(unsafe { convert(guard.0) });
                    p.next = m.offset + 1;
                }
                if p.next >= p.end {
                    p.done = true;
                    pending -= 1;
                }
            }
            (E::RD_KAFKA_RESP_ERR__PARTITION_EOF, Some(p)) if !p.done => {
                // the fetch position: everything below it was delivered or is not a record
                if m.offset > p.next {
                    p.next = m.offset.min(p.end);
                }
                p.done = true;
                pending -= 1;
            }
            (
                E::RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE | E::RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET,
                _,
            ) => {
                let (topic, partition) = key.unwrap_or_default();
                let requested = progress
                    .get(&(topic.clone(), partition))
                    .map_or(-1, |p| p.start);
                return Err(Error::OffsetOutOfRange {
                    topic,
                    partition,
                    requested,
                });
            }
            (E::RD_KAFKA_RESP_ERR_NO_ERROR | E::RD_KAFKA_RESP_ERR__PARTITION_EOF, _) => {}
            (err, _) => {
                let reason = unsafe { CStr::from_ptr(rd::rd_kafka_err2str(err)) }.to_string_lossy();
                warn!(?topic, partition = m.partition, %reason, "consumer error");
            }
        }
    }
    if pending > 0 {
        debug!(pending, "fetch deadline reached; committing partial ranges");
    }
    Ok(ranges
        .iter()
        .map(|r| {
            let p = progress.remove(&(r.topic.clone(), r.partition)).unwrap();
            FetchedPartition {
                topic: r.topic.clone(),
                partition: r.partition,
                start: p.start,
                next_offset: p.next,
                records: p.records,
            }
        })
        .collect())
}

/// # Safety
/// `msg` must be a valid, non-error message returned by `rd_kafka_consumer_poll`.
unsafe fn convert(msg: *mut rd::rd_kafka_message_t) -> Record {
    unsafe {
        let m = &*msg;
        let slice = |p: *mut std::ffi::c_void, len: usize| -> Option<&[u8]> {
            if p.is_null() {
                None
            } else {
                Some(std::slice::from_raw_parts(p as *const u8, len))
            }
        };
        let key = slice(m.key, m.key_len).map(<[u8]>::to_vec);
        let value = slice(m.payload, m.len);
        let mut tstype = rd::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let ts = rd::rd_kafka_message_timestamp(msg, &mut tstype);
        let timestamp_type = match tstype {
            rd::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                TimestampType::LogAppendTime
            }
            _ => TimestampType::CreateTime,
        };
        let mut headers = Vec::new();
        let mut hdrs: *mut rd::rd_kafka_headers_t = std::ptr::null_mut();
        if rd::rd_kafka_message_headers(msg, &mut hdrs)
            == rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
        {
            let n = rd::rd_kafka_header_cnt(hdrs);
            for i in 0..n {
                let mut name: *const std::ffi::c_char = std::ptr::null();
                let mut val: *const std::ffi::c_void = std::ptr::null();
                let mut size: usize = 0;
                if rd::rd_kafka_header_get_all(hdrs, i, &mut name, &mut val, &mut size)
                    == rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                {
                    headers.push(Header {
                        key: CStr::from_ptr(name).to_string_lossy().into_owned(),
                        value: slice(val as *mut _, size).map(<[u8]>::to_vec),
                    });
                }
            }
        }
        Record {
            offset: m.offset,
            timestamp: ts,
            timestamp_type,
            key,
            value_size: value.map(|v| v.len() as u32),
            value_schema_id: value.and_then(confluent_schema_id),
            headers,
        }
    }
}
