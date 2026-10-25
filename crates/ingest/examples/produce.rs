//! Test data generator.
//!
//! cargo run -p ingest --example produce -- brokers=localhost:9092 topic=orders count=100000 \
//!     corr_step=1 [transactional=true batch=100 abort_every=3] [start=0]
//!
//! Message `i` gets `correlation-id = corr-{start + i * corr_step:08}`, so topics produced
//! with different steps share some correlation ids. With `transactional=true` every
//! `abort_every`-th batch is aborted; those messages must never show up in the index.
//! Prints the number of committed messages.

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

fn main() {
    let args: HashMap<String, String> = std::env::args()
        .skip(1)
        .filter_map(|a| a.split_once('=').map(|(k, v)| (k.to_owned(), v.to_owned())))
        .collect();
    let get = |k: &str, d: &str| args.get(k).cloned().unwrap_or_else(|| d.to_owned());
    let brokers = get("brokers", "localhost:9092");
    let topic = get("topic", "orders");
    let count: u64 = get("count", "10000").parse().unwrap();
    let start: u64 = get("start", "0").parse().unwrap();
    let corr_step: u64 = get("corr_step", "1").parse().unwrap();
    let transactional = get("transactional", "false") == "true";
    let batch: u64 = get("batch", "100").parse().unwrap();
    let abort_every: u64 = get("abort_every", "0").parse().unwrap();

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &brokers)
        .set("linger.ms", "20")
        .set("compression.type", "lz4");
    if transactional {
        cfg.set("transactional.id", format!("kafkaopsdb-produce-{topic}"));
    }
    let producer: BaseProducer = cfg.create().expect("producer");
    let timeout = Duration::from_secs(30);
    if transactional {
        producer
            .init_transactions(timeout)
            .expect("init transactions");
    }

    let sources = ["svc-orders", "svc-payments", "svc-shipping"];
    let mut committed = 0u64;
    let mut i = 0u64;
    let mut batch_no = 0u64;
    while i < count {
        let n = batch.min(count - i);
        if transactional {
            producer.begin_transaction().expect("begin");
        }
        for j in i..i + n {
            let corr = format!("corr-{:08}", start + j * corr_step);
            let key = format!("key-{}", start + j);
            let mut value = Vec::with_capacity(200);
            if j % 3 == 0 {
                // Confluent wire format: magic byte + schema id 17
                value.extend_from_slice(&[0, 0, 0, 0, 17]);
            }
            value.extend_from_slice(
                format!(
                    "{{\"order\":{j},\"pad\":\"{}\"}}",
                    "x".repeat((j % 100) as usize)
                )
                .as_bytes(),
            );
            let mut headers = OwnedHeaders::new()
                .insert(Header {
                    key: "correlation-id",
                    value: Some(corr.as_str()),
                })
                .insert(Header {
                    key: "source",
                    value: Some(sources[(j % 3) as usize]),
                })
                .insert(Header {
                    key: "content-type",
                    value: Some("application/json"),
                });
            if j % 10 == 0 {
                headers = headers
                    .insert(Header {
                        key: "retry",
                        value: Some("1"),
                    })
                    .insert(Header {
                        key: "retry",
                        value: Some("2"),
                    });
            }
            let mut record = BaseRecord::to(&topic)
                .key(&key)
                .payload(&value)
                .headers(headers);
            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((e, r)) => {
                        if !matches!(
                            e,
                            rdkafka::error::KafkaError::MessageProduction(
                                rdkafka::types::RDKafkaErrorCode::QueueFull
                            )
                        ) {
                            panic!("produce failed: {e}");
                        }
                        record = r;
                        producer.poll(Duration::from_millis(50));
                    }
                }
            }
            producer.poll(Duration::ZERO);
        }
        if transactional {
            // delivery reports must be served before the transaction can end
            producer.flush(timeout).expect("flush");
            if abort_every > 0 && batch_no % abort_every == abort_every - 1 {
                producer.abort_transaction(timeout).expect("abort");
            } else {
                producer.commit_transaction(timeout).expect("commit");
                committed += n;
            }
        } else {
            committed += n;
        }
        batch_no += 1;
        i += n;
    }
    producer.flush(timeout).expect("flush");
    println!("{committed}");
}
