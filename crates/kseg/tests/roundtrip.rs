use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use kseg::bloom::hash_value;
use kseg::{
    BytesReader, Header, HeaderProjection, Projection, RangeReader, Record, SegmentReader,
    SegmentType, SegmentWriter, TimestampType, WriterOptions, columns,
};
use proptest::prelude::*;
use uuid::Uuid;

type Parts = Vec<(String, i32, Vec<Record>)>;

fn record_strategy() -> impl Strategy<Value = (i64, Record)> {
    let header_key =
        prop::sample::select(vec!["correlation-id", "source", "retry", "trace", "bin"]);
    let header_value = prop_oneof![
        Just(None),
        "[a-z0-9-]{0,40}".prop_map(|s| Some(s.into_bytes())),
        prop::collection::vec(any::<u8>(), 0..20).prop_map(Some),
        (0u32..5)
            .prop_map(|i| Some(format!("0192f3a4-5b6c-7d8e-9f00-00000000000{i}").into_bytes())),
    ];
    let headers = prop::collection::vec(
        (header_key, header_value).prop_map(|(k, v)| Header {
            key: k.to_owned(),
            value: v,
        }),
        0..5,
    );
    (
        0i64..4,
        -1_000_000i64..1_000_000,
        any::<bool>(),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..24)),
        prop::option::of(0u32..1_000_000),
        prop::option::of(0i32..5),
        headers,
    )
        .prop_map(|(gap, ts, log_append, key, value_size, schema, headers)| {
            (
                gap,
                Record {
                    offset: 0,
                    timestamp: 1_761_379_200_000 + ts,
                    timestamp_type: if log_append {
                        TimestampType::LogAppendTime
                    } else {
                        TimestampType::CreateTime
                    },
                    key,
                    value_size,
                    value_schema_id: schema,
                    headers,
                },
            )
        })
}

fn parts_strategy() -> impl Strategy<Value = Parts> {
    prop::collection::btree_map(
        (
            prop::sample::select(vec!["orders.v1", "payments", "audit"]),
            0i32..3,
        ),
        (0i64..1000, prop::collection::vec(record_strategy(), 1..200)),
        1..5,
    )
    .prop_map(|m| {
        m.into_iter()
            .map(|((topic, partition), (start, recs))| {
                let mut offset = start;
                let records = recs
                    .into_iter()
                    .map(|(gap, mut r)| {
                        offset += 1 + gap;
                        r.offset = offset;
                        r
                    })
                    .collect();
                (topic.to_owned(), partition, records)
            })
            .collect()
    })
}

fn write(
    parts: &Parts,
    segment_type: SegmentType,
    opts: WriterOptions,
) -> (Bytes, kseg::SegmentSummary) {
    let mut w = SegmentWriter::new(segment_type, 0, opts);
    for (t, p, r) in parts {
        w.add(t, *p, r.clone()).unwrap();
    }
    let (data, summary) = w.finish(Uuid::now_v7(), 1, "test").unwrap();
    (Bytes::from(data), summary)
}

async fn read_all(
    data: Bytes,
    footer_offset: u64,
    footer_len: u64,
) -> Vec<(String, i32, Vec<Record>)> {
    let reader = SegmentReader::open(Arc::new(BytesReader(data)), footer_offset, footer_len)
        .await
        .unwrap();
    let mut out: Vec<(String, i32, Vec<Record>)> = Vec::new();
    for i in 0..reader.footer().blocks.len() {
        let b = reader.read_block(i, &Projection::all()).await.unwrap();
        let topic = b.topic().to_owned();
        match out.last_mut() {
            Some((t, p, recs)) if *t == topic && *p == b.partition => recs.extend(b.to_records()),
            _ => out.push((topic, b.partition, b.to_records())),
        }
    }
    out
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn segment_roundtrip(parts in parts_strategy(), small_blocks in any::<bool>()) {
        let opts = WriterOptions {
            max_block_rows: if small_blocks { 7 } else { 65_536 },
            ..Default::default()
        };
        let (data, summary) = write(&parts, SegmentType::Mixed, opts);
        let rows: usize = parts.iter().map(|(_, _, r)| r.len()).sum();
        prop_assert_eq!(summary.rows as usize, rows);
        prop_assert_eq!(summary.size as usize, data.len());
        let got = rt().block_on(read_all(data, summary.footer_offset, summary.footer_len));
        prop_assert_eq!(got, parts);
    }

    #[test]
    fn index_has_no_false_negatives(parts in parts_strategy()) {
        let (data, summary) = write(&parts, SegmentType::Mixed, WriterOptions { max_block_rows: 16, ..Default::default() });
        rt().block_on(async {
            let reader = SegmentReader::open(Arc::new(BytesReader(data)), summary.footer_offset, summary.footer_len).await.unwrap();
            let footer = reader.footer().clone();
            for (key_id, key) in footer.header_keys.iter().enumerate() {
                let col = columns::HEADER_BASE + key_id as u32;
                let index = reader.column_index(col).await.unwrap();
                for (i, block) in footer.blocks.iter().enumerate() {
                    if block.chunk(col).is_none() { continue; }
                    let b = reader.read_block(i, &Projection { headers: HeaderProjection::Keys(vec![key_id as u32]), ..Default::default() }).await.unwrap();
                    for rec in b.to_records() {
                        for h in rec.headers.iter().filter(|h| &h.key == key) {
                            if let (Some(v), Some(index)) = (&h.value, &index) {
                                assert!(index.might_contain(i as u32, hash_value(v)).unwrap());
                                let rows = b.rows_where_header(key_id as u32, |x| x == Some(v.as_slice()));
                                assert!(!rows.is_empty());
                            }
                        }
                    }
                }
            }
        });
    }
}

struct CountingReader {
    inner: BytesReader,
    reads: AtomicU64,
    bytes: AtomicU64,
}

#[async_trait]
impl RangeReader for CountingReader {
    async fn read_range(&self, range: Range<u64>) -> kseg::Result<Bytes> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(range.end - range.start, Ordering::Relaxed);
        self.inner.read_range(range).await
    }
}

fn realistic(n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            offset: 1000 + i as i64,
            timestamp: 1_761_379_200_000 + (i as i64) * 3,
            timestamp_type: TimestampType::CreateTime,
            key: Some(format!("0192f3a4-5b6c-7d8e-9f00-{i:012x}").into_bytes()),
            value_size: Some(500 + (i % 300) as u32),
            value_schema_id: Some(17),
            headers: vec![
                Header::new(
                    "correlation-id",
                    format!("{:08x}-aaaa-bbbb-cccc-{i:012x}", i * 7),
                ),
                Header::new("source", "svc-orders"),
                Header::new("content-type", "application/json"),
            ],
        })
        .collect()
}

#[tokio::test]
async fn large_partition_and_selective_reads() {
    let records = realistic(150_000);
    let mut w = SegmentWriter::new(SegmentType::Topic, 0, WriterOptions::default());
    w.add("orders.v1", 0, records.clone()).unwrap();
    let (data, summary) = w.finish(Uuid::now_v7(), 1, "test").unwrap();
    let total = data.len() as u64;
    assert_eq!(summary.topics[0].partitions[0].rows, 150_000);

    let counting = Arc::new(CountingReader {
        inner: BytesReader(Bytes::from(data)),
        reads: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
    });
    let reader = SegmentReader::open(counting.clone(), summary.footer_offset, summary.footer_len)
        .await
        .unwrap();
    let nblocks = reader.footer().blocks.len();
    assert!(nblocks >= 3, "150k rows must span several blocks");

    let key_id = reader.footer().header_key_id("correlation-id").unwrap();
    let needle = format!("{:08x}-aaaa-bbbb-cccc-{:012x}", 123_456 * 7, 123_456);
    let h = hash_value(needle.as_bytes());
    let index = reader
        .column_index(columns::HEADER_BASE + key_id)
        .await
        .unwrap()
        .unwrap();
    let candidates: Vec<usize> = (0..nblocks)
        .filter(|b| index.might_contain(*b as u32, h).unwrap())
        .collect();
    assert!(
        candidates.len() <= 2,
        "bloom should prune most blocks: {candidates:?}"
    );
    let mut found = Vec::new();
    for b in candidates {
        let block = reader
            .read_block(
                b,
                &Projection {
                    headers: HeaderProjection::Keys(vec![key_id]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        for row in block.rows_where_header(key_id, |v| v == Some(needle.as_bytes())) {
            found.push(block.offsets[row]);
        }
    }
    assert_eq!(found, vec![1000 + 123_456]);
    let read = counting.bytes.load(Ordering::Relaxed);
    assert!(
        read < total / 2,
        "lookup read {read} of {total} bytes; expected only the needed columns"
    );
    let bpr = total as f64 / 150_000.0;
    assert!(bpr < 60.0, "{bpr:.1} bytes per row");
}

#[tokio::test]
async fn open_with_size_and_validation() {
    let mut w = SegmentWriter::new(SegmentType::Topic, 0, WriterOptions::default());
    w.add("a", 0, realistic(10)).unwrap();
    assert!(
        w.add("a", 0, realistic(1)).is_err(),
        "offset regression must be rejected"
    );
    w.add("b", 0, realistic(10)).unwrap();
    assert!(
        w.finish(Uuid::now_v7(), 0, "t").is_err(),
        "topic segment with 2 topics"
    );

    let mut w = SegmentWriter::new(SegmentType::Topic, 0, WriterOptions::default());
    w.add("a", 0, realistic(10)).unwrap();
    let (data, _) = w.finish(Uuid::now_v7(), 0, "t").unwrap();
    let size = data.len() as u64;
    let mut corrupted = data.clone();
    let reader = SegmentReader::open_with_size(Arc::new(BytesReader(Bytes::from(data))), size)
        .await
        .unwrap();
    assert_eq!(reader.footer().topics, vec!["a".to_owned()]);
    assert_eq!(
        reader
            .read_block(0, &Projection::all())
            .await
            .unwrap()
            .to_records(),
        realistic(10)
    );

    corrupted[20] ^= 0xff;
    let reader = SegmentReader::open_with_size(Arc::new(BytesReader(Bytes::from(corrupted))), size)
        .await
        .unwrap();
    assert!(reader.read_block(0, &Projection::all()).await.is_err());
}
