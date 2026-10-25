use bytes::Bytes;
use model::{
    FileFormat, FileStats, Header, ParquetOptions, PivotConfig, Record, TimestampType,
    batch_to_records, choose_pivot_keys, concat_base, read_file, records_to_batch, sort_batch,
    split_by_topic, with_pivots, write_avro, write_parquet,
};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use proptest::prelude::*;

type Parts = Vec<(String, i32, Vec<Record>)>;

fn record_strategy() -> impl Strategy<Value = Record> {
    let header_key = prop::sample::select(vec!["correlation-id", "source", "retry", "bin"]);
    let header_value = prop_oneof![
        Just(None),
        "[a-z0-9-]{0,40}".prop_map(|s| Some(s.into_bytes())),
        prop::collection::vec(any::<u8>(), 0..20).prop_map(Some),
    ];
    (
        -1_000_000i64..1_000_000,
        any::<bool>(),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..24)),
        prop::option::of(0u32..1_000_000),
        prop::option::of(0i32..5),
        prop::collection::vec(
            (header_key, header_value).prop_map(|(k, v)| Header {
                key: k.to_owned(),
                value: v,
            }),
            0..5,
        ),
    )
        .prop_map(
            |(ts, log_append, key, value_size, schema, headers)| Record {
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
}

fn parts_strategy() -> impl Strategy<Value = Parts> {
    prop::collection::btree_map(
        (
            prop::sample::select(vec!["orders.v1", "payments", "audit"]),
            0i32..3,
        ),
        prop::collection::vec(record_strategy(), 1..100),
        1..5,
    )
    .prop_map(|m| {
        m.into_iter()
            .map(|((t, p), recs)| {
                let recs = recs
                    .into_iter()
                    .enumerate()
                    .map(|(i, mut r)| {
                        r.offset = 10 + 2 * i as i64;
                        r
                    })
                    .collect();
                (t.to_owned(), p, recs)
            })
            .collect()
    })
}

fn flatten(parts: &Parts) -> Vec<(String, i32, Record)> {
    parts
        .iter()
        .flat_map(|(t, p, rs)| rs.iter().map(move |r| (t.clone(), *p, r.clone())))
        .collect()
}

fn batch_of(parts: &Parts) -> arrow::array::RecordBatch {
    records_to_batch(parts.iter().map(|(t, p, r)| (t.as_str(), *p, r.as_slice()))).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn avro_and_parquet_roundtrip(parts in parts_strategy()) {
        let batch = batch_of(&parts);
        let expected = flatten(&parts);
        prop_assert_eq!(batch_to_records(&batch).unwrap(), expected.clone());

        let avro = write_avro(&batch).unwrap();
        let back = concat_base(&read_file(FileFormat::Avro, Bytes::from(avro)).unwrap()).unwrap();
        prop_assert_eq!(batch_to_records(&back).unwrap(), expected.clone());

        let keys = choose_pivot_keys(&batch, &PivotConfig::default()).unwrap();
        let pq = write_parquet(&with_pivots(&batch, &keys).unwrap(), &ParquetOptions::l0()).unwrap();
        let back = concat_base(&read_file(FileFormat::Parquet, Bytes::from(pq)).unwrap()).unwrap();
        prop_assert_eq!(batch_to_records(&back).unwrap(), expected);
    }
}

fn rec(offset: i64, headers: Vec<Header>) -> Record {
    Record {
        offset,
        timestamp: 1000 + offset,
        key: Some(format!("k{offset}").into_bytes()),
        headers,
        ..Default::default()
    }
}

#[test]
fn pivots_hold_first_value_and_get_bloom_filters() {
    let records = vec![
        rec(
            0,
            vec![
                Header::new("correlation-id", "a"),
                Header::new("retry", "1"),
                Header::new("retry", "2"),
            ],
        ),
        rec(1, vec![Header::new("source", "svc")]),
        rec(
            2,
            vec![
                Header {
                    key: "correlation-id".into(),
                    value: None,
                },
                Header::new("correlation-id", "b"),
            ],
        ),
    ];
    let batch = records_to_batch([("t", 0, records.as_slice())]).unwrap();
    let keys = choose_pivot_keys(
        &batch,
        &PivotConfig {
            min_fraction: 0.5,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(keys, vec!["correlation-id".to_owned()]);
    let all = choose_pivot_keys(&batch, &PivotConfig::default()).unwrap();
    assert_eq!(all, vec!["correlation-id", "retry", "source"]);

    let pivoted = with_pivots(&batch, &all).unwrap();
    use arrow::array::{Array, AsArray};
    let corr = pivoted
        .column_by_name("__h:correlation-id")
        .unwrap()
        .as_binary::<i32>();
    assert_eq!(corr.value(0), b"a");
    assert!(corr.is_null(1));
    assert!(corr.is_null(2), "the first occurrence is a null value");
    let retry = pivoted
        .column_by_name("__h:retry")
        .unwrap()
        .as_binary::<i32>();
    assert_eq!(retry.value(0), b"1");

    let data = write_parquet(&pivoted, &ParquetOptions::compacted()).unwrap();
    let opts = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    let reader = SerializedFileReader::new_with_options(Bytes::from(data), opts).unwrap();
    let rg = reader.get_row_group(0).unwrap();
    let meta = reader.metadata().row_group(0);
    let bloom_cols: Vec<String> = (0..meta.num_columns())
        .filter(|i| rg.get_column_bloom_filter(*i).is_some())
        .map(|i| meta.column(i).column_path().string())
        .collect();
    assert_eq!(
        bloom_cols,
        vec!["key", "__h:correlation-id", "__h:retry", "__h:source"]
    );
}

#[test]
fn stats_sort_and_split() {
    let a: Vec<Record> = (0..5)
        .map(|i| rec(100 + i, vec![Header::new("x", "1")]))
        .collect();
    let b: Vec<Record> = (0..3).map(|i| rec(7 + i, vec![])).collect();
    let batch = records_to_batch([
        ("zeta", 1, a.as_slice()),
        ("alpha", 0, b.as_slice()),
        ("zeta", 0, b.as_slice()),
    ])
    .unwrap();
    let stats = FileStats::from_batch(&batch).unwrap();
    assert_eq!(stats.rows, 11);
    assert_eq!(stats.header_keys, vec!["x"]);
    assert_eq!(stats.topics[0].topic, "alpha");
    let zeta = &stats.topics[1];
    assert_eq!(zeta.partitions.len(), 2);
    assert_eq!(zeta.partitions[1].first_offset, 100);
    assert_eq!(zeta.partitions[1].last_offset, 104);
    assert_eq!(zeta.ts_min, 1007);

    let sorted = sort_batch(&batch).unwrap();
    let split = split_by_topic(&sorted).unwrap();
    assert_eq!(split.keys().collect::<Vec<_>>(), vec!["alpha", "zeta"]);
    let z = batch_to_records(&split["zeta"]).unwrap();
    assert_eq!(
        z.iter().map(|(_, p, r)| (*p, r.offset)).collect::<Vec<_>>(),
        vec![
            (0, 7),
            (0, 8),
            (0, 9),
            (1, 100),
            (1, 101),
            (1, 102),
            (1, 103),
            (1, 104)
        ]
    );
    assert!(
        split_by_topic(&batch).is_err(),
        "unsorted input is rejected"
    );
}
