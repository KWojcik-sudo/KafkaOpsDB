//! Compares KSEG with Parquet (zstd) on synthetic but realistic Kafka metadata.
//!
//! cargo run -p kseg --example compression_report --release [rows]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use kseg::{Header, Record, SegmentType, SegmentWriter, TimestampType, WriterOptions, columns};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn hex(&mut self, n: usize) -> String {
        let mut s = String::with_capacity(n);
        while s.len() < n {
            s.push_str(&format!("{:016x}", self.next()));
        }
        s.truncate(n);
        s
    }
    fn uuid(&mut self) -> String {
        let h = self.hex(32);
        format!(
            "{}-{}-{}-{}-{}",
            &h[..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..]
        )
    }
}

fn generate(rows: usize, partitions: i32) -> Vec<(i32, Vec<Record>)> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let content_types = ["application/json", "application/avro", "text/plain"];
    let sources = [
        "svc-orders",
        "svc-payments",
        "svc-shipping",
        "svc-billing",
        "svc-users",
    ];
    let mut parts: BTreeMap<i32, Vec<Record>> = BTreeMap::new();
    let mut ts = 1_761_379_200_000i64;
    for i in 0..rows {
        let p = (rng.next() % partitions as u64) as i32;
        ts += (rng.next() % 5) as i64;
        let part = parts.entry(p).or_default();
        let offset = part.last().map_or(0, |r| r.offset + 1);
        let mut headers = vec![
            Header::new("correlation-id", rng.uuid()),
            Header::new(
                "traceparent",
                format!("00-{}-{}-01", rng.hex(32), rng.hex(16)),
            ),
            Header::new("content-type", content_types[i % 3 / 2]),
            Header::new("source", sources[(rng.next() % 5) as usize]),
            Header::new("event-type", format!("OrderEvent{}", rng.next() % 10)),
        ];
        if rng.next().is_multiple_of(20) {
            headers.push(Header::new("retry", (rng.next() % 5).to_string()));
        }
        part.push(Record {
            offset,
            timestamp: ts,
            timestamp_type: TimestampType::CreateTime,
            key: Some(rng.uuid().into_bytes()),
            value_size: Some(200 + (rng.next() % 2000) as u32),
            value_schema_id: Some(42),
            headers,
        });
    }
    parts.into_iter().collect()
}

fn raw_size(parts: &[(i32, Vec<Record>)]) -> usize {
    parts
        .iter()
        .flat_map(|(_, r)| r)
        .map(|r| {
            8 + 8
                + 4
                + 4
                + r.key.as_ref().map_or(0, |k| k.len())
                + r.headers
                    .iter()
                    .map(|h| h.key.len() + h.value.as_ref().map_or(0, |v| v.len()))
                    .sum::<usize>()
        })
        .sum()
}

fn parquet_size(parts: &[(i32, Vec<Record>)]) -> usize {
    let header_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Binary, true),
        Field::new("value_size", DataType::Int32, true),
        Field::new("value_schema_id", DataType::Int32, true),
        Field::new(
            "headers",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(header_fields.clone()),
                true,
            ))),
            false,
        ),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema.clone(), Some(props)).unwrap();
    for (p, records) in parts {
        let mut part = Int32Builder::new();
        let mut off = Int64Builder::new();
        let mut ts = Int64Builder::new();
        let mut key = BinaryBuilder::new();
        let mut vs = Int32Builder::new();
        let mut sid = Int32Builder::new();
        let mut headers = ListBuilder::new(StructBuilder::from_fields(header_fields.clone(), 0))
            .with_field(Arc::new(Field::new(
                "item",
                DataType::Struct(header_fields.clone()),
                true,
            )));
        for r in records {
            part.append_value(*p);
            off.append_value(r.offset);
            ts.append_value(r.timestamp);
            key.append_option(r.key.as_deref());
            vs.append_option(r.value_size.map(|v| v as i32));
            sid.append_option(r.value_schema_id);
            let s = headers.values();
            for h in &r.headers {
                s.field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(&h.key);
                s.field_builder::<BinaryBuilder>(1)
                    .unwrap()
                    .append_option(h.value.as_deref());
                s.append(true);
            }
            headers.append(true);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(part.finish()),
            Arc::new(off.finish()),
            Arc::new(ts.finish()),
            Arc::new(key.finish()),
            Arc::new(vs.finish()),
            Arc::new(sid.finish()),
            Arc::new(headers.finish()),
        ];
        writer
            .write(&RecordBatch::try_new(schema.clone(), cols).unwrap())
            .unwrap();
    }
    writer.close().unwrap();
    out.len()
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(500_000);
    let parts = generate(rows, 6);
    let raw = raw_size(&parts);

    let mut w = SegmentWriter::new(SegmentType::Topic, 0, WriterOptions::default());
    for (p, r) in &parts {
        w.add("orders.v1", *p, r.clone()).unwrap();
    }
    let started = std::time::Instant::now();
    let (data, summary) = w.finish(Uuid::now_v7(), 0, "report").unwrap();
    let encode_time = started.elapsed();

    let footer =
        kseg::footer::decode_footer_and_trailer(&data[summary.footer_offset as usize..]).unwrap();
    let mut per_column: BTreeMap<String, u64> = BTreeMap::new();
    for b in &footer.blocks {
        for c in &b.chunks {
            let name = match c.column_id {
                columns::OFFSET => "offset".to_owned(),
                columns::TIMESTAMP => "timestamp".to_owned(),
                columns::TIMESTAMP_TYPE => "timestamp_type".to_owned(),
                columns::KEY => "key".to_owned(),
                columns::VALUE_SIZE => "value_size".to_owned(),
                columns::VALUE_SCHEMA_ID => "value_schema_id".to_owned(),
                columns::HEADER_SHAPE => "header_shape".to_owned(),
                id => format!(
                    "header:{}",
                    footer.header_keys[(id - columns::HEADER_BASE) as usize]
                ),
            };
            *per_column.entry(name).or_default() += c.length;
        }
    }
    let parquet = parquet_size(&parts);
    let per_row = |b: f64| b / rows as f64;

    println!(
        "rows: {rows}, partitions: 6, blocks: {}",
        footer.blocks.len()
    );
    println!("raw metadata         {:>8.1} B/row", per_row(raw as f64));
    println!(
        "parquet (zstd 3)     {:>8.1} B/row",
        per_row(parquet as f64)
    );
    println!(
        "kseg total           {:>8.1} B/row  (encode {:.0} ms, {:.1}x vs parquet)",
        per_row(data.len() as f64),
        encode_time.as_secs_f64() * 1000.0,
        parquet as f64 / data.len() as f64
    );
    println!("  breakdown:");
    for (name, bytes) in &per_column {
        println!("    {name:<24} {:>8.2} B/row", per_row(*bytes as f64));
    }
    println!(
        "    {:<24} {:>8.2} B/row",
        "index section",
        per_row(footer.index_section.1 as f64)
    );
    println!(
        "    {:<24} {:>8.2} B/row",
        "footer",
        per_row(summary.footer_len as f64)
    );
}
