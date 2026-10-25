//! Conversions between records and Arrow batches, sorting, splitting and header pivots.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, Int32Builder, Int64Builder, ListArray, ListBuilder,
    RecordBatch, StringBuilder, StructArray, StructBuilder, TimestampMillisecondBuilder,
    new_null_array,
};
use arrow::compute::{SortColumn, cast, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{Int32Type, Int64Type, SchemaRef, TimestampMillisecondType};

use crate::error::{Error, Result};
use crate::record::{Header, Record, TimestampType};
use crate::schema::{self, base_schema, header_fields, header_item_field, pivot_column};

/// Builds a base-schema batch from records of several partitions.
pub fn records_to_batch<'a>(
    parts: impl IntoIterator<Item = (&'a str, i32, &'a [Record])>,
) -> Result<RecordBatch> {
    let mut topic = StringBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();
    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut ts_type = StringBuilder::new();
    let mut key = BinaryBuilder::new();
    let mut value_size = Int32Builder::new();
    let mut schema_id = Int32Builder::new();
    let mut headers = ListBuilder::new(StructBuilder::from_fields(header_fields(), 0))
        .with_field(header_item_field());
    for (t, p, records) in parts {
        for r in records {
            topic.append_value(t);
            partition.append_value(p);
            offset.append_value(r.offset);
            ts.append_value(r.timestamp);
            ts_type.append_value(r.timestamp_type.as_str());
            key.append_option(r.key.as_deref());
            value_size.append_option(r.value_size.map(|v| v as i32));
            schema_id.append_option(r.value_schema_id);
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
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(topic.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
        Arc::new(ts.finish()),
        Arc::new(ts_type.finish()),
        Arc::new(key.finish()),
        Arc::new(value_size.finish()),
        Arc::new(schema_id.finish()),
        Arc::new(headers.finish()),
    ];
    Ok(RecordBatch::try_new(base_schema(), columns)?)
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::Invalid(format!("missing column {name}")))
}

/// Header entries of one row of a `headers` column: (key, value).
pub fn row_headers(headers: &ListArray, row: usize) -> Vec<(&str, Option<&[u8]>)> {
    let entries: &StructArray = headers.values().as_struct();
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1).as_binary::<i32>();
    let offsets = headers.value_offsets();
    (offsets[row] as usize..offsets[row + 1] as usize)
        .map(|i| (keys.value(i), (!values.is_null(i)).then(|| values.value(i))))
        .collect()
}

/// Converts a batch (base schema, extra columns ignored) back into records.
pub fn batch_to_records(batch: &RecordBatch) -> Result<Vec<(String, i32, Record)>> {
    let topic = column(batch, schema::TOPIC)?.as_string::<i32>();
    let partition = column(batch, schema::PARTITION)?.as_primitive::<Int32Type>();
    let offset = column(batch, schema::OFFSET)?.as_primitive::<Int64Type>();
    let ts = column(batch, schema::TIMESTAMP)?.as_primitive::<TimestampMillisecondType>();
    let ts_type = column(batch, schema::TIMESTAMP_TYPE)?.as_string::<i32>();
    let key = column(batch, schema::KEY)?.as_binary::<i32>();
    let value_size = column(batch, schema::VALUE_SIZE)?.as_primitive::<Int32Type>();
    let schema_id = column(batch, schema::VALUE_SCHEMA_ID)?.as_primitive::<Int32Type>();
    let headers = column(batch, schema::HEADERS)?.as_list::<i32>();
    Ok((0..batch.num_rows())
        .map(|i| {
            (
                topic.value(i).to_owned(),
                partition.value(i),
                Record {
                    offset: offset.value(i),
                    timestamp: ts.value(i),
                    timestamp_type: TimestampType::parse(ts_type.value(i)),
                    key: (!key.is_null(i)).then(|| key.value(i).to_vec()),
                    value_size: (!value_size.is_null(i)).then(|| value_size.value(i) as u32),
                    value_schema_id: (!schema_id.is_null(i)).then(|| schema_id.value(i)),
                    headers: row_headers(headers, i)
                        .into_iter()
                        .map(|(k, v)| Header {
                            key: k.to_owned(),
                            value: v.map(<[u8]>::to_vec),
                        })
                        .collect(),
                },
            )
        })
        .collect())
}

/// Casts/reorders `batch` into `schema`; missing nullable columns become nulls,
/// extra columns are dropped.
pub fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| match batch.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
            Some(c) => Ok(cast(c, f.data_type())?),
            None if f.is_nullable() => Ok(new_null_array(f.data_type(), batch.num_rows())),
            None => Err(Error::Invalid(format!("missing column {}", f.name()))),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Concatenates batches of possibly different file schemas into one base-schema batch.
pub fn concat_base(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let base = base_schema();
    let conformed = batches
        .iter()
        .map(|b| conform(b, &base))
        .collect::<Result<Vec<_>>>()?;
    Ok(concat_batches(&base, &conformed)?)
}

/// Sorts rows by (topic, partition, offset).
pub fn sort_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let cols = [schema::TOPIC, schema::PARTITION, schema::OFFSET]
        .iter()
        .map(|n| {
            Ok(SortColumn {
                values: column(batch, n)?.clone(),
                options: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = lexsort_to_indices(&cols, None)?;
    Ok(take_record_batch(batch, &indices)?)
}

/// Splits a batch sorted by topic into one slice per topic.
pub fn split_by_topic(batch: &RecordBatch) -> Result<BTreeMap<String, RecordBatch>> {
    let topic = column(batch, schema::TOPIC)?.as_string::<i32>();
    let mut out = BTreeMap::new();
    let mut start = 0;
    for i in 1..=batch.num_rows() {
        if i == batch.num_rows() || topic.value(i) != topic.value(start) {
            let t = topic.value(start).to_owned();
            if out.contains_key(&t) {
                return Err(Error::Invalid("batch is not sorted by topic".into()));
            }
            out.insert(t, batch.slice(start, i - start));
            start = i;
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct PivotConfig {
    /// Always pivoted when present in a file.
    pub always: Vec<String>,
    /// Keys present in at least this fraction of rows are pivoted.
    pub min_fraction: f64,
    pub max_keys: usize,
}

impl Default for PivotConfig {
    fn default() -> Self {
        Self {
            always: Vec::new(),
            min_fraction: 0.01,
            max_keys: 64,
        }
    }
}

/// Picks the header keys that get pivot columns in a file holding `batch`.
pub fn choose_pivot_keys(batch: &RecordBatch, cfg: &PivotConfig) -> Result<Vec<String>> {
    let headers = column(batch, schema::HEADERS)?.as_list::<i32>();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for row in 0..batch.num_rows() {
        let mut seen = HashSet::new();
        for (k, _) in row_headers(headers, row) {
            if seen.insert(k) {
                *counts.entry(k).or_default() += 1;
            }
        }
    }
    let rows = batch.num_rows().max(1) as f64;
    let mut keys: Vec<(&str, usize)> = counts
        .iter()
        .filter(|(k, c)| {
            cfg.always.iter().any(|a| a == *k) || **c as f64 / rows >= cfg.min_fraction
        })
        .map(|(k, c)| (*k, *c))
        .collect();
    keys.sort_by(|a, b| {
        let fa = cfg.always.iter().any(|x| x == a.0);
        let fb = cfg.always.iter().any(|x| x == b.0);
        fb.cmp(&fa).then(b.1.cmp(&a.1)).then(a.0.cmp(b.0))
    });
    keys.truncate(cfg.max_keys);
    let mut out: Vec<String> = keys.into_iter().map(|(k, _)| k.to_owned()).collect();
    out.sort();
    Ok(out)
}

/// Appends `__h:<key>` columns holding the first value of each header key.
pub fn with_pivots(batch: &RecordBatch, keys: &[String]) -> Result<RecordBatch> {
    let base = conform(batch, &base_schema())?;
    if keys.is_empty() {
        return Ok(base);
    }
    let headers = column(&base, schema::HEADERS)?.as_list::<i32>();
    let index: HashMap<&str, usize> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect();
    let mut builders: Vec<BinaryBuilder> = keys.iter().map(|_| BinaryBuilder::new()).collect();
    let mut row_values: Vec<Option<Option<&[u8]>>> = vec![None; keys.len()];
    for row in 0..base.num_rows() {
        row_values.iter_mut().for_each(|v| *v = None);
        for (k, v) in row_headers(headers, row) {
            if let Some(i) = index.get(k)
                && row_values[*i].is_none()
            {
                row_values[*i] = Some(v);
            }
        }
        for (b, v) in builders.iter_mut().zip(&row_values) {
            b.append_option(v.flatten());
        }
    }
    let mut columns = base.columns().to_vec();
    columns.extend(
        builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as ArrayRef),
    );
    Ok(RecordBatch::try_new(
        schema::schema_with_pivots(keys),
        columns,
    )?)
}

/// Pivot keys present in a batch's schema.
pub fn pivot_keys_of(schema: &SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|f| schema::pivot_key(f.name()).map(str::to_owned))
        .collect()
}

pub fn pivot_name(key: &str) -> String {
    pivot_column(key)
}
