//! Query results as JSON, NDJSON or a text table. Binary values are shown as UTF-8 when
//! valid, otherwise as `{"base64": "..."}`.

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use base64::Engine;
use serde_json::{Map, Value, json};

fn bytes_value(b: &[u8]) -> Value {
    match std::str::from_utf8(b) {
        Ok(s) => Value::String(s.to_owned()),
        Err(_) => json!({ "base64": base64::engine::general_purpose::STANDARD.encode(b) }),
    }
}

fn value_at(array: &ArrayRef, i: usize) -> Value {
    if array.is_null(i) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => Value::String(array.as_string::<i64>().value(i).to_owned()),
        DataType::Utf8View => Value::String(array.as_string_view().value(i).to_owned()),
        DataType::Binary => bytes_value(array.as_binary::<i32>().value(i)),
        DataType::LargeBinary => bytes_value(array.as_binary::<i64>().value(i)),
        DataType::BinaryView => bytes_value(array.as_binary_view().value(i)),
        DataType::Boolean => Value::Bool(array.as_boolean().value(i)),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => {
            let s = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
                .map(|f| f.value(i).to_string())
                .unwrap_or_default();
            serde_json::from_str(&s).unwrap_or(Value::String(s))
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let values = list.value(i);
            Value::Array((0..values.len()).map(|j| value_at(&values, j)).collect())
        }
        DataType::Struct(fields) => {
            let s = array.as_struct();
            Value::Object(
                fields
                    .iter()
                    .zip(s.columns())
                    .map(|(f, c)| (f.name().clone(), value_at(c, i)))
                    .collect(),
            )
        }
        _ => ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
            .map(|f| Value::String(f.value(i).to_string()))
            .unwrap_or(Value::Null),
    }
}

pub fn to_json_rows(batches: &[RecordBatch]) -> Vec<Value> {
    let mut rows = Vec::new();
    for b in batches {
        let schema = b.schema();
        for i in 0..b.num_rows() {
            let mut row = Map::new();
            for (f, c) in schema.fields().iter().zip(b.columns()) {
                row.insert(f.name().clone(), value_at(c, i));
            }
            rows.push(Value::Object(row));
        }
    }
    rows
}

pub fn to_json(schema: &SchemaRef, batches: &[RecordBatch]) -> Value {
    json!({
        "columns": schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
        "rows": to_json_rows(batches),
    })
}

pub fn to_ndjson(batches: &[RecordBatch]) -> String {
    let mut out = String::new();
    for r in to_json_rows(batches) {
        out.push_str(&r.to_string());
        out.push('\n');
    }
    out
}

fn cell(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Plain text table, one line per row.
pub fn to_table(schema: &SchemaRef, batches: &[RecordBatch]) -> String {
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let rows: Vec<Vec<String>> = to_json_rows(batches)
        .iter()
        .map(|r| columns.iter().map(|c| cell(&r[c.as_str()])).collect())
        .collect();
    let mut widths: Vec<usize> = columns.iter().map(|c| c.chars().count()).collect();
    for r in &rows {
        for (w, c) in widths.iter_mut().zip(r) {
            *w = (*w).max(c.chars().count()).min(120);
        }
    }
    let line = |cells: &[String]| {
        let parts: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(c, w)| {
                let c: String = c.chars().take(*w).collect();
                format!("{c:<w$}", w = *w)
            })
            .collect();
        format!("| {} |\n", parts.join(" | "))
    };
    let sep = format!(
        "+{}+\n",
        widths
            .iter()
            .map(|w| "-".repeat(w + 2))
            .collect::<Vec<_>>()
            .join("+")
    );
    let mut out = sep.clone();
    out.push_str(&line(&columns));
    out.push_str(&sep);
    for r in &rows {
        out.push_str(&line(r));
    }
    out.push_str(&sep);
    out.push_str(&format!("{} row(s)\n", rows.len()));
    out
}
