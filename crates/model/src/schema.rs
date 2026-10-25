//! Arrow schema shared by all data files and the SQL table `topics`.

use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

pub const TOPIC: &str = "topic";
pub const PARTITION: &str = "partition";
pub const OFFSET: &str = "offset";
pub const TIMESTAMP: &str = "timestamp";
pub const TIMESTAMP_TYPE: &str = "timestamp_type";
pub const KEY: &str = "key";
pub const VALUE_SIZE: &str = "value_size";
pub const VALUE_SCHEMA_ID: &str = "value_schema_id";
pub const HEADERS: &str = "headers";

/// Pivot columns hold the first value of one header key: `__h:<key>`.
pub const PIVOT_PREFIX: &str = "__h:";

pub fn pivot_column(key: &str) -> String {
    format!("{PIVOT_PREFIX}{key}")
}

pub fn pivot_key(column: &str) -> Option<&str> {
    column.strip_prefix(PIVOT_PREFIX)
}

pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

pub fn header_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Binary, true),
    ])
}

pub fn header_item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Struct(header_fields()), false))
}

pub fn headers_type() -> DataType {
    DataType::List(header_item_field())
}

/// The schema of every data file without pivot columns.
pub fn base_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new(TOPIC, DataType::Utf8, false),
                Field::new(PARTITION, DataType::Int32, false),
                Field::new(OFFSET, DataType::Int64, false),
                Field::new(TIMESTAMP, timestamp_type(), false),
                Field::new(TIMESTAMP_TYPE, DataType::Utf8, false),
                Field::new(KEY, DataType::Binary, true),
                Field::new(VALUE_SIZE, DataType::Int32, true),
                Field::new(VALUE_SCHEMA_ID, DataType::Int32, true),
                Field::new(HEADERS, headers_type(), false),
            ]))
        })
        .clone()
}

/// Base schema plus one nullable binary pivot column per header key.
pub fn schema_with_pivots(keys: &[String]) -> SchemaRef {
    let base = base_schema();
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.extend(
        keys.iter()
            .map(|k| Field::new(pivot_column(k), DataType::Binary, true)),
    );
    Arc::new(Schema::new(fields))
}
