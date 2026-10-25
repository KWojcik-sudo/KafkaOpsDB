//! KafkaOpsDB data model: the indexed record, its Arrow schema and the Parquet/Avro
//! data files (docs/storage-format.md).

pub mod batch;
mod error;
pub mod files;
pub mod record;
pub mod schema;
pub mod stats;

pub use batch::{
    PivotConfig, batch_to_records, choose_pivot_keys, concat_base, conform, records_to_batch,
    sort_batch, split_by_topic, with_pivots,
};
pub use error::{Error, Result};
pub use files::{
    FileFormat, ParquetCompression, ParquetOptions, read_file, write_avro, write_parquet,
};
pub use record::{Header, Record, TimestampType, confluent_schema_id};
pub use stats::{FileStats, PartitionStats, TopicStats};
