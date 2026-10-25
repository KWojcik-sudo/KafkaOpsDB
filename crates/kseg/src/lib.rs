//! KSEG: immutable columnar segment format for Kafka message metadata.
//!
//! See `docs/format-kseg.md` for the binary specification.

pub mod bitpack;
pub mod bloom;
pub mod encoding;
mod error;
pub mod footer;
pub mod reader;
pub mod record;
pub mod varint;
pub mod writer;

pub use error::{Error, Result};
pub use footer::{BlockMeta, Footer, SegmentType};
pub use reader::{
    BytesReader, ColumnIndex, DecodedBlock, HeaderProjection, Projection, RangeReader,
    SegmentReader,
};
pub use record::{Header, Record, TimestampType, confluent_schema_id};
pub use writer::{PartitionSummary, SegmentSummary, SegmentWriter, TopicSummary, WriterOptions};

/// Column ids (docs/format-kseg.md §5).
pub mod columns {
    pub const OFFSET: u32 = 0;
    pub const TIMESTAMP: u32 = 1;
    pub const TIMESTAMP_TYPE: u32 = 2;
    pub const KEY: u32 = 3;
    pub const KEY_SIZE: u32 = 4;
    pub const VALUE_SIZE: u32 = 5;
    pub const VALUE_SCHEMA_ID: u32 = 6;
    pub const HEADER_SHAPE: u32 = 7;
    pub const HEADER_BASE: u32 = 16;
}
