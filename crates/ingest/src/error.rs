use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("kafka: {0}")]
    Kafka(String),
    #[error("offset {requested} of {topic}/{partition} is no longer available")]
    OffsetOutOfRange {
        topic: String,
        partition: i32,
        requested: i64,
    },
    #[error("table: {0}")]
    Table(#[from] table::Error),
    #[error("segment: {0}")]
    Segment(#[from] kseg::Error),
    #[error("configuration: {0}")]
    Config(String),
    #[error("task panicked: {0}")]
    Join(String),
}

pub type Result<T> = std::result::Result<T, Error>;
