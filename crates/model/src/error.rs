use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("avro: {0}")]
    Avro(String),
    #[error("invalid data: {0}")]
    Invalid(String),
}

impl From<arrow_avro::errors::AvroError> for Error {
    fn from(e: arrow_avro::errors::AvroError) -> Self {
        Self::Avro(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
