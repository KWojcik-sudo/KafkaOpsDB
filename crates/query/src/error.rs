use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sql: {0}")]
    Sql(String),
    #[error("{0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("table: {0}")]
    Table(#[from] table::Error),
    #[error("data file: {0}")]
    Model(#[from] model::Error),
    #[error("storage: {0}")]
    Storage(#[from] object_store::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
