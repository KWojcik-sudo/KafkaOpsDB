use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] object_store::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt table: {0}")]
    Corrupt(String),
    /// The commit's preconditions no longer hold; the caller must discard its work.
    #[error("commit conflict: {0}")]
    Conflict(String),
    #[error("configuration: {0}")]
    Config(String),
    #[error("unsupported protocol: {0}")]
    Protocol(String),
    #[error("commit gave up after {attempts} attempts: {last}")]
    CommitRetriesExhausted { attempts: u32, last: String },
}

impl Error {
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict(_))
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Storage(object_store::Error::NotFound { .. }))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
