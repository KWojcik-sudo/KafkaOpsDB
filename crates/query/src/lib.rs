//! KafkaOpsDB query engine: the `topics` table on DataFusion with pruning by the commit log
//! manifest, Parquet statistics, bloom filters and header pivot columns.

pub mod cache;
pub mod engine;
mod error;
pub mod format;
pub mod predicates;
mod scan;
pub mod sql;
pub mod udf;

pub use cache::{CacheConfig, FileCache};
pub use engine::{EngineConfig, QueryEngine, QueryResult};
pub use error::{Error, Result};
pub use scan::{ScanMetrics, TopicsTable};
