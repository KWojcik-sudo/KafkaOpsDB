//! KafkaOpsDB indexer: reads Kafka ranges, writes KSEG segments and commits them
//! atomically with the offsets (docs/architecture.md §6, docs/distributed.md).

pub mod error;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod planner;
pub mod service;
pub mod source;
pub mod worker;

pub use error::{Error, Result};
pub use planner::{IngestTask, InitialPosition, Plan, Planner, PlannerConfig};
pub use service::{IngestService, IngestStats, PartitionLag, ServiceConfig, StepReport};
pub use source::{
    FetchRange, FetchedPartition, KafkaSource, MemorySource, PartitionMeta, TopicMeta,
};
pub use worker::{TaskOutcome, TaskResult, TaskStats, WorkerConfig, execute_task};
