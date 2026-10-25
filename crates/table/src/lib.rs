//! KafkaOpsDB table: an append-only commit log on object storage that atomically
//! tracks segments and Kafka offsets (docs/commit-protocol.md).

pub mod actions;
mod error;
mod log;
pub mod snapshot;
pub mod storage;
mod vacuum;

pub use actions::{
    Action, AddSegment, OffsetRange, OffsetReset, Operation, RemoveSegment, SegmentKind,
};
pub use error::{Error, Result};
pub use log::{Table, TableConfig, decode_actions, encode_actions, now_ms, segment_rel_path};
pub use snapshot::{Snapshot, TopicPartition};
pub use storage::{open_store, self_test};
pub use vacuum::{VacuumConfig, VacuumReport};
