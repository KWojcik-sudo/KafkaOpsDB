//! Ranged reads of segment files from the object store.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::actions::AddSegment;
use crate::error::Result;
use crate::log::Table;

pub struct ObjectRangeReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectRangeReader {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

#[async_trait]
impl kseg::RangeReader for ObjectRangeReader {
    async fn read_range(&self, range: Range<u64>) -> kseg::Result<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { path, .. } => kseg::Error::NotFound(path),
                other => kseg::Error::Read(other.to_string()),
            })
    }
}

impl Table {
    /// Opens a committed segment directly from the object store (no caching).
    pub async fn open_segment(&self, add: &AddSegment) -> Result<kseg::SegmentReader> {
        let reader = Arc::new(ObjectRangeReader::new(
            self.store().clone(),
            self.path_of(&add.path),
        ));
        Ok(kseg::SegmentReader::open(reader, add.footer_offset, add.footer_len).await?)
    }
}
