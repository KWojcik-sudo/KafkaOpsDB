//! Garbage collection of removed segments, orphans and old log files.

use std::collections::HashSet;
use std::time::Duration;

use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use tracing::{info, warn};

use crate::error::Result;
use crate::log::{LogFileKind, Table, now_ms};

#[derive(Debug, Clone)]
pub struct VacuumConfig {
    /// Removed segments are deleted this long after their `remove` commit.
    pub tombstone_grace: Duration,
    /// Unreferenced files younger than this may still be in-flight uploads.
    pub orphan_grace: Duration,
    /// Log entries and checkpoints older than the newest checkpoint are kept this long.
    pub log_retention: Duration,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        Self {
            tombstone_grace: Duration::from_secs(3600),
            orphan_grace: Duration::from_secs(24 * 3600),
            log_retention: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VacuumReport {
    pub removed_segments: usize,
    pub orphans: usize,
    pub log_files: usize,
}

impl Table {
    pub async fn vacuum(&self, cfg: &VacuumConfig) -> Result<VacuumReport> {
        let snap = self.refresh().await?;
        let now = now_ms();
        let mut report = VacuumReport::default();
        let store = self.store();

        for t in snap.tombstones.values() {
            if t.deletion_timestamp < now - cfg.tombstone_grace.as_millis() as i64 {
                match store.delete(&self.path_of(&t.path)).await {
                    Ok(()) => report.removed_segments += 1,
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => warn!(path = %t.path, error = %e, "failed to delete segment"),
                }
            }
        }

        let referenced: HashSet<String> = snap
            .segments
            .values()
            .map(|s| s.path.clone())
            .chain(snap.tombstones.values().map(|t| t.path.clone()))
            .collect();
        let data_dir = self.root().clone().join("data");
        let files: Vec<_> = store.list(Some(&data_dir)).try_collect().await?;
        let orphan_cutoff = now - cfg.orphan_grace.as_millis() as i64;
        let root_prefix = self.root().as_ref().len();
        for f in files {
            let full = f.location.as_ref();
            let rel = full[root_prefix..].trim_start_matches('/');
            if referenced.contains(rel) || f.last_modified.timestamp_millis() >= orphan_cutoff {
                continue;
            }
            match store.delete(&f.location).await {
                Ok(()) => report.orphans += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => warn!(path = %f.location, error = %e, "failed to delete orphan"),
            }
        }

        let log = self.list_log().await?;
        if let Some(newest_cp) = log
            .iter()
            .filter(|(_, k)| *k == LogFileKind::Checkpoint)
            .map(|(v, _)| *v)
            .max()
        {
            let log_cutoff = now - cfg.log_retention.as_millis() as i64;
            let metas: Vec<_> = store
                .list(Some(&self.root().clone().join("_log")))
                .try_collect()
                .await?;
            for m in metas {
                let Some(name) = m.location.filename() else {
                    continue;
                };
                let old = m.last_modified.timestamp_millis() < log_cutoff;
                let deletable = match crate::log::parse_log_name(name) {
                    Some((v, _)) => v < newest_cp && old,
                    None => name.starts_with("_probe-") && old,
                };
                if deletable && store.delete(&m.location).await.is_ok() {
                    report.log_files += 1;
                }
            }
        }
        if report != VacuumReport::default() {
            info!(?report, "vacuum");
        }
        Ok(report)
    }
}
