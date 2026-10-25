//! Byte-range cache for immutable data files: memory LRU in front of a disk LRU.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::file::metadata::ParquetMetaData;
use tracing::warn;
use uuid::Uuid;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub memory_bytes: u64,
    pub disk_dir: Option<PathBuf>,
    pub disk_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            memory_bytes: 512 << 20,
            disk_dir: None,
            disk_bytes: 10 << 30,
        }
    }
}

/// Size-bounded LRU index; values are stored by the caller (memory or disk).
struct Lru<V> {
    budget: u64,
    used: u64,
    tick: u64,
    entries: HashMap<String, (V, u64, u64)>,
    order: BTreeMap<u64, String>,
}

impl<V: Clone> Lru<V> {
    fn new(budget: u64) -> Self {
        Self {
            budget,
            used: 0,
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    fn get(&mut self, key: &str) -> Option<V> {
        let (v, _, tick) = self.entries.get_mut(key)?;
        self.order.remove(tick);
        self.tick += 1;
        *tick = self.tick;
        self.order.insert(self.tick, key.to_owned());
        Some(v.clone())
    }

    /// Inserts and returns the evicted entries.
    fn insert(&mut self, key: String, value: V, size: u64) -> Vec<(String, V)> {
        if size > self.budget {
            return Vec::new();
        }
        if let Some((_, old_size, tick)) = self.entries.remove(&key) {
            self.order.remove(&tick);
            self.used -= old_size;
        }
        self.tick += 1;
        self.order.insert(self.tick, key.clone());
        self.entries.insert(key, (value, size, self.tick));
        self.used += size;
        let mut evicted = Vec::new();
        while self.used > self.budget {
            let Some((_, k)) = self.order.pop_first() else {
                break;
            };
            if let Some((v, s, _)) = self.entries.remove(&k) {
                self.used -= s;
                evicted.push((k, v));
            }
        }
        evicted
    }
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub memory_hits: AtomicU64,
    pub disk_hits: AtomicU64,
    pub misses: AtomicU64,
    pub bytes_fetched: AtomicU64,
}

pub struct FileCache {
    store: Arc<dyn ObjectStore>,
    memory: Mutex<Lru<Bytes>>,
    disk: Option<(PathBuf, Mutex<Lru<()>>)>,
    metadata: Mutex<HashMap<Uuid, Arc<ParquetMetaData>>>,
    pub stats: CacheStats,
}

impl FileCache {
    pub fn new(store: Arc<dyn ObjectStore>, cfg: &CacheConfig) -> Result<Arc<Self>> {
        let disk = match &cfg.disk_dir {
            Some(dir) => {
                std::fs::create_dir_all(dir)?;
                let mut lru = Lru::new(cfg.disk_bytes);
                // rebuild the index from what a previous run left behind, oldest first
                let mut files: Vec<(std::time::SystemTime, String, u64)> = std::fs::read_dir(dir)?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| {
                        let m = e.metadata().ok()?;
                        let name = e.file_name().into_string().ok()?;
                        if !m.is_file() || name.ends_with(".tmp") {
                            return None;
                        }
                        Some((m.modified().ok()?, name, m.len()))
                    })
                    .collect();
                files.sort();
                for (_, name, size) in files {
                    for (evicted, ()) in lru.insert(name, (), size) {
                        let _ = std::fs::remove_file(dir.join(evicted));
                    }
                }
                Some((dir.clone(), Mutex::new(lru)))
            }
            None => None,
        };
        Ok(Arc::new(Self {
            store,
            memory: Mutex::new(Lru::new(cfg.memory_bytes)),
            disk,
            metadata: Mutex::new(HashMap::new()),
            stats: CacheStats::default(),
        }))
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn cached_metadata(&self, id: &Uuid) -> Option<Arc<ParquetMetaData>> {
        self.metadata.lock().unwrap().get(id).cloned()
    }

    pub fn put_metadata(&self, id: Uuid, m: Arc<ParquetMetaData>) {
        let mut map = self.metadata.lock().unwrap();
        if map.len() > 100_000 {
            map.clear();
        }
        map.insert(id, m);
    }

    /// Reads `range` of the immutable file `id` stored at `path`.
    pub async fn get_range(&self, id: &Uuid, path: &Path, range: Range<u64>) -> Result<Bytes> {
        let key = format!("{id}_{}_{}", range.start, range.end);
        if let Some(b) = self.memory.lock().unwrap().get(&key) {
            self.stats.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(b);
        }
        if let Some((dir, lru)) = &self.disk {
            let present = lru.lock().unwrap().get(&key).is_some();
            if present {
                match tokio::fs::read(dir.join(&key)).await {
                    Ok(data) => {
                        self.stats.disk_hits.fetch_add(1, Ordering::Relaxed);
                        let b = Bytes::from(data);
                        self.memory
                            .lock()
                            .unwrap()
                            .insert(key, b.clone(), b.len() as u64);
                        return Ok(b);
                    }
                    Err(e) => warn!(%key, error = %e, "cache file unreadable"),
                }
            }
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        let data = self.store.get_range(path, range).await?;
        self.stats
            .bytes_fetched
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.memory
            .lock()
            .unwrap()
            .insert(key.clone(), data.clone(), data.len() as u64);
        if let Some((dir, lru)) = &self.disk {
            let tmp = dir.join(format!("{key}.tmp"));
            let target = dir.join(&key);
            let written = async {
                tokio::fs::write(&tmp, &data).await?;
                tokio::fs::rename(&tmp, &target).await
            }
            .await;
            match written {
                Ok(()) => {
                    let evicted = lru.lock().unwrap().insert(key, (), data.len() as u64);
                    for (k, ()) in evicted {
                        let _ = tokio::fs::remove_file(dir.join(k)).await;
                    }
                }
                Err(e) => warn!(error = %e, "failed to write cache file"),
            }
        }
        Ok(data)
    }
}
