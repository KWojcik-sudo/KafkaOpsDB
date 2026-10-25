//! `kafkaopsdb.toml` (docs/architecture.md §8). Sizes accept `16MiB`, `256MB`, `1024`;
//! durations accept humantime strings such as `30s`, `5m`, `30d`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num.parse().map_err(|_| format!("invalid size {s:?}"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "kib" | "k" => 1 << 10,
        "mb" => 1_000_000,
        "mib" | "m" => 1 << 20,
        "gb" => 1_000_000_000,
        "gib" | "g" => 1 << 30,
        "tb" => 1_000_000_000_000,
        "tib" | "t" => 1 << 40,
        other => return Err(format!("unknown size unit {other:?}")),
    };
    Ok((num * mult as f64) as u64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size(pub u64);

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Int(u64),
            Str(String),
        }
        match Raw::deserialize(d)? {
            Raw::Int(v) => Ok(Size(v)),
            Raw::Str(s) => parse_size(&s).map(Size).map_err(serde::de::Error::custom),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dur(pub Duration);

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        humantime::parse_duration(&s)
            .map(Dur)
            .map_err(|e| serde::de::Error::custom(format!("invalid duration {s:?}: {e}")))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_node")]
    pub node_id: String,
    pub storage: StorageConfig,
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub compaction: CompactionConfig,
    #[serde(default)]
    pub pivot: PivotConfig,
    #[serde(default)]
    pub retention: Vec<RetentionConfig>,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub vacuum: VacuumConfig,
}

fn default_node() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "kafkaopsdb".into())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// `s3://bucket/prefix`, `az://container/prefix`, `gs://bucket/prefix`, `file:///path`.
    pub url: String,
    /// object_store options such as `aws_endpoint`, `aws_region`, `aws_allow_http`.
    #[serde(default)]
    pub options: BTreeMap<String, String>,
    #[serde(default = "d_checkpoint_interval")]
    pub checkpoint_interval: u64,
}

fn d_checkpoint_interval() -> u64 {
    100
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    /// Passed verbatim to librdkafka (security.protocol, sasl.*, ssl.*).
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default = "d_fetch_timeout")]
    pub fetch_timeout: Dur,
    #[serde(default = "d_metadata_timeout")]
    pub metadata_timeout: Dur,
}

fn d_fetch_timeout() -> Dur {
    Dur(Duration::from_secs(30))
}
fn d_metadata_timeout() -> Dur {
    Dur(Duration::from_secs(10))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngestConfig {
    pub enabled: bool,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    /// `earliest` or `latest` for partitions the table has never seen.
    pub initial_position: String,
    pub parallelism: usize,
    pub task_max_bytes: Size,
    pub flush_min_bytes: Size,
    pub max_flush_age: Dur,
    pub max_records_per_partition: i64,
    pub l0_parquet_min_rows: usize,
    pub tick: Dur,
    pub metadata_refresh: Dur,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            include: vec![".*".into()],
            exclude: vec!["^__".into(), "^_schemas$".into()],
            initial_position: "earliest".into(),
            parallelism: 4,
            task_max_bytes: Size(256 << 20),
            flush_min_bytes: Size(16 << 20),
            max_flush_age: Dur(Duration::from_secs(30)),
            max_records_per_partition: 2_000_000,
            l0_parquet_min_rows: 25_000,
            tick: Dur(Duration::from_secs(1)),
            metadata_refresh: Dur(Duration::from_secs(5)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CompactionConfig {
    pub enabled: bool,
    pub l0_min_files: usize,
    pub l0_max_age: Dur,
    pub l0_max_bytes: Size,
    pub max_input_bytes: Size,
    pub l1_target_bytes: Size,
    pub l2_promote_min_bytes: Size,
    pub l2_target_bytes: Size,
    pub merge_min_files: usize,
    pub parallelism: usize,
    pub tick: Dur,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        let d = ingest::CompactionConfig::default();
        Self {
            enabled: true,
            l0_min_files: d.l0_min_files,
            l0_max_age: Dur(d.l0_max_age),
            l0_max_bytes: Size(d.l0_max_bytes),
            max_input_bytes: Size(d.max_input_bytes),
            l1_target_bytes: Size(d.l1_target_bytes),
            l2_promote_min_bytes: Size(d.l2_promote_min_bytes),
            l2_target_bytes: Size(d.l2_target_bytes),
            merge_min_files: d.merge_min_files,
            parallelism: d.parallelism,
            tick: Dur(d.tick),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PivotConfig {
    pub always: Vec<String>,
    pub min_fraction: f64,
    pub max_keys: usize,
}

impl Default for PivotConfig {
    fn default() -> Self {
        let d = model::PivotConfig::default();
        Self {
            always: d.always,
            min_fraction: d.min_fraction,
            max_keys: d.max_keys,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    /// Regex matched against topic names; the first matching rule wins.
    pub topics: String,
    pub retention: Dur,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QueryConfig {
    pub listen: String,
    pub cache_dir: Option<PathBuf>,
    pub cache_disk_bytes: Size,
    pub cache_memory_bytes: Size,
    pub refresh_interval: Dur,
    pub target_partitions: Option<usize>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8080".into(),
            cache_dir: None,
            cache_disk_bytes: Size(10 << 30),
            cache_memory_bytes: Size(512 << 20),
            refresh_interval: Dur(Duration::from_secs(1)),
            target_partitions: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct VacuumConfig {
    pub enabled: bool,
    pub interval: Dur,
    pub tombstone_grace: Dur,
    pub orphan_grace: Dur,
    pub log_retention: Dur,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        let d = table::VacuumConfig::default();
        Self {
            enabled: true,
            interval: Dur(Duration::from_secs(600)),
            tombstone_grace: Dur(d.tombstone_grace),
            orphan_grace: Dur(d.orphan_grace),
            log_retention: Dur(d.log_retention),
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        toml::from_str(&text).map_err(|e| format!("invalid config {}: {e}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_config() {
        let cfg: Config = toml::from_str(include_str!("../../../deploy/kafkaopsdb.toml")).unwrap();
        assert_eq!(cfg.ingest.flush_min_bytes, Size(16 << 20));
        assert_eq!(cfg.ingest.max_flush_age, Dur(Duration::from_secs(30)));
        assert_eq!(
            cfg.retention[0].retention,
            Dur(Duration::from_secs(30 * 86400))
        );
        assert_eq!(parse_size("1.5GiB").unwrap(), 3 << 29);
        assert_eq!(parse_size("256MB").unwrap(), 256_000_000);
        assert!(parse_size("12 parsecs").is_err());
    }
}
