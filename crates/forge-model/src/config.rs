//! `forge.toml` configuration. Every key can be overridden by an environment variable named
//! `FORGE_<SECTION>_<KEY>` in upper case, e.g. `FORGE_STORAGE_URL` or `FORGE_MEMTABLE_FLUSH_BYTES`.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub metastore: MetastoreConfig,
    pub wal: WalConfig,
    pub memtable: MemtableConfig,
    pub cache: CacheConfig,
    pub compaction: CompactionConfig,
    pub query: QueryConfig,
    pub auth: AuthConfig,
    pub ingest: IngestConfig,
    /// Per-model prices in USD per million tokens, overriding the built-in table.
    pub pricing: BTreeMap<String, ModelPrice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub otlp_grpc_addr: String,
    pub otlp_http_addr: String,
    pub api_addr: String,
    pub metrics_addr: String,
    /// Identifies this process in WAL object names.
    pub node_id: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            otlp_grpc_addr: "0.0.0.0:4317".into(),
            otlp_http_addr: "0.0.0.0:4318".into(),
            api_addr: "0.0.0.0:8080".into(),
            metrics_addr: "0.0.0.0:9090".into(),
            node_id: "node-0".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// `file:///path`, `memory://`, `s3://bucket/prefix`, `gs://…`, `az://…`.
    pub url: String,
    /// Extra options passed to the object store builder (e.g. `aws_endpoint`, `aws_region`).
    pub options: BTreeMap<String, String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            url: "file:///var/lib/forge/data".into(),
            options: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetastoreConfig {
    pub url: String,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        MetastoreConfig {
            url: "sqlite:///var/lib/forge/meta.db".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalMode {
    Local,
    ObjectStore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    pub mode: WalMode,
    pub dir: String,
    /// Group commit window.
    pub commit_interval: HumanDuration,
    /// Group commit size trigger.
    pub commit_bytes: ByteSize,
    /// Rotate local WAL files at this size.
    pub file_bytes: ByteSize,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            mode: WalMode::Local,
            dir: "/var/lib/forge/wal".into(),
            commit_interval: HumanDuration(Duration::from_millis(10)),
            commit_bytes: ByteSize(1 << 20),
            file_bytes: ByteSize(64 << 20),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemtableConfig {
    pub flush_bytes: ByteSize,
    pub flush_interval: HumanDuration,
    pub memory_budget: ByteSize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        MemtableConfig {
            flush_bytes: ByteSize(64 << 20),
            flush_interval: HumanDuration(Duration::from_secs(30)),
            memory_budget: ByteSize(1 << 30),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub memory: ByteSize,
    pub disk: ByteSize,
    pub disk_dir: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            memory: ByteSize(512 << 20),
            disk: ByteSize(10 << 30),
            disk_dir: "/var/lib/forge/cache".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactionConfig {
    pub target_segment: ByteSize,
    pub concurrency: usize,
    /// How often the compactor looks for work.
    pub interval: HumanDuration,
    /// Compact a partition once it has this many L0 segments, or once its hour has closed.
    pub min_l0_segments: usize,
    /// How long deleted segments stay readable before their objects are removed.
    pub gc_grace: HumanDuration,
    /// Unreferenced objects older than this are orphans.
    pub orphan_age: HumanDuration,
    /// Upper bound on input rows merged in memory at once; partitions are processed in
    /// trace-id ranges of at most this many rows.
    pub max_rows_per_merge: usize,
    pub enabled: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            target_segment: ByteSize(256 << 20),
            concurrency: 2,
            interval: HumanDuration(Duration::from_secs(60)),
            min_l0_segments: 8,
            gc_grace: HumanDuration(Duration::from_secs(300)),
            orphan_age: HumanDuration(Duration::from_secs(24 * 3600)),
            max_rows_per_merge: 200_000,
            enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryConfig {
    pub memory_limit: ByteSize,
    pub timeout: HumanDuration,
}

impl Default for QueryConfig {
    fn default() -> Self {
        QueryConfig {
            memory_limit: ByteSize(2 << 30),
            timeout: HumanDuration(Duration::from_secs(30)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub struct AuthConfig {
    /// When false, any request is accepted and the project comes from the `x-forge-project`
    /// header (or `default`). Turn on for anything reachable from a network.
    pub enabled: bool,
    /// Key for `/v1/projects` admin calls and cross-project SQL. Required when `enabled`.
    pub admin_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    pub max_request: ByteSize,
    /// Payload values larger than this are stored as separate blob objects.
    pub blob_threshold: ByteSize,
    pub default_project: String,
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig {
            max_request: ByteSize(64 << 20),
            blob_threshold: ByteSize(1 << 20),
            default_project: "default".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ModelPrice {
    /// USD per million input tokens.
    pub input: f64,
    /// USD per million output tokens.
    pub output: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {0}: {1}")]
    Io(String, std::io::Error),
    #[error("parsing config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("environment override {0}: {1}")]
    Env(String, String),
}

impl Config {
    /// Load a config file (if given) and apply `FORGE_*` environment overrides.
    pub fn load(path: Option<&Path>) -> Result<Config, ConfigError> {
        let text = match path {
            Some(p) => std::fs::read_to_string(p)
                .map_err(|e| ConfigError::Io(p.display().to_string(), e))?,
            None => String::new(),
        };
        Self::from_toml_with_env(&text, std::env::vars())
    }

    pub fn from_toml_with_env(
        text: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Config, ConfigError> {
        let mut value: toml::Table = toml::from_str(text)?;
        let defaults = toml::Table::try_from(Config::default()).expect("default config serializes");
        let env: BTreeMap<String, String> = env
            .into_iter()
            .filter(|(k, _)| k.starts_with("FORGE_"))
            .collect();
        for (section, keys) in &defaults {
            let Some(keys) = keys.as_table() else {
                continue;
            };
            for key in keys.keys() {
                let var = format!("FORGE_{}_{}", section.to_uppercase(), key.to_uppercase());
                if let Some(raw) = env.get(&var) {
                    let parsed = parse_env_value(raw);
                    let table = value
                        .entry(section.clone())
                        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                        .as_table_mut()
                        .ok_or_else(|| {
                            ConfigError::Env(var.clone(), "section is not a table".into())
                        })?;
                    table.insert(key.clone(), parsed);
                }
            }
        }
        Ok(value.try_into()?)
    }

    /// Point every local path at one directory. Used by tests and `forge serve --data-dir`.
    pub fn with_data_dir(mut self, dir: &Path) -> Config {
        let d = dir.display();
        self.storage.url = format!("file://{d}/data");
        self.metastore.url = format!("sqlite://{d}/meta.db");
        self.wal.dir = format!("{d}/wal");
        self.cache.disk_dir = format!("{d}/cache");
        self
    }
}

fn parse_env_value(raw: &str) -> toml::Value {
    if let Ok(b) = raw.parse::<bool>() {
        return toml::Value::Boolean(b);
    }
    if let Ok(i) = raw.parse::<i64>() {
        return toml::Value::Integer(i);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return toml::Value::Float(f);
    }
    toml::Value::String(raw.to_string())
}

/// A byte count written as `64MiB`, `1GiB`, `512KB` or a plain integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub fn parse(s: &str) -> Option<ByteSize> {
        let s = s.trim();
        let split = s
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(s.len());
        let (num, unit) = s.split_at(split);
        let num: f64 = num.parse().ok()?;
        let mul: u64 = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "k" | "kb" => 1000,
            "kib" => 1 << 10,
            "m" | "mb" => 1_000_000,
            "mib" => 1 << 20,
            "g" | "gb" => 1_000_000_000,
            "gib" => 1 << 30,
            "t" | "tb" => 1_000_000_000_000,
            "tib" => 1 << 40,
            _ => return None,
        };
        Some(ByteSize((num * mul as f64) as u64))
    }

    pub fn bytes(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ByteSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.0;
        for (unit, size) in [("GiB", 1u64 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
            if b >= size && b.is_multiple_of(size) {
                return write!(f, "{}{}", b / size, unit);
            }
        }
        write!(f, "{b}")
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            N(u64),
            S(String),
        }
        match Raw::deserialize(d)? {
            Raw::N(n) => Ok(ByteSize(n)),
            Raw::S(s) => ByteSize::parse(&s)
                .ok_or_else(|| serde::de::Error::custom(format!("invalid byte size {s:?}"))),
        }
    }
}

/// A duration written as `10ms`, `30s`, `5m`, `2h`, `7d`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

impl HumanDuration {
    pub fn parse(s: &str) -> Option<HumanDuration> {
        let s = s.trim();
        let split = s.find(|c: char| !c.is_ascii_digit() && c != '.')?;
        let (num, unit) = s.split_at(split);
        let num: f64 = num.parse().ok()?;
        let secs = match unit {
            "ns" => num / 1e9,
            "us" => num / 1e6,
            "ms" => num / 1e3,
            "s" => num,
            "m" => num * 60.0,
            "h" => num * 3600.0,
            "d" => num * 86400.0,
            _ => return None,
        };
        Some(HumanDuration(Duration::from_secs_f64(secs)))
    }

    pub fn get(self) -> Duration {
        self.0
    }
}

impl std::fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ms = self.0.as_millis();
        if !ms.is_multiple_of(1000) {
            write!(f, "{ms}ms")
        } else {
            write!(f, "{}s", ms / 1000)
        }
    }
}

impl Serialize for HumanDuration {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        HumanDuration::parse(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid duration {s:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spec_example() {
        let text = r#"
[storage]
url = "file:///var/lib/forge/data"
[metastore]
url = "sqlite:///var/lib/forge/meta.db"
[wal]
mode = "local"
dir = "/var/lib/forge/wal"
[memtable]
flush_bytes = "64MiB"
flush_interval = "30s"
memory_budget = "1GiB"
[cache]
memory = "512MiB"
disk = "10GiB"
disk_dir = "/var/lib/forge/cache"
[compaction]
target_segment = "256MiB"
concurrency = 2
[query]
memory_limit = "2GiB"
timeout = "30s"
"#;
        let c = Config::from_toml_with_env(text, []).unwrap();
        assert_eq!(c.memtable.flush_bytes.bytes(), 64 << 20);
        assert_eq!(c.query.timeout.get(), Duration::from_secs(30));
        assert_eq!(c.wal.mode, WalMode::Local);
    }

    #[test]
    fn env_overrides() {
        let c = Config::from_toml_with_env(
            "",
            [
                ("FORGE_STORAGE_URL".to_string(), "s3://b/p".to_string()),
                ("FORGE_MEMTABLE_FLUSH_BYTES".to_string(), "8MiB".to_string()),
                ("FORGE_COMPACTION_CONCURRENCY".to_string(), "4".to_string()),
                ("FORGE_WAL_MODE".to_string(), "object_store".to_string()),
                ("FORGE_AUTH_ENABLED".to_string(), "true".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(c.storage.url, "s3://b/p");
        assert_eq!(c.memtable.flush_bytes.bytes(), 8 << 20);
        assert_eq!(c.compaction.concurrency, 4);
        assert_eq!(c.wal.mode, WalMode::ObjectStore);
        assert!(c.auth.enabled);
    }

    #[test]
    fn sizes_and_durations() {
        assert_eq!(ByteSize::parse("1GiB").unwrap().0, 1 << 30);
        assert_eq!(ByteSize::parse("1024").unwrap().0, 1024);
        assert_eq!(
            HumanDuration::parse("10ms").unwrap().0,
            Duration::from_millis(10)
        );
        assert_eq!(
            HumanDuration::parse("2h").unwrap().0,
            Duration::from_secs(7200)
        );
        assert!(HumanDuration::parse("10").is_none());
    }
}
