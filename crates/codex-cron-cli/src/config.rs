//! `config.toml`: defaults re-read fresh on every tick.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::paths;
use crate::store::atomic_write;

/// User configuration. Every field has a default, so a missing file is valid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Default executor for `add` when `--executor` is omitted.
    pub default_executor: String,
    /// Max jobs run concurrently per tick; `0` means "number of CPUs".
    pub max_parallel: usize,
    /// Path/name of the `codex` binary.
    pub codex_path: String,
    /// Path/name of the `ao2` binary.
    pub ao2_path: String,
    /// Webhook URL applied to `add --deliver webhook` when no URL is given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_webhook: Option<String>,
    /// If non-empty, webhook delivery is allowed only to these hosts (SSRF guard).
    pub webhook_allowlist: Vec<String>,
    /// Informational timezone label for display.
    pub timezone: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            default_executor: "codex".to_string(),
            max_parallel: 0,
            codex_path: "codex".to_string(),
            ao2_path: "ao2".to_string(),
            default_webhook: None,
            webhook_allowlist: Vec::new(),
            timezone: "UTC".to_string(),
        }
    }
}

impl Config {
    /// Read `<home>/config.toml`, or the defaults if it is absent.
    pub fn load(home: &Path) -> anyhow::Result<Config> {
        let path = paths::config_file(home);
        match std::fs::read_to_string(&path) {
            Ok(s) => Ok(toml::from_str(&s)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Atomically write this config to `<home>/config.toml`.
    pub fn save(&self, home: &Path) -> anyhow::Result<()> {
        let text = toml::to_string_pretty(self)?;
        crate::store::ensure_secure_dir(home)?;
        atomic_write(&paths::config_file(home), text.as_bytes())?;
        Ok(())
    }

    /// The concurrency to actually use, resolving `0` to the CPU count.
    pub fn effective_max_parallel(&self) -> usize {
        if self.max_parallel == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            self.max_parallel
        }
    }

    /// Read a single setting by key (for `config get`).
    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "default_executor" => Some(self.default_executor.clone()),
            "max_parallel" => Some(self.max_parallel.to_string()),
            "codex_path" => Some(self.codex_path.clone()),
            "ao2_path" => Some(self.ao2_path.clone()),
            "default_webhook" => Some(self.default_webhook.clone().unwrap_or_default()),
            "webhook_allowlist" => Some(self.webhook_allowlist.join(",")),
            "timezone" => Some(self.timezone.clone()),
            _ => None,
        }
    }

    /// Mutate a single setting by key (for `config set`).
    pub fn set(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        match key {
            "default_executor" => self.default_executor = value.to_string(),
            "max_parallel" => {
                self.max_parallel = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("max_parallel must be a non-negative integer"))?
            }
            "codex_path" => self.codex_path = value.to_string(),
            "ao2_path" => self.ao2_path = value.to_string(),
            "default_webhook" => {
                self.default_webhook = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                }
            }
            "webhook_allowlist" => {
                self.webhook_allowlist = value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            }
            "timezone" => self.timezone = value.to_string(),
            other => anyhow::bail!("unknown config key '{other}'"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn load_missing_returns_defaults() {
        let dir = tempdir().unwrap();
        assert_eq!(Config::load(dir.path()).unwrap(), Config::default());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempdir().unwrap();
        let cfg = Config {
            max_parallel: 4,
            default_webhook: Some("https://hook.test/x".to_string()),
            webhook_allowlist: vec!["hook.test".to_string()],
            ..Config::default()
        };
        cfg.save(dir.path()).unwrap();
        assert_eq!(Config::load(dir.path()).unwrap(), cfg);
    }

    #[test]
    fn get_reads_known_keys() {
        let cfg = Config::default();
        assert_eq!(cfg.get("default_executor").as_deref(), Some("codex"));
        assert_eq!(cfg.get("max_parallel").as_deref(), Some("0"));
        assert!(cfg.get("nope").is_none());
    }

    #[test]
    fn set_updates_and_validates() {
        let mut cfg = Config::default();
        cfg.set("max_parallel", "8").unwrap();
        assert_eq!(cfg.max_parallel, 8);
        cfg.set("webhook_allowlist", "a.com, b.com").unwrap();
        assert_eq!(cfg.webhook_allowlist, vec!["a.com", "b.com"]);
        assert!(cfg.set("max_parallel", "lots").is_err());
        assert!(cfg.set("bogus", "x").is_err());
    }

    #[test]
    fn effective_parallel_resolves_zero() {
        let cfg = Config::default();
        assert!(cfg.effective_max_parallel() >= 1);
        let c2 = Config {
            max_parallel: 3,
            ..Config::default()
        };
        assert_eq!(c2.effective_max_parallel(), 3);
    }
}
