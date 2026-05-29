//! Resolution of the `~/.codex-cron` home directory and the files inside it.
//!
//! The home is `$CODEX_CRON_HOME` when set, else `<user home>/.codex-cron`.
//! [`resolve_home`] is the pure core (so precedence is unit-testable); the rest
//! are path joins.

use std::path::{Path, PathBuf};

/// The env var that overrides the home directory.
pub const HOME_ENV: &str = "CODEX_CRON_HOME";

/// Pure home resolution: a non-empty `env_override` wins, else `user_home`
/// joined with `.codex-cron`, else a relative `.codex-cron`.
pub fn resolve_home(env_override: Option<String>, user_home: Option<PathBuf>) -> PathBuf {
    if let Some(o) = env_override {
        if !o.trim().is_empty() {
            return PathBuf::from(o);
        }
    }
    match user_home {
        Some(h) => h.join(".codex-cron"),
        None => PathBuf::from(".codex-cron"),
    }
}

/// The effective home directory, reading the process environment.
pub fn home_dir() -> PathBuf {
    let env_override = std::env::var(HOME_ENV).ok();
    let user_home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from);
    resolve_home(env_override, user_home)
}

pub fn jobs_file(home: &Path) -> PathBuf {
    home.join("jobs.json")
}
pub fn config_file(home: &Path) -> PathBuf {
    home.join("config.toml")
}
pub fn lock_file(home: &Path) -> PathBuf {
    home.join(".tick.lock")
}
pub fn output_root(home: &Path) -> PathBuf {
    home.join("output")
}
pub fn job_output_dir(home: &Path, id: &str) -> PathBuf {
    output_root(home).join(id)
}
pub fn runs_log(home: &Path, id: &str) -> PathBuf {
    job_output_dir(home, id).join("runs.jsonl")
}
pub fn run_md(home: &Path, id: &str, stamp: &str) -> PathBuf {
    job_output_dir(home, id).join(format!("{stamp}.md"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_override_wins() {
        let home = resolve_home(Some("/tmp/cc".to_string()), Some(PathBuf::from("/home/u")));
        assert_eq!(home, PathBuf::from("/tmp/cc"));
    }

    #[test]
    fn falls_back_to_user_home() {
        let home = resolve_home(None, Some(PathBuf::from("/home/u")));
        assert_eq!(home, PathBuf::from("/home/u/.codex-cron"));
    }

    #[test]
    fn empty_env_override_is_ignored() {
        let home = resolve_home(Some("   ".to_string()), Some(PathBuf::from("/home/u")));
        assert_eq!(home, PathBuf::from("/home/u/.codex-cron"));
    }

    #[test]
    fn builds_expected_child_paths() {
        let home = PathBuf::from("/h/.codex-cron");
        assert_eq!(jobs_file(&home), PathBuf::from("/h/.codex-cron/jobs.json"));
        assert_eq!(config_file(&home), PathBuf::from("/h/.codex-cron/config.toml"));
        assert_eq!(lock_file(&home), PathBuf::from("/h/.codex-cron/.tick.lock"));
        assert_eq!(
            run_md(&home, "abc123", "2026-06-01T10-00-00Z"),
            PathBuf::from("/h/.codex-cron/output/abc123/2026-06-01T10-00-00Z.md")
        );
        assert_eq!(
            runs_log(&home, "abc123"),
            PathBuf::from("/h/.codex-cron/output/abc123/runs.jsonl")
        );
    }
}
