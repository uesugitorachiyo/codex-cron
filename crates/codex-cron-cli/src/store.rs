//! Durable, atomic, single-file job storage and the cross-process tick lock.
//!
//! Writes go to a temp file in the same directory, are `fsync`'d, then renamed
//! over the target — so a crash mid-write never leaves a half-written
//! `jobs.json`. On unix the home dir is `0700` and the files `0600`. The tick
//! lock is an advisory `flock` (via `fs2`) so only one tick mutates the store
//! at a time, whether driven by the built-in daemon or an external scheduler.

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};

use codex_cron_core::{jobs_to_json, parse_jobs, Job, JobStore, StoreError};
use fs2::FileExt;

use crate::paths;

/// A [`JobStore`] backed by `<home>/jobs.json`.
#[derive(Debug, Clone)]
pub struct FileJobStore {
    home: PathBuf,
}

impl FileJobStore {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        FileJobStore { home: home.into() }
    }

    fn jobs_path(&self) -> PathBuf {
        paths::jobs_file(&self.home)
    }
}

impl JobStore for FileJobStore {
    fn load(&self) -> Result<Vec<Job>, StoreError> {
        let path = self.jobs_path();
        match fs::read_to_string(&path) {
            Ok(s) if s.trim().is_empty() => Ok(Vec::new()),
            Ok(s) => parse_jobs(&s)
                .map_err(|e| StoreError::new(format!("{}: {e}", path.display()))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(StoreError::new(format!("{}: {e}", path.display()))),
        }
    }

    fn save(&self, jobs: &[Job]) -> Result<(), StoreError> {
        ensure_secure_dir(&self.home)
            .map_err(|e| StoreError::new(format!("{}: {e}", self.home.display())))?;
        let json = jobs_to_json(jobs).map_err(|e| StoreError::new(e.to_string()))?;
        let path = self.jobs_path();
        atomic_write(&path, json.as_bytes())
            .map_err(|e| StoreError::new(format!("{}: {e}", path.display())))
    }
}

/// Create `dir` (and parents) if needed; on unix tighten it to `0700`.
pub fn ensure_secure_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Durably replace `target` with `bytes`: write a sibling temp file, `fsync` it,
/// rename over the target, then `fsync` the directory. On unix the temp file is
/// created `0600`.
pub fn atomic_write(target: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = target.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir)?;
    let stem = target
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("codex-cron");
    let tmp = dir.join(format!(".{stem}.tmp.{}", std::process::id()));

    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| {
        let mut f = opts.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, target)?;
        // Best-effort durability of the rename itself.
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// Held while a tick mutates the store; releasing it (drop) unlocks.
#[derive(Debug)]
pub struct TickLock {
    _file: File,
}

/// Try to take the tick lock without blocking. `Ok(None)` means another tick
/// already holds it — the caller should skip this pass.
pub fn try_acquire_tick_lock(home: &Path) -> io::Result<Option<TickLock>> {
    ensure_secure_dir(home)?;
    let path = paths::lock_file(home);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(TickLock { _file: file })),
        Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use codex_cron_core::{DeliveryTarget, ExecutorKind, NewJob, Repeat, Schedule};
    use tempfile::tempdir;

    fn sample_job(id: &str) -> Job {
        Job::new(
            NewJob {
                id: id.to_string(),
                name: id.to_string(),
                prompt: "p".to_string(),
                executor: ExecutorKind::Shell,
                script: Some("echo hi".to_string()),
                schedule: Schedule::Interval { minutes: 60 },
                schedule_display: "every 1h".to_string(),
                repeat: Repeat::default(),
                deliver: vec![DeliveryTarget::File],
                workdir: None,
                context_from: None,
                codex_model: None,
                event_loop: None,
            },
            Utc.with_ymd_and_hms(2026, 6, 1, 10, 0, 0).unwrap(),
        )
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let dir = tempdir().unwrap();
        let store = FileJobStore::new(dir.path());
        assert!(store.load().unwrap().is_empty());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempdir().unwrap();
        let store = FileJobStore::new(dir.path());
        let jobs = vec![sample_job("a"), sample_job("b")];
        store.save(&jobs).unwrap();
        assert_eq!(store.load().unwrap(), jobs);
    }

    #[test]
    fn save_overwrites_previous_contents() {
        let dir = tempdir().unwrap();
        let store = FileJobStore::new(dir.path());
        store.save(&[sample_job("a")]).unwrap();
        store.save(&[sample_job("b")]).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "b");
    }

    #[test]
    fn atomic_write_leaves_no_temp_file() {
        let dir = tempdir().unwrap();
        let store = FileJobStore::new(dir.path());
        store.save(&[sample_job("a")]).unwrap();
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "stray temp files: {leftovers:?}");
    }

    #[cfg(unix)]
    #[test]
    fn saved_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().unwrap();
        let store = FileJobStore::new(dir.path());
        store.save(&[sample_job("a")]).unwrap();
        let mode = fs::metadata(paths::jobs_file(dir.path()))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "mode was {mode:o}");
    }

    #[test]
    fn tick_lock_is_exclusive_then_reusable() {
        let dir = tempdir().unwrap();
        let first = try_acquire_tick_lock(dir.path()).unwrap();
        assert!(first.is_some(), "first acquire should succeed");
        // A second, independent handle must be refused while the first is held.
        assert!(
            try_acquire_tick_lock(dir.path()).unwrap().is_none(),
            "second acquire should be blocked"
        );
        drop(first);
        // Once released, it is available again.
        assert!(try_acquire_tick_lock(dir.path()).unwrap().is_some());
    }
}
