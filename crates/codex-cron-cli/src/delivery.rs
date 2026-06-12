//! Delivery sinks: the always-on per-run file, and an optional webhook POST.

use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::Utc;
use codex_cron_core::{Delivery, DeliveryError, DeliveryTarget, Job, RunOutput};

use crate::paths;
use crate::store::{atomic_write, ensure_secure_dir};

fn de(e: impl std::fmt::Display) -> DeliveryError {
    DeliveryError::new(e.to_string())
}

/// Writes each run's Markdown to `output/<id>/<ts>.md` and appends a line to
/// `output/<id>/runs.jsonl`. Always on, regardless of `job.deliver`.
#[derive(Debug, Clone)]
pub struct FileDelivery {
    home: PathBuf,
}

impl FileDelivery {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        FileDelivery { home: home.into() }
    }
}

impl Delivery for FileDelivery {
    fn deliver(&self, job: &Job, out: &RunOutput) -> Result<(), DeliveryError> {
        let now = Utc::now();
        let stamp = now.format("%Y-%m-%dT%H-%M-%SZ").to_string();
        let dir = paths::job_output_dir(&self.home, &job.id);
        ensure_secure_dir(&dir).map_err(de)?;

        let md_path = paths::run_md(&self.home, &job.id, &stamp);
        atomic_write(&md_path, out.markdown.as_bytes()).map_err(de)?;

        let line = serde_json::json!({
            "ts": now.to_rfc3339(),
            "status": out.status.as_str(),
            "summary": out.summary,
            "file": md_path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
        })
        .to_string();
        append_line(&paths::runs_log(&self.home, &job.id), &line).map_err(de)?;
        Ok(())
    }
}

fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    writeln!(f, "{line}")
}

/// POSTs run output to each `Webhook { url }` in `job.deliver`, with a short
/// bounded retry. An optional host allowlist guards against SSRF.
#[derive(Debug, Clone)]
pub struct WebhookDelivery {
    allowlist: Vec<String>,
    client: reqwest::blocking::Client,
}

impl WebhookDelivery {
    pub fn new(allowlist: Vec<String>) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        WebhookDelivery { allowlist, client }
    }

    fn allowed(&self, url: &str) -> bool {
        if self.allowlist.is_empty() {
            return true;
        }
        match host_of(url) {
            Some(host) => self.allowlist.iter().any(|h| h == &host),
            None => false,
        }
    }

    fn post_with_retry(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
        let mut last = String::new();
        for attempt in 0..3 {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_millis(200 * attempt as u64));
            }
            match self.client.post(url).json(body).send() {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => last = format!("HTTP {}", resp.status()),
                Err(e) => last = e.to_string(),
            }
        }
        Err(last)
    }
}

impl Delivery for WebhookDelivery {
    fn deliver(&self, job: &Job, out: &RunOutput) -> Result<(), DeliveryError> {
        let mut error: Option<String> = None;
        for target in &job.deliver {
            let DeliveryTarget::Webhook { url } = target else {
                continue;
            };
            if !self.allowed(url) {
                error = Some(format!("webhook blocked by allowlist: {url}"));
                continue;
            }
            let payload = serde_json::json!({
                "job_id": job.id,
                "name": job.name,
                "status": out.status.as_str(),
                "output_md": out.markdown,
                "ts": Utc::now().to_rfc3339(),
            });
            if let Err(e) = self.post_with_retry(url, &payload) {
                error = Some(format!("{url}: {e}"));
            }
        }
        match error {
            Some(e) => Err(DeliveryError::new(e)),
            None => Ok(()),
        }
    }
}

/// Extract the host from a URL without pulling in the `url` crate.
fn host_of(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // Strip userinfo and port.
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    let host = host_port.split(':').next().unwrap_or(host_port);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_cron_core::RunStatus;
    use std::io::{BufRead, BufReader, Read};
    use std::net::TcpListener;
    use tempfile::tempdir;

    fn output() -> RunOutput {
        RunOutput {
            status: RunStatus::Success,
            summary: "ok".to_string(),
            markdown: "# Run\n\nhello\n".to_string(),
            error: None,
        }
    }

    fn job_with(deliver: Vec<DeliveryTarget>) -> Job {
        use chrono::TimeZone;
        use codex_cron_core::{ExecutorKind, NewJob, Repeat, Schedule};
        Job::new(
            NewJob {
                id: "job1".to_string(),
                name: "n".to_string(),
                prompt: "p".to_string(),
                executor: ExecutorKind::Shell,
                script: None,
                schedule: Schedule::Interval { minutes: 60 },
                schedule_display: "every 1h".to_string(),
                repeat: Repeat::default(),
                deliver,
                workdir: None,
                context_from: None,
                codex_model: None,
                event_loop: None,
            },
            Utc.with_ymd_and_hms(2026, 6, 1, 10, 0, 0).unwrap(),
        )
    }

    #[test]
    fn host_of_extracts_host() {
        assert_eq!(
            host_of("https://example.com/x").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            host_of("http://user@1.2.3.4:8080/p").as_deref(),
            Some("1.2.3.4")
        );
        assert_eq!(host_of("not a url"), Some("not a url".to_string()));
    }

    #[test]
    fn file_delivery_writes_md_and_jsonl() {
        let dir = tempdir().unwrap();
        let d = FileDelivery::new(dir.path());
        let job = job_with(vec![DeliveryTarget::File]);
        d.deliver(&job, &output()).unwrap();

        let out_dir = paths::job_output_dir(dir.path(), "job1");
        let mds: Vec<_> = std::fs::read_dir(&out_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".md"))
            .collect();
        assert_eq!(mds.len(), 1, "expected one md, got {mds:?}");

        let log = std::fs::read_to_string(paths::runs_log(dir.path(), "job1")).unwrap();
        assert_eq!(log.lines().count(), 1);
        assert!(log.contains("\"status\":\"success\""), "got {log}");
    }

    #[test]
    fn webhook_allowlist_blocks_disallowed_host() {
        let d = WebhookDelivery::new(vec!["allowed.example".to_string()]);
        let job = job_with(vec![DeliveryTarget::Webhook {
            url: "http://127.0.0.1:9/hook".to_string(),
        }]);
        let err = d.deliver(&job, &output()).unwrap_err();
        assert!(err.to_string().contains("allowlist"), "got {err}");
    }

    #[test]
    fn webhook_posts_payload_to_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(sock.try_clone().unwrap());
            // Read headers, find Content-Length.
            let mut len = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(v) = line.to_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0u8; len];
            reader.read_exact(&mut body).unwrap();
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .unwrap();
            String::from_utf8_lossy(&body).into_owned()
        });

        let d = WebhookDelivery::new(vec![]);
        let job = job_with(vec![DeliveryTarget::Webhook {
            url: format!("http://127.0.0.1:{port}/hook"),
        }]);
        d.deliver(&job, &output()).unwrap();

        let body = handle.join().unwrap();
        assert!(body.contains("\"job_id\":\"job1\""), "got {body}");
        assert!(body.contains("\"status\":\"success\""), "got {body}");
    }
}
