use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use codex_cron_core::{parse_event_loop_decision, EventLoopAction, EventLoopPolicy, JobStore};
use serde_json::json;

use crate::cli::run_target_tick;
use crate::store::{atomic_write, FileJobStore};

pub fn run_loop(home: &Path, id: &str, override_policy: Option<EventLoopPolicy>) -> Result<()> {
    let store = FileJobStore::new(home);
    let jobs = store.load()?;
    let job = jobs
        .iter()
        .find(|job| job.id == id)
        .with_context(|| format!("no job with id {id}"))?;
    let stored_policy = job.event_loop.clone();
    let policy = match override_policy {
        Some(mut policy) => {
            if policy.decision_file.is_none() {
                policy.decision_file = stored_policy
                    .as_ref()
                    .and_then(|stored| stored.decision_file.clone());
            }
            policy
        }
        None => stored_policy.context(
            "job is not configured for event-loop; add --event-loop or pass run-loop overrides",
        )?,
    };
    let decision_file = policy
        .decision_file
        .as_ref()
        .map(|path| resolve_decision_file(home, job.workdir.as_deref(), path));

    let started = Instant::now();
    let mut iterations = 0u32;
    let mut decisions = Vec::new();
    let mut status = "max_chain_reached".to_string();

    while iterations < policy.max_chain_runs
        && started.elapsed() < Duration::from_secs(policy.max_runtime_seconds)
    {
        {
            let mut jobs = store.load()?;
            let job = jobs
                .iter_mut()
                .find(|j| j.id == id)
                .with_context(|| format!("no job with id {id}"))?;
            job.enabled = true;
            job.state = codex_cron_core::JobState::Scheduled;
            job.next_run_at = Some(chrono::Utc::now());
            store.save(&jobs)?;
        }

        iterations += 1;
        let report = run_target_tick(home, id)?;
        let fired = report
            .fired
            .iter()
            .find(|item| item.id == id)
            .with_context(|| format!("event-loop target job {id} did not fire"))?;
        if fired.status.as_str() != "success" {
            status = "failed".to_string();
            decisions.push(json!({
                "iteration": iterations,
                "action": "fail",
                "reason": fired
                    .output
                    .error
                    .as_deref()
                    .unwrap_or_else(|| fired.status.as_str()),
                "run_status": fired.status.as_str(),
                "run_summary": &fired.output.summary,
                "run_error": &fired.output.error,
                "run_markdown_excerpt": excerpt(&fired.output.markdown, 2000)
            }));
            break;
        }

        let (decision, decision_source) = match decision_file.as_ref() {
            Some(path) => (
                parse_event_loop_decision_file(path),
                json!({
                    "decision_source": "file",
                    "decision_file": path.display().to_string()
                }),
            ),
            None => (
                parse_event_loop_decision(&fired.output.markdown),
                json!({
                    "decision_source": "output"
                }),
            ),
        };
        decisions.push(json!({
            "iteration": iterations,
            "action": format!("{:?}", decision.action).to_lowercase(),
            "reason": decision.reason,
            "next_task_id": decision.next_task_id,
            "decision_source": decision_source["decision_source"],
            "decision_file": decision_source.get("decision_file").cloned()
        }));

        match decision.action {
            EventLoopAction::Continue => continue,
            EventLoopAction::Stop => {
                status = "stopped".to_string();
                break;
            }
            EventLoopAction::Backoff => {
                status = "backoff".to_string();
                break;
            }
            EventLoopAction::Fail => {
                status = "failed".to_string();
                break;
            }
        }
    }

    if started.elapsed() >= Duration::from_secs(policy.max_runtime_seconds) {
        status = "max_runtime_reached".to_string();
    }

    let payload = json!({
        "schema_version": "codex-cron.event-loop-run.v1",
        "job_id": id,
        "status": status,
        "iterations": iterations,
        "max_chain_runs": policy.max_chain_runs,
        "max_runtime_seconds": policy.max_runtime_seconds,
        "decisions": decisions
    });
    let path = crate::paths::event_loop_latest(home, id);
    std::fs::create_dir_all(path.parent().unwrap())?;
    atomic_write(&path, serde_json::to_string_pretty(&payload)?.as_bytes())?;
    println!(
        "event-loop job {id}: status={} iterations={iterations}",
        payload["status"]
    );
    println!("summary={}", path.display());
    Ok(())
}

fn excerpt(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for ch in text.chars().take(max_chars) {
        out.push(ch);
    }
    if text.chars().count() > max_chars {
        out.push_str("\n...[truncated]");
    }
    out
}

fn resolve_decision_file(home: &Path, workdir: Option<&Path>, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(workdir) = workdir {
        workdir.join(path)
    } else {
        home.join(path)
    }
}

fn parse_event_loop_decision_file(path: &Path) -> codex_cron_core::EventLoopDecision {
    match std::fs::read_to_string(path) {
        Ok(text) => parse_event_loop_decision(&text),
        Err(error) => codex_cron_core::EventLoopDecision {
            action: EventLoopAction::Fail,
            reason: Some(format!(
                "event-loop decision file unavailable: {} ({})",
                path.display(),
                error
            )),
            next_task_id: None,
        },
    }
}
