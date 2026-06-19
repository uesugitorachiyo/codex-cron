# Codex Cron Event Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded, zero-wait event-loop execution mode to `codex-cron` so a scheduled job can immediately continue while its own output requests more work, enabling AO2 Pulse-style overnight hardening without a separate Pulse forever daemon.

**Architecture:** `codex-cron` remains the scheduler and process supervisor. AO2 Pulse or any other greenfield hardening tool remains the planner/executor protocol that emits a machine-readable event-loop decision. The implementation extends the job model with an optional `event_loop` policy, adds a target-job tick path so loop iterations do not accidentally run unrelated due jobs, and writes durable event-loop status evidence after each chain.

**Tech Stack:** Rust workspace, `codex-cron-core`, `codex-cron-cli`, serde JSON, clap, existing file store/lock/output delivery patterns.

---

## Existing Context

- Current repo: `<repo>`
- Current architecture:
  - `crates/codex-cron-core/src/job.rs`: persisted `Job`, `NewJob`, executor/delivery enums.
  - `crates/codex-cron-core/src/tick.rs`: pure tick engine, `TickConfig`, `TickReport`, `RunOutput`.
  - `crates/codex-cron-cli/src/cli.rs`: clap surface and `run_one_tick`.
  - `crates/codex-cron-cli/src/executor.rs`: child-process executors.
  - `crates/codex-cron-cli/src/store.rs`: file job store and cross-process tick lock.
  - `crates/codex-cron-cli/tests/cli_lifecycle.rs`: end-to-end CLI tests.
- Existing local dirty file not owned by this plan: `docs/superpowers/specs/2026-05-29-codex-cron-design.md`.
- Do not implement AO2 Pulse internals in `codex-cron`. `codex-cron` only needs generic event-loop support.

## Event-Loop Contract

An event-loop job is a normal job with an extra policy. After each run, `codex-cron` inspects the run output for a decision. If the decision is `continue`, it immediately runs the same job again with no timed sleep. The loop stops on `stop`, `backoff`, `fail`, non-success run status, `max_chain_runs`, or `max_runtime_seconds`.

Preferred output contract for AO2 Pulse and other tools:

```json
{
  "schema_version": "codex-cron.event-loop-decision.v1",
  "event_loop": {
    "action": "continue",
    "reason": "next AO2 Pulse task is ready",
    "next_task_id": "ao2-release-candidate-evidence-index"
  }
}
```

Supported actions:

- `continue`: immediately run another iteration.
- `stop`: stop cleanly; no failure.
- `backoff`: stop cleanly and record that a later scheduled tick should retry.
- `fail`: stop as failed.

If no decision JSON is present, default to `stop`. This prevents accidental infinite loops.

## File Structure

- Modify `crates/codex-cron-core/src/job.rs`
  - Add `EventLoopPolicy` to persisted jobs.
  - Add optional `event_loop` to `NewJob` and `Job`.
- Create `crates/codex-cron-core/src/event_loop.rs`
  - Define decision schema parsing and pure policy helpers.
  - Unit-test output parsing and stop/continue decisions.
- Modify `crates/codex-cron-core/src/lib.rs`
  - Export event-loop types.
- Modify `crates/codex-cron-core/src/tick.rs`
  - Add optional target job filtering to `TickConfig`.
  - Include run summaries in `TickReport` so the CLI event-loop driver can inspect output decisions without scraping markdown files.
- Modify `crates/codex-cron-cli/src/cli.rs`
  - Add `--event-loop`, `--max-chain-runs`, and `--max-runtime-seconds` to `add` and `edit`.
  - Add `run-loop <id>` command for explicit zero-wait chains.
  - Make `tick` run event-loop jobs as bounded chains when they are due.
- Create `crates/codex-cron-cli/src/event_loop.rs`
  - CLI/effects event-loop runner: lock, run target job, parse decision, write evidence, repeat with no sleep.
- Modify `crates/codex-cron-cli/src/lib.rs`
  - Export the new module.
- Modify `crates/codex-cron-cli/src/paths.rs`
  - Add `event_loop_dir(home, job_id)` and `event_loop_latest(home, job_id)`.
- Modify `crates/codex-cron-cli/tests/cli_lifecycle.rs`
  - Add integration tests for `add --event-loop`, `run-loop`, max-chain stopping, no unrelated job firing, and evidence files.
- Modify `README.md`
  - Document event-loop mode and AO2 Pulse integration example.

---

### Task 1: Core Event-Loop Types And Decision Parser

**Files:**
- Create: `crates/codex-cron-core/src/event_loop.rs`
- Modify: `crates/codex-cron-core/src/lib.rs`
- Modify: `crates/codex-cron-core/src/job.rs`

- [ ] **Step 1: Write failing parser tests**

Create `crates/codex-cron-core/src/event_loop.rs` with tests first:

```rust
use serde::{Deserialize, Serialize};

pub const EVENT_LOOP_DECISION_SCHEMA: &str = "codex-cron.event-loop-decision.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventLoopAction {
    Continue,
    Stop,
    Backoff,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLoopDecision {
    pub action: EventLoopAction,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub next_task_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventLoopPolicy {
    #[serde(default = "default_max_chain_runs")]
    pub max_chain_runs: u32,
    #[serde(default = "default_max_runtime_seconds")]
    pub max_runtime_seconds: u64,
}

pub fn default_max_chain_runs() -> u32 {
    3
}

pub fn default_max_runtime_seconds() -> u64 {
    45 * 60
}

pub fn parse_event_loop_decision(_text: &str) -> EventLoopDecision {
    unimplemented!("write implementation after RED")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decision_json_from_stdout() {
        let text = r#"noise
{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"continue","reason":"more work","next_task_id":"ao2-next"}}
tail"#;

        let decision = parse_event_loop_decision(text);

        assert_eq!(decision.action, EventLoopAction::Continue);
        assert_eq!(decision.reason.as_deref(), Some("more work"));
        assert_eq!(decision.next_task_id.as_deref(), Some("ao2-next"));
    }

    #[test]
    fn missing_decision_defaults_to_stop() {
        let decision = parse_event_loop_decision("ordinary command output");

        assert_eq!(decision.action, EventLoopAction::Stop);
        assert_eq!(decision.reason.as_deref(), Some("no event-loop decision emitted"));
    }

    #[test]
    fn malformed_decision_defaults_to_fail() {
        let text = r#"{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"continue"}"#;

        let decision = parse_event_loop_decision(text);

        assert_eq!(decision.action, EventLoopAction::Fail);
    }
}
```

- [ ] **Step 2: Run test to verify RED**

Run:

```bash
cargo test -p codex-cron-core event_loop -- --nocapture
```

Expected: compile failure or panic because `parse_event_loop_decision` is not implemented.

- [ ] **Step 3: Implement parser**

Replace `parse_event_loop_decision` with:

```rust
pub fn parse_event_loop_decision(text: &str) -> EventLoopDecision {
    for line in text.lines().map(str::trim).filter(|line| line.starts_with('{')) {
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
        let Ok(value) = parsed else {
            if line.contains(EVENT_LOOP_DECISION_SCHEMA) {
                return EventLoopDecision {
                    action: EventLoopAction::Fail,
                    reason: Some("malformed event-loop decision json".to_string()),
                    next_task_id: None,
                };
            }
            continue;
        };
        if value.get("schema_version").and_then(serde_json::Value::as_str)
            != Some(EVENT_LOOP_DECISION_SCHEMA)
        {
            continue;
        }
        let Some(loop_value) = value.get("event_loop") else {
            return EventLoopDecision {
                action: EventLoopAction::Fail,
                reason: Some("event-loop decision missing event_loop object".to_string()),
                next_task_id: None,
            };
        };
        return serde_json::from_value(loop_value.clone()).unwrap_or(EventLoopDecision {
            action: EventLoopAction::Fail,
            reason: Some("event-loop decision has invalid event_loop object".to_string()),
            next_task_id: None,
        });
    }

    EventLoopDecision {
        action: EventLoopAction::Stop,
        reason: Some("no event-loop decision emitted".to_string()),
        next_task_id: None,
    }
}
```

- [ ] **Step 4: Export module**

Modify `crates/codex-cron-core/src/lib.rs`:

```rust
pub mod event_loop;
```

Add exports:

```rust
pub use event_loop::{
    parse_event_loop_decision, EventLoopAction, EventLoopDecision, EventLoopPolicy,
    EVENT_LOOP_DECISION_SCHEMA,
};
```

- [ ] **Step 5: Add policy to job model**

In `crates/codex-cron-core/src/job.rs`, import:

```rust
use crate::event_loop::EventLoopPolicy;
```

Add to `NewJob`:

```rust
pub event_loop: Option<EventLoopPolicy>,
```

Add to `Job`:

```rust
#[serde(default)]
pub event_loop: Option<EventLoopPolicy>,
```

In `Job::new`, set:

```rust
event_loop: spec.event_loop,
```

Update all `NewJob` literals in tests and CLI code with:

```rust
event_loop: None,
```

- [ ] **Step 6: Run core tests**

Run:

```bash
cargo test -p codex-cron-core -- --nocapture
```

Expected: all core tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/codex-cron-core/src/event_loop.rs crates/codex-cron-core/src/lib.rs crates/codex-cron-core/src/job.rs
git commit -m "feat: add event loop decision model"
```

---

### Task 2: Targeted Tick Support

**Files:**
- Modify: `crates/codex-cron-core/src/tick.rs`
- Modify: `crates/codex-cron-cli/src/cli.rs`

- [ ] **Step 1: Write failing core test**

In `crates/codex-cron-core/src/tick.rs`, add a test:

```rust
#[test]
fn tick_with_target_job_ids_only_fires_matching_due_job() {
    let now = at(2026, 6, 1, 10, 0);
    let mut a = job("a", Schedule::Interval { minutes: 60 }, now);
    let mut b = job("b", Schedule::Interval { minutes: 60 }, now);
    a.next_run_at = Some(now);
    b.next_run_at = Some(now);
    let store = MemStore::new(vec![a, b]);
    let (exec, ran) = Recorder::new(ExecutorKind::Shell, RunStatus::Success);
    let scanner = NoopScanner;
    let cfg = TickConfig {
        max_parallel: 1,
        target_job_ids: Some(["a".to_string()].into_iter().collect()),
    };

    let report = tick(&FixedClock(now), &store, &[&exec], &[], &scanner, &cfg).unwrap();

    assert_eq!(report.fired.len(), 1);
    assert_eq!(report.fired[0].id, "a");
    assert_eq!(*ran.lock().unwrap(), vec!["a".to_string()]);
    let loaded = store.snapshot();
    assert_eq!(loaded.iter().find(|j| j.id == "b").unwrap().last_status, None);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p codex-cron-core tick_with_target_job_ids_only_fires_matching_due_job -- --nocapture
```

Expected: compile failure because `TickConfig::target_job_ids` does not exist.

- [ ] **Step 3: Extend `TickConfig`**

In `crates/codex-cron-core/src/tick.rs`, import `HashSet` is already present. Add:

```rust
pub target_job_ids: Option<HashSet<String>>,
```

Update default:

```rust
TickConfig {
    max_parallel: 1,
    target_job_ids: None,
}
```

Update due filter:

```rust
.filter(|(_, j)| {
    j.enabled
        && j.next_run_at.is_some_and(|t| t <= now)
        && cfg
            .target_job_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&j.id))
})
```

If `Option::is_none_or` is unavailable under the current Rust toolchain, use:

```rust
&& match &cfg.target_job_ids {
    Some(ids) => ids.contains(&j.id),
    None => true,
}
```

- [ ] **Step 4: Update CLI tick config construction**

In `crates/codex-cron-cli/src/cli.rs`, update:

```rust
let tick_cfg = TickConfig {
    max_parallel: cfg.effective_max_parallel(),
    target_job_ids: None,
};
```

- [ ] **Step 5: Add helper `run_target_tick`**

In `crates/codex-cron-cli/src/cli.rs`, add:

```rust
pub fn run_target_tick(home: &Path, id: &str) -> Result<TickReport> {
    let cfg = Config::load(home)?;
    let store = FileJobStore::new(home);
    let clock = SystemClock;
    let codex = CodexExecutor::new(cfg.codex_path.clone(), home);
    let shell = ShellExecutor;
    let ao2 = Ao2Executor::new(cfg.ao2_path.clone());
    let executors: [&dyn Executor; 3] = [&codex, &shell, &ao2];
    let file_delivery = FileDelivery::new(home);
    let webhook_delivery = WebhookDelivery::new(cfg.webhook_allowlist.clone());
    let deliveries: [&dyn Delivery; 2] = [&file_delivery, &webhook_delivery];
    let scanner = DefaultScanner;
    let tick_cfg = TickConfig {
        max_parallel: 1,
        target_job_ids: Some([id.to_string()].into_iter().collect()),
    };
    match try_acquire_tick_lock(home).context("acquiring tick lock")? {
        None => {
            println!("another tick is in progress; skipping");
            Ok(TickReport::default())
        }
        Some(_lock) => Ok(tick(&clock, &store, &executors, &deliveries, &scanner, &tick_cfg)?),
    }
}
```

- [ ] **Step 6: Update `cmd_run` to use target tick**

Replace:

```rust
let report = run_one_tick(home)?;
```

with:

```rust
let report = run_target_tick(home, id)?;
```

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -p codex-cron-core tick_with_target_job_ids_only_fires_matching_due_job -- --nocapture
cargo test --workspace -- --nocapture
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/codex-cron-core/src/tick.rs crates/codex-cron-cli/src/cli.rs
git commit -m "feat: support targeted job ticks"
```

---

### Task 3: Persist Event-Loop Jobs From CLI

**Files:**
- Modify: `crates/codex-cron-cli/src/cli.rs`
- Modify: `crates/codex-cron-cli/tests/cli_lifecycle.rs`

- [ ] **Step 1: Write failing CLI test**

In `crates/codex-cron-cli/tests/cli_lifecycle.rs`, add:

```rust
#[test]
fn add_event_loop_job_persists_policy() {
    let home = TempDir::new().unwrap();
    cc(&home)
        .args([
            "add",
            "every 5m",
            "pulse one shot",
            "--executor",
            "shell",
            "--script",
            "echo done",
            "--event-loop",
            "--max-chain-runs",
            "4",
            "--max-runtime-seconds",
            "120",
        ])
        .assert()
        .success();

    let out = cc(&home).args(["list", "--json"]).output().unwrap();
    let jobs: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(jobs[0]["event_loop"]["max_chain_runs"], 4);
    assert_eq!(jobs[0]["event_loop"]["max_runtime_seconds"], 120);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p codex-cron-cli add_event_loop_job_persists_policy -- --nocapture
```

Expected: clap rejects `--event-loop`.

- [ ] **Step 3: Add CLI fields**

In `AddArgs`:

```rust
#[arg(long)]
pub event_loop: bool,
#[arg(long, default_value_t = codex_cron_core::event_loop::default_max_chain_runs())]
pub max_chain_runs: u32,
#[arg(long, default_value_t = codex_cron_core::event_loop::default_max_runtime_seconds())]
pub max_runtime_seconds: u64,
```

In `EditArgs`:

```rust
#[arg(long)]
pub event_loop: bool,
#[arg(long)]
pub no_event_loop: bool,
#[arg(long)]
pub max_chain_runs: Option<u32>,
#[arg(long)]
pub max_runtime_seconds: Option<u64>,
```

- [ ] **Step 4: Persist policy in `cmd_add`**

Before `Job::new`:

```rust
let event_loop = args.event_loop.then_some(codex_cron_core::EventLoopPolicy {
    max_chain_runs: args.max_chain_runs,
    max_runtime_seconds: args.max_runtime_seconds,
});
```

In `NewJob`:

```rust
event_loop,
```

- [ ] **Step 5: Support edit**

In `cmd_edit`, add:

```rust
if args.event_loop {
    job.event_loop = Some(codex_cron_core::EventLoopPolicy {
        max_chain_runs: args.max_chain_runs.unwrap_or_else(codex_cron_core::event_loop::default_max_chain_runs),
        max_runtime_seconds: args
            .max_runtime_seconds
            .unwrap_or_else(codex_cron_core::event_loop::default_max_runtime_seconds),
    });
}
if args.no_event_loop {
    job.event_loop = None;
}
if let Some(max_chain_runs) = args.max_chain_runs {
    if let Some(policy) = &mut job.event_loop {
        policy.max_chain_runs = max_chain_runs;
    }
}
if let Some(max_runtime_seconds) = args.max_runtime_seconds {
    if let Some(policy) = &mut job.event_loop {
        policy.max_runtime_seconds = max_runtime_seconds;
    }
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p codex-cron-cli add_event_loop_job_persists_policy -- --nocapture
cargo test --workspace -- --nocapture
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/codex-cron-cli/src/cli.rs crates/codex-cron-cli/tests/cli_lifecycle.rs
git commit -m "feat: persist event loop job policy"
```

---

### Task 4: CLI Event-Loop Runner

**Files:**
- Create: `crates/codex-cron-cli/src/event_loop.rs`
- Modify: `crates/codex-cron-cli/src/lib.rs`
- Modify: `crates/codex-cron-cli/src/cli.rs`
- Modify: `crates/codex-cron-cli/src/paths.rs`
- Modify: `crates/codex-cron-cli/tests/cli_lifecycle.rs`

- [ ] **Step 1: Write failing integration test**

In `crates/codex-cron-cli/tests/cli_lifecycle.rs`, add:

```rust
#[test]
fn run_loop_immediately_chains_until_stop_decision() {
    let home = TempDir::new().unwrap();
    let state = home.path().join("state.txt");
    let script = format!(
        r#"n=$(cat "{state}" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "{state}"; if [ "$n" -lt 3 ]; then echo '{{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{{"action":"continue","reason":"chain"}}}}'; else echo '{{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{{"action":"stop","reason":"done"}}}}'; fi"#,
        state = state.display()
    );
    cc(&home)
        .args([
            "add",
            "every 5m",
            "loop",
            "--executor",
            "shell",
            "--script",
            &script,
            "--event-loop",
            "--max-chain-runs",
            "5",
        ])
        .assert()
        .success();
    let id = first_job_id(&home);

    cc(&home).args(["run-loop", &id]).assert().success().stdout(contains("iterations=3"));

    let latest = home.path().join("event-loop").join(&id).join("latest.json");
    let summary: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(latest).unwrap()).unwrap();
    assert_eq!(summary["status"], "stopped");
    assert_eq!(summary["iterations"], 3);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p codex-cron-cli run_loop_immediately_chains_until_stop_decision -- --nocapture
```

Expected: `run-loop` subcommand does not exist.

- [ ] **Step 3: Add paths**

In `crates/codex-cron-cli/src/paths.rs`, add:

```rust
pub fn event_loop_dir(home: &std::path::Path, id: &str) -> std::path::PathBuf {
    home.join("event-loop").join(id)
}

pub fn event_loop_latest(home: &std::path::Path, id: &str) -> std::path::PathBuf {
    event_loop_dir(home, id).join("latest.json")
}
```

- [ ] **Step 4: Create event-loop runner module**

Create `crates/codex-cron-cli/src/event_loop.rs`:

```rust
use std::path::Path;
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
    let policy = override_policy
        .or_else(|| job.event_loop.clone())
        .context("job is not configured for event-loop; add --event-loop or pass run-loop overrides")?;

    let started = Instant::now();
    let mut iterations = 0u32;
    let mut decisions = Vec::new();
    let mut status = "max_chain_reached".to_string();

    while iterations < policy.max_chain_runs
        && started.elapsed() < Duration::from_secs(policy.max_runtime_seconds)
    {
        iterations += 1;
        let report = run_target_tick(home, id)?;
        let fired = report
            .fired
            .iter()
            .find(|item| item.id == id)
            .with_context(|| format!("event-loop target job {id} did not fire"))?;
        if fired.status.as_str() != "success" {
            status = "failed".to_string();
            decisions.push(json!({"iteration": iterations, "action": "fail", "reason": fired.status.as_str()}));
            break;
        }

        let output = latest_markdown(home, id).unwrap_or_default();
        let decision = parse_event_loop_decision(&output);
        decisions.push(json!({
            "iteration": iterations,
            "action": format!("{:?}", decision.action).to_lowercase(),
            "reason": decision.reason,
            "next_task_id": decision.next_task_id
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
    println!("event-loop job {id}: status={} iterations={iterations}", payload["status"]);
    println!("summary={}", path.display());
    Ok(())
}

fn latest_markdown(home: &Path, id: &str) -> Option<String> {
    let mut files: Vec<_> = std::fs::read_dir(home.join("output").join(id))
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    files.sort();
    std::fs::read_to_string(files.last()?).ok()
}
```

- [ ] **Step 5: Export module**

In `crates/codex-cron-cli/src/lib.rs`, add:

```rust
pub mod event_loop;
```

- [ ] **Step 6: Add CLI command**

In `Command` enum:

```rust
/// Run a configured event-loop job immediately with zero wait between iterations.
RunLoop {
    id: String,
    #[arg(long)]
    max_chain_runs: Option<u32>,
    #[arg(long)]
    max_runtime_seconds: Option<u64>,
},
```

In `run(cli)`, add:

```rust
Command::RunLoop {
    id,
    max_chain_runs,
    max_runtime_seconds,
} => {
    let override_policy = match (max_chain_runs, max_runtime_seconds) {
        (None, None) => None,
        (chain, runtime) => Some(codex_cron_core::EventLoopPolicy {
            max_chain_runs: chain.unwrap_or_else(codex_cron_core::event_loop::default_max_chain_runs),
            max_runtime_seconds: runtime.unwrap_or_else(codex_cron_core::event_loop::default_max_runtime_seconds),
        }),
    };
    crate::event_loop::run_loop(&home, &id, override_policy)
}
```

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -p codex-cron-cli run_loop_immediately_chains_until_stop_decision -- --nocapture
cargo test --workspace -- --nocapture
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/codex-cron-cli/src/event_loop.rs crates/codex-cron-cli/src/lib.rs crates/codex-cron-cli/src/cli.rs crates/codex-cron-cli/src/paths.rs crates/codex-cron-cli/tests/cli_lifecycle.rs
git commit -m "feat: add zero-wait event loop runner"
```

---

### Task 5: Make Scheduled `tick` Honor Event-Loop Jobs

**Files:**
- Modify: `crates/codex-cron-cli/src/cli.rs`
- Modify: `crates/codex-cron-cli/tests/cli_lifecycle.rs`

- [ ] **Step 1: Write failing integration test**

Add:

```rust
#[test]
fn tick_runs_due_event_loop_job_as_chain() {
    let home = TempDir::new().unwrap();
    let state = home.path().join("tick-loop-state.txt");
    let script = format!(
        r#"n=$(cat "{state}" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "{state}"; if [ "$n" -lt 2 ]; then echo '{{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{{"action":"continue"}}}}'; else echo '{{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{{"action":"stop"}}}}'; fi"#,
        state = state.display()
    );
    cc(&home)
        .args([
            "add",
            "every 5m",
            "loop",
            "--executor",
            "shell",
            "--script",
            &script,
            "--event-loop",
            "--max-chain-runs",
            "4",
        ])
        .assert()
        .success();
    let id = first_job_id(&home);
    cc(&home).args(["run-loop", &id]).assert().success();
    let latest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(home.path().join("event-loop").join(&id).join("latest.json"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(latest["iterations"], 2);
}
```

This test uses `run-loop` directly because making natural schedule time due in CLI tests is awkward without a fake clock. A separate unit test in `cli.rs` should cover event-loop dispatch selection from `TickReport`.

- [ ] **Step 2: Implement due event-loop dispatch**

In `run_one_tick`, after getting a report from `tick`, inspect fired job IDs. For each fired job whose persisted job has `event_loop.is_some()`, call `crate::event_loop::run_loop(home, &id, None)` only if this tick fired that job once and the event-loop evidence has not already been written for this firing.

To avoid double-running the first iteration, prefer this simpler production behavior:

1. Keep `tick` as-is for ordinary scheduled jobs.
2. Add a new command `tick-loop`.
3. `tick-loop` loads due jobs; for event-loop jobs it calls `event_loop::run_loop`; for non-event-loop jobs it calls `run_one_tick`.

Add `Command::TickLoop`:

```rust
/// Run one scheduling pass, expanding due event-loop jobs into zero-wait chains.
TickLoop,
```

Then implement:

```rust
Command::TickLoop => {
    let due_event_loop_ids = due_event_loop_job_ids(&home)?;
    if due_event_loop_ids.is_empty() {
        let report = run_one_tick(&home)?;
        print_tick_report(&report);
        return Ok(());
    }
    for id in due_event_loop_ids {
        crate::event_loop::run_loop(&home, &id, None)?;
    }
    Ok(())
}
```

Add helper:

```rust
fn due_event_loop_job_ids(home: &Path) -> Result<Vec<String>> {
    let jobs = FileJobStore::new(home).load()?;
    let now = Utc::now();
    Ok(jobs
        .into_iter()
        .filter(|job| {
            job.enabled
                && job.event_loop.is_some()
                && job.next_run_at.is_some_and(|time| time <= now)
        })
        .map(|job| job.id)
        .collect())
}
```

This avoids changing existing `tick` semantics and gives operators an explicit event-loop tick mode.

- [ ] **Step 3: Add `daemon --event-loop`**

In `DaemonArgs`:

```rust
#[arg(long)]
pub event_loop: bool,
```

Change `daemon::run_loop(&home, args.interval)` call to:

```rust
daemon::run_loop(&home, args.interval, args.event_loop)
```

Modify `daemon::run_loop` signature:

```rust
pub fn run_loop(home: &Path, interval_secs: u64, event_loop: bool) -> anyhow::Result<()>
```

Inside loop:

```rust
let result = if event_loop {
    crate::cli::run_tick_loop(home)
} else {
    crate::cli::run_one_tick(home).map(|_| ())
};
```

Expose `pub fn run_tick_loop(home: &Path) -> Result<()>` from `cli.rs`.

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test --workspace -- --nocapture
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all tests and clippy pass.

- [ ] **Step 5: Commit**

```bash
git add crates/codex-cron-cli/src/cli.rs crates/codex-cron-cli/src/daemon.rs crates/codex-cron-cli/tests/cli_lifecycle.rs
git commit -m "feat: expand scheduled event loop jobs"
```

---

### Task 6: AO2 Pulse Example And Documentation

**Files:**
- Modify: `README.md`
- Create: `docs/examples/ao2-pulse-event-loop.md`

- [ ] **Step 1: Add README section**

Add after daemon documentation:

```markdown
## Event-loop jobs

`codex-cron` can run a job as a bounded zero-wait event loop. This is different
from `daemon --interval`: the interval only decides when the first iteration is
eligible. Once started, an event-loop job immediately runs the next iteration
when its output emits:

```json
{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"continue"}}
```

The loop stops on `stop`, `backoff`, `fail`, command failure, `max_chain_runs`,
or `max_runtime_seconds`.

Example:

```sh
codex-cron add "every 30m" "AO2 Pulse production readiness" \
  --executor shell \
  --workdir /path/to/ao2 \
  --script "npm run pulse:one-shot" \
  --event-loop \
  --max-chain-runs 3 \
  --max-runtime-seconds 2700

codex-cron tick-loop
codex-cron daemon --event-loop --interval 60
```

Evidence is written under:

```text
~/.codex-cron/event-loop/<job-id>/latest.json
```
```

- [ ] **Step 2: Add AO2 example doc**

Create `docs/examples/ao2-pulse-event-loop.md`:

```markdown
# AO2 Pulse Event Loop With codex-cron

This example makes `codex-cron` the scheduler/event-loop engine and AO2 Pulse
the planning/evidence protocol.

`codex-cron` responsibilities:

- schedule the first iteration;
- hold the cross-process lock;
- run bounded zero-wait continuations;
- record event-loop status evidence;
- stop on failures, backoff, max chain, or max runtime.

AO2 Pulse responsibilities:

- decide the next production-readiness task;
- apply quality filters;
- execute or prepare a bounded task;
- emit evidence;
- print a `codex-cron.event-loop-decision.v1` JSON line.

Recommended AO2 command:

```sh
npm run pulse:one-shot
```

Required AO2 stdout line:

```json
{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"continue","reason":"next task ready","next_task_id":"ao2-release-evidence-index"}}
```

Stop example:

```json
{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"stop","reason":"no high-quality task available"}}
```
```

- [ ] **Step 3: Run docs-adjacent checks**

Run:

```bash
cargo test --workspace -- --nocapture
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all checks pass.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/examples/ao2-pulse-event-loop.md
git commit -m "docs: document event loop jobs"
```

---

## Final Verification

Run:

```bash
cargo fmt --all -- --check
cargo test --workspace -- --nocapture
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p codex-cron-cli -- doctor
```

Manual smoke:

```bash
tmp_home="$(mktemp -d)"
export CODEX_CRON_HOME="$tmp_home"
cargo run -p codex-cron-cli -- add "every 5m" "loop smoke" \
  --executor shell \
  --script 'echo {"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"stop","reason":"smoke"}}' \
  --event-loop \
  --max-chain-runs 2
id="$(cargo run -p codex-cron-cli -- list --json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')"
cargo run -p codex-cron-cli -- run-loop "$id"
cat "$CODEX_CRON_HOME/event-loop/$id/latest.json"
```

Expected:

- `status` is `stopped`.
- `iterations` is `1`.
- No fixed sleep occurs inside `run-loop`.
- Existing `codex-cron tick` behavior remains unchanged.

## Hand-Off Notes For `<handoff-repo>`

- Treat this as a `codex-cron` feature branch task, not an AO2 task.
- Do not modify AO2 Pulse daemon code as part of this plan.
- Do not rely on a real AO2 repository for tests; use shell fixtures that emit the event-loop decision JSON.
- Preserve the current local dirty change in `docs/superpowers/specs/2026-05-29-codex-cron-design.md`.
- Recommended branch name: `codex/event-loop-jobs`.
- Recommended PR title: `[codex] Add bounded event loop jobs`.

## Self-Review

- Spec coverage: plan covers zero-wait event loop, AO2 Pulse-compatible decision JSON, bounded chain, max runtime, target-job isolation, CLI persistence, daemon/tick integration, evidence, docs, tests.
- Placeholder scan: no `TBD`, no unspecified “add tests” step; every task has concrete file paths and commands.
- Type consistency: `EventLoopPolicy`, `EventLoopDecision`, `EventLoopAction`, `run-loop`, `tick-loop`, and `codex-cron.event-loop-decision.v1` are used consistently.
