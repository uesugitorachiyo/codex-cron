# codex-cron — Design Spec

Date: 2026-05-29
Status: Approved (brainstorming gate passed)
Author: Claude (Opus 4.8)

## 1. Summary

`codex-cron` is a durable, governed cron scheduler for agent jobs, written in
Rust, modeled on the cron subsystem of NousResearch/hermes-agent. It schedules
recurring or one-shot jobs and, when each job fires, runs it through a pluggable
executor: the OpenAI `codex` CLI agent (default), a raw shell script, or a
governed `ao2 run`. Results are persisted as per-run Markdown and delivered to a
local file (always) and an optional webhook.

It is a standalone project that **shells out** to the `codex` and `ao2`
binaries. It does **not** edit or depend on the ao2 source tree; ao2 stays
untouched. This is the external "queue / cron / bookkeeping" layer that ao2's
own schemas explicitly leave to an outside orchestrator.

## 2. Goals / Non-goals

### Goals (v1)
- Reproduce the *correctness-critical* behavior of hermes-agent's cron:
  - Three schedule kinds: `interval` ("every 30m"), `cron` (5/6-field), `once`
    (bare duration or ISO timestamp).
  - Durable JSON job store with atomic writes.
  - A tick engine that fires due jobs, guarded by a cross-process lock.
  - **At-most-once** firing: advance `next_run_at` and persist *before* running.
  - **No drift**: next run anchored to the scheduled time / last run.
  - **No burst after downtime**: fast-forward stale recurring jobs to the next
    future occurrence.
- Pluggable executors: `codex` (default), `shell`, `ao2`.
- Delivery: local per-run Markdown file (always) + optional webhook POST.
- A complete CLI: add/list/show/edit/remove/pause/resume/run/tick/daemon/doctor/config.
- Two operating modes: a built-in `daemon` that owns a 60s tick loop, OR an
  external OS scheduler (system cron / launchd / systemd timer) driving
  `codex-cron tick`. Both safe via the cross-process lock.
- Service install: register the daemon as a launchd agent (macOS), systemd user
  unit (Linux), or Task Scheduler entry / startup (Windows).
- Cross-OS: builds and runs on macOS, Linux, Windows.

### Non-goals (v1, explicitly deferred — YAGNI)
- Chat-platform delivery (Telegram/Slack/Discord/etc.).
- SQLite/FTS5 session search and cross-session memory (per-run `.md` + a
  per-job `runs.jsonl` history log is enough for v1).
- Agent self-scheduling tool (an agent creating its own follow-up jobs).
- Sandboxed execution backends (Docker/SSH/Modal); executors run as local
  child processes.
- A web UI / TUI.

## 3. Architecture

A 2-crate Cargo workspace.

- **`codex-cron-core`** (library, pure logic, no process/network/clock
  side-effects): schedule parsing, the job data model, `compute_next_run`,
  due-selection, the tick *algorithm*, and the trait boundaries
  `Clock`, `JobStore`, `Executor`, `Delivery`. Everything here is
  deterministically unit-testable with a fake clock and an in-memory store.
- **`codex-cron-cli`** (binary `codex-cron`): the concrete implementations that
  perform effects — `SystemClock`, `FileJobStore`, the three executors,
  file + webhook delivery — plus the daemon loop, the clap CLI, and service
  install.

Keeping all side-effects out of `codex-cron-core` is what makes the hard
durability properties (at-most-once, fast-forward, no-drift) testable without
real time, real files, or real subprocesses.

### Trait boundaries (core)

```text
trait Clock        { fn now_utc(&self) -> DateTime<Utc>; }
trait JobStore     { fn load(&self) -> Result<Vec<Job>>; fn save(&self, jobs:&[Job]) -> Result<()>; }
trait Executor     { fn kind(&self) -> ExecutorKind; fn run(&self, job:&Job, ctx:&RunContext) -> Result<RunOutput>; }
trait Delivery     { fn deliver(&self, job:&Job, out:&RunOutput) -> Result<()>; }
```

`tick()` is a free function in core that takes `&dyn Clock`, `&dyn JobStore`,
the executor registry, the delivery list, and a `max_parallel` bound, and runs
one full scheduling pass against those abstractions.

## 4. Schedule semantics (hermes parity)

`Schedule` enum:
- `Interval { minutes: u64 }` — parsed from a string beginning with `"every "`,
  e.g. `"every 30m"`, `"every 2h"`, `"every 1d"`.
- `Cron { expr: String }` — a 5- or 6-field cron expression validated and
  advanced by a Rust cron crate.
- `Once { run_at: DateTime<Utc> }` — parsed from a bare duration `"2h"`
  (meaning now + duration) or an ISO-8601 timestamp.

`parse_duration(&str) -> Result<u64 /*minutes*/>`: accepts `<n><unit>` where
unit is `m|min|h|hour|d|day` (plural tolerated); multipliers m=1, h=60, d=1440.

`parse_schedule(&str) -> Result<(Schedule, String /*display*/)>`:
1. If it starts with `"every "`, the remainder is a duration → `Interval`.
2. Else if it splits into >=5 whitespace fields all matching the cron charset
   `[0-9*\-,/]`, validate with the cron crate → `Cron`.
3. Else try `parse_duration` → `Once { now + dur }`, else parse ISO-8601 →
   `Once { run_at }`. Otherwise error.

`compute_next_run(schedule, last_run_at: Option<DateTime<Utc>>, now) -> Option<DateTime<Utc>>`:
- `Interval`: anchor = `last_run_at.unwrap_or(now)`; candidate = anchor + period;
  while candidate <= now, add period (fast-forward). Returns the first instant
  strictly after `now`. Anchoring to `last_run_at` guarantees no drift.
- `Cron`: the first occurrence strictly after `max(last_run_at, now-epsilon)`;
  inherently fast-forwards.
- `Once`: `Some(run_at)` if it has not yet fired, else `None` (job completes).

## 5. Data model

```text
Job {
  id: String,                 // 12 hex chars
  name: String,
  prompt: String,             // for codex/agent executor
  executor: ExecutorKind,     // Codex | Shell | Ao2
  script: Option<String>,     // Shell: command; Ao2: spec path or inline
  schedule: Schedule,
  schedule_display: String,
  repeat: Repeat,             // { times: Option<u64>, completed: u64 }
  enabled: bool,
  state: JobState,            // Scheduled | Running | Paused | Done | Failed
  created_at: DateTime<Utc>,
  next_run_at: Option<DateTime<Utc>>,
  last_run_at: Option<DateTime<Utc>>,
  last_status: Option<String>,
  last_error: Option<String>,
  deliver: Vec<DeliveryTarget>,   // [File] by default; + Webhook(url)
  workdir: Option<PathBuf>,
  context_from: Option<String>,   // another job id: inject its last output
  codex_model: Option<String>,
}
```

Persisted as a versioned envelope `{ "schema_version": "codex-cron.jobs.v1",
"jobs": [ ... ] }`.

## 6. Tick engine (the correctness core)

`tick()` performs one pass:

1. Acquire the cross-process tick lock `~/.codex-cron/.tick.lock`
   (non-blocking, advisory, via `fs2`). If already held, return immediately —
   another tick is in progress.
2. Load jobs. Select due = jobs where `enabled && state != Running &&
   next_run_at.map_or(false, |t| t <= now)`.
3. **Advance before run**: for each due job, set `state = Running`, set
   `last_run_at = now`, compute and store the new `next_run_at` (recurring) or
   mark `once` for completion, then **persist atomically**. Then release the
   tick lock. This is the at-most-once guarantee: if the process crashes mid-run,
   the next tick sees an already-advanced `next_run_at` and a `Running` state and
   will not double-fire.
4. Run due jobs with bounded concurrency (`max_parallel`, default = CPU count;
   `1` = serial). Each executor spawns a child process with an explicit working
   directory, so there is no shared global state to serialize (an improvement
   over hermes' workdir/profile global-state hazard).
5. Before running a `codex`/agent job, run the assembled prompt through a
   prompt-injection scanner; on a hit, refuse the run (unattended runs
   auto-approve tools, so a poisoned prompt must be blocked) and record the
   refusal as the run error.
6. `mark_job_run`: record `last_status` / `last_error`, increment
   `repeat.completed`, set `state` back to `Scheduled` (or `Done` / `Failed`).
   Delete the job when `repeat.times` is reached or a `once` job has completed.
   Persist atomically.
7. Save the run output to `~/.codex-cron/output/<id>/<timestamp>.md` and append a
   line to `~/.codex-cron/output/<id>/runs.jsonl`. Deliver: file always; webhook
   if configured.

Refinement over hermes: the cross-process lock is held only for the fast
select→advance→persist critical section, not for the (possibly long) run phase,
so a long-running job never causes the daemon to skip later ticks. The
`state = Running` flag plus the already-advanced `next_run_at` still prevent any
double-fire.

## 7. Executors

`ExecutorKind = Codex | Shell | Ao2`. All live in the cli crate; all spawn child
processes and capture stdout/stderr/exit status into a `RunOutput`.

- **CodexExecutor**: runs the `codex` CLI non-interactively on `job.prompt`
  (exact headless subcommand/flag verified against `codex --help` at build
  time), in `job.workdir`, with optional `--model`. The prompt is passed as an
  argument or via stdin — never interpolated into a shell string.
- **ShellExecutor**: runs `job.script` via `sh -c` (unix) or `cmd /C` /
  PowerShell (Windows), captures stdout. If stdout begins with a JSON object
  `{"wakeAgent": false}`, the run is treated as silent (nothing to deliver) —
  the hermes "no_agent watchdog" gate.
- **Ao2Executor**: runs `ao2 run --spec <job.script>` (or another governed
  `ao2` subcommand) in `job.workdir`, capturing the run summary / evidence path.

An executor whose binary is missing fails *that job* with a clear error; it
never crashes the daemon.

## 8. Delivery

`DeliveryTarget = File | Webhook(url)`.
- **FileDelivery**: the per-run `.md` write (always on).
- **WebhookDelivery**: HTTP POST JSON `{ job_id, name, status, output_md, ts }`
  to the configured URL with a short bounded retry + backoff. Basic SSRF
  guidance: optional host allowlist in config; document the risk.

## 9. CLI (`codex-cron`)

clap-based subcommands:
- `add <schedule> <prompt> [--name N] [--executor codex|shell|ao2] [--script @file|STR] [--deliver file|webhook:URL]... [--repeat N] [--workdir P] [--context-from ID] [--model M]`
- `list [--json]`
- `show <id>`
- `edit <id> [same flags as add]`
- `remove <id>`
- `pause <id>` / `resume <id>`
- `run <id>` — fire now (still goes through advance-before-run)
- `tick` — run exactly one tick (for OS-scheduler-driven mode)
- `daemon [--interval 60]` — loop { tick(); sleep(interval) }
- `daemon install` / `daemon uninstall` — register/remove the OS service
- `doctor` — config + `codex`/`ao2` on PATH + lock health + next-due summary
- `config [get|set] [key] [value]` — read/write `config.toml`

## 10. Config & paths

Home directory `~/.codex-cron/` (override with `CODEX_CRON_HOME`):
- `config.toml` — default executor, `max_parallel`, default delivery, `codex`/
  `ao2` binary paths, default webhook, timezone, optional webhook host allowlist.
- `jobs.json` — the job store (atomic writes).
- `.tick.lock` — cross-process tick lock.
- `output/<id>/<timestamp>.md` and `output/<id>/runs.jsonl` — per-run results.

Config and environment are re-read fresh on each tick so changes take effect
without restarting the daemon (hermes parity).

## 11. Security

- Prompt-injection scan before agent/codex runs; refuse on hit.
- Secure file permissions (0700 dirs / 0600 files) on unix; best-effort on
  Windows.
- Agent prompts passed as args/stdin, never via a shell (no injection on the
  agent path). The shell executor is arbitrary by design — documented.
- Webhook SSRF: optional config allowlist; documented default-open risk.

## 12. Testing strategy (TDD)

- **core unit tests** (fake clock + in-memory store):
  - `parse_duration`, `parse_schedule` for all three kinds + error cases.
  - `compute_next_run`: interval no-drift; fast-forward after long downtime
    (single catch-up, not a burst); cron next occurrence; once.
  - `tick`: at-most-once across a simulated mid-run crash (advanced state
    persisted before run → no double-fire); fast-forward; `repeat.times`
    exhaustion → deletion; `once` completion → deletion; disabled/paused skipped;
    `max_parallel` respected.
- **store integration tests**: atomic write leaves no partial file; round-trip;
  permissions on unix.
- **executor tests**: ShellExecutor stdout + `wakeAgent:false` gate with real
  `sh -c`; Codex/Ao2 executors against a fake binary placed on `PATH` (so CI
  needs neither real `codex` nor real `ao2`).
- **cli integration tests** (`assert_cmd`): add → list → run → remove lifecycle
  against a temp `CODEX_CRON_HOME`.
- **delivery test**: WebhookDelivery against a local mock HTTP server.
- **cross-process lock test**: two concurrent ticks; the second is skipped.
- Gates: `cargo test` green, `cargo clippy --workspace -- -D warnings`,
  `codex-cron doctor` passes.

## 13. Dependencies (Rust)

clap (derive), serde + serde_json, chrono, a cron crate (e.g. `croner` or
`cron`), `fs2` (advisory file lock), `reqwest` (webhook), `tokio` (runtime +
concurrency), anyhow + thiserror, regex, a 12-hex id generator (`rand`),
`directories` (home dir), `num_cpus`; dev: `tempfile`, `assert_cmd`,
`predicates`, a mock HTTP server (`wiremock` or a tiny `hyper` server).

## 14. Implementation steps

1. Create the Cargo workspace with the two crates `codex-cron-core` and
   `codex-cron-cli`, plus a workspace `Cargo.toml`, `rust-toolchain.toml`, and
   `.gitignore`.
2. Implement and unit-test schedule parsing: add `parse_duration`,
   `parse_schedule`, and the `Schedule` type with full coverage of the three
   kinds and error cases.
3. Implement and unit-test `compute_next_run`: verify interval no-drift,
   fast-forward after downtime, cron next occurrence, and once semantics.
4. Implement the `Job` model with serde and the versioned store envelope;
   verify round-trip serialization.
5. Implement the core trait boundaries (`Clock`, `JobStore`, `Executor`,
   `Delivery`) and the `tick` algorithm; verify at-most-once, fast-forward,
   repeat exhaustion, once-deletion, paused/disabled skip, and concurrency cap
   with a fake clock and in-memory store.
6. Implement `FileJobStore` with atomic temp-fsync-rename writes and an
   in-process mutex; verify durability and round-trip on disk.
7. Implement the cross-process tick lock with `fs2`; verify two concurrent ticks
   serialize (second skips).
8. Implement the three executors (`codex`, `shell`, `ao2`) as child-process
   spawners; verify shell stdout capture and the `wakeAgent:false` gate, and
   verify codex/ao2 executors against a fake on-PATH binary.
9. Implement file and webhook delivery; verify file output and a webhook POST
   against a mock server.
10. Implement the prompt-injection scanner and wire it into the tick before
    agent runs; verify a poisoned prompt is refused.
11. Implement the clap CLI (add/list/show/edit/remove/pause/resume/run/tick/
    daemon/doctor/config); verify the add→list→run→remove lifecycle.
12. Implement the daemon loop and `daemon install`/`uninstall` for launchd,
    systemd user units, and Windows Task Scheduler; verify the generated unit on
    the host OS.
13. Run the full verification: `cargo test`, `cargo clippy --workspace -- -D
    warnings`, and a manual smoke run; capture evidence.
14. Write the README (install, quickstart, schedule grammar, executors,
    daemon/service, config) and commit the work in atomic commits.

## 15. Acceptance criteria (v1)

- The `codex-cron` binary builds on macOS, Linux, and Windows.
- All three schedule kinds parse and compute correct next-run times.
- The daemon ticks on its interval; due jobs fire at most once; the schedule
  survives a process restart; a stale recurring job fast-forwards to a single
  catch-up rather than a burst.
- The codex, shell, and ao2 executors each run a job and capture output.
- Per-run Markdown output is written and webhook delivery fires when configured.
- `daemon install` registers a working service on the host OS.
- `cargo test` passes, `cargo clippy --workspace -- -D warnings` is clean, and
  `codex-cron doctor` reports healthy.
