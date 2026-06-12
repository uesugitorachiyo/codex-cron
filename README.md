# codex-cron

A durable, governed cron scheduler for agent jobs, written in Rust.

`codex-cron` is a from-scratch reimplementation of the cron subsystem of
[nousresearch/hermes-agent](https://github.com/nousresearch/hermes-agent),
keeping its durability and safety behavior while making the executor pluggable.
By default a job's prompt is run by the OpenAI `codex` CLI; a job can also run a
raw shell command or a governed `ao2 run`.

It does the boring-but-hard parts of a scheduler correctly:

- **At-most-once firing** — a job's next run time is advanced and persisted to
  disk *before* the job runs, so a crash mid-run never re-fires it.
- **No drift** — interval jobs anchor each next run to the scheduled instant, not
  to wall-clock "now", so `every 30m` stays on the half hour forever.
- **No thundering herd** — a job that fell behind (laptop asleep, machine off)
  fast-forwards to a single next occurrence instead of firing a burst of
  catch-up runs.
- **Crash recovery** — a job left mid-run by a killed process is reset on the
  next load.
- **One writer at a time** — every scheduling pass takes a cross-process file
  lock, so the built-in daemon and an external `codex-cron tick` can coexist.
- **Durable writes** — the job store is written to a temp file, `fsync`'d, then
  atomically renamed; the home dir is `0700` and its files `0600` on Unix.

## Install

Requires a Rust toolchain (see `rust-toolchain.toml`).

```sh
cargo install --path crates/codex-cron-cli
# or, from a clone:
cargo build --release            # binary at target/release/codex-cron
```

For the `codex` executor (the default), the `codex` CLI must be on `PATH`. The
`shell` executor needs nothing extra; the `ao2` executor needs `ao2` on `PATH`.
`codex-cron doctor` tells you what it can find.

## Quickstart

```sh
# A daily agent job, run by `codex`, delivered to a per-run markdown file:
codex-cron add "0 9 * * *" "Summarize my unread email and list action items"

# A shell job every 30 minutes:
codex-cron add "every 30m" "disk check" --executor shell \
  --script "df -h | tail -n +2"

# See everything, run one now, inspect it, remove it:
codex-cron list
codex-cron run <id>
codex-cron show <id>
codex-cron remove <id>

# Then either run the built-in loop…
codex-cron daemon --interval 60
# …or drive it from your OS scheduler once a minute:
#   * * * * * codex-cron tick
```

## Schedule grammar

The first argument to `add` (and `edit --schedule`) is one of:

| Form | Example | Meaning |
|------|---------|---------|
| `every <dur>` | `every 30m`, `every 2h`, `every 1d` | Recurring interval |
| cron (5 or 6 field) | `0 9 * * *`, `*/15 * * * *` | Recurring cron expression |
| bare duration | `2h`, `90m`, `1d` | Run **once**, at now + duration |
| ISO-8601 timestamp | `2026-06-01T09:00:00Z` | Run **once**, at that instant |

Durations accept `m`/`min`, `h`/`hour`, `d`/`day` (a trailing `s` is fine):
`15m`, `15min`, `3h`, `3hours`, `2d`, `2days`.

## Executors

Choose with `--executor` (default `codex`, configurable):

- **`codex`** — runs `codex exec [--model <m>] <prompt>`. The prompt is the
  job's prompt, optionally prefixed with another job's latest output (see
  `--context-from`). The prompt is passed as a process argument, never through a
  shell.
- **`shell`** — runs the job's `--script` via `sh -c` (Unix) or `cmd /C`
  (Windows).
- **`ao2`** — runs `ao2 run --spec <script>`, where `--script` is a spec path or
  inline spec.

`--script` accepts `@path/to/file` to load the script/spec from a file.

A run is recorded with one of four statuses: `success`, `failed`, `silent`
(see the wakeAgent gate below), or `refused` (see prompt-injection guard).

## Delivery

Every run always writes a markdown record to
`<home>/output/<id>/<timestamp>.md` and appends a line to
`<home>/output/<id>/runs.jsonl`. This file sink is unconditional.

Add a webhook with `--deliver`:

```sh
codex-cron add "every 1h" "hourly report" \
  --deliver webhook:https://hooks.example.com/abc
```

Webhook delivery POSTs JSON `{job_id, name, status, output_md, ts}` with a short
bounded retry. `--deliver` is repeatable and also accepts `file`. With a
`default_webhook` configured you can write just `--deliver webhook`.

## Safety

These mirror hermes-agent's guardrails:

- **Prompt-injection scan** — `codex` prompts are screened for known injection
  phrases (e.g. "ignore previous instructions") and a match yields a `refused`
  run *before* any process is spawned. The assembled prompt — including any
  injected `--context-from` output — is re-scanned in the executor as defense in
  depth.
- **wakeAgent gate** — if a run's first stdout line is `{"wakeAgent": false}`,
  the run is `silent`: nothing is delivered. This is the hermes "no-agent
  watchdog" convention for "I checked; nothing to report."
- **Webhook allowlist** — set `webhook_allowlist` to restrict webhook delivery
  to specific hosts (an SSRF guard). Empty means allow all.

## Daemon and OS services

Two ways to run on a schedule:

1. **Built-in loop:** `codex-cron daemon --interval 60` ticks every 60s.
2. **OS scheduler:** invoke `codex-cron tick` (one pass, then exit) from cron,
   launchd, or Task Scheduler.

Install the built-in loop as a managed service:

```sh
codex-cron daemon install --interval 60   # launchd (macOS) / systemd --user (Linux)
codex-cron daemon uninstall
```

On Windows, `daemon install` prints the `schtasks` command to register it at
logon.

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

For tools that write durable artifacts, configure
`--event-loop-decision-file <path>` instead of relying on stdout. Relative
decision-file paths resolve against the job `--workdir`; this lets AO2 Pulse
write `target/pulse-next-recommended-tasks/codex-cron-event-loop-decision.json`
and lets `codex-cron` consume the structured decision directly.

Example:

```sh
codex-cron add "every 30m" "AO2 Pulse production readiness" \
  --executor shell \
  --workdir /path/to/ao2 \
  --script "npm run pulse:generate-next" \
  --event-loop \
  --event-loop-decision-file target/pulse-next-recommended-tasks/codex-cron-event-loop-decision.json \
  --max-chain-runs 3 \
  --max-runtime-seconds 2700

codex-cron tick-loop
codex-cron daemon --event-loop --interval 60
```

Evidence is written under:

```text
~/.codex-cron/event-loop/<job-id>/latest.json
```

For a detailed integration guide (e.g. with AO2 Pulse), see [docs/examples/ao2-pulse-event-loop.md](docs/examples/ao2-pulse-event-loop.md).

## Configuration

`codex-cron config show | get <key> | set <key> <value>` reads and writes
`<home>/config.toml`. Keys:

| Key | Default | Purpose |
|-----|---------|---------|
| `default_executor` | `codex` | Executor for `add` when `--executor` is omitted |
| `max_parallel` | `0` | Jobs run concurrently per tick (`0` = CPU count) |
| `codex_path` | `codex` | Path/name of the `codex` binary |
| `ao2_path` | `ao2` | Path/name of the `ao2` binary |
| `default_webhook` | _(unset)_ | URL used by `--deliver webhook` with no URL |
| `webhook_allowlist` | _(empty)_ | Comma-separated allowed webhook hosts |
| `timezone` | `UTC` | Informational display label |

## Files

Everything lives under `$CODEX_CRON_HOME` (default `~/.codex-cron`):

```
~/.codex-cron/
  jobs.json            # the durable job store (schema-versioned)
  config.toml          # configuration
  .tick.lock           # cross-process scheduling lock
  output/<id>/
    <timestamp>.md     # one markdown file per run
    runs.jsonl         # append-only run log
```

## Architecture

A two-crate Cargo workspace:

- **`codex-cron-core`** — pure scheduling logic with no I/O: the schedule
  grammar, the `Job` model, the durable `tick` algorithm, the injection scanner,
  and the trait boundaries (`Clock`, `JobStore`, `Executor`, `Delivery`,
  `InjectionScanner`). Keeping side effects out is what makes the durability
  properties deterministically testable with a fake clock and an in-memory store.
- **`codex-cron-cli`** — the effects and the `codex-cron` binary: the filesystem
  job store and lock, the three process executors, file and webhook delivery, the
  daemon, and the clap command surface.

Run the test suite and lints:

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## License

See `Cargo.toml` for the workspace license.
