# Integration Guide: AO2 Pulse Event-Loop with codex-cron

## Overview
This guide describes how to integrate `AO2 Pulse` (or similar autonomous planner/executor systems) with `codex-cron` using the event-loop jobs feature.

In this architecture:
- **`codex-cron`** acts as the generic **coordinator, runner, and supervisor**. It manages job state, schedule triggers, execution, safety boundaries, and evidence persistence.
- **`AO2 Pulse`** acts as the **planner and executor** of the actual tasks. It analyzes the workspace, makes decisions on what actions to take next, and outputs the next steps.

By emitting a standardized machine-readable JSON line in its output, `AO2 Pulse` signals to `codex-cron` whether it needs to run again immediately (e.g., to continue executing a multi-step task chain) or if it has completed its work.

---

## The Event-Loop Protocol Contract
After every execution iteration of a job configured with `--event-loop`, `codex-cron` scans the output (stdout/stderr) for a JSON line conforming to the schema version `codex-cron.event-loop-decision.v1`.

### Decision JSON Schema
The JSON line must be formatted exactly as follows:
```json
{"schema_version":"codex-cron.event-loop-decision.v1","event_loop":{"action":"continue","reason":"next task is ready","next_task_id":"some-task-id"}}
```

#### Fields:
- `schema_version` (string, required): Must be `"codex-cron.event-loop-decision.v1"`.
- `event_loop` (object, required):
  - `action` (string, required): The action `codex-cron` should take. Supported values:
    - `"continue"`: Immediately run another iteration of the same job.
    - `"stop"`: Cleanly stop execution. No errors are reported.
    - `"backoff"`: Cleanly stop execution and reschedule.
    - `"fail"`: Stop execution and report a failure.
  - `reason` (string, optional): A description or reasoning for the decision.
  - `next_task_id` (string, optional): Identifier for the next task or phase in the chain.

---

## Integration Example

### 1. The Executor Script (`pulse-executor.sh`)
Below is an example of an executor script that represents how `AO2 Pulse` (or a similar tool) would execute a single step and produce the event-loop decision JSON:

```bash
#!/bin/bash
# pulse-executor.sh - Example AO2 Pulse integration wrapper script

STATE_FILE="/tmp/pulse-state.json"

# Read current state or initialize
if [ ! -f "$STATE_FILE" ]; then
  echo '{"step": 1}' > "$STATE_FILE"
fi

CURRENT_STEP=$(ruby -e "require 'json'; puts JSON.parse(File.read('$STATE_FILE'))['step']")

echo "Executing step $CURRENT_STEP..."

# Simulate processing work
sleep 2

# Plan the next step
NEXT_STEP=$((CURRENT_STEP + 1))

if [ "$NEXT_STEP" -le 3 ]; then
  # Save the new state
  echo "{\"step\": $NEXT_STEP}" > "$STATE_FILE"
  # Output the continuation decision JSON line
  echo "{\"schema_version\":\"codex-cron.event-loop-decision.v1\",\"event_loop\":{\"action\":\"continue\",\"reason\":\"Proceeding to step $NEXT_STEP\",\"next_task_id\":\"step-$NEXT_STEP\"}}"
else
  # Cleanup state
  rm -f "$STATE_FILE"
  # Output the stop decision JSON line
  echo "{\"schema_version\":\"codex-cron.event-loop-decision.v1\",\"event_loop\":{\"action\":\"stop\",\"reason\":\"All steps completed successfully\"}}"
fi
```

### 2. Registering the Event-Loop Job
To register the script as an event-loop job in `codex-cron`, use the CLI with the `--event-loop` flag and configure safety limits:

```sh
codex-cron add "every 30m" "AO2 Pulse Integration" \
  --executor shell \
  --script "/path/to/pulse-executor.sh" \
  --event-loop \
  --max-chain-runs 5 \
  --max-runtime-seconds 1800
```

### 3. Execution
Once configured, the job will run on its schedule (every 30 minutes). When it runs, the event-loop execution is triggered:
- The first run starts because the job is due.
- Upon completion, `codex-cron` reads the output of the run and detects the `continue` action.
- `codex-cron` immediately starts the second run of the same job.
- This continues until `action` is `stop`, or one of the safety limits is hit (e.g. 5 runs, or 1800 seconds total runtime).

---

## Safety Boundaries & Fallbacks
To prevent runaway processes or infinite loops, `codex-cron` enforces the following rules:
1. **Missing Decisions**: If a job run does not emit any schema-matching JSON line, the loop defaults to `stop`.
2. **Malformed JSON**: If a schema-matching line is found but the JSON is malformed or invalid, the loop terminates immediately with `fail`.
3. **Execution Failures**: If the script or command exits with a non-zero status code, the loop terminates immediately and is marked as failed.
4. **Limits**:
   - `max_chain_runs`: The maximum number of consecutive zero-wait iterations allowed. Defaults to 3.
   - `max_runtime_seconds`: The maximum total time allowed for the entire chain of iterations. Defaults to 2700 (45 minutes).

---

## Durability & Evidence Tracking
`codex-cron` writes a JSON summary of the event-loop run chain to:
`~/.codex-cron/event-loop/<job-id>/latest.json`

This file contains the final status, total iterations, and a list of decisions made during each step of the loop.

Example `latest.json` output:
```json
{
  "schema_version": "codex-cron.event-loop-run.v1",
  "job_id": "r8m3k2v7y9p0",
  "status": "stopped",
  "iterations": 3,
  "max_chain_runs": 5,
  "max_runtime_seconds": 1800,
  "decisions": [
    {
      "iteration": 1,
      "action": "continue",
      "reason": "Proceeding to step 2",
      "next_task_id": "step-2"
    },
    {
      "iteration": 2,
      "action": "continue",
      "reason": "Proceeding to step 3",
      "next_task_id": "step-3"
    },
    {
      "iteration": 3,
      "action": "stop",
      "reason": "All steps completed successfully",
      "next_task_id": null
    }
  ]
}
```

This ensures complete auditability and makes it easy to monitor and debug background loops.
