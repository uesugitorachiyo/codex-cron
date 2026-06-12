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
