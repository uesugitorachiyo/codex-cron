//! Schedule grammar: `interval` / `cron` / `once`, plus next-run computation.
//!
//! Mirrors the human-string grammar of hermes-agent's cron, with the two
//! correctness properties that matter most: interval next-runs are anchored to
//! the last run so they never drift, and a stale recurring job fast-forwards to
//! a single future occurrence rather than firing a burst of catch-ups.

use chrono::{DateTime, Duration, Utc};
use croner::Cron;
use serde::{Deserialize, Serialize};

/// A job's firing schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Schedule {
    /// Fire every `minutes` minutes (`"every 30m"`).
    Interval { minutes: u64 },
    /// Fire on a 5/6-field cron expression (`"0 9 * * *"`).
    Cron { expr: String },
    /// Fire exactly once at `run_at` (a bare duration from now, or an ISO time).
    Once { run_at: DateTime<Utc> },
}

/// Errors returned when a schedule or duration string cannot be parsed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("empty schedule string")]
    Empty,
    #[error("invalid duration '{0}': expected <n><m|min|h|hour|d|day>, n >= 1")]
    BadDuration(String),
    #[error("invalid cron expression '{expr}': {message}")]
    BadCron { expr: String, message: String },
    #[error("unrecognized schedule '{0}': not an interval, cron, duration, or ISO timestamp")]
    Unrecognized(String),
}

/// Parse a duration like `"30m"`, `"2h"`, `"1d"`, `"15min"`, `"3hours"`,
/// `"2days"` into a number of minutes. Units: `m`/`min` = 1, `h`/`hour` = 60,
/// `d`/`day` = 1440 (trailing `s` tolerated). Zero and negative are rejected.
pub fn parse_duration(input: &str) -> Result<u64, ScheduleError> {
    let s = input.trim();
    if s.is_empty() {
        return Err(ScheduleError::Empty);
    }
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    if num.is_empty() {
        return Err(ScheduleError::BadDuration(input.to_string()));
    }
    let n: u64 = num
        .parse()
        .map_err(|_| ScheduleError::BadDuration(input.to_string()))?;
    if n == 0 {
        return Err(ScheduleError::BadDuration(input.to_string()));
    }
    let unit = unit.trim().trim_end_matches('s');
    let mult = match unit {
        "m" | "min" => 1,
        "h" | "hour" => 60,
        "d" | "day" => 1440,
        _ => return Err(ScheduleError::BadDuration(input.to_string())),
    };
    Ok(n * mult)
}

/// Parse a schedule string into a [`Schedule`] plus its display form.
///
/// 1. `"every <dur>"` -> [`Schedule::Interval`].
/// 2. A >=5-field cron-charset expression -> [`Schedule::Cron`] (validated).
/// 3. A bare duration -> [`Schedule::Once`] at `now + dur`.
/// 4. An ISO-8601 timestamp -> [`Schedule::Once`].
pub fn parse_schedule(
    input: &str,
    now: DateTime<Utc>,
) -> Result<(Schedule, String), ScheduleError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ScheduleError::Empty);
    }
    let display = trimmed.to_string();

    // 1. interval: "every <dur>"
    if let Some(rest) = trimmed.strip_prefix("every ") {
        let minutes = parse_duration(rest.trim())?;
        return Ok((Schedule::Interval { minutes }, display));
    }

    // 2. cron: >=5 whitespace fields, each matching the cron charset.
    let fields: Vec<&str> = trimmed.split_whitespace().collect();
    if fields.len() >= 5
        && fields.iter().all(|f| {
            !f.is_empty()
                && f.chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '*' | '-' | ',' | '/'))
        })
    {
        Cron::new(trimmed)
            .parse()
            .map_err(|e| ScheduleError::BadCron {
                expr: display.clone(),
                message: e.to_string(),
            })?;
        return Ok((
            Schedule::Cron {
                expr: display.clone(),
            },
            display,
        ));
    }

    // 3. bare duration -> once at now + dur.
    if let Ok(minutes) = parse_duration(trimmed) {
        let run_at = now + Duration::minutes(minutes as i64);
        return Ok((Schedule::Once { run_at }, display));
    }

    // 4. ISO-8601 timestamp -> once.
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok((
            Schedule::Once {
                run_at: dt.with_timezone(&Utc),
            },
            display,
        ));
    }

    Err(ScheduleError::Unrecognized(display))
}

/// Compute the next firing instant strictly after `now`.
///
/// * `Interval` anchors to `last_run_at` (else `now`) and fast-forwards past
///   `now`, so runs never drift and downtime yields a single catch-up.
/// * `Cron` returns the first occurrence strictly after `max(last_run_at, now)`.
/// * `Once` returns `run_at` until it has fired (`last_run_at` set), then `None`.
pub fn compute_next_run(
    schedule: &Schedule,
    last_run_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match schedule {
        Schedule::Interval { minutes } => {
            let period = Duration::minutes(*minutes as i64);
            if period <= Duration::zero() {
                return None;
            }
            let anchor = last_run_at.unwrap_or(now);
            let mut next = anchor + period;
            while next <= now {
                next += period;
            }
            Some(next)
        }
        Schedule::Cron { expr } => {
            let cron = Cron::new(expr).parse().ok()?;
            let after = match last_run_at {
                Some(last) if last > now => last,
                _ => now,
            };
            cron.find_next_occurrence(&after, false).ok()
        }
        Schedule::Once { run_at } => {
            if last_run_at.is_some() {
                None
            } else {
                Some(*run_at)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    // ---- parse_duration ----

    #[test]
    fn duration_minutes() {
        assert_eq!(parse_duration("30m").unwrap(), 30);
        assert_eq!(parse_duration("15min").unwrap(), 15);
    }

    #[test]
    fn duration_hours() {
        assert_eq!(parse_duration("2h").unwrap(), 120);
        assert_eq!(parse_duration("3hours").unwrap(), 180);
    }

    #[test]
    fn duration_days() {
        assert_eq!(parse_duration("1d").unwrap(), 1440);
        assert_eq!(parse_duration("2days").unwrap(), 2880);
    }

    #[test]
    fn duration_rejects_empty_and_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("h").is_err());
    }

    #[test]
    fn duration_rejects_zero() {
        assert!(parse_duration("0m").is_err());
    }

    // ---- parse_schedule ----

    #[test]
    fn schedule_interval() {
        let now = at(2026, 6, 1, 10, 0);
        let (s, display) = parse_schedule("every 30m", now).unwrap();
        assert_eq!(s, Schedule::Interval { minutes: 30 });
        assert_eq!(display, "every 30m");
    }

    #[test]
    fn schedule_interval_hours() {
        let now = at(2026, 6, 1, 10, 0);
        let (s, _) = parse_schedule("every 2h", now).unwrap();
        assert_eq!(s, Schedule::Interval { minutes: 120 });
    }

    #[test]
    fn schedule_interval_rejects_bad_duration() {
        let now = at(2026, 6, 1, 10, 0);
        assert!(parse_schedule("every wat", now).is_err());
    }

    #[test]
    fn schedule_cron() {
        let now = at(2026, 6, 1, 10, 0);
        let (s, display) = parse_schedule("0 9 * * *", now).unwrap();
        assert_eq!(
            s,
            Schedule::Cron {
                expr: "0 9 * * *".to_string()
            }
        );
        assert_eq!(display, "0 9 * * *");
    }

    #[test]
    fn schedule_cron_step() {
        let now = at(2026, 6, 1, 10, 0);
        let (s, _) = parse_schedule("*/5 * * * *", now).unwrap();
        assert!(matches!(s, Schedule::Cron { .. }));
    }

    #[test]
    fn schedule_cron_rejects_invalid_fields() {
        let now = at(2026, 6, 1, 10, 0);
        assert!(parse_schedule("99 99 * * *", now).is_err());
    }

    #[test]
    fn schedule_once_bare_duration() {
        let now = at(2026, 6, 1, 10, 0);
        let (s, _) = parse_schedule("2h", now).unwrap();
        assert_eq!(
            s,
            Schedule::Once {
                run_at: at(2026, 6, 1, 12, 0)
            }
        );
    }

    #[test]
    fn schedule_once_iso_timestamp() {
        let now = at(2026, 6, 1, 10, 0);
        let (s, _) = parse_schedule("2026-06-01T09:00:00Z", now).unwrap();
        assert_eq!(
            s,
            Schedule::Once {
                run_at: at(2026, 6, 1, 9, 0)
            }
        );
    }

    #[test]
    fn schedule_rejects_garbage() {
        let now = at(2026, 6, 1, 10, 0);
        assert!(parse_schedule("totally not a schedule", now).is_err());
    }

    // ---- compute_next_run ----

    #[test]
    fn interval_first_run_anchors_to_now() {
        let s = Schedule::Interval { minutes: 60 };
        let now = at(2026, 6, 1, 10, 0);
        assert_eq!(compute_next_run(&s, None, now), Some(at(2026, 6, 1, 11, 0)));
    }

    #[test]
    fn interval_no_drift_anchors_to_last_run() {
        // Last fired exactly at 10:00, we tick at 10:30 -> next is 11:00, not 11:30.
        let s = Schedule::Interval { minutes: 60 };
        let last = at(2026, 6, 1, 10, 0);
        let now = at(2026, 6, 1, 10, 30);
        assert_eq!(
            compute_next_run(&s, Some(last), now),
            Some(at(2026, 6, 1, 11, 0))
        );
    }

    #[test]
    fn interval_fast_forwards_after_downtime() {
        // Last fired at 10:00, the daemon was down until 15:25. We want a single
        // catch-up at 16:00 -- the first hourly boundary strictly after now --
        // never a burst of the five missed runs.
        let s = Schedule::Interval { minutes: 60 };
        let last = at(2026, 6, 1, 10, 0);
        let now = at(2026, 6, 1, 15, 25);
        assert_eq!(
            compute_next_run(&s, Some(last), now),
            Some(at(2026, 6, 1, 16, 0))
        );
    }

    #[test]
    fn cron_next_occurrence() {
        // Top of every hour; at 10:30 the next is 11:00.
        let s = Schedule::Cron {
            expr: "0 * * * *".to_string(),
        };
        let now = at(2026, 6, 1, 10, 30);
        assert_eq!(compute_next_run(&s, None, now), Some(at(2026, 6, 1, 11, 0)));
    }

    #[test]
    fn once_returns_run_at_until_fired() {
        let s = Schedule::Once {
            run_at: at(2026, 6, 1, 12, 0),
        };
        let now = at(2026, 6, 1, 10, 0);
        assert_eq!(compute_next_run(&s, None, now), Some(at(2026, 6, 1, 12, 0)));
    }

    #[test]
    fn once_returns_none_after_fired() {
        let s = Schedule::Once {
            run_at: at(2026, 6, 1, 12, 0),
        };
        let last = at(2026, 6, 1, 12, 0);
        let now = at(2026, 6, 1, 12, 1);
        assert_eq!(compute_next_run(&s, Some(last), now), None);
    }
}
