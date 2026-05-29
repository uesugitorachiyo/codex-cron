//! The real wall clock.

use chrono::{DateTime, Utc};
use codex_cron_core::Clock;

/// A [`Clock`] backed by the system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
