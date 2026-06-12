//! codex-cron-core — pure scheduling logic.
//!
//! This crate holds everything that can be reasoned about without touching the
//! filesystem, the network, real time, or real subprocesses: the schedule
//! grammar, the [`Job`] model, the durable tick algorithm, and the trait
//! boundaries (`Clock`, `JobStore`, `Executor`, `Delivery`) that the cli crate
//! supplies concrete implementations for.
//!
//! Keeping side effects out of this crate is what makes the hard durability
//! properties — at-most-once firing, no schedule drift, no burst after
//! downtime — deterministically testable with a fake clock and an in-memory
//! store.

pub mod event_loop;
pub mod job;
pub mod scan;
pub mod schedule;
pub mod tick;

pub use scan::DefaultScanner;

pub use event_loop::{
    parse_event_loop_decision, EventLoopAction, EventLoopDecision, EventLoopPolicy,
    EVENT_LOOP_DECISION_SCHEMA,
};
pub use job::{
    jobs_to_json, parse_jobs, DeliveryTarget, ExecutorKind, Job, JobError, JobState, NewJob, Repeat,
    JOBS_SCHEMA_VERSION,
};
pub use schedule::{compute_next_run, parse_duration, parse_schedule, Schedule, ScheduleError};
pub use tick::{
    tick, Clock, Delivery, DeliveryError, Executor, FiredJob, InjectionScanner, JobStore,
    RunContext, RunOutput, RunStatus, StoreError, TickConfig, TickError, TickReport,
};
