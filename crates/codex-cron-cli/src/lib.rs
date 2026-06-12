//! codex-cron-cli — the effects layer.
//!
//! Concrete implementations of the `codex-cron-core` trait boundaries
//! (`SystemClock`, `FileJobStore`, the three executors, file + webhook
//! delivery), plus configuration, the clap command surface, the daemon loop,
//! and OS service installation. The binary (`main.rs`) is a thin wire-up over
//! these modules.

pub mod clock;
pub mod config;
pub mod daemon;
pub mod delivery;
pub mod executor;
pub mod id;
pub mod paths;
pub mod store;

pub mod cli;
pub mod event_loop;
