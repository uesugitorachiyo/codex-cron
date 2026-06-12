//! The clap command surface and handlers, plus the shared tick wiring used by
//! both `tick` and the daemon.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use codex_cron_core::{
    compute_next_run, parse_schedule, tick, DefaultScanner, Delivery, DeliveryTarget, Executor,
    ExecutorKind, Job, JobState, JobStore, NewJob, Repeat, TickConfig, TickReport,
};

use crate::clock::SystemClock;
use crate::config::Config;
use crate::delivery::{FileDelivery, WebhookDelivery};
use crate::executor::{Ao2Executor, CodexExecutor, ShellExecutor};
use crate::store::{try_acquire_tick_lock, FileJobStore};
use crate::{daemon, id, paths};

/// Durable, governed cron for agent jobs.
#[derive(Debug, Parser)]
#[command(name = "codex-cron", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Add a new job.
    Add(AddArgs),
    /// List all jobs.
    List {
        /// Emit JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Show one job in detail.
    Show { id: String },
    /// Edit fields of an existing job.
    Edit(EditArgs),
    /// Remove a job.
    Remove { id: String },
    /// Pause a job (kept, never fires until resumed).
    Pause { id: String },
    /// Resume a paused job.
    Resume { id: String },
    /// Run a job now (still advance-before-run).
    Run { id: String },
    /// Run exactly one scheduling pass (for OS-scheduler-driven mode).
    Tick,
    /// Run the built-in tick loop, or install/uninstall the OS service.
    Daemon(DaemonArgs),
    /// Check configuration, binaries, lock health, and next-due jobs.
    Doctor,
    /// Read or write configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Debug, Clone, ValueEnum)]
pub enum ExecutorArg {
    Codex,
    Shell,
    Ao2,
}

impl From<ExecutorArg> for ExecutorKind {
    fn from(a: ExecutorArg) -> Self {
        match a {
            ExecutorArg::Codex => ExecutorKind::Codex,
            ExecutorArg::Shell => ExecutorKind::Shell,
            ExecutorArg::Ao2 => ExecutorKind::Ao2,
        }
    }
}

#[derive(Debug, Args)]
pub struct AddArgs {
    /// Schedule: "every 30m", a cron expr "0 9 * * *", a duration "2h", or an ISO time.
    pub schedule: String,
    /// The prompt (codex) or description for the job.
    pub prompt: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long, value_enum)]
    pub executor: Option<ExecutorArg>,
    /// Shell command / ao2 spec. Prefix with `@` to read from a file.
    #[arg(long)]
    pub script: Option<String>,
    /// Delivery target(s): `file` or `webhook:URL` (repeatable).
    #[arg(long = "deliver")]
    pub deliver: Vec<String>,
    /// Stop and delete after this many runs.
    #[arg(long)]
    pub repeat: Option<u64>,
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    /// Inject another job's latest output into this prompt.
    #[arg(long = "context-from")]
    pub context_from: Option<String>,
    /// Codex model override.
    #[arg(long)]
    pub model: Option<String>,
    /// Enable bounded zero-wait event loop
    #[arg(long)]
    pub event_loop: bool,
    /// Maximum number of runs in a single loop chain
    #[arg(long, default_value_t = codex_cron_core::event_loop::default_max_chain_runs())]
    pub max_chain_runs: u32,
    /// Maximum seconds a loop chain is allowed to run
    #[arg(long, default_value_t = codex_cron_core::event_loop::default_max_runtime_seconds())]
    pub max_runtime_seconds: u64,
}

#[derive(Debug, Args)]
pub struct EditArgs {
    pub id: String,
    #[arg(long)]
    pub schedule: Option<String>,
    #[arg(long)]
    pub prompt: Option<String>,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long, value_enum)]
    pub executor: Option<ExecutorArg>,
    #[arg(long)]
    pub script: Option<String>,
    #[arg(long)]
    pub repeat: Option<u64>,
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    #[arg(long)]
    pub model: Option<String>,
    #[arg(long)]
    pub event_loop: bool,
    #[arg(long)]
    pub no_event_loop: bool,
    #[arg(long)]
    pub max_chain_runs: Option<u32>,
    #[arg(long)]
    pub max_runtime_seconds: Option<u64>,
}

#[derive(Debug, Args)]
pub struct DaemonArgs {
    #[command(subcommand)]
    pub action: Option<DaemonAction>,
    /// Seconds between ticks when running the loop.
    #[arg(long, default_value_t = 60)]
    pub interval: u64,
}

#[derive(Debug, Subcommand)]
pub enum DaemonAction {
    /// Install the daemon as an OS service.
    Install {
        #[arg(long, default_value_t = 60)]
        interval: u64,
    },
    /// Remove the OS service.
    Uninstall,
}

#[derive(Debug, Subcommand)]
pub enum ConfigAction {
    /// Print all settings.
    Show,
    /// Print one setting.
    Get { key: String },
    /// Change one setting.
    Set { key: String, value: String },
}

/// Entry point used by `main`.
pub fn run(cli: Cli) -> Result<()> {
    let home = paths::home_dir();
    match cli.command {
        Command::Add(args) => cmd_add(&home, args),
        Command::List { json } => cmd_list(&home, json),
        Command::Show { id } => cmd_show(&home, &id),
        Command::Edit(args) => cmd_edit(&home, args),
        Command::Remove { id } => cmd_remove(&home, &id),
        Command::Pause { id } => cmd_set_paused(&home, &id, true),
        Command::Resume { id } => cmd_set_paused(&home, &id, false),
        Command::Run { id } => cmd_run(&home, &id),
        Command::Tick => {
            let report = run_one_tick(&home)?;
            print_tick_report(&report);
            Ok(())
        }
        Command::Daemon(args) => match args.action {
            Some(DaemonAction::Install { interval }) => daemon::install(&home, interval),
            Some(DaemonAction::Uninstall) => daemon::uninstall(),
            None => daemon::run_loop(&home, args.interval),
        },
        Command::Doctor => cmd_doctor(&home),
        Command::Config { action } => cmd_config(&home, action),
    }
}

/// Build the concrete clock/store/executors/deliveries and run one core tick,
/// guarded by the cross-process lock. The shared heart of `tick` and `daemon`.
pub fn run_one_tick(home: &Path) -> Result<TickReport> {
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
        max_parallel: cfg.effective_max_parallel(),
        target_job_ids: None,
    };

    match try_acquire_tick_lock(home).context("acquiring tick lock")? {
        None => {
            println!("another tick is in progress; skipping");
            Ok(TickReport::default())
        }
        Some(_lock) => {
            let report = tick(
                &clock,
                &store,
                &executors,
                &deliveries,
                &scanner,
                &tick_cfg,
            )?;
            Ok(report)
        }
    }
}

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
        Some(_lock) => {
            let report = tick(
                &clock,
                &store,
                &executors,
                &deliveries,
                &scanner,
                &tick_cfg,
            )?;
            Ok(report)
        }
    }
}

// ---- handlers ----

fn cmd_add(home: &Path, args: AddArgs) -> Result<()> {
    let cfg = Config::load(home)?;
    let now = Utc::now();
    let (schedule, display) = parse_schedule(&args.schedule, now)
        .map_err(|e| anyhow::anyhow!("invalid schedule: {e}"))?;

    let executor = args
        .executor
        .map(ExecutorKind::from)
        .unwrap_or_else(|| executor_from_str(&cfg.default_executor));
    let script = resolve_script(args.script)?;
    let deliver = parse_deliver(&args.deliver, &cfg)?;
    let name = args
        .name
        .unwrap_or_else(|| default_name(&args.prompt));

    let event_loop = args.event_loop.then_some(codex_cron_core::EventLoopPolicy {
        max_chain_runs: args.max_chain_runs,
        max_runtime_seconds: args.max_runtime_seconds,
    });

    let job = Job::new(
        NewJob {
            id: id::new_id(),
            name,
            prompt: args.prompt,
            executor,
            script,
            schedule,
            schedule_display: display,
            repeat: Repeat {
                times: args.repeat,
                completed: 0,
            },
            deliver,
            workdir: args.workdir,
            context_from: args.context_from,
            codex_model: args.model,
            event_loop,
        },
        now,
    );

    let store = FileJobStore::new(home);
    let mut jobs = store.load()?;
    let id = job.id.clone();
    let next = fmt_dt(job.next_run_at);
    jobs.push(job);
    store.save(&jobs)?;
    println!("added job {id} (next run: {next})");
    Ok(())
}

fn cmd_list(home: &Path, json: bool) -> Result<()> {
    let jobs = FileJobStore::new(home).load()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&jobs)?);
        return Ok(());
    }
    if jobs.is_empty() {
        println!("no jobs. Add one with: codex-cron add \"every 30m\" \"your prompt\"");
        return Ok(());
    }
    println!(
        "{:<14} {:<20} {:<16} {:<22} {:<10} LAST",
        "ID", "NAME", "SCHEDULE", "NEXT RUN", "STATE"
    );
    for j in &jobs {
        println!(
            "{:<14} {:<20} {:<16} {:<22} {:<10} {}",
            j.id,
            truncate(&j.name, 20),
            truncate(&j.schedule_display, 16),
            fmt_dt(j.next_run_at),
            format!("{:?}", j.state),
            j.last_status.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

fn cmd_show(home: &Path, id: &str) -> Result<()> {
    let jobs = FileJobStore::new(home).load()?;
    let job = jobs
        .iter()
        .find(|j| j.id == id)
        .with_context(|| format!("no job with id {id}"))?;
    println!("id:             {}", job.id);
    println!("name:           {}", job.name);
    println!("executor:       {:?}", job.executor);
    println!("schedule:       {}", job.schedule_display);
    println!("state:          {:?}", job.state);
    println!("enabled:        {}", job.enabled);
    println!("created_at:     {}", fmt_dt(Some(job.created_at)));
    println!("next_run_at:    {}", fmt_dt(job.next_run_at));
    println!("last_run_at:    {}", fmt_dt(job.last_run_at));
    println!("last_status:    {}", job.last_status.as_deref().unwrap_or("-"));
    if let Some(err) = &job.last_error {
        println!("last_error:     {err}");
    }
    println!(
        "repeat:         {}/{}",
        job.repeat.completed,
        job.repeat
            .times
            .map(|t| t.to_string())
            .unwrap_or_else(|| "inf".to_string())
    );
    if let Some(s) = &job.script {
        println!("script:         {s}");
    }
    if let Some(w) = &job.workdir {
        println!("workdir:        {}", w.display());
    }
    println!("deliver:        {:?}", job.deliver);
    println!("\nprompt:\n{}", job.prompt);
    Ok(())
}

fn cmd_edit(home: &Path, args: EditArgs) -> Result<()> {
    let store = FileJobStore::new(home);
    let mut jobs = store.load()?;
    let now = Utc::now();
    let job = jobs
        .iter_mut()
        .find(|j| j.id == args.id)
        .with_context(|| format!("no job with id {}", args.id))?;

    if let Some(s) = args.schedule {
        let (schedule, display) =
            parse_schedule(&s, now).map_err(|e| anyhow::anyhow!("invalid schedule: {e}"))?;
        job.next_run_at = compute_next_run(&schedule, job.last_run_at, now);
        job.schedule = schedule;
        job.schedule_display = display;
    }
    if let Some(p) = args.prompt {
        job.prompt = p;
    }
    if let Some(n) = args.name {
        job.name = n;
    }
    if let Some(e) = args.executor {
        job.executor = e.into();
    }
    if let Some(s) = args.script {
        job.script = resolve_script(Some(s))?;
    }
    if let Some(r) = args.repeat {
        job.repeat.times = Some(r);
    }
    if let Some(w) = args.workdir {
        job.workdir = Some(w);
    }
    if let Some(m) = args.model {
        job.codex_model = Some(m);
    }
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
    store.save(&jobs)?;
    println!("updated job {}", args.id);
    Ok(())
}

fn cmd_remove(home: &Path, id: &str) -> Result<()> {
    let store = FileJobStore::new(home);
    let mut jobs = store.load()?;
    let before = jobs.len();
    jobs.retain(|j| j.id != id);
    anyhow::ensure!(jobs.len() < before, "no job with id {id}");
    store.save(&jobs)?;
    println!("removed job {id}");
    Ok(())
}

fn cmd_set_paused(home: &Path, id: &str, paused: bool) -> Result<()> {
    let store = FileJobStore::new(home);
    let mut jobs = store.load()?;
    let now = Utc::now();
    let job = jobs
        .iter_mut()
        .find(|j| j.id == id)
        .with_context(|| format!("no job with id {id}"))?;
    if paused {
        job.enabled = false;
        job.state = JobState::Paused;
    } else {
        job.enabled = true;
        job.state = JobState::Scheduled;
        job.next_run_at = compute_next_run(&job.schedule, job.last_run_at, now);
    }
    store.save(&jobs)?;
    println!("{} job {id}", if paused { "paused" } else { "resumed" });
    Ok(())
}

fn cmd_run(home: &Path, id: &str) -> Result<()> {
    let store = FileJobStore::new(home);
    let mut jobs = store.load()?;
    {
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .with_context(|| format!("no job with id {id}"))?;
        job.enabled = true;
        job.state = JobState::Scheduled;
        job.next_run_at = Some(Utc::now());
    }
    store.save(&jobs)?;

    let report = run_target_tick(home, id)?;
    if let Some(f) = report.fired.iter().find(|f| f.id == id) {
        println!("ran job {id}: {}", f.status.as_str());
    } else {
        println!("job {id} did not fire (already running elsewhere?)");
    }
    print_delivery_errors(&report);
    Ok(())
}

fn cmd_doctor(home: &Path) -> Result<()> {
    println!("codex-cron doctor");
    println!("  home: {}", home.display());

    let mut ok = true;
    match crate::store::ensure_secure_dir(home) {
        Ok(()) => println!("  [ok]   home directory is writable"),
        Err(e) => {
            ok = false;
            println!("  [FAIL] home not writable: {e}");
        }
    }

    let cfg = match Config::load(home) {
        Ok(c) => {
            println!("  [ok]   config.toml parses");
            c
        }
        Err(e) => {
            ok = false;
            println!("  [FAIL] config.toml: {e}");
            Config::default()
        }
    };

    report_binary("codex", &cfg.codex_path);
    report_binary("ao2", &cfg.ao2_path);

    match try_acquire_tick_lock(home) {
        Ok(Some(_)) => println!("  [ok]   tick lock is free"),
        Ok(None) => println!("  [warn] tick lock is held (daemon running?)"),
        Err(e) => println!("  [warn] tick lock check failed: {e}"),
    }

    match FileJobStore::new(home).load() {
        Ok(jobs) => {
            let due = jobs
                .iter()
                .filter(|j| j.enabled && j.next_run_at.is_some_and(|t| t <= Utc::now()))
                .count();
            let next = jobs
                .iter()
                .filter_map(|j| j.next_run_at)
                .min()
                .map(|t| fmt_dt(Some(t)))
                .unwrap_or_else(|| "-".to_string());
            println!("  [ok]   {} job(s); {due} due now; next at {next}", jobs.len());
        }
        Err(e) => {
            ok = false;
            println!("  [FAIL] cannot load jobs: {e}");
        }
    }

    if ok {
        println!("healthy");
        Ok(())
    } else {
        anyhow::bail!("doctor found problems");
    }
}

fn cmd_config(home: &Path, action: ConfigAction) -> Result<()> {
    match action {
        ConfigAction::Show => {
            let cfg = Config::load(home)?;
            for key in [
                "default_executor",
                "max_parallel",
                "codex_path",
                "ao2_path",
                "default_webhook",
                "webhook_allowlist",
                "timezone",
            ] {
                println!("{key} = {}", cfg.get(key).unwrap_or_default());
            }
            Ok(())
        }
        ConfigAction::Get { key } => {
            let cfg = Config::load(home)?;
            match cfg.get(&key) {
                Some(v) => {
                    println!("{v}");
                    Ok(())
                }
                None => anyhow::bail!("unknown config key '{key}'"),
            }
        }
        ConfigAction::Set { key, value } => {
            let mut cfg = Config::load(home)?;
            cfg.set(&key, &value)?;
            cfg.save(home)?;
            println!("set {key} = {value}");
            Ok(())
        }
    }
}

// ---- helpers ----

fn print_tick_report(report: &TickReport) {
    println!(
        "tick: {} fired, {} skipped",
        report.fired.len(),
        report.skipped
    );
    for f in &report.fired {
        let tag = if f.deleted { " (retired)" } else { "" };
        println!("  {} -> {}{tag}", f.id, f.status.as_str());
    }
    print_delivery_errors(report);
}

fn print_delivery_errors(report: &TickReport) {
    for (id, err) in &report.delivery_errors {
        eprintln!("  delivery warning for {id}: {err}");
    }
}

fn report_binary(label: &str, bin: &str) {
    let found = std::process::Command::new(bin)
        .arg("--version")
        .output()
        .is_ok();
    if found {
        println!("  [ok]   {label} binary '{bin}' is runnable");
    } else {
        println!("  [warn] {label} binary '{bin}' not found on PATH");
    }
}

fn executor_from_str(s: &str) -> ExecutorKind {
    match s.to_lowercase().as_str() {
        "shell" => ExecutorKind::Shell,
        "ao2" => ExecutorKind::Ao2,
        _ => ExecutorKind::Codex,
    }
}

fn resolve_script(opt: Option<String>) -> Result<Option<String>> {
    match opt {
        None => Ok(None),
        Some(s) => {
            if let Some(path) = s.strip_prefix('@') {
                let body = std::fs::read_to_string(path)
                    .with_context(|| format!("reading script file {path}"))?;
                Ok(Some(body))
            } else {
                Ok(Some(s))
            }
        }
    }
}

fn parse_deliver(specs: &[String], cfg: &Config) -> Result<Vec<DeliveryTarget>> {
    if specs.is_empty() {
        return Ok(vec![DeliveryTarget::File]);
    }
    let mut out = Vec::new();
    for spec in specs {
        match spec.as_str() {
            "file" => out.push(DeliveryTarget::File),
            "webhook" => {
                let url = cfg
                    .default_webhook
                    .clone()
                    .context("`--deliver webhook` needs a default_webhook in config, or use webhook:URL")?;
                out.push(DeliveryTarget::Webhook { url });
            }
            other => {
                if let Some(url) = other.strip_prefix("webhook:") {
                    out.push(DeliveryTarget::Webhook {
                        url: url.to_string(),
                    });
                } else {
                    anyhow::bail!("invalid --deliver '{other}': use file or webhook:URL");
                }
            }
        }
    }
    Ok(out)
}

fn default_name(prompt: &str) -> String {
    let first = prompt.lines().next().unwrap_or("job").trim();
    truncate(first, 50)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max.saturating_sub(1)).collect();
        t.push('.');
        t
    }
}

fn fmt_dt(dt: Option<DateTime<Utc>>) -> String {
    dt.map(|t| t.format("%Y-%m-%d %H:%M:%SZ").to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_deliver_defaults_to_file() {
        let cfg = Config::default();
        assert_eq!(parse_deliver(&[], &cfg).unwrap(), vec![DeliveryTarget::File]);
    }

    #[test]
    fn parse_deliver_reads_webhook_url() {
        let cfg = Config::default();
        let got = parse_deliver(&["webhook:https://x.test/h".to_string()], &cfg).unwrap();
        assert_eq!(
            got,
            vec![DeliveryTarget::Webhook {
                url: "https://x.test/h".to_string()
            }]
        );
    }

    #[test]
    fn parse_deliver_webhook_without_url_needs_default() {
        let cfg = Config::default();
        assert!(parse_deliver(&["webhook".to_string()], &cfg).is_err());
    }

    #[test]
    fn default_name_truncates() {
        let name = default_name(
            "a very long prompt that goes well beyond the fifty character display limit for names",
        );
        assert!(name.chars().count() <= 50);
    }

    #[test]
    fn executor_from_str_defaults_to_codex() {
        assert_eq!(executor_from_str("nonsense"), ExecutorKind::Codex);
        assert_eq!(executor_from_str("shell"), ExecutorKind::Shell);
        assert_eq!(executor_from_str("ao2"), ExecutorKind::Ao2);
    }

    #[test]
    fn cli_parses_add_subcommand() {
        let cli = Cli::try_parse_from(["codex-cron", "add", "every 30m", "do it"]).unwrap();
        match cli.command {
            Command::Add(a) => {
                assert_eq!(a.schedule, "every 30m");
                assert_eq!(a.prompt, "do it");
            }
            _ => panic!("expected add"),
        }
    }
}
