//! `codex-cron` binary entry point: parse args, dispatch, map errors to exit 1.

use clap::Parser;

fn main() {
    let cli = codex_cron_cli::cli::Cli::parse();
    if let Err(e) = codex_cron_cli::cli::run(cli) {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}
