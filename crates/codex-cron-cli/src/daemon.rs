//! The built-in daemon loop and OS service installation.
//!
//! The loop is `tick → sleep(interval)`. Each tick takes the cross-process lock,
//! so running the daemon and an external `codex-cron tick` at once is safe — one
//! simply skips. The service-unit *generators* are pure strings (unit-tested);
//! install/uninstall write them to the platform's service directory.

use std::path::Path;

/// The launchd / systemd / Task Scheduler identifier.
pub const SERVICE_LABEL: &str = "com.harufumi.codex-cron";

/// Run ticks forever, sleeping `interval_secs` between passes.
pub fn run_loop(home: &Path, interval_secs: u64, event_loop: bool) -> anyhow::Result<()> {
    let interval = std::time::Duration::from_secs(interval_secs.max(1));
    println!(
        "codex-cron daemon: home={}, interval={}s, event_loop={} (ctrl-c to stop)",
        home.display(),
        interval_secs,
        event_loop
    );
    loop {
        let result = if event_loop {
            crate::cli::run_tick_loop(home)
        } else {
            crate::cli::run_one_tick(home).map(|_| ())
        };
        if let Err(e) = result {
            eprintln!("tick error: {e:#}");
        }
        std::thread::sleep(interval);
    }
}

/// A launchd agent plist that keeps the daemon alive.
pub fn launchd_plist(label: &str, program: &str, args: &[String], home: &Path) -> String {
    let mut argv = format!("    <string>{program}</string>\n");
    for a in args {
        argv.push_str(&format!("    <string>{}</string>\n", xml_escape(a)));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{argv}  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>CODEX_CRON_HOME</key>
    <string>{home}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
</dict>
</plist>
"#,
        home = xml_escape(&home.display().to_string()),
    )
}

/// A systemd *user* service unit that restarts the daemon on exit.
pub fn systemd_service(program: &str, args: &[String], home: &Path) -> String {
    let exec = std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "[Unit]\n\
         Description=codex-cron durable agent scheduler\n\
         After=network-online.target\n\n\
         [Service]\n\
         Type=simple\n\
         Environment=CODEX_CRON_HOME={home}\n\
         ExecStart={exec}\n\
         Restart=always\n\
         RestartSec=10\n\n\
         [Install]\n\
         WantedBy=default.target\n",
        home = home.display(),
    )
}

/// The `schtasks` command that registers the daemon to run at logon (Windows).
pub fn windows_schtasks_create(program: &str, home: &Path) -> String {
    format!(
        "schtasks /Create /TN {SERVICE_LABEL} /SC ONLOGON /TR \"cmd /C set CODEX_CRON_HOME={home}&& \\\"{program}\\\" daemon\" /F",
        home = home.display(),
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Register the daemon as an OS service for the current platform.
pub fn install(home: &Path, interval: u64) -> anyhow::Result<()> {
    let exe = std::env::current_exe()?;
    let program = exe.to_string_lossy().to_string();
    let args = vec![
        "daemon".to_string(),
        "--interval".to_string(),
        interval.to_string(),
    ];

    #[cfg(target_os = "macos")]
    {
        let plist = launchd_plist(SERVICE_LABEL, &program, &args, home);
        let dir = dirs_home()?.join("Library/LaunchAgents");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{SERVICE_LABEL}.plist"));
        crate::store::atomic_write(&path, plist.as_bytes())?;
        let _ = std::process::Command::new("launchctl")
            .args(["unload", &path.to_string_lossy()])
            .output();
        let status = std::process::Command::new("launchctl")
            .args(["load", "-w", &path.to_string_lossy()])
            .status()?;
        anyhow::ensure!(status.success(), "launchctl load failed");
        println!("installed launchd agent: {}", path.display());
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        let unit = systemd_service(&program, &args, home);
        let dir = dirs_home()?.join(".config/systemd/user");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("codex-cron.service");
        crate::store::atomic_write(&path, unit.as_bytes())?;
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        let status = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", "codex-cron.service"])
            .status()?;
        anyhow::ensure!(status.success(), "systemctl enable failed");
        println!("installed systemd user unit: {}", path.display());
        Ok(())
    }
    #[cfg(target_os = "windows")]
    {
        let _ = (&program, home, &args);
        anyhow::bail!(
            "automatic install on Windows is not wired; run this to register at logon:\n  {}",
            windows_schtasks_create(&program, home)
        );
    }
}

/// Remove the OS service for the current platform.
pub fn uninstall() -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let path = dirs_home()?
            .join("Library/LaunchAgents")
            .join(format!("{SERVICE_LABEL}.plist"));
        let _ = std::process::Command::new("launchctl")
            .args(["unload", "-w", &path.to_string_lossy()])
            .output();
        let _ = std::fs::remove_file(&path);
        println!("removed launchd agent");
    }
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "disable", "--now", "codex-cron.service"])
            .status();
        let path = dirs_home()?.join(".config/systemd/user/codex-cron.service");
        let _ = std::fs::remove_file(&path);
        println!("removed systemd user unit");
    }
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("schtasks")
            .args(["/Delete", "/TN", SERVICE_LABEL, "/F"])
            .status();
        println!("removed scheduled task");
    }
    Ok(())
}

#[allow(dead_code)]
fn dirs_home() -> anyhow::Result<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("no HOME/USERPROFILE in environment"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn args() -> Vec<String> {
        vec![
            "daemon".to_string(),
            "--interval".to_string(),
            "60".to_string(),
        ]
    }

    #[test]
    fn launchd_plist_has_label_program_and_keepalive() {
        let p = launchd_plist(
            SERVICE_LABEL,
            "/usr/local/bin/codex-cron",
            &args(),
            &PathBuf::from("/home/u/.codex-cron"),
        );
        assert!(p.contains(&format!("<string>{SERVICE_LABEL}</string>")));
        assert!(p.contains("<string>/usr/local/bin/codex-cron</string>"));
        assert!(p.contains("<string>daemon</string>"));
        assert!(p.contains("<key>KeepAlive</key>"));
        assert!(p.contains("CODEX_CRON_HOME"));
        assert!(p.contains("/home/u/.codex-cron"));
    }

    #[test]
    fn systemd_unit_has_execstart_and_restart() {
        let u = systemd_service(
            "/usr/local/bin/codex-cron",
            &args(),
            &PathBuf::from("/home/u/.codex-cron"),
        );
        assert!(u.contains("ExecStart=/usr/local/bin/codex-cron daemon --interval 60"));
        assert!(u.contains("Restart=always"));
        assert!(u.contains("WantedBy=default.target"));
        assert!(u.contains("Environment=CODEX_CRON_HOME=/home/u/.codex-cron"));
    }

    #[test]
    fn windows_command_targets_label_and_daemon() {
        let c = windows_schtasks_create("C:\\cc\\codex-cron.exe", &PathBuf::from("C:\\cc\\home"));
        assert!(c.contains(SERVICE_LABEL));
        assert!(c.contains("daemon"));
        assert!(c.contains("ONLOGON"));
    }

    #[test]
    fn xml_escape_escapes_specials() {
        assert_eq!(xml_escape("a&b<c>d"), "a&amp;b&lt;c&gt;d");
    }
}
