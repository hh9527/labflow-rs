use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

use crate::artifact::{ArtifactName, unpublish};

const FAILURE_LIMIT: u8 = 3;
const STABLE_AFTER: Duration = Duration::from_secs(30);

pub async fn run(root: PathBuf) -> Result<()> {
    let control = ArtifactName::parse("system-supervisor")?;
    let mut child: Option<(Child, i64, tokio::time::Instant)> = None;
    let mut failed_generation = None;
    let mut failures = 0_u8;
    let mut shutdown = Box::pin(shutdown_signal());

    loop {
        tokio::select! {
            signal = &mut shutdown => {
                signal?;
                let _ = unpublish(&root, &control)?;
                stop(&mut child).await;
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }

        let generation = crate::runtime::artifact_timestamp(&control.path(&root))?;
        if generation.is_none() {
            stop(&mut child).await;
            failed_generation = None;
            failures = 0;
            continue;
        }
        let generation = generation.expect("checked above");

        if child
            .as_ref()
            .is_some_and(|(_, active, _)| *active != generation)
        {
            stop(&mut child).await;
        }
        let exited = match child.as_mut() {
            Some((process, active, started)) => process
                .try_wait()?
                .map(|status| (status, *active, started.elapsed() >= STABLE_AFTER)),
            None => None,
        };
        if let Some((status, active, stable)) = exited {
            eprintln!("supervisor exited with {status}");
            child = None;
            if stable || failed_generation != Some(active) {
                failures = 0;
                failed_generation = Some(active);
            }
            failures += 1;
            if failures >= FAILURE_LIMIT {
                eprintln!(
                    "supervisor failed {FAILURE_LIMIT} times; unpublishing system-supervisor"
                );
                let _ = unpublish(&root, &control)?;
                failures = 0;
                failed_generation = None;
                continue;
            }
        }
        if child.is_none() {
            child = Some((
                start_supervisor(&root)?,
                generation,
                tokio::time::Instant::now(),
            ));
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal?,
        _ = terminate.recv() => {},
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn start_supervisor(root: &Path) -> Result<Child> {
    let executable = std::env::current_exe().context("failed to locate labflow executable")?;
    Command::new(executable)
        .arg("--root")
        .arg(root)
        .arg("supervisor")
        .stdin(Stdio::null())
        .spawn()
        .context("failed to start supervisor")
}

async fn stop(child: &mut Option<(Child, i64, tokio::time::Instant)>) {
    if let Some((mut process, _, _)) = child.take() {
        #[cfg(unix)]
        if let Some(pid) = process.id() {
            // Child has its own signal handlers and must be allowed to reap OpenCode.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
        #[cfg(not(unix))]
        let _ = process.start_kill();

        if tokio::time::timeout(Duration::from_secs(5), process.wait())
            .await
            .is_err()
        {
            let _ = process.kill().await;
        }
    }
}
