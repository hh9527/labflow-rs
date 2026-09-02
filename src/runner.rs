use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

use crate::artifact::{ArtifactName, unpublish};
use crate::domain::{ProcessStatus, Timestamp};

const FAILURE_LIMIT: u8 = 3;
const STABLE_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct State {
    control_generation: Option<Timestamp>,
    process: ProcessStatus,
    active_generation: Option<Timestamp>,
    failed_generation: Option<Timestamp>,
    failures: u8,
    shutting_down: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            control_generation: None,
            process: ProcessStatus::Stopped,
            active_generation: None,
            failed_generation: None,
            failures: 0,
            shutting_down: false,
        }
    }
}

#[derive(Debug)]
enum Event {
    ArtifactObserved(Option<Timestamp>),
    SupervisorStarted {
        generation: Timestamp,
    },
    SupervisorStartFailed {
        generation: Timestamp,
        reason: String,
    },
    SupervisorExited {
        generation: Timestamp,
        stable: bool,
        status: String,
    },
    ShutdownRequested,
}

#[derive(Debug, Eq, PartialEq)]
enum Effect {
    StartSupervisor { generation: Timestamp },
    UnpublishSupervisor,
    Log(String),
    Exit,
}

impl Event {
    fn reduce(self, state: &mut State) -> Vec<Effect> {
        let mut effects = match self {
            Event::ArtifactObserved(generation) => {
                state.control_generation = generation;
                Vec::new()
            }
            Event::SupervisorStarted { generation } => {
                if state.process == ProcessStatus::Starting {
                    state.process = ProcessStatus::Running;
                    state.active_generation = Some(generation);
                }
                Vec::new()
            }
            Event::SupervisorStartFailed { generation, reason } => {
                state.process = ProcessStatus::Stopped;
                state.active_generation = None;
                if state.shutting_down {
                    vec![Effect::Exit]
                } else {
                    record_failure(
                        state,
                        generation,
                        false,
                        format!("supervisor failed to start: {reason}"),
                    )
                }
            }
            Event::SupervisorExited {
                generation,
                stable,
                status,
            } => {
                state.process = ProcessStatus::Stopped;
                state.active_generation = None;
                if state.shutting_down {
                    vec![Effect::Exit]
                } else if state.control_generation != Some(generation) {
                    state.failures = 0;
                    state.failed_generation = state.control_generation;
                    vec![Effect::Log(format!("supervisor exited with {status}"))]
                } else {
                    record_failure(
                        state,
                        generation,
                        stable,
                        format!("supervisor exited with {status}"),
                    )
                }
            }
            Event::ShutdownRequested => {
                state.shutting_down = true;
                let mut effects = vec![Effect::UnpublishSupervisor];
                if state.process == ProcessStatus::Stopped {
                    effects.push(Effect::Exit);
                }
                effects
            }
        };

        if !state.shutting_down
            && state.process == ProcessStatus::Stopped
            && let Some(generation) = state.control_generation
            && !effects
                .iter()
                .any(|effect| matches!(effect, Effect::UnpublishSupervisor))
        {
            state.process = ProcessStatus::Starting;
            effects.push(Effect::StartSupervisor { generation });
        }
        effects
    }
}

fn record_failure(
    state: &mut State,
    generation: Timestamp,
    stable: bool,
    message: String,
) -> Vec<Effect> {
    if stable || state.failed_generation != Some(generation) {
        state.failures = 0;
        state.failed_generation = Some(generation);
    }
    state.failures += 1;
    let mut effects = vec![Effect::Log(message)];
    if state.failures >= FAILURE_LIMIT {
        effects.push(Effect::Log(format!(
            "supervisor failed {FAILURE_LIMIT} times; unpublishing system-supervisor"
        )));
        effects.push(Effect::UnpublishSupervisor);
    }
    effects
}

struct ProcessActor {
    child: Option<Child>,
    generation: Option<Timestamp>,
    started: Option<tokio::time::Instant>,
}

impl ProcessActor {
    fn new() -> Self {
        Self {
            child: None,
            generation: None,
            started: None,
        }
    }

    fn start(&mut self, root: &Path, generation: Timestamp) -> Event {
        match start_supervisor(root, generation) {
            Ok(child) => {
                self.child = Some(child);
                self.generation = Some(generation);
                self.started = Some(tokio::time::Instant::now());
                Event::SupervisorStarted { generation }
            }
            Err(error) => Event::SupervisorStartFailed {
                generation,
                reason: error.to_string(),
            },
        }
    }

    fn poll(&mut self) -> Result<Option<Event>> {
        let Some(child) = &mut self.child else {
            return Ok(None);
        };
        let Some(status) = child.try_wait()? else {
            return Ok(None);
        };
        self.child = None;
        let generation = self
            .generation
            .take()
            .context("running supervisor has no generation")?;
        let stable = self
            .started
            .take()
            .is_some_and(|started| started.elapsed() >= STABLE_AFTER);
        Ok(Some(Event::SupervisorExited {
            generation,
            stable,
            status: status.to_string(),
        }))
    }
}

pub async fn run(root: PathBuf) -> Result<()> {
    let control = ArtifactName::parse("system-supervisor")?;
    let initial = crate::runtime::artifact_timestamp(&control.path(&root))?;
    let mut state = State::default();
    let mut process = ProcessActor::new();
    let mut events = VecDeque::from([Event::ArtifactObserved(initial)]);
    let mut observed = initial;
    let mut shutdown = Box::pin(shutdown_signal());
    let mut stopping = false;

    loop {
        if let Some(event) = events.pop_front() {
            for effect in event.reduce(&mut state) {
                match effect {
                    Effect::StartSupervisor { generation } => {
                        events.push_back(process.start(&root, generation));
                    }
                    Effect::UnpublishSupervisor => {
                        let _ = unpublish(&root, &control)?;
                    }
                    Effect::Log(message) => eprintln!("{message}"),
                    Effect::Exit => return Ok(()),
                }
            }
            continue;
        }

        tokio::select! {
            signal = &mut shutdown, if !stopping => {
                signal?;
                stopping = true;
                events.push_back(Event::ShutdownRequested);
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                let current = crate::runtime::artifact_timestamp(&control.path(&root))?;
                if current != observed {
                    observed = current;
                    events.push_back(Event::ArtifactObserved(current));
                }
                if let Some(event) = process.poll()? {
                    events.push_back(event);
                }
            }
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

fn start_supervisor(root: &Path, generation: Timestamp) -> Result<Child> {
    let executable = std::env::current_exe().context("failed to locate labflow executable")?;
    Command::new(executable)
        .arg("--root")
        .arg(root)
        .arg("supervisor")
        .arg("--generation")
        .arg(generation.to_string())
        .stdin(Stdio::null())
        .spawn()
        .context("failed to start supervisor")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reducer_restarts_on_generation_change_and_fuses_crashes() {
        let mut state = State::default();
        assert_eq!(
            Event::ArtifactObserved(Some(10)).reduce(&mut state),
            vec![Effect::StartSupervisor { generation: 10 }]
        );
        Event::SupervisorStarted { generation: 10 }.reduce(&mut state);
        Event::ArtifactObserved(Some(11)).reduce(&mut state);
        let effects = Event::SupervisorExited {
            generation: 10,
            stable: false,
            status: "0".into(),
        }
        .reduce(&mut state);
        assert!(effects.contains(&Effect::StartSupervisor { generation: 11 }));

        Event::SupervisorStarted { generation: 11 }.reduce(&mut state);
        for attempt in 1..=3 {
            let effects = Event::SupervisorExited {
                generation: 11,
                stable: false,
                status: "1".into(),
            }
            .reduce(&mut state);
            assert_eq!(effects.contains(&Effect::UnpublishSupervisor), attempt == 3);
            if attempt < 3 {
                Event::SupervisorStarted { generation: 11 }.reduce(&mut state);
            }
        }
    }

    #[test]
    fn shutdown_unpublishes_and_waits_for_exit() {
        let mut state = State::default();
        Event::ArtifactObserved(Some(1)).reduce(&mut state);
        Event::SupervisorStarted { generation: 1 }.reduce(&mut state);
        assert_eq!(
            Event::ShutdownRequested.reduce(&mut state),
            vec![Effect::UnpublishSupervisor]
        );
        assert_eq!(
            Event::SupervisorExited {
                generation: 1,
                stable: true,
                status: "0".into(),
            }
            .reduce(&mut state),
            vec![Effect::Exit]
        );
    }

    #[test]
    fn shutdown_finishes_when_pending_start_fails() {
        let mut state = State::default();
        Event::ArtifactObserved(Some(1)).reduce(&mut state);
        assert_eq!(state.process, ProcessStatus::Starting);
        Event::ShutdownRequested.reduce(&mut state);
        assert_eq!(
            Event::SupervisorStartFailed {
                generation: 1,
                reason: "spawn failed".into(),
            }
            .reduce(&mut state),
            vec![Effect::Exit]
        );
    }
}
