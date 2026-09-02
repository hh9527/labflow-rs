use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use inotify::{Inotify, WatchMask};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::artifact::{ARTIFACTS_DIR, ArtifactName, publish};
use crate::db::Databases;
use crate::domain::{Effect, Event, Timestamp};
use crate::plan::{Backend, Plan};
use crate::prompt::{build_task_prompt, check_task};

enum BackendCommand {
    Restart,
    Stop,
}

#[derive(Clone)]
struct EffectContext {
    root: Arc<PathBuf>,
    plan: Arc<Plan>,
    databases: Databases,
    client: Client,
    backend_url: Arc<String>,
    event_tx: mpsc::Sender<Event>,
    backend_tx: mpsc::Sender<BackendCommand>,
    shutdown_tx: watch::Sender<bool>,
}

pub async fn run(root: PathBuf) -> Result<()> {
    let plan = Arc::new(Plan::load(&root)?);
    let databases = Databases::initialize(&root)?;
    let mut state = databases.restore(plan.clone())?;
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(256);
    let (backend_tx, backend_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let backend_url = Arc::new(format!(
        "http://{}:{}",
        plan.backend.hostname, plan.backend.port
    ));
    let client = Client::builder()
        .timeout(Duration::from_secs(30 * 60))
        .build()?;
    let context = Arc::new(EffectContext {
        root: Arc::new(root.clone()),
        plan: plan.clone(),
        databases: databases.clone(),
        client: client.clone(),
        backend_url: backend_url.clone(),
        event_tx: event_tx.clone(),
        backend_tx: backend_tx.clone(),
        shutdown_tx: shutdown_tx.clone(),
    });

    let backend_handle = spawn_backend_manager(
        root.clone(),
        plan.backend.clone(),
        backend_rx,
        shutdown_rx.clone(),
    );
    if let Err(error) = wait_for_backend(&client, &backend_url).await {
        let _ = backend_tx.send(BackendCommand::Stop).await;
        let _ = backend_handle.await;
        return Err(error);
    }
    let watcher_handle =
        spawn_artifact_watcher(root.clone(), event_tx.clone(), shutdown_rx.clone())?;
    let timeline_handle = spawn_timeline_collector(
        client,
        backend_url,
        root.clone(),
        databases,
        event_tx.clone(),
        shutdown_rx.clone(),
    );
    scan_artifacts(&root, &event_tx).await?;
    event_tx
        .send(Event::SupervisorStarted)
        .await
        .map_err(|_| anyhow!("event loop stopped"))?;

    let mut signal_shutdown = shutdown_rx.clone();
    loop {
        tokio::select! {
            event = event_rx.recv() => {
                let Some(event) = event else { break };
                let effects = event.reduce(&mut state);
                for effect in effects {
                    if is_persistence(&effect) {
                        apply_effect(effect, context.clone()).await;
                    } else {
                        tokio::spawn(apply_effect(effect, context.clone()));
                    }
                }
            }
            changed = signal_shutdown.changed() => {
                if changed.is_err() || *signal_shutdown.borrow() {
                    break;
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = backend_tx.send(BackendCommand::Stop).await;
    watcher_handle.abort();
    timeline_handle.abort();
    let _ = backend_handle.await;
    Ok(())
}

fn is_persistence(effect: &Effect) -> bool {
    matches!(
        effect,
        Effect::PersistArtifact { .. }
            | Effect::PersistVirtualArtifact { .. }
            | Effect::PersistObserverSession { .. }
            | Effect::PersistSession { .. }
            | Effect::PersistTask { .. }
    )
}

async fn apply_effect(effect: Effect, context: Arc<EffectContext>) {
    let failure_target = effect_target(&effect);
    let result = effect.apply(context.clone()).await;
    if let Err(error) = result {
        eprintln!("effect failed: {error:#}");
        if let Some(target) = failure_target {
            let event = match target {
                FailureTarget::Observer => Event::ObserverSessionCreateFailed {
                    reason: error.to_string(),
                },
                FailureTarget::Session { role } => Event::SessionCreateFailed {
                    role,
                    reason: error.to_string(),
                },
                FailureTarget::Task {
                    artifact,
                    request_id,
                } => Event::EffectFailed {
                    artifact: Some(artifact),
                    request_id,
                    reason: error.to_string(),
                },
            };
            let _ = context.event_tx.send(event).await;
        }
    }
}

enum FailureTarget {
    Observer,
    Session {
        role: String,
    },
    Task {
        artifact: ArtifactName,
        request_id: u64,
    },
}

fn effect_target(effect: &Effect) -> Option<FailureTarget> {
    match effect {
        Effect::CreateObserverSession => Some(FailureTarget::Observer),
        Effect::CreateSession { role, .. } => Some(FailureTarget::Session { role: role.clone() }),
        Effect::PrepareTask {
            artifact,
            request_id,
            ..
        }
        | Effect::PromptSession {
            artifact,
            request_id,
            ..
        }
        | Effect::CheckTask {
            artifact,
            request_id,
        }
        | Effect::PublishArtifact {
            artifact,
            request_id,
        } => Some(FailureTarget::Task {
            artifact: artifact.clone(),
            request_id: *request_id,
        }),
        _ => None,
    }
}

impl Effect {
    async fn apply(self, context: Arc<EffectContext>) -> Result<()> {
        match self {
            Effect::PersistArtifact { name, modified } => {
                context.databases.persist_artifact(&name, modified)
            }
            Effect::PersistVirtualArtifact { name, modified } => {
                context.databases.persist_virtual(&name, modified)
            }
            Effect::PersistObserverSession { session_id } => {
                context.databases.persist_observer_session(&session_id)
            }
            Effect::PersistSession { role, session } => {
                context.databases.persist_session(&role, &session)
            }
            Effect::PersistTask { artifact, task } => {
                context.databases.persist_task(&artifact, task.as_ref())
            }
            Effect::CreateObserverSession => {
                let response = context
                    .client
                    .post(format!("{}/session", context.backend_url))
                    .query(&[("directory", context.root.to_string_lossy().as_ref())])
                    .json(&json!({ "title": "lab-ob" }))
                    .send()
                    .await?
                    .error_for_status()?;
                let value: Value = response.json().await?;
                let session_id = value["id"]
                    .as_str()
                    .context("OpenCode observer session response has no id")?
                    .to_owned();
                context
                    .event_tx
                    .send(Event::ObserverSessionCreated { session_id })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))?;
                Ok(())
            }
            Effect::CreateSession {
                role,
                parent_session_id,
                request_id,
            } => {
                let permissions = context.plan.roles[&role]
                    .permissions
                    .iter()
                    .map(|permission| {
                        json!({
                            "permission": permission,
                            "pattern": "*",
                            "action": "allow",
                        })
                    })
                    .collect::<Vec<_>>();
                let response = context
                    .client
                    .post(format!("{}/session", context.backend_url))
                    .query(&[("directory", context.root.to_string_lossy().as_ref())])
                    .json(&json!({
                        "parentID": parent_session_id,
                        "title": format!("labflow:{role}"),
                        "permission": permissions,
                    }))
                    .send()
                    .await?
                    .error_for_status()?;
                let value: Value = response.json().await?;
                let session_id = value["id"]
                    .as_str()
                    .context("OpenCode session response has no id")?
                    .to_owned();
                context
                    .event_tx
                    .send(Event::SessionCreated { role, session_id })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))?;
                let _ = request_id;
                Ok(())
            }
            Effect::PrepareTask {
                artifact,
                request_id,
                failures,
            } => {
                let prompt = build_task_prompt(&context.root, &context.plan, &artifact, &failures)?;
                context
                    .event_tx
                    .send(Event::TaskPrepared {
                        artifact,
                        request_id,
                        prompt,
                    })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))?;
                Ok(())
            }
            Effect::PromptSession {
                artifact,
                role: _,
                session_id,
                request_id,
                prompt,
            } => {
                let response = context
                    .client
                    .post(format!(
                        "{}/session/{session_id}/message",
                        context.backend_url
                    ))
                    .query(&[("directory", context.root.to_string_lossy().as_ref())])
                    .json(&json!({ "parts": [{ "type": "text", "text": prompt }] }))
                    .send()
                    .await?
                    .error_for_status()?;
                let value: Value = response.json().await?;
                let content = value["parts"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|part| part["type"] == "text")
                    .filter_map(|part| part["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                context
                    .event_tx
                    .send(Event::TaskAnswered {
                        artifact,
                        request_id,
                        content,
                    })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))?;
                Ok(())
            }
            Effect::CheckTask {
                artifact,
                request_id,
            } => {
                let definition = context
                    .plan
                    .artifacts
                    .get(&artifact)
                    .context("unknown artifact in check effect")?;
                let missing = check_task(&context.root, definition);
                context
                    .event_tx
                    .send(Event::TaskChecked {
                        artifact,
                        request_id,
                        missing,
                    })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))?;
                Ok(())
            }
            Effect::PublishArtifact {
                artifact,
                request_id: _,
            } => publish(&context.root, &artifact),
            Effect::RestartBackend => context
                .backend_tx
                .send(BackendCommand::Restart)
                .await
                .map_err(|_| anyhow!("backend manager stopped")),
            Effect::ExitSupervisor => {
                context
                    .shutdown_tx
                    .send(true)
                    .map_err(|_| anyhow!("supervisor already stopped"))?;
                Ok(())
            }
        }
    }
}

fn spawn_backend_manager(
    root: PathBuf,
    backend: Backend,
    mut commands: mpsc::Receiver<BackendCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut child = start_backend(&root, &backend)?;
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(BackendCommand::Restart) => {
                        stop_child(&mut child).await;
                        child = start_backend(&root, &backend)?;
                    }
                    Some(BackendCommand::Stop) | None => break,
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                status = child.wait() => {
                    let status = status?;
                    if *shutdown.borrow() { break; }
                    eprintln!("OpenCode backend exited with {status}; restarting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    child = start_backend(&root, &backend)?;
                }
            }
        }
        stop_child(&mut child).await;
        Ok(())
    })
}

fn start_backend(root: &Path, backend: &Backend) -> Result<Child> {
    let (program, arguments) = backend
        .command
        .split_first()
        .context("backend command cannot be empty")?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .arg("--hostname")
        .arg(&backend.hostname)
        .arg("--port")
        .arg(backend.port.to_string())
        .current_dir(root)
        .kill_on_drop(true);
    command.spawn().with_context(|| {
        format!(
            "failed to start backend command `{}`",
            backend.command.join(" ")
        )
    })
}

async fn stop_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill().await;
    }
}

async fn wait_for_backend(client: &Client, url: &str) -> Result<()> {
    for _ in 0..100 {
        if client
            .get(format!("{url}/global/health"))
            .timeout(Duration::from_millis(300))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("OpenCode backend did not become healthy at {url}")
}

fn spawn_artifact_watcher(
    root: PathBuf,
    event_tx: mpsc::Sender<Event>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>> {
    let directory = root.join(ARTIFACTS_DIR);
    std::fs::create_dir_all(&directory)?;
    let inotify = Inotify::init()?;
    inotify.watches().add(
        &directory,
        WatchMask::CREATE
            | WatchMask::MODIFY
            | WatchMask::ATTRIB
            | WatchMask::CLOSE_WRITE
            | WatchMask::DELETE
            | WatchMask::MOVED_FROM
            | WatchMask::MOVED_TO,
    )?;
    Ok(tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        let Ok(mut stream) = inotify.into_event_stream(&mut buffer) else {
            return;
        };
        loop {
            tokio::select! {
                event = stream.next() => {
                    let Some(Ok(event)) = event else { break };
                    let Some(name) = event.name.as_deref().and_then(OsStr::to_str) else {
                        continue;
                    };
                    let Ok(name) = ArtifactName::parse(name) else { continue };
                    let modified = observed_timestamp(&name.path(&root)).ok().flatten();
                    if event_tx.send(Event::ArtifactObserved { name, modified }).await.is_err() {
                        break;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
            }
        }
    }))
}

async fn scan_artifacts(root: &Path, event_tx: &mpsc::Sender<Event>) -> Result<()> {
    let directory = root.join(ARTIFACTS_DIR);
    let mut names = BTreeSet::new();
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && let Some(name) = entry.file_name().to_str()
            && let Ok(name) = ArtifactName::parse(name)
        {
            names.insert(name);
        }
    }
    for built_in in ["system-active", "system-supervisor", "system-backend"] {
        names.insert(ArtifactName::parse(built_in).expect("built-in name"));
    }
    for name in names {
        let modified = observed_timestamp(&name.path(root))?;
        event_tx
            .send(Event::ArtifactObserved { name, modified })
            .await
            .map_err(|_| anyhow!("event loop stopped"))?;
    }
    Ok(())
}

fn observed_timestamp(path: &Path) -> Result<Option<Timestamp>> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(system_time_micros(metadata.modified()?))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn system_time_micros(time: SystemTime) -> Timestamp {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn spawn_timeline_collector(
    client: Client,
    backend_url: Arc<String>,
    root: PathBuf,
    databases: Databases,
    event_tx: mpsc::Sender<Event>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while !*shutdown.borrow() {
            let response = client
                .get(format!("{backend_url}/event"))
                .query(&[("directory", root.to_string_lossy().as_ref())])
                .send()
                .await;
            let Ok(response) = response.and_then(reqwest::Response::error_for_status) else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            let mut stream = response.bytes_stream();
            let mut buffer = String::new();
            loop {
                tokio::select! {
                    chunk = stream.next() => match chunk {
                        Some(Ok(chunk)) => {
                            buffer.push_str(&String::from_utf8_lossy(&chunk));
                            while let Some(end) = buffer.find("\n\n") {
                                let frame = buffer[..end].to_owned();
                                buffer.drain(..end + 2);
                                for data in frame.lines().filter_map(|line| line.strip_prefix("data: ")) {
                                    if let Ok(value) = serde_json::from_str::<Value>(data) {
                                        let kind = value["type"].as_str().unwrap_or("unknown");
                                        let _ = databases.append_timeline(
                                            system_time_micros(SystemTime::now()),
                                            "opencode-event",
                                            kind,
                                            &value,
                                        );
                                        if let Some(event) = extract_opencode_event(&value) {
                                            let _ = event_tx.send(event).await;
                                        }
                                    }
                                }
                            }
                        }
                        Some(Err(error)) => {
                            eprintln!("OpenCode event stream failed: {error}");
                            break;
                        }
                        None => break,
                    },
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return; }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

fn extract_opencode_event(value: &Value) -> Option<Event> {
    let kind = value["type"].as_str()?;
    let session_id = value["properties"]["sessionID"].as_str()?.to_owned();
    match kind {
        "session.idle" => Some(Event::SessionStatusChanged {
            session_id,
            busy: false,
        }),
        "session.status" => {
            let status = &value["properties"]["status"];
            let status = status
                .as_str()
                .or_else(|| status["type"].as_str())
                .unwrap_or("busy");
            Some(Event::SessionStatusChanged {
                session_id,
                busy: status != "idle",
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_session_status_events() {
        let event = extract_opencode_event(&json!({
            "type": "session.status",
            "properties": {
                "sessionID": "ses_1",
                "status": { "type": "busy" }
            }
        }));
        assert!(matches!(
            event,
            Some(Event::SessionStatusChanged { session_id, busy: true }) if session_id == "ses_1"
        ));
    }
}
