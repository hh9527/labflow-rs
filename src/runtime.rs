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
use tokio::task::JoinSet;

use crate::artifact::{ARTIFACTS_DIR, ArtifactName, publish};
use crate::config::Config;
use crate::db::Databases;
use crate::domain::{Effect, Event, Timestamp};
use crate::plan::{Backend, Plan};
use crate::prompt::{build_task_prompt, check_task};

enum BackendCommand {
    Start(Timestamp),
    Stop,
}

struct BackendManagerContext {
    root: PathBuf,
    backend: Backend,
    port: u16,
    client: Client,
    url: Arc<String>,
    event_tx: mpsc::Sender<Event>,
}

enum SupervisorControlEvent {
    Started,
    PlanLoaded {
        request_id: u64,
        plan: Arc<Plan>,
    },
    PlanLoadFailed {
        request_id: u64,
        generation: Option<Timestamp>,
        reason: String,
    },
    PlanPublishObserved {
        request_id: u64,
    },
    SupervisorDisabled,
    GenerationExited {
        request_id: u64,
        reload: bool,
    },
}

enum SupervisorControlEffect {
    LoadPlan {
        request_id: u64,
    },
    RunGeneration {
        request_id: u64,
        plan: Arc<Plan>,
    },
    WaitForPlanPublish {
        request_id: u64,
        generation: Option<Timestamp>,
    },
    PersistHostTasks(crate::db::HostTasks),
    ClearTasks,
    Log(String),
    Exit,
}

struct SupervisorControlState {
    active_request: Option<u64>,
    next_request: u64,
    resume_tasks: bool,
}

impl Default for SupervisorControlState {
    fn default() -> Self {
        Self {
            active_request: None,
            next_request: 0,
            resume_tasks: true,
        }
    }
}

impl SupervisorControlEvent {
    fn reduce(self, state: &mut SupervisorControlState) -> Vec<SupervisorControlEffect> {
        match self {
            Self::Started => load_plan(state),
            Self::PlanLoaded { request_id, plan } if state.active_request == Some(request_id) => {
                let mut effects =
                    vec![SupervisorControlEffect::PersistHostTasks(Default::default())];
                if !state.resume_tasks {
                    effects.push(SupervisorControlEffect::ClearTasks);
                }
                state.resume_tasks = true;
                effects.push(SupervisorControlEffect::RunGeneration { request_id, plan });
                effects
            }
            Self::PlanLoadFailed {
                request_id,
                generation,
                reason,
            } if state.active_request == Some(request_id) => vec![
                SupervisorControlEffect::Log(format!("plan load failed: {reason}")),
                SupervisorControlEffect::PersistHostTasks(crate::db::HostTasks {
                    tasks: vec![ArtifactName::parse("system-plan").expect("built-in name")],
                    opt: Vec::new(),
                }),
                SupervisorControlEffect::WaitForPlanPublish {
                    request_id,
                    generation,
                },
            ],
            Self::PlanPublishObserved { request_id }
                if state.active_request == Some(request_id) =>
            {
                load_plan(state)
            }
            Self::GenerationExited { request_id, reload }
                if state.active_request == Some(request_id) =>
            {
                if reload {
                    state.resume_tasks = false;
                    load_plan(state)
                } else {
                    vec![SupervisorControlEffect::Exit]
                }
            }
            Self::SupervisorDisabled => vec![SupervisorControlEffect::Exit],
            _ => Vec::new(),
        }
    }
}

fn load_plan(state: &mut SupervisorControlState) -> Vec<SupervisorControlEffect> {
    state.next_request += 1;
    state.active_request = Some(state.next_request);
    vec![SupervisorControlEffect::LoadPlan {
        request_id: state.next_request,
    }]
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
    reload_tx: watch::Sender<bool>,
}

pub async fn run(root: PathBuf, expected_supervisor_generation: Option<Timestamp>) -> Result<()> {
    let databases = Databases::initialize(&root)?;
    let mut state = SupervisorControlState::default();
    let mut events = std::collections::VecDeque::from([SupervisorControlEvent::Started]);
    loop {
        let event = events
            .pop_front()
            .expect("control reducer always produces a terminal event");
        for effect in event.reduce(&mut state) {
            match effect {
                SupervisorControlEffect::LoadPlan { request_id } => {
                    let event = match Plan::load(&root) {
                        Ok(plan) => SupervisorControlEvent::PlanLoaded {
                            request_id,
                            plan: Arc::new(plan),
                        },
                        Err(error) => SupervisorControlEvent::PlanLoadFailed {
                            request_id,
                            generation: artifact_timestamp(
                                &ArtifactName::parse("system-plan")?.path(&root),
                            )?,
                            reason: format!("{error:#}"),
                        },
                    };
                    events.push_back(event);
                }
                SupervisorControlEffect::RunGeneration { request_id, plan } => {
                    let reload = run_generation(
                        root.clone(),
                        plan,
                        databases.clone(),
                        expected_supervisor_generation,
                    )
                    .await?;
                    events
                        .push_back(SupervisorControlEvent::GenerationExited { request_id, reload });
                }
                SupervisorControlEffect::WaitForPlanPublish {
                    request_id,
                    generation,
                } => {
                    if wait_for_artifact_generation(
                        &root,
                        "system-plan",
                        generation,
                        expected_supervisor_generation,
                    )
                    .await?
                    {
                        events
                            .push_back(SupervisorControlEvent::PlanPublishObserved { request_id });
                    } else {
                        events.push_back(SupervisorControlEvent::SupervisorDisabled);
                    }
                }
                SupervisorControlEffect::PersistHostTasks(tasks) => {
                    databases.persist_host_tasks(&tasks)?;
                }
                SupervisorControlEffect::ClearTasks => databases.clear_tasks()?,
                SupervisorControlEffect::Log(message) => eprintln!("{message}"),
                SupervisorControlEffect::Exit => return Ok(()),
            }
        }
    }
}

async fn run_generation(
    root: PathBuf,
    plan: Arc<Plan>,
    databases: Databases,
    expected_supervisor_generation: Option<Timestamp>,
) -> Result<bool> {
    let mut state = databases.restore(plan.clone())?;
    state.supervisor_generation = Some(expected_supervisor_generation);
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(256);
    let (backend_tx, backend_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (reload_tx, mut reload_rx) = watch::channel(false);
    let config = Config::load(&root)?;
    let backend_url = Arc::new(format!("http://{}:{}", plan.backend.hostname, config.port));
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
        reload_tx,
    });

    let backend_handle = spawn_backend_manager(
        BackendManagerContext {
            root: root.clone(),
            backend: plan.backend.clone(),
            port: config.port,
            client: client.clone(),
            url: backend_url.clone(),
            event_tx: event_tx.clone(),
        },
        backend_rx,
        shutdown_rx.clone(),
    );
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
    let mut process_signal = Box::pin(shutdown_signal());
    let mut reload = false;
    let mut effect_tasks = JoinSet::new();
    loop {
        tokio::select! {
            event = event_rx.recv() => {
                let Some(event) = event else { break };
                let effects = event.reduce(&mut state);
                for effect in effects {
                    if is_persistence(&effect) {
                        apply_effect(effect, context.clone()).await?;
                    } else {
                        effect_tasks.spawn(apply_effect(effect, context.clone()));
                    }
                }
            }
            completed = effect_tasks.join_next(), if !effect_tasks.is_empty() => {
                match completed {
                    Some(Ok(Err(error))) => eprintln!("effect failed: {error:#}"),
                    Some(Err(error)) => eprintln!("effect task failed: {error}"),
                    _ => {}
                }
            }
            changed = signal_shutdown.changed() => {
                if changed.is_err() || *signal_shutdown.borrow() {
                    break;
                }
            }
            changed = reload_rx.changed() => {
                if changed.is_err() || *reload_rx.borrow() {
                    reload = true;
                    break;
                }
            }
            signal = &mut process_signal => {
                signal?;
                break;
            }
        }
    }

    let _ = shutdown_tx.send(true);
    effect_tasks.abort_all();
    while effect_tasks.join_next().await.is_some() {}
    let _ = backend_tx.send(BackendCommand::Stop).await;
    watcher_handle.abort();
    timeline_handle.abort();
    let _ = backend_handle.await;
    Ok(reload)
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

async fn wait_for_artifact_generation(
    root: &Path,
    name: &str,
    previous: Option<Timestamp>,
    expected_supervisor_generation: Option<Timestamp>,
) -> Result<bool> {
    let artifact = ArtifactName::parse(name)?;
    loop {
        let generation = artifact_timestamp(&artifact.path(root))?;
        if generation.is_some() && generation != previous {
            return Ok(true);
        }
        let supervisor = ArtifactName::parse("system-supervisor")?;
        if artifact_timestamp(&supervisor.path(root))? != expected_supervisor_generation {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn is_persistence(effect: &Effect) -> bool {
    matches!(
        effect,
        Effect::PersistArtifact { .. }
            | Effect::PersistVirtualArtifact { .. }
            | Effect::PersistObserverSession { .. }
            | Effect::PersistSession { .. }
            | Effect::PersistTask { .. }
            | Effect::PersistHostTasks { .. }
            | Effect::PersistNextRequest { .. }
    )
}

async fn apply_effect(effect: Effect, context: Arc<EffectContext>) -> Result<()> {
    let failure_target = effect_target(&effect);
    let result = effect.apply(context.clone()).await;
    if let Err(error) = result {
        eprintln!("effect failed: {error:#}");
        if let Some(target) = failure_target {
            let event = match target {
                FailureTarget::Observer { request_id } => Event::ObserverSessionCreateFailed {
                    request_id,
                    reason: error.to_string(),
                },
                FailureTarget::Session { role, request_id } => Event::SessionCreateFailed {
                    role,
                    request_id,
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
            return Ok(());
        }
        return Err(error);
    }
    Ok(())
}

enum FailureTarget {
    Observer {
        request_id: u64,
    },
    Session {
        role: String,
        request_id: u64,
    },
    Task {
        artifact: ArtifactName,
        request_id: u64,
    },
}

fn effect_target(effect: &Effect) -> Option<FailureTarget> {
    match effect {
        Effect::CreateObserverSession { request_id } => Some(FailureTarget::Observer {
            request_id: *request_id,
        }),
        Effect::CreateSession {
            role, request_id, ..
        } => Some(FailureTarget::Session {
            role: role.clone(),
            request_id: *request_id,
        }),
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
            Effect::PersistHostTasks { tasks } => context.databases.persist_host_tasks(&tasks),
            Effect::PersistNextRequest { next } => context.databases.persist_next_request_id(next),
            Effect::CreateObserverSession { request_id } => {
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
                    .send(Event::ObserverSessionCreated {
                        request_id,
                        session_id,
                    })
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
                    .send(Event::SessionCreated {
                        role,
                        request_id,
                        session_id,
                    })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))?;
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
            Effect::StartBackend { generation } => context
                .backend_tx
                .send(BackendCommand::Start(generation))
                .await
                .map_err(|_| anyhow!("backend manager stopped")),
            Effect::StopBackend => context
                .backend_tx
                .send(BackendCommand::Stop)
                .await
                .map_err(|_| anyhow!("backend manager stopped")),
            Effect::DelayBackendRetry { generation } => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                context
                    .event_tx
                    .send(Event::BackendRetry { generation })
                    .await
                    .map_err(|_| anyhow!("event loop stopped"))
            }
            Effect::Log { message } => {
                eprintln!("{message}");
                Ok(())
            }
            Effect::ExitSupervisor => {
                context
                    .shutdown_tx
                    .send(true)
                    .map_err(|_| anyhow!("supervisor already stopped"))?;
                Ok(())
            }
            Effect::ReloadPlan => {
                context
                    .reload_tx
                    .send(true)
                    .map_err(|_| anyhow!("supervisor already stopped"))?;
                Ok(())
            }
        }
    }
}

fn spawn_backend_manager(
    context: BackendManagerContext,
    mut commands: mpsc::Receiver<BackendCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut child: Option<Child> = None;
        let mut generation = None;
        let mut poll = tokio::time::interval(Duration::from_millis(100));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(BackendCommand::Start(requested)) => {
                        match start_backend(&context.root, &context.backend, context.port) {
                            Ok(process) => {
                                child = Some(process);
                                generation = Some(requested);
                                let _ = context.event_tx.send(Event::BackendStarted {
                                    generation: requested,
                                }).await;
                                spawn_backend_health_check(&context, requested);
                            }
                            Err(error) => {
                                let _ = context.event_tx.send(Event::BackendStartFailed {
                                    generation: requested,
                                    reason: error.to_string(),
                                }).await;
                            }
                        }
                    }
                    Some(BackendCommand::Stop) => {
                        if let (Some(process), Some(active)) = (&mut child, generation) {
                            stop_child(process).await;
                            let status = process.try_wait()?.map_or_else(
                                || "stopped".to_owned(),
                                |status| status.to_string(),
                            );
                            child = None;
                            generation = None;
                            let _ = context.event_tx.send(Event::BackendExited {
                                generation: active,
                                status,
                            }).await;
                        }
                    }
                    None => break,
                },
                _ = poll.tick() => {
                    let exited = child
                        .as_mut()
                        .map(|process| process.try_wait())
                        .transpose()?
                        .flatten();
                    if let Some(status) = exited {
                        child = None;
                        if let Some(active) = generation.take() {
                            let _ = context.event_tx.send(Event::BackendExited {
                                generation: active,
                                status: status.to_string(),
                            }).await;
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
            }
        }
        if let Some(process) = &mut child {
            stop_child(process).await;
        }
        Ok(())
    })
}

fn spawn_backend_health_check(context: &BackendManagerContext, generation: Timestamp) {
    let client = context.client.clone();
    let url = context.url.clone();
    let event_tx = context.event_tx.clone();
    tokio::spawn(async move {
        if wait_for_backend(&client, &url).await.is_ok() {
            let _ = event_tx.send(Event::BackendReady { generation }).await;
        } else {
            eprintln!("OpenCode backend did not become healthy");
        }
    });
}

fn start_backend(root: &Path, backend: &Backend, port: u16) -> Result<Child> {
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
        .arg(port.to_string())
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
                    let modified = artifact_timestamp(&name.path(&root)).ok().flatten();
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
    for built_in in [
        "system-active",
        "system-supervisor",
        "system-backend",
        "system-plan",
    ] {
        names.insert(ArtifactName::parse(built_in).expect("built-in name"));
    }
    for name in names {
        let modified = artifact_timestamp(&name.path(root))?;
        event_tx
            .send(Event::ArtifactObserved { name, modified })
            .await
            .map_err(|_| anyhow!("event loop stopped"))?;
    }
    Ok(())
}

pub(crate) fn artifact_timestamp(path: &Path) -> Result<Option<Timestamp>> {
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

    #[test]
    fn plan_controller_ignores_stale_completions() {
        let mut state = SupervisorControlState::default();
        let effects = SupervisorControlEvent::Started.reduce(&mut state);
        assert!(matches!(
            effects.as_slice(),
            [SupervisorControlEffect::LoadPlan { request_id: 1 }]
        ));
        assert!(
            SupervisorControlEvent::PlanLoadFailed {
                request_id: 99,
                generation: None,
                reason: "stale".into(),
            }
            .reduce(&mut state)
            .is_empty()
        );
        let effects = SupervisorControlEvent::PlanLoadFailed {
            request_id: 1,
            generation: Some(10),
            reason: "invalid".into(),
        }
        .reduce(&mut state);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            SupervisorControlEffect::WaitForPlanPublish {
                request_id: 1,
                generation: Some(10)
            }
        )));
        let effects =
            SupervisorControlEvent::PlanPublishObserved { request_id: 1 }.reduce(&mut state);
        assert!(matches!(
            effects.as_slice(),
            [SupervisorControlEffect::LoadPlan { request_id: 2 }]
        ));
        let plan = Arc::new(Plan::parse("version = 1").unwrap());
        SupervisorControlEvent::PlanLoaded {
            request_id: 2,
            plan,
        }
        .reduce(&mut state);
        let effects = SupervisorControlEvent::GenerationExited {
            request_id: 2,
            reload: true,
        }
        .reduce(&mut state);
        assert!(matches!(
            effects.as_slice(),
            [SupervisorControlEffect::LoadPlan { request_id: 3 }]
        ));
        let effects = SupervisorControlEvent::PlanLoaded {
            request_id: 3,
            plan: Arc::new(Plan::parse("version = 1").unwrap()),
        }
        .reduce(&mut state);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, SupervisorControlEffect::ClearTasks))
        );
    }
}
