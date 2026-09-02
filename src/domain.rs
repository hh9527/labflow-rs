use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::artifact::ArtifactName;
use crate::db::HostTasks;
use crate::plan::Plan;

pub type Timestamp = i64;

#[derive(Clone, Debug)]
pub struct State {
    pub plan: Arc<Plan>,
    pub artifacts: BTreeMap<ArtifactName, Option<Timestamp>>,
    pub virtual_artifacts: BTreeMap<ArtifactName, Timestamp>,
    pub observer_session: Option<String>,
    pub observer_request: Option<u64>,
    pub sessions: BTreeMap<String, Session>,
    pub tasks: BTreeMap<ArtifactName, Task>,
    pub backend_generation: Option<Option<Timestamp>>,
    pub backend_ready: bool,
    pub backend_process: ProcessStatus,
    pub backend_process_generation: Option<Timestamp>,
    pub supervisor_generation: Option<Option<Timestamp>>,
    pub plan_generation: Option<Option<Timestamp>>,
    pub next_request_id: u64,
    pub host_tasks: HostTasks,
}

impl State {
    pub fn new(plan: Arc<Plan>) -> Self {
        let artifacts = plan
            .artifacts
            .keys()
            .cloned()
            .map(|name| (name, None))
            .collect();
        Self {
            plan,
            artifacts,
            virtual_artifacts: BTreeMap::new(),
            observer_session: None,
            observer_request: None,
            sessions: BTreeMap::new(),
            tasks: BTreeMap::new(),
            backend_generation: None,
            backend_ready: false,
            backend_process: ProcessStatus::Stopped,
            backend_process_generation: None,
            supervisor_generation: None,
            plan_generation: None,
            next_request_id: 1,
            host_tasks: HostTasks::default(),
        }
    }

    pub fn timestamp(&self, name: &ArtifactName) -> Option<Timestamp> {
        self.virtual_artifacts
            .get(name)
            .copied()
            .or_else(|| self.artifacts.get(name).copied().flatten())
    }

    pub fn is_active(&self) -> bool {
        let active = ArtifactName::parse("system-active").expect("built-in name");
        let blocked = ArtifactName::parse("_blocked").expect("built-in name");
        self.timestamp(&active).is_some_and(|active_at| {
            self.timestamp(&blocked)
                .is_none_or(|blocked_at| active_at > blocked_at)
        })
    }

    fn allocate_request(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    fn next_timestamp(&self) -> Timestamp {
        self.artifacts
            .values()
            .filter_map(|value| *value)
            .chain(self.virtual_artifacts.values().copied())
            .max()
            .unwrap_or(0)
            .saturating_add(1)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Session {
    pub id: String,
    pub busy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStatus {
    Stopped,
    Starting,
    Running,
    Stopping,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Task {
    pub status: TaskStatus,
    pub retries: u8,
    pub failures: Vec<String>,
    pub request_id: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskStatus {
    WaitingSession,
    Preparing,
    Running,
    Checking,
    Publishing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskAnswer {
    Completed,
    Unable,
    Invalid,
}

impl TaskAnswer {
    pub fn parse(content: &str) -> Self {
        if content.starts_with("完成任务。") {
            Self::Completed
        } else if content.starts_with("无法完成任务。") {
            Self::Unable
        } else {
            Self::Invalid
        }
    }
}

#[derive(Clone, Debug)]
pub enum Event {
    SupervisorStarted,
    ArtifactObserved {
        name: ArtifactName,
        modified: Option<Timestamp>,
    },
    BackendReady {
        generation: Timestamp,
    },
    BackendStarted {
        generation: Timestamp,
    },
    BackendStartFailed {
        generation: Timestamp,
        reason: String,
    },
    BackendExited {
        generation: Timestamp,
        status: String,
    },
    BackendRetry {
        generation: Timestamp,
    },
    ObserverSessionCreated {
        request_id: u64,
        session_id: String,
    },
    ObserverSessionCreateFailed {
        request_id: u64,
        reason: String,
    },
    SessionCreated {
        role: String,
        request_id: u64,
        session_id: String,
    },
    SessionCreateFailed {
        role: String,
        request_id: u64,
        reason: String,
    },
    SessionStatusChanged {
        session_id: String,
        busy: bool,
    },
    TaskPrepared {
        artifact: ArtifactName,
        request_id: u64,
        prompt: String,
    },
    TaskPrepareFailed {
        artifact: ArtifactName,
        request_id: u64,
        reason: String,
    },
    TaskAnswered {
        artifact: ArtifactName,
        request_id: u64,
        content: String,
    },
    TaskChecked {
        artifact: ArtifactName,
        request_id: u64,
        missing: Vec<String>,
    },
    EffectFailed {
        artifact: Option<ArtifactName>,
        request_id: u64,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    PersistArtifact {
        name: ArtifactName,
        modified: Option<Timestamp>,
    },
    PersistVirtualArtifact {
        name: ArtifactName,
        modified: Timestamp,
    },
    PersistObserverSession {
        session_id: String,
    },
    PersistSession {
        role: String,
        session: Session,
    },
    PersistTask {
        artifact: ArtifactName,
        task: Option<Task>,
    },
    PersistHostTasks {
        tasks: HostTasks,
    },
    PersistNextRequest {
        next: u64,
    },
    CreateObserverSession {
        request_id: u64,
    },
    CreateSession {
        role: String,
        parent_session_id: String,
        request_id: u64,
    },
    PrepareTask {
        artifact: ArtifactName,
        request_id: u64,
        failures: Vec<String>,
    },
    PromptSession {
        artifact: ArtifactName,
        role: String,
        session_id: String,
        request_id: u64,
        prompt: String,
    },
    CheckTask {
        artifact: ArtifactName,
        request_id: u64,
    },
    PublishArtifact {
        artifact: ArtifactName,
        request_id: u64,
    },
    StartBackend {
        generation: Timestamp,
    },
    StopBackend,
    DelayBackendRetry {
        generation: Timestamp,
    },
    Log {
        message: String,
    },
    ReloadPlan,
    ExitSupervisor,
}

impl Event {
    pub fn reduce(self, state: &mut State) -> Vec<Effect> {
        let previous_next_request = state.next_request_id;
        let mut effects = match self {
            Event::SupervisorStarted => reduce_supervisor_started(state),
            Event::ArtifactObserved { name, modified } => {
                reduce_artifact_observed(state, name, modified)
            }
            Event::BackendReady { generation } => {
                if state.backend_generation == Some(Some(generation))
                    && state.backend_process == ProcessStatus::Running
                {
                    state.backend_ready = true;
                }
                Vec::new()
            }
            Event::BackendStarted { generation } => {
                if state.backend_generation == Some(Some(generation))
                    && state.backend_process == ProcessStatus::Starting
                    && state.backend_process_generation == Some(generation)
                {
                    state.backend_process = ProcessStatus::Running;
                }
                Vec::new()
            }
            Event::BackendStartFailed { generation, reason } => {
                if state.backend_process_generation != Some(generation) {
                    Vec::new()
                } else {
                    state.backend_process = ProcessStatus::Stopped;
                    state.backend_process_generation = None;
                    let desired = state.backend_generation.flatten();
                    if desired == Some(generation) {
                        vec![
                            Effect::Log {
                                message: format!("backend start failed: {reason}"),
                            },
                            Effect::DelayBackendRetry { generation },
                        ]
                    } else if let Some(desired) = desired {
                        state.backend_process = ProcessStatus::Starting;
                        state.backend_process_generation = Some(desired);
                        vec![Effect::StartBackend {
                            generation: desired,
                        }]
                    } else {
                        Vec::new()
                    }
                }
            }
            Event::BackendExited { generation, status } => {
                reduce_backend_exited(state, generation, status)
            }
            Event::BackendRetry { generation } => {
                if state.backend_generation == Some(Some(generation))
                    && state.backend_process == ProcessStatus::Stopped
                {
                    state.backend_process = ProcessStatus::Starting;
                    state.backend_process_generation = Some(generation);
                    vec![Effect::StartBackend { generation }]
                } else {
                    Vec::new()
                }
            }
            Event::ObserverSessionCreated {
                request_id,
                session_id,
            } => reduce_observer_created(state, request_id, session_id),
            Event::ObserverSessionCreateFailed { request_id, reason } => {
                reduce_observer_create_failed(state, request_id, reason)
            }
            Event::SessionCreated {
                role,
                request_id,
                session_id,
            } => reduce_session_created(state, role, request_id, session_id),
            Event::SessionCreateFailed {
                role,
                request_id,
                reason,
            } => reduce_session_create_failed(state, role, request_id, reason),
            Event::SessionStatusChanged { session_id, busy } => {
                let Some((role, session)) = state
                    .sessions
                    .iter_mut()
                    .find(|(_, session)| session.id == session_id)
                else {
                    return Vec::new();
                };
                if session.busy == busy {
                    Vec::new()
                } else {
                    session.busy = busy;
                    vec![Effect::PersistSession {
                        role: role.clone(),
                        session: session.clone(),
                    }]
                }
            }
            Event::TaskPrepared {
                artifact,
                request_id,
                prompt,
            } => reduce_task_prepared(state, artifact, request_id, prompt),
            Event::TaskPrepareFailed {
                artifact,
                request_id,
                reason,
            } => match current_task(state, &artifact, request_id) {
                Some(_) => fail_task(state, artifact, reason),
                None => Vec::new(),
            },
            Event::TaskAnswered {
                artifact,
                request_id,
                content,
            } => reduce_task_answered(state, artifact, request_id, &content),
            Event::TaskChecked {
                artifact,
                request_id,
                missing,
            } => reduce_task_checked(state, artifact, request_id, missing),
            Event::EffectFailed {
                artifact,
                request_id,
                reason,
            } => artifact
                .filter(|artifact| current_task(state, artifact, request_id).is_some())
                .map_or_else(Vec::new, |artifact| fail_task(state, artifact, reason)),
        };
        if !effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReloadPlan | Effect::ExitSupervisor))
        {
            effects.extend(schedule(state));
        }
        let host_tasks = compute_host_tasks(state);
        if host_tasks != state.host_tasks {
            state.host_tasks = host_tasks.clone();
            effects.push(Effect::PersistHostTasks { tasks: host_tasks });
        }
        if state.next_request_id != previous_next_request {
            effects.push(Effect::PersistNextRequest {
                next: state.next_request_id,
            });
        }
        effects
    }
}

fn reduce_supervisor_started(state: &mut State) -> Vec<Effect> {
    let supervisor = ArtifactName::parse("system-supervisor").expect("built-in name");
    if state.timestamp(&supervisor).is_none() {
        return vec![Effect::ExitSupervisor];
    }
    let mut effects = Vec::new();
    for (role, session) in &mut state.sessions {
        if session.busy {
            session.busy = false;
            effects.push(Effect::PersistSession {
                role: role.clone(),
                session: session.clone(),
            });
        }
    }

    let artifacts: Vec<_> = state.tasks.keys().cloned().collect();
    let mut roles_creating = BTreeSet::new();
    let mut observer_needed = false;
    for artifact in artifacts {
        let role = artifact.role().expect("tasks belong to roles").to_owned();
        let task = state.tasks.get_mut(&artifact).expect("task exists");
        if state.sessions.contains_key(&role) {
            task.status = TaskStatus::Preparing;
            effects.push(Effect::PersistTask {
                artifact: artifact.clone(),
                task: Some(task.clone()),
            });
            effects.push(Effect::PrepareTask {
                artifact,
                request_id: task.request_id,
                failures: task.failures.clone(),
            });
        } else {
            task.status = TaskStatus::WaitingSession;
            effects.push(Effect::PersistTask {
                artifact: artifact.clone(),
                task: Some(task.clone()),
            });
            if let Some(parent_session_id) = &state.observer_session {
                if roles_creating.insert(role.clone()) {
                    effects.push(Effect::CreateSession {
                        role,
                        parent_session_id: parent_session_id.clone(),
                        request_id: task.request_id,
                    });
                }
            } else {
                observer_needed = true;
            }
        }
    }
    if observer_needed {
        request_observer(state, &mut effects);
    }
    effects
}

fn reduce_observer_create_failed(
    state: &mut State,
    request_id: u64,
    reason: String,
) -> Vec<Effect> {
    if state.observer_request != Some(request_id) {
        return Vec::new();
    }
    state.observer_request = None;
    let waiting: Vec<_> = state
        .tasks
        .iter()
        .filter(|(_, task)| task.status == TaskStatus::WaitingSession)
        .map(|(artifact, _)| artifact.clone())
        .collect();
    if waiting.is_empty() {
        return Vec::new();
    }
    if waiting
        .iter()
        .any(|artifact| state.tasks[artifact].retries >= 3)
    {
        return block_task(state, waiting[0].clone(), None);
    }
    let mut effects = Vec::new();
    for artifact in waiting {
        let task = state.tasks.get_mut(&artifact).expect("task exists");
        task.retries += 1;
        task.failures = vec![reason.clone()];
        effects.push(Effect::PersistTask {
            artifact,
            task: Some(task.clone()),
        });
    }
    request_observer(state, &mut effects);
    effects
}

fn reduce_session_create_failed(
    state: &mut State,
    role: String,
    request_id: u64,
    reason: String,
) -> Vec<Effect> {
    let Some(artifact) = state
        .tasks
        .iter()
        .find(|(name, task)| {
            name.role() == Some(role.as_str())
                && task.status == TaskStatus::WaitingSession
                && task.request_id == request_id
        })
        .map(|(name, _)| name.clone())
    else {
        return Vec::new();
    };
    if state.tasks[&artifact].retries >= 3 {
        return block_task(state, artifact, None);
    }
    let request_id = state.allocate_request();
    let task = state.tasks.get_mut(&artifact).expect("task exists");
    task.retries += 1;
    task.failures = vec![reason];
    task.request_id = request_id;
    let mut effects = vec![Effect::PersistTask {
        artifact,
        task: Some(task.clone()),
    }];
    if let Some(parent_session_id) = &state.observer_session {
        effects.push(Effect::CreateSession {
            role,
            parent_session_id: parent_session_id.clone(),
            request_id,
        });
    } else {
        request_observer(state, &mut effects);
    }
    effects
}

fn reduce_observer_created(state: &mut State, request_id: u64, session_id: String) -> Vec<Effect> {
    if state.observer_request != Some(request_id) {
        return Vec::new();
    }
    state.observer_request = None;
    state.observer_session = Some(session_id.clone());
    let mut effects = vec![Effect::PersistObserverSession {
        session_id: session_id.clone(),
    }];
    let roles: BTreeSet<_> = state
        .tasks
        .iter()
        .filter(|(_, task)| task.status == TaskStatus::WaitingSession)
        .filter_map(|(artifact, _)| artifact.role().map(str::to_owned))
        .collect();
    for role in roles {
        let request_id = state
            .tasks
            .iter()
            .find(|(artifact, task)| {
                artifact.role() == Some(role.as_str()) && task.status == TaskStatus::WaitingSession
            })
            .map(|(_, task)| task.request_id)
            .expect("waiting role has a task");
        effects.push(Effect::CreateSession {
            role,
            parent_session_id: session_id.clone(),
            request_id,
        });
    }
    effects
}

fn request_observer(state: &mut State, effects: &mut Vec<Effect>) {
    if state.observer_session.is_some() || state.observer_request.is_some() {
        return;
    }
    let request_id = state.allocate_request();
    state.observer_request = Some(request_id);
    effects.push(Effect::CreateObserverSession { request_id });
}

fn reduce_artifact_observed(
    state: &mut State,
    name: ArtifactName,
    modified: Option<Timestamp>,
) -> Vec<Effect> {
    state.artifacts.insert(name.clone(), modified);
    let mut effects = vec![Effect::PersistArtifact {
        name: name.clone(),
        modified,
    }];

    match name.as_str() {
        "system-backend" => {
            if state.backend_generation != Some(modified) {
                state.backend_ready = false;
                match state.backend_process {
                    ProcessStatus::Stopped => {
                        if let Some(generation) = modified {
                            state.backend_process = ProcessStatus::Starting;
                            state.backend_process_generation = Some(generation);
                            effects.push(Effect::StartBackend { generation });
                        }
                    }
                    ProcessStatus::Starting | ProcessStatus::Running => {
                        state.backend_process = ProcessStatus::Stopping;
                        effects.push(Effect::StopBackend);
                    }
                    ProcessStatus::Stopping => {}
                }
            }
            state.backend_generation = Some(modified);
        }
        "system-supervisor" => {
            if state.supervisor_generation.is_some()
                && state.supervisor_generation != Some(modified)
            {
                effects.push(Effect::ExitSupervisor);
            }
            state.supervisor_generation = Some(modified);
        }
        "system-plan" => {
            if modified.is_some()
                && state.plan_generation.is_some()
                && state.plan_generation != Some(modified)
            {
                effects.push(Effect::ReloadPlan);
            }
            state.plan_generation = Some(modified);
        }
        _ => {}
    }

    if modified.is_some()
        && let Some(task) = state.tasks.get(&name)
        && task.status == TaskStatus::Publishing
    {
        state.tasks.remove(&name);
        effects.push(Effect::PersistTask {
            artifact: name,
            task: None,
        });
    }
    effects
}

fn reduce_backend_exited(state: &mut State, generation: Timestamp, status: String) -> Vec<Effect> {
    if state.backend_process_generation != Some(generation) {
        return Vec::new();
    }
    if !matches!(
        state.backend_process,
        ProcessStatus::Running | ProcessStatus::Stopping
    ) {
        return Vec::new();
    }
    state.backend_ready = false;
    let was_stopping = state.backend_process == ProcessStatus::Stopping;
    state.backend_process = ProcessStatus::Stopped;
    state.backend_process_generation = None;
    let Some(desired) = state.backend_generation.flatten() else {
        return Vec::new();
    };
    if was_stopping || desired != generation {
        state.backend_process = ProcessStatus::Starting;
        state.backend_process_generation = Some(desired);
        vec![Effect::StartBackend {
            generation: desired,
        }]
    } else {
        vec![
            Effect::Log {
                message: format!("backend exited with {status}"),
            },
            Effect::DelayBackendRetry {
                generation: desired,
            },
        ]
    }
}

fn reduce_session_created(
    state: &mut State,
    role: String,
    request_id: u64,
    session_id: String,
) -> Vec<Effect> {
    if !state.tasks.iter().any(|(artifact, task)| {
        artifact.role() == Some(role.as_str())
            && task.status == TaskStatus::WaitingSession
            && task.request_id == request_id
    }) {
        return Vec::new();
    }
    let session = Session {
        id: session_id,
        busy: false,
    };
    state.sessions.insert(role.clone(), session.clone());
    let ready = ArtifactName::parse(&format!("_ready.{role}")).expect("validated role");
    let ready_at = state.next_timestamp();
    state.virtual_artifacts.insert(ready.clone(), ready_at);
    let mut effects = vec![
        Effect::PersistSession {
            role: role.clone(),
            session,
        },
        Effect::PersistVirtualArtifact {
            name: ready,
            modified: ready_at,
        },
    ];
    for (artifact, task) in &mut state.tasks {
        if artifact.role() == Some(role.as_str()) && task.status == TaskStatus::WaitingSession {
            task.status = TaskStatus::Preparing;
            effects.push(Effect::PersistTask {
                artifact: artifact.clone(),
                task: Some(task.clone()),
            });
            effects.push(Effect::PrepareTask {
                artifact: artifact.clone(),
                request_id: task.request_id,
                failures: task.failures.clone(),
            });
        }
    }
    effects
}

fn reduce_task_prepared(
    state: &mut State,
    artifact: ArtifactName,
    request_id: u64,
    prompt: String,
) -> Vec<Effect> {
    let Some(task) = current_task(state, &artifact, request_id) else {
        return Vec::new();
    };
    if task.status != TaskStatus::Preparing {
        return Vec::new();
    }
    let role = artifact
        .role()
        .expect("only worker artifacts are scheduled")
        .to_owned();
    let Some(session) = state.sessions.get(&role) else {
        return Vec::new();
    };
    if session.busy {
        return Vec::new();
    }
    state.tasks.get_mut(&artifact).expect("task exists").status = TaskStatus::Running;
    state.sessions.get_mut(&role).expect("session exists").busy = true;
    let task = state.tasks[&artifact].clone();
    let session = state.sessions[&role].clone();
    vec![
        Effect::PersistTask {
            artifact: artifact.clone(),
            task: Some(task),
        },
        Effect::PersistSession {
            role: role.clone(),
            session: session.clone(),
        },
        Effect::PromptSession {
            artifact,
            role,
            session_id: session.id.clone(),
            request_id,
            prompt,
        },
    ]
}

fn reduce_task_answered(
    state: &mut State,
    artifact: ArtifactName,
    request_id: u64,
    content: &str,
) -> Vec<Effect> {
    let Some(task) = current_task(state, &artifact, request_id) else {
        return Vec::new();
    };
    if task.status != TaskStatus::Running {
        return Vec::new();
    }
    let session_effect = if let Some(role) = artifact.role()
        && let Some(session) = state.sessions.get_mut(role)
    {
        session.busy = false;
        Some(Effect::PersistSession {
            role: role.to_owned(),
            session: session.clone(),
        })
    } else {
        None
    };
    match TaskAnswer::parse(content) {
        TaskAnswer::Completed => {
            state.tasks.get_mut(&artifact).expect("task exists").status = TaskStatus::Checking;
            let task = state.tasks[&artifact].clone();
            let mut effects = vec![
                Effect::PersistTask {
                    artifact: artifact.clone(),
                    task: Some(task),
                },
                Effect::CheckTask {
                    artifact,
                    request_id,
                },
            ];
            effects.extend(session_effect);
            effects
        }
        TaskAnswer::Unable => block_task(state, artifact, session_effect),
        TaskAnswer::Invalid => fail_task(state, artifact, "回答内容不符合要求".into()),
    }
}

fn reduce_task_checked(
    state: &mut State,
    artifact: ArtifactName,
    request_id: u64,
    missing: Vec<String>,
) -> Vec<Effect> {
    let Some(task) = current_task_mut(state, &artifact, request_id) else {
        return Vec::new();
    };
    if task.status != TaskStatus::Checking {
        return Vec::new();
    }
    if missing.is_empty() {
        task.status = TaskStatus::Publishing;
        vec![
            Effect::PersistTask {
                artifact: artifact.clone(),
                task: Some(task.clone()),
            },
            Effect::PublishArtifact {
                artifact,
                request_id,
            },
        ]
    } else {
        let reason = missing
            .into_iter()
            .map(|path| format!("{path} 文件不存在"))
            .collect::<Vec<_>>()
            .join("\n");
        fail_task(state, artifact, reason)
    }
}

fn fail_task(state: &mut State, artifact: ArtifactName, reason: String) -> Vec<Effect> {
    let Some(task) = state.tasks.get(&artifact) else {
        return Vec::new();
    };
    let session_effect = if let Some(role) = artifact.role()
        && let Some(session) = state.sessions.get_mut(role)
    {
        session.busy = false;
        Some(Effect::PersistSession {
            role: role.to_owned(),
            session: session.clone(),
        })
    } else {
        None
    };
    if task.retries >= 3 {
        return block_task(state, artifact, session_effect);
    }
    let request_id = state.allocate_request();
    let task = state.tasks.get_mut(&artifact).expect("task exists");
    task.retries += 1;
    task.failures = reason.lines().map(str::to_owned).collect();
    task.status = TaskStatus::Preparing;
    task.request_id = request_id;
    let mut effects = vec![
        Effect::PersistTask {
            artifact: artifact.clone(),
            task: Some(task.clone()),
        },
        Effect::PrepareTask {
            artifact,
            request_id: task.request_id,
            failures: task.failures.clone(),
        },
    ];
    effects.extend(session_effect);
    effects
}

fn block(state: &mut State) -> Vec<Effect> {
    let name = ArtifactName::parse("_blocked").expect("built-in name");
    let modified = state.next_timestamp();
    state.virtual_artifacts.insert(name.clone(), modified);
    vec![Effect::PersistVirtualArtifact { name, modified }]
}

fn block_task(
    state: &mut State,
    artifact: ArtifactName,
    session_effect: Option<Effect>,
) -> Vec<Effect> {
    state.tasks.remove(&artifact);
    let mut effects = vec![Effect::PersistTask {
        artifact,
        task: None,
    }];
    effects.extend(session_effect);
    effects.extend(block(state));
    effects
}

fn schedule(state: &mut State) -> Vec<Effect> {
    if !state.is_active() || !state.backend_ready {
        return Vec::new();
    }
    let candidates: Vec<_> = state
        .plan
        .artifacts
        .iter()
        .filter(|(name, _)| name.role().is_some())
        .filter(|(name, _)| !state.tasks.contains_key(*name))
        .filter(|(name, artifact)| {
            let output = state.timestamp(name);
            let required_exist = artifact.dependencies.iter().all(|dependency| {
                dependency.optional
                    || dependency.name.as_str().starts_with("_ready.")
                    || state.timestamp(&dependency.name).is_some()
            });
            let stale = output.is_none()
                || artifact.dependencies.iter().any(|dependency| {
                    state
                        .timestamp(&dependency.name)
                        .is_some_and(|input| output.is_none_or(|output| input > output))
                });
            required_exist && stale
        })
        .map(|(name, _)| name.clone())
        .collect();

    let mut effects = Vec::new();
    let mut roles_creating = BTreeSet::new();
    let mut observer_needed = false;
    for artifact in candidates {
        let role = artifact
            .role()
            .expect("filtered worker artifact")
            .to_owned();
        if state
            .sessions
            .get(&role)
            .is_some_and(|session| session.busy)
            || state
                .tasks
                .keys()
                .any(|active| active.role() == Some(role.as_str()))
        {
            continue;
        }
        let request_id = state.allocate_request();
        let status = if state.sessions.contains_key(&role) {
            TaskStatus::Preparing
        } else {
            TaskStatus::WaitingSession
        };
        let task = Task {
            status,
            retries: 0,
            failures: Vec::new(),
            request_id,
        };
        state.tasks.insert(artifact.clone(), task.clone());
        effects.push(Effect::PersistTask {
            artifact: artifact.clone(),
            task: Some(task),
        });
        if status == TaskStatus::Preparing {
            effects.push(Effect::PrepareTask {
                artifact,
                request_id,
                failures: Vec::new(),
            });
        } else if let Some(parent_session_id) = &state.observer_session {
            if roles_creating.insert(role.clone()) {
                effects.push(Effect::CreateSession {
                    role,
                    parent_session_id: parent_session_id.clone(),
                    request_id,
                });
            }
        } else {
            observer_needed = true;
        }
    }
    if observer_needed {
        request_observer(state, &mut effects);
    }
    effects
}

fn compute_host_tasks(state: &State) -> HostTasks {
    let mut required = BTreeSet::new();
    let mut optional = BTreeSet::new();
    for (name, artifact) in &state.plan.artifacts {
        if name.role().is_none() {
            continue;
        }
        let output = state.timestamp(name);
        let stale = output.is_none()
            || artifact.dependencies.iter().any(|dependency| {
                state
                    .timestamp(&dependency.name)
                    .is_some_and(|input| output.is_none_or(|output| input > output))
            });
        if !stale {
            continue;
        }
        for dependency in &artifact.dependencies {
            let active_decision = dependency.name.as_str() == "system-active" && !state.is_active();
            if dependency.name.role().is_some()
                || dependency.name.is_supervisor()
                || (state.timestamp(&dependency.name).is_some() && !active_decision)
            {
                continue;
            }
            if dependency.optional {
                optional.insert(dependency.name.clone());
            } else {
                required.insert(dependency.name.clone());
            }
        }
    }
    optional.retain(|name| !required.contains(name));
    HostTasks {
        tasks: required.into_iter().collect(),
        opt: optional.into_iter().collect(),
    }
}

fn current_task<'a>(
    state: &'a State,
    artifact: &ArtifactName,
    request_id: u64,
) -> Option<&'a Task> {
    state
        .tasks
        .get(artifact)
        .filter(|task| task.request_id == request_id)
}

fn current_task_mut<'a>(
    state: &'a mut State,
    artifact: &ArtifactName,
    request_id: u64,
) -> Option<&'a mut Task> {
    state
        .tasks
        .get_mut(artifact)
        .filter(|task| task.request_id == request_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::EXAMPLE_PLAN;

    fn state() -> State {
        let mut state = State::new(Arc::new(Plan::parse(EXAMPLE_PLAN).unwrap()));
        state.backend_generation = Some(Some(1));
        state.backend_ready = true;
        state
            .artifacts
            .insert(ArtifactName::parse("system-supervisor").unwrap(), Some(1));
        state
    }

    fn observed(name: &str, modified: Timestamp) -> Event {
        Event::ArtifactObserved {
            name: ArtifactName::parse(name).unwrap(),
            modified: Some(modified),
        }
    }

    #[test]
    fn schedules_only_when_active_and_required_dependencies_exist() {
        let mut state = state();
        assert!(
            observed("query-request", 1)
                .reduce(&mut state)
                .iter()
                .all(|effect| !matches!(effect, Effect::CreateObserverSession { .. }))
        );
        let effects = observed("system-active", 2).reduce(&mut state);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::CreateObserverSession { .. }))
        );
        let observer_request = state.observer_request.unwrap();
        let duplicate = observed("query-request", 3).reduce(&mut state);
        assert!(
            duplicate
                .iter()
                .all(|effect| !matches!(effect, Effect::CreateObserverSession { .. }))
        );
        Event::ObserverSessionCreated {
            request_id: observer_request + 1,
            session_id: "stale".into(),
        }
        .reduce(&mut state);
        assert!(state.observer_session.is_none());
        Event::ObserverSessionCreated {
            request_id: observer_request,
            session_id: "current".into(),
        }
        .reduce(&mut state);
        assert_eq!(state.observer_session.as_deref(), Some("current"));
    }

    #[test]
    fn unavailable_blocks_immediately() {
        let mut state = state();
        observed("query-request", 1).reduce(&mut state);
        observed("system-active", 2).reduce(&mut state);
        let artifact = ArtifactName::parse("answer.researcher").unwrap();
        let request = state.tasks[&artifact].request_id;
        Event::SessionCreated {
            role: "researcher".into(),
            request_id: request,
            session_id: "ses_1".into(),
        }
        .reduce(&mut state);
        Event::TaskPrepared {
            artifact: artifact.clone(),
            request_id: request,
            prompt: "p".into(),
        }
        .reduce(&mut state);
        Event::TaskAnswered {
            artifact,
            request_id: request,
            content: "无法完成任务。".into(),
        }
        .reduce(&mut state);
        assert!(
            state
                .virtual_artifacts
                .contains_key(&ArtifactName::parse("_blocked").unwrap())
        );
        assert!(!state.is_active());
        assert!(
            state
                .host_tasks
                .tasks
                .contains(&ArtifactName::parse("system-active").unwrap())
        );

        let effects = observed("system-active", 10).reduce(&mut state);
        assert!(state.is_active());
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::PrepareTask { .. }))
        );
    }

    #[test]
    fn exposes_required_and_optional_host_decisions() {
        let plan = Plan::parse(
            r#"
version = 1
[roles.r]
kind = "lab-worker"
[artifacts.request]
[artifacts.feedback]
[artifacts."answer.r"]
goal = "goal.md"
depends-on = ["request", "feedback?"]
"#,
        )
        .unwrap();
        let mut state = State::new(Arc::new(plan));
        state.backend_ready = true;
        Event::SupervisorStarted.reduce(&mut state);
        assert_eq!(
            state.host_tasks.tasks,
            vec![ArtifactName::parse("request").unwrap()]
        );
        assert_eq!(
            state.host_tasks.opt,
            vec![ArtifactName::parse("feedback").unwrap()]
        );

        observed("request", 2).reduce(&mut state);
        assert!(state.host_tasks.tasks.is_empty());
        assert_eq!(
            state.host_tasks.opt,
            vec![ArtifactName::parse("feedback").unwrap()]
        );
    }

    #[test]
    fn system_plan_only_reloads_on_publish() {
        let mut state = state();
        state.plan_generation = Some(Some(10));
        let removed = Event::ArtifactObserved {
            name: ArtifactName::parse("system-plan").unwrap(),
            modified: None,
        }
        .reduce(&mut state);
        assert!(
            removed
                .iter()
                .all(|effect| !matches!(effect, Effect::ReloadPlan))
        );
        let published = observed("system-plan", 11).reduce(&mut state);
        assert!(
            published
                .iter()
                .any(|effect| matches!(effect, Effect::ReloadPlan))
        );
    }

    #[test]
    fn backend_process_transitions_are_reducer_owned() {
        let mut state = State::new(Arc::new(Plan::parse(EXAMPLE_PLAN).unwrap()));
        let effects = observed("system-backend", 10).reduce(&mut state);
        assert_eq!(state.backend_process, ProcessStatus::Starting);
        assert!(effects.contains(&Effect::StartBackend { generation: 10 }));

        Event::BackendStarted { generation: 10 }.reduce(&mut state);
        Event::BackendReady { generation: 10 }.reduce(&mut state);
        assert_eq!(state.backend_process, ProcessStatus::Running);
        assert!(state.backend_ready);

        let effects = observed("system-backend", 11).reduce(&mut state);
        assert_eq!(state.backend_process, ProcessStatus::Stopping);
        assert!(effects.contains(&Effect::StopBackend));
        let stale = Event::BackendStartFailed {
            generation: 99,
            reason: "stale".into(),
        }
        .reduce(&mut state);
        assert!(
            stale
                .iter()
                .all(|effect| !matches!(effect, Effect::StartBackend { .. }))
        );
        assert_eq!(state.backend_process, ProcessStatus::Stopping);
        let effects = Event::BackendExited {
            generation: 10,
            status: "stopped".into(),
        }
        .reduce(&mut state);
        assert_eq!(state.backend_process, ProcessStatus::Starting);
        assert!(effects.contains(&Effect::StartBackend { generation: 11 }));

        Event::BackendStarted { generation: 11 }.reduce(&mut state);
        let effects = Event::ArtifactObserved {
            name: ArtifactName::parse("system-backend").unwrap(),
            modified: None,
        }
        .reduce(&mut state);
        assert!(effects.contains(&Effect::StopBackend));
        Event::BackendExited {
            generation: 11,
            status: "stopped".into(),
        }
        .reduce(&mut state);
        assert_eq!(state.backend_process, ProcessStatus::Stopped);
    }

    #[test]
    fn retries_three_times_then_blocks() {
        let mut state = state();
        let artifact = ArtifactName::parse("answer.researcher").unwrap();
        state.tasks.insert(
            artifact.clone(),
            Task {
                status: TaskStatus::Running,
                retries: 0,
                failures: vec![],
                request_id: 1,
            },
        );
        for expected_retry in 1..=3 {
            let request = state.tasks[&artifact].request_id;
            Event::TaskAnswered {
                artifact: artifact.clone(),
                request_id: request,
                content: "bad".into(),
            }
            .reduce(&mut state);
            assert_eq!(state.tasks[&artifact].retries, expected_retry);
            state.tasks.get_mut(&artifact).unwrap().status = TaskStatus::Running;
        }
        let request = state.tasks[&artifact].request_id;
        Event::TaskAnswered {
            artifact,
            request_id: request,
            content: "bad".into(),
        }
        .reduce(&mut state);
        assert!(
            state
                .virtual_artifacts
                .contains_key(&ArtifactName::parse("_blocked").unwrap())
        );
    }

    #[test]
    fn startup_resumes_interrupted_tasks() {
        let mut state = state();
        state.observer_session = Some("ses_observer".into());
        state.sessions.insert(
            "researcher".into(),
            Session {
                id: "ses_worker".into(),
                busy: true,
            },
        );
        let artifact = ArtifactName::parse("answer.researcher").unwrap();
        state.tasks.insert(
            artifact.clone(),
            Task {
                status: TaskStatus::Running,
                retries: 1,
                failures: vec!["previous".into()],
                request_id: 7,
            },
        );
        let effects = Event::SupervisorStarted.reduce(&mut state);
        assert!(!state.sessions["researcher"].busy);
        assert_eq!(state.tasks[&artifact].status, TaskStatus::Preparing);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::PrepareTask { request_id: 7, failures, .. } if failures == &["previous"]
        )));
    }

    #[test]
    fn session_creation_failure_retries_session_creation() {
        let mut state = state();
        state.observer_session = Some("ses_observer".into());
        let artifact = ArtifactName::parse("answer.researcher").unwrap();
        state.tasks.insert(
            artifact.clone(),
            Task {
                status: TaskStatus::WaitingSession,
                retries: 0,
                failures: vec![],
                request_id: 1,
            },
        );
        let stale = Event::SessionCreateFailed {
            role: "researcher".into(),
            request_id: 2,
            reason: "old backend error".into(),
        }
        .reduce(&mut state);
        assert!(
            stale
                .iter()
                .all(|effect| !matches!(effect, Effect::CreateSession { .. }))
        );
        assert_eq!(state.tasks[&artifact].retries, 0);
        let effects = Event::SessionCreateFailed {
            role: "researcher".into(),
            request_id: 1,
            reason: "backend error".into(),
        }
        .reduce(&mut state);
        assert_eq!(state.tasks[&artifact].retries, 1);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::CreateSession { parent_session_id, .. } if parent_session_id == "ses_observer"
        )));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::PrepareTask { .. }))
        );
    }
}
