use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::artifact::ArtifactName;
use crate::plan::Plan;

pub type Timestamp = i64;

#[derive(Clone, Debug)]
pub struct State {
    pub plan: Arc<Plan>,
    pub artifacts: BTreeMap<ArtifactName, Option<Timestamp>>,
    pub virtual_artifacts: BTreeMap<ArtifactName, Timestamp>,
    pub observer_session: Option<String>,
    pub sessions: BTreeMap<String, Session>,
    pub tasks: BTreeMap<ArtifactName, Task>,
    pub backend_generation: Option<Option<Timestamp>>,
    pub supervisor_generation: Option<Option<Timestamp>>,
    pub next_request_id: u64,
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
            sessions: BTreeMap::new(),
            tasks: BTreeMap::new(),
            backend_generation: None,
            supervisor_generation: None,
            next_request_id: 1,
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
    ArtifactObserved {
        name: ArtifactName,
        modified: Option<Timestamp>,
    },
    ObserverSessionCreated {
        session_id: String,
    },
    SessionCreated {
        role: String,
        session_id: String,
    },
    SessionCreateFailed {
        role: String,
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
    CreateObserverSession,
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
    RestartBackend,
    ExitSupervisor,
}

impl Event {
    pub fn reduce(self, state: &mut State) -> Vec<Effect> {
        let mut effects = match self {
            Event::ArtifactObserved { name, modified } => {
                reduce_artifact_observed(state, name, modified)
            }
            Event::ObserverSessionCreated { session_id } => {
                reduce_observer_created(state, session_id)
            }
            Event::SessionCreated { role, session_id } => {
                reduce_session_created(state, role, session_id)
            }
            Event::SessionCreateFailed { role, reason } => {
                let artifacts: Vec<_> = state
                    .tasks
                    .iter()
                    .filter(|(name, task)| {
                        name.role() == Some(role.as_str())
                            && task.status == TaskStatus::WaitingSession
                    })
                    .map(|(name, _)| name.clone())
                    .collect();
                artifacts
                    .into_iter()
                    .flat_map(|artifact| fail_task(state, artifact, reason.clone()))
                    .collect()
            }
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
        effects.extend(schedule(state));
        effects
    }
}

fn reduce_observer_created(state: &mut State, session_id: String) -> Vec<Effect> {
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
            if state.backend_generation.is_some()
                && modified.is_some()
                && state.backend_generation != Some(modified)
            {
                effects.push(Effect::RestartBackend);
            }
            state.backend_generation = Some(modified);
        }
        "system-supervisor" => {
            if state.supervisor_generation.is_some()
                && modified.is_some()
                && state.supervisor_generation != Some(modified)
            {
                effects.push(Effect::ExitSupervisor);
            }
            state.supervisor_generation = Some(modified);
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

fn reduce_session_created(state: &mut State, role: String, session_id: String) -> Vec<Effect> {
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
    if !state.is_active() {
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
    let mut observer_requested = false;
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
        } else if !observer_requested {
            observer_requested = true;
            effects.push(Effect::CreateObserverSession);
        }
    }
    effects
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
        State::new(Arc::new(Plan::parse(EXAMPLE_PLAN).unwrap()))
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
                .all(|effect| !matches!(effect, Effect::CreateObserverSession))
        );
        let effects = observed("system-active", 2).reduce(&mut state);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::CreateObserverSession))
        );
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
}
