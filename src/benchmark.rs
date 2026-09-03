use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use reqwest::Client;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::artifact::ArtifactName;
use crate::config::Config;
use crate::plan::{ArtifactKind, AssetPath, Bench, BenchName, PLAN_FILE, Plan};
use crate::query::{QueryOutput, query_database};

pub fn query(root: &Path, name: &BenchName, sql: &str) -> Result<QueryOutput> {
    let path = root
        .join(".labflow/benchmarks")
        .join(format!("{}.sqlite", name.as_str()));
    query_database(&path, "benchmark", sql)
}

#[derive(Clone, Debug)]
pub enum Command {
    Start,
    Next,
    Clarify(String),
    Archive,
    Finish,
}

#[derive(Clone, Debug)]
struct Question {
    ordinal: i64,
    id: String,
    q: String,
    k: String,
    reference_answer: Option<String>,
    tags: Vec<String>,
    status: String,
    clarifications: u8,
}

#[derive(Clone, Debug)]
struct Round {
    id: i64,
    session_id: Option<String>,
    questions: Vec<Question>,
}

#[derive(Clone, Debug)]
struct BenchAction {
    kind: String,
    subject: Option<String>,
    started_at: i64,
    finished_at: Option<i64>,
    result: Option<String>,
}

#[derive(Default)]
struct State {
    round: Option<Round>,
}

enum Event {
    Requested(Command),
    InputsLoaded(Vec<Question>),
    RoundCreated {
        round_id: i64,
        questions: Vec<Question>,
    },
    SessionCreateFailed(String),
    SessionCreated(String),
    SessionAttachFailed {
        session_id: String,
        error: String,
    },
    SessionAttached,
    Answered {
        round_id: i64,
        question: Question,
        reply: String,
        clarification: bool,
        started_at: i64,
        finished_at: i64,
        actions: Vec<BenchAction>,
    },
    AnswerSaved {
        question: Question,
        reply: String,
        clarification: bool,
    },
    Archived,
    SessionDeleted(DeleteAfter),
    RoundCommitted,
}

enum Effect {
    LoadInputs,
    CreateRound(Vec<Question>),
    CreateSession {
        round_id: i64,
    },
    AttachSession {
        round_id: i64,
        session_id: String,
    },
    Ask {
        round_id: i64,
        session_id: String,
        question: Question,
        prompt: String,
        clarification: bool,
    },
    SaveAnswer {
        round_id: i64,
        question: Question,
        reply: String,
        clarification: bool,
        started_at: i64,
        finished_at: i64,
        actions: Vec<BenchAction>,
    },
    Archive {
        round_id: i64,
        question_id: String,
    },
    DeleteSession {
        session_id: String,
        after: DeleteAfter,
    },
    CommitRound {
        round_id: i64,
    },
    Output(Output),
    Fail(String),
}

#[derive(Clone, Debug)]
enum DeleteAfter {
    Commit(i64),
    Fail(String),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Output {
    Empty(()),
    Round {
        round: i64,
    },
    Answer {
        #[serde(rename = "Q")]
        q: String,
        #[serde(rename = "K")]
        k: String,
        reply: String,
    },
    Reply {
        reply: String,
    },
}

#[derive(Serialize)]
struct CreateSessionRequest<'a> {
    title: String,
    agent: &'a str,
    permission: Vec<PermissionRule>,
}

#[derive(Deserialize)]
struct CreateSessionResponse {
    id: String,
}

#[derive(Serialize)]
struct MessageRequest<'a> {
    agent: &'a str,
    parts: [TextPart<'a>; 1],
}

#[derive(Serialize)]
struct TextPart<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
}

#[derive(Deserialize)]
struct MessageResponse {
    #[serde(default)]
    parts: Vec<MessagePart>,
}

#[derive(Deserialize)]
struct MessagePart {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    state: Option<PartState>,
    #[serde(default)]
    time: Option<PartTime>,
}

#[derive(Deserialize)]
struct PartState {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    time: Option<PartTime>,
}

#[derive(Clone, Deserialize)]
struct PartTime {
    start: i64,
    #[serde(default)]
    end: Option<i64>,
}

#[derive(Serialize)]
struct PermissionRule {
    permission: String,
    pattern: String,
    action: &'static str,
}

impl Event {
    fn reduce(self, state: &mut State) -> Result<Vec<Effect>> {
        match self {
            Self::Requested(Command::Start) => {
                if let Some(round) = &state.round {
                    if round.session_id.is_none() {
                        return Ok(vec![Effect::CreateSession { round_id: round.id }]);
                    }
                    bail!("benchmark already has a current round");
                }
                Ok(vec![Effect::LoadInputs])
            }
            Self::InputsLoaded(questions) => Ok(vec![Effect::CreateRound(questions)]),
            Self::RoundCreated {
                round_id,
                questions,
            } => {
                state.round = Some(Round {
                    id: round_id,
                    session_id: None,
                    questions,
                });
                Ok(vec![Effect::CreateSession { round_id }])
            }
            Self::SessionCreated(session_id) => {
                let round = state
                    .round
                    .as_mut()
                    .context("created session has no round")?;
                round.session_id = Some(session_id.clone());
                Ok(vec![Effect::AttachSession {
                    round_id: round.id,
                    session_id,
                }])
            }
            Self::SessionCreateFailed(error) => Ok(vec![Effect::Fail(error)]),
            Self::SessionAttachFailed { session_id, error } => Ok(vec![Effect::DeleteSession {
                session_id,
                after: DeleteAfter::Fail(error),
            }]),
            Self::SessionAttached => {
                let round = state
                    .round
                    .as_ref()
                    .context("attached session has no round")?;
                Ok(vec![Effect::Output(Output::Round { round: round.id })])
            }
            Self::Requested(Command::Next) => {
                let round = current_round(state)?;
                if round
                    .questions
                    .iter()
                    .any(|question| question.status == "current")
                {
                    bail!("current question must be archived before selecting the next one");
                }
                let Some(question) = round
                    .questions
                    .iter_mut()
                    .find(|question| question.status == "pending")
                else {
                    return Ok(vec![Effect::Output(Output::Empty(()))]);
                };
                question.status = "current".into();
                let round_id = round.id;
                let session_id = round
                    .session_id
                    .clone()
                    .context("current round has no respondent session")?;
                Ok(vec![Effect::Ask {
                    round_id,
                    session_id,
                    question: question.clone(),
                    prompt: question.q.clone(),
                    clarification: false,
                }])
            }
            Self::Requested(Command::Clarify(text)) => {
                let round = current_round(state)?;
                let round_id = round.id;
                let session_id = round
                    .session_id
                    .clone()
                    .context("current round has no respondent session")?;
                let question = current_question_mut(round)?;
                if question.clarifications >= 3 {
                    bail!("current question already has three clarifications");
                }
                question.clarifications += 1;
                Ok(vec![Effect::Ask {
                    round_id,
                    session_id,
                    question: question.clone(),
                    prompt: text,
                    clarification: true,
                }])
            }
            Self::Answered {
                round_id,
                question,
                reply,
                clarification,
                started_at,
                finished_at,
                actions,
            } => Ok(vec![Effect::SaveAnswer {
                round_id,
                question,
                reply,
                clarification,
                started_at,
                finished_at,
                actions,
            }]),
            Self::AnswerSaved {
                question,
                reply,
                clarification,
            } => {
                let stored = current_question_mut(current_round(state)?)?;
                *stored = question.clone();
                let value = if !clarification {
                    Output::Answer {
                        q: question.q,
                        k: question.k,
                        reply,
                    }
                } else {
                    Output::Reply { reply }
                };
                Ok(vec![Effect::Output(value)])
            }
            Self::Requested(Command::Archive) => {
                let round = current_round(state)?;
                let question = round
                    .questions
                    .iter()
                    .find(|question| question.status == "current")
                    .context("benchmark has no current question")?;
                Ok(vec![Effect::Archive {
                    round_id: round.id,
                    question_id: question.id.clone(),
                }])
            }
            Self::Archived => {
                let question = current_question_mut(current_round(state)?)?;
                question.status = "archived".into();
                Ok(vec![Effect::Output(Output::Empty(()))])
            }
            Self::Requested(Command::Finish) => {
                let round = current_round(state)?;
                if round
                    .questions
                    .iter()
                    .any(|question| question.status != "archived")
                {
                    bail!("all questions must be archived before finishing");
                }
                let session_id = round
                    .session_id
                    .clone()
                    .context("current round has no respondent session")?;
                Ok(vec![Effect::DeleteSession {
                    session_id,
                    after: DeleteAfter::Commit(round.id),
                }])
            }
            Self::SessionDeleted(DeleteAfter::Commit(round_id)) => {
                Ok(vec![Effect::CommitRound { round_id }])
            }
            Self::SessionDeleted(DeleteAfter::Fail(error)) => Ok(vec![Effect::Fail(error)]),
            Self::RoundCommitted => {
                state.round = None;
                Ok(vec![Effect::Output(Output::Empty(()))])
            }
        }
    }
}

struct ContextData {
    root: PathBuf,
    name: String,
    benchmark: Bench,
    respondent_id: String,
    records: PathBuf,
    backend_url: String,
    client: Client,
}

pub async fn run(root: PathBuf, name: String, command: Command) -> Result<()> {
    let plan = Plan::load(&root)?;
    let artifact_name = ArtifactName::parse(&name)?;
    let artifact = plan
        .artifacts
        .get(&artifact_name)
        .with_context(|| format!("unknown artifact `{name}`"))?;
    if artifact.kind != ArtifactKind::Bench {
        bail!("artifact `{name}` is not a bench artifact");
    }
    let benchmark = artifact.bench.clone().expect("normalized bench artifact");
    let records = root
        .join(".labflow/benchmarks")
        .join(format!("{}.sqlite", benchmark.name.as_str()));
    let profile = crate::agent::respondent_profile(&artifact_name, &benchmark);
    crate::agent::materialize_profile(&root, &profile)?;
    if let Some(parent) = records.parent() {
        fs::create_dir_all(parent)?;
    }
    let _lock = Lock::acquire(&root, &name)?;
    initialize(&records)?;
    let state = restore(&records)?;
    let config = Config::load(&root)?;
    let context = Arc::new(ContextData {
        root,
        name,
        benchmark,
        respondent_id: profile.id,
        records,
        backend_url: format!("http://127.0.0.1:{}", config.port),
        client: Client::builder()
            .timeout(Duration::from_secs(30 * 60))
            .build()?,
    });
    drive(state, Event::Requested(command), context).await
}

async fn drive(mut state: State, initial: Event, context: Arc<ContextData>) -> Result<()> {
    let mut events = VecDeque::from([initial]);
    while let Some(event) = events.pop_front() {
        for effect in event.reduce(&mut state)? {
            match effect.apply(context.clone()).await? {
                Some(event) => events.push_back(event),
                None => return Ok(()),
            }
        }
    }
    Ok(())
}

impl Effect {
    async fn apply(self, context: Arc<ContextData>) -> Result<Option<Event>> {
        match self {
            Self::LoadInputs => Ok(Some(Event::InputsLoaded(load_questions(&context)?))),
            Self::CreateRound(questions) => {
                let mut connection = Connection::open(&context.records)?;
                let now = now()?;
                let configuration_revision = format!(
                    "{:x}",
                    Sha256::digest(fs::read(context.root.join(PLAN_FILE))?)
                );
                let transaction = connection.transaction()?;
                transaction.execute(
                    "INSERT INTO bench_round(status, respondent, started_at, configuration_revision) VALUES ('current', ?1, ?2, ?3)",
                    params![context.respondent_id, now, configuration_revision],
                )?;
                let round_id = transaction.last_insert_rowid();
                for question in &questions {
                    transaction.execute(
                        "INSERT INTO question(bench_round_id, ordinal, question_id, k, reference_answer, status)
                         VALUES (?1, ?2, ?3, ?4, ?5, 'pending')",
                        params![
                            round_id,
                            question.ordinal,
                            question.id,
                            question.k,
                            question.reference_answer
                        ],
                    )?;
                    for tag in &question.tags {
                        transaction.execute(
                            "INSERT INTO question_tag(bench_round_id, question_id, tag)
                             VALUES (?1, ?2, ?3)",
                            params![round_id, question.id, tag],
                        )?;
                    }
                    transaction.execute(
                        "INSERT INTO turn(bench_round_id, question_id, turn_index, is_last_turn, q)
                         VALUES (?1, ?2, 0, 0, ?3)",
                        params![round_id, question.id, question.q],
                    )?;
                }
                transaction.commit()?;
                Ok(Some(Event::RoundCreated {
                    round_id,
                    questions,
                }))
            }
            Self::CreateSession { round_id } => {
                let result: Result<String> = async {
                    let response = context
                        .client
                        .post(format!("{}/session", context.backend_url))
                        .query(&[("directory", context.root.to_string_lossy().as_ref())])
                        .json(&CreateSessionRequest {
                            title: format!("labflow:bench:{}:{round_id}", context.name),
                            agent: &context.respondent_id,
                            permission: respondent_permissions(&context.benchmark),
                        })
                        .send()
                        .await?
                        .error_for_status()?;
                    let response: CreateSessionResponse = response.json().await?;
                    Ok(response.id)
                }
                .await;
                Ok(Some(match result {
                    Ok(session_id) => Event::SessionCreated(session_id),
                    Err(error) => Event::SessionCreateFailed(format!(
                        "failed to create respondent session: {error:#}"
                    )),
                }))
            }
            Self::AttachSession {
                round_id,
                session_id,
            } => {
                let result = Connection::open(&context.records).and_then(|connection| {
                    connection.execute(
                    "UPDATE bench_round SET session_id = ?1 WHERE id = ?2 AND status = 'current'",
                    params![session_id, round_id],
                )
                });
                Ok(Some(match result {
                    Ok(1) => Event::SessionAttached,
                    Ok(_) => Event::SessionAttachFailed {
                        session_id,
                        error:
                            "current benchmark round disappeared while attaching respondent session"
                                .into(),
                    },
                    Err(error) => Event::SessionAttachFailed {
                        session_id,
                        error: format!("failed to attach respondent session: {error:#}"),
                    },
                }))
            }
            Self::Ask {
                round_id,
                session_id,
                question,
                prompt,
                clarification,
            } => {
                let started_at = now()?;
                let mut connection = Connection::open(&context.records)?;
                let transaction = connection.transaction()?;
                if clarification {
                    transaction.execute(
                        "INSERT INTO turn(bench_round_id, question_id, turn_index, is_last_turn, q, started_at)
                         VALUES (?1, ?2, ?3, 0, ?4, ?5)",
                        params![round_id, question.id, question.clarifications, prompt, started_at],
                    )?;
                } else {
                    transaction.execute(
                        "UPDATE turn SET started_at = ?1 WHERE bench_round_id = ?2 AND question_id = ?3 AND turn_index = 0",
                        params![started_at, round_id, question.id],
                    )?;
                }
                transaction.execute(
                    "UPDATE question SET status = 'current' WHERE bench_round_id = ?1 AND question_id = ?2",
                    params![round_id, question.id],
                )?;
                transaction.commit()?;
                let response = context
                    .client
                    .post(format!(
                        "{}/session/{session_id}/message",
                        context.backend_url
                    ))
                    .query(&[("directory", context.root.to_string_lossy().as_ref())])
                    .json(&MessageRequest {
                        agent: &context.respondent_id,
                        parts: [TextPart {
                            kind: "text",
                            text: &prompt,
                        }],
                    })
                    .send()
                    .await?
                    .error_for_status()?;
                let response: MessageResponse = response.json().await?;
                let finished_at = now()?;
                let actions = response_actions(&response, started_at, finished_at);
                let reply = response_text(response);
                let mut question = question;
                if !clarification {
                    question.clarifications = 0;
                }
                Ok(Some(Event::Answered {
                    round_id,
                    question,
                    reply,
                    clarification,
                    started_at,
                    finished_at,
                    actions,
                }))
            }
            Self::SaveAnswer {
                round_id,
                question,
                reply,
                clarification,
                started_at,
                finished_at,
                actions,
            } => {
                let connection = Connection::open(&context.records)?;
                let transaction = connection.unchecked_transaction()?;
                if clarification {
                    transaction.execute(
                        "UPDATE turn SET a = ?1, finished_at = ?2
                         WHERE bench_round_id = ?3 AND question_id = ?4 AND turn_index = ?5",
                        params![
                            reply,
                            finished_at,
                            round_id,
                            question.id,
                            question.clarifications
                        ],
                    )?;
                } else {
                    transaction.execute(
                        "UPDATE turn SET a = ?1, started_at = ?2, finished_at = ?3
                         WHERE bench_round_id = ?4 AND question_id = ?5 AND turn_index = 0",
                        params![reply, started_at, finished_at, round_id, question.id],
                    )?;
                }
                for (action_index, action) in actions.into_iter().enumerate() {
                    transaction.execute(
                        "INSERT INTO action(bench_round_id, question_id, turn_index, action_index, kind, subject, started_at, finished_at, result)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![round_id, question.id, question.clarifications, action_index as i64, action.kind, action.subject, action.started_at, action.finished_at, action.result],
                    )?;
                }
                transaction.commit()?;
                Ok(Some(Event::AnswerSaved {
                    question,
                    reply,
                    clarification,
                }))
            }
            Self::Archive {
                round_id,
                question_id,
            } => {
                let mut connection = Connection::open(&context.records)?;
                let transaction = connection.transaction()?;
                transaction.execute(
                    "UPDATE question SET status = 'archived', archived_at = ?1 WHERE bench_round_id = ?2 AND question_id = ?3 AND status = 'current'",
                    params![now()?, round_id, question_id],
                )?;
                transaction.execute(
                    "UPDATE turn SET is_last_turn = 1 WHERE bench_round_id = ?1 AND question_id = ?2 AND turn_index = (SELECT max(turn_index) FROM turn WHERE bench_round_id = ?1 AND question_id = ?2)",
                    params![round_id, question_id],
                )?;
                transaction.commit()?;
                Ok(Some(Event::Archived))
            }
            Self::DeleteSession { session_id, after } => {
                context
                    .client
                    .delete(format!("{}/session/{session_id}", context.backend_url))
                    .query(&[("directory", context.root.to_string_lossy().as_ref())])
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(Some(Event::SessionDeleted(after)))
            }
            Self::CommitRound { round_id } => {
                Connection::open(&context.records)?.execute(
                    "UPDATE bench_round SET status = 'committed', finished_at = ?1, session_id = NULL WHERE id = ?2 AND status = 'current'",
                    params![now()?, round_id],
                )?;
                Ok(Some(Event::RoundCommitted))
            }
            Self::Output(value) => {
                println!("{}", serde_json::to_string(&value)?);
                Ok(None)
            }
            Self::Fail(error) => Err(anyhow!(error)),
        }
    }
}

fn initialize(path: &Path) -> Result<()> {
    let connection = Connection::open(path)?;
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version != 3 {
        connection.execute_batch(
            "DROP TABLE IF EXISTS question_turns;
             DROP TABLE IF EXISTS round_questions;
             DROP TABLE IF EXISTS bench_rounds;
             DROP TABLE IF EXISTS action;
             DROP TABLE IF EXISTS turn;
             DROP TABLE IF EXISTS question_tag;
             DROP TABLE IF EXISTS question;
             DROP TABLE IF EXISTS bench_round;",
        )?;
    }
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS bench_round (
           id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT NOT NULL,
           respondent TEXT NOT NULL, session_id TEXT, started_at INTEGER NOT NULL,
           finished_at INTEGER, configuration_revision TEXT
         );
         CREATE UNIQUE INDEX IF NOT EXISTS one_current_round ON bench_round(status) WHERE status = 'current';
         CREATE TABLE IF NOT EXISTS question (
           bench_round_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, question_id TEXT NOT NULL,
           k TEXT NOT NULL, reference_answer TEXT, status TEXT NOT NULL, archived_at INTEGER,
           PRIMARY KEY(bench_round_id, question_id)
         );
         CREATE TABLE IF NOT EXISTS question_tag (
           bench_round_id INTEGER NOT NULL, question_id TEXT NOT NULL, tag TEXT NOT NULL,
           PRIMARY KEY(bench_round_id, question_id, tag)
         );
         CREATE TABLE IF NOT EXISTS turn (
           bench_round_id INTEGER NOT NULL, question_id TEXT NOT NULL, turn_index INTEGER NOT NULL,
           is_last_turn INTEGER NOT NULL, q TEXT NOT NULL, a TEXT,
           started_at INTEGER, finished_at INTEGER,
           PRIMARY KEY(bench_round_id, question_id, turn_index)
         );
         CREATE UNIQUE INDEX IF NOT EXISTS one_last_turn ON turn(bench_round_id, question_id) WHERE is_last_turn = 1;
         CREATE TABLE IF NOT EXISTS action (
           bench_round_id INTEGER NOT NULL, question_id TEXT NOT NULL, turn_index INTEGER NOT NULL,
           action_index INTEGER NOT NULL, kind TEXT NOT NULL, subject TEXT,
           started_at INTEGER NOT NULL, finished_at INTEGER, result TEXT,
           PRIMARY KEY(bench_round_id, question_id, turn_index, action_index)
         );
         PRAGMA user_version = 3;",
    )?;
    Ok(())
}

fn restore(path: &Path) -> Result<State> {
    let connection = Connection::open(path)?;
    let row = connection
        .query_row(
            "SELECT id, session_id FROM bench_round WHERE status = 'current'",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((id, session_id)) = row else {
        return Ok(State::default());
    };
    let mut statement = connection.prepare(
        "SELECT qn.ordinal, qn.question_id, t.q, qn.k, qn.reference_answer, qn.status,
                COALESCE((SELECT max(turn_index) FROM turn WHERE bench_round_id = qn.bench_round_id AND question_id = qn.question_id), 0)
         FROM question qn JOIN turn t ON t.bench_round_id = qn.bench_round_id AND t.question_id = qn.question_id AND t.turn_index = 0
         WHERE qn.bench_round_id = ?1 ORDER BY qn.ordinal",
    )?;
    let questions = statement
        .query_map([id], |row| {
            Ok(Question {
                ordinal: row.get(0)?,
                id: row.get(1)?,
                q: row.get(2)?,
                k: row.get(3)?,
                reference_answer: row.get(4)?,
                tags: Vec::new(),
                status: row.get(5)?,
                clarifications: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(State {
        round: Some(Round {
            id,
            session_id,
            questions,
        }),
    })
}

#[derive(Deserialize)]
struct SourceQuestion {
    id: String,
    #[serde(rename = "Q")]
    q: String,
    #[serde(default, rename = "K")]
    k: Option<String>,
    #[serde(default, rename = "R")]
    reference_answer: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

fn load_questions(context: &ContextData) -> Result<Vec<Question>> {
    let source = fs::read_to_string(context.benchmark.source.resolve(&context.root))?;
    let mut ids = BTreeSet::new();
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(ordinal, line)| {
            let source: SourceQuestion = serde_json::from_str(line)
                .with_context(|| format!("invalid challenge source line {}", ordinal + 1))?;
            if source.id.trim().is_empty() {
                bail!("question id on line {} cannot be empty", ordinal + 1);
            }
            if !ids.insert(source.id.clone()) {
                bail!("duplicate question id `{}`", source.id);
            }
            let mut tags = BTreeSet::new();
            for tag in source.tags {
                if tag.trim().is_empty() {
                    bail!("question `{}` has an empty tag", source.id);
                }
                if !tags.insert(tag.clone()) {
                    bail!("question `{}` has duplicate tag `{tag}`", source.id);
                }
            }
            Ok(Question {
                ordinal: ordinal as i64,
                id: source.id,
                q: source.q,
                k: source.k.unwrap_or_default(),
                reference_answer: source.reference_answer,
                tags: tags.into_iter().collect(),
                status: "pending".into(),
                clarifications: 0,
            })
        })
        .collect()
}

fn respondent_permissions(benchmark: &Bench) -> Vec<PermissionRule> {
    let mut rules = vec![
        rule("*", "*", "deny"),
        rule("glob", "*", "allow"),
        rule("grep", "*", "deny"),
    ];
    for path in benchmark
        .public_knowledge
        .iter()
        .chain(&benchmark.permissions.read)
        .chain(&benchmark.permissions.write)
    {
        for pattern in path_patterns(path) {
            rules.push(rule("read", &pattern, "allow"));
        }
    }
    for path in &benchmark.permissions.write {
        for pattern in path_patterns(path) {
            rules.push(rule("edit", &pattern, "allow"));
        }
    }
    for command in &benchmark.permissions.commands {
        rules.push(rule("bash", command, "allow"));
    }
    rules
}

fn rule(permission: &str, pattern: &str, action: &'static str) -> PermissionRule {
    PermissionRule {
        permission: permission.to_owned(),
        pattern: pattern.to_owned(),
        action,
    }
}

fn path_patterns(path: &AssetPath) -> Vec<String> {
    let value = path.as_str().trim_end_matches('/').to_owned();
    if path.is_directory() {
        vec![value.clone(), format!("{value}/*")]
    } else {
        vec![value]
    }
}

fn response_text(response: MessageResponse) -> String {
    response
        .parts
        .into_iter()
        .filter(|part| part.kind == "text")
        .filter_map(|part| part.text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn response_actions(
    response: &MessageResponse,
    turn_started_at: i64,
    turn_finished_at: i64,
) -> Vec<BenchAction> {
    response
        .parts
        .iter()
        .filter_map(|part| {
            let raw_kind = part.tool.as_deref().unwrap_or(&part.kind);
            let kind = match raw_kind.to_ascii_lowercase().as_str() {
                "reasoning" => "reasoning",
                "text" => "text",
                "read" => "read",
                "edit" | "apply_patch" => "edit",
                "write" => "write",
                "glob" => "glob",
                "bash" => "bash",
                _ if part.tool.is_some() || part.kind == "tool" => "other-tool",
                _ => return None,
            }
            .to_owned();
            let state = part.state.as_ref();
            let time = state
                .and_then(|state| state.time.as_ref())
                .or(part.time.as_ref());
            let subject = state.and_then(|state| action_subject(&kind, &state.input));
            let result = state
                .and_then(|state| state.status.as_deref())
                .and_then(action_result);
            let completed = state.is_none() || result.is_some();
            Some(BenchAction {
                kind,
                subject,
                started_at: time.map_or(turn_started_at, |time| time.start),
                finished_at: time
                    .and_then(|time| time.end)
                    .or(completed.then_some(turn_finished_at)),
                result: result
                    .map(str::to_owned)
                    .or_else(|| state.is_none().then(|| "succeeded".into())),
            })
        })
        .collect()
}

fn action_subject(kind: &str, input: &Value) -> Option<String> {
    let field = match kind {
        "read" | "edit" | "write" => ["filePath", "path"].as_slice(),
        "glob" => ["path", "pattern"].as_slice(),
        "bash" => ["command"].as_slice(),
        _ => return None,
    };
    field
        .iter()
        .find_map(|name| input.get(name).and_then(Value::as_str))
        .map(str::to_owned)
}

fn action_result(status: &str) -> Option<&'static str> {
    match status {
        "completed" | "success" | "succeeded" => Some("succeeded"),
        "error" | "failed" => Some("failed"),
        "cancelled" | "interrupted" => Some("interrupted"),
        _ => None,
    }
}

fn current_round(state: &mut State) -> Result<&mut Round> {
    state
        .round
        .as_mut()
        .context("benchmark has no current round")
}

fn current_question_mut(round: &mut Round) -> Result<&mut Question> {
    round
        .questions
        .iter_mut()
        .find(|question| question.status == "current")
        .context("benchmark has no current question")
}

fn now() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

struct Lock {
    file: File,
}

impl Lock {
    fn acquire(root: &Path, name: &str) -> Result<Self> {
        let directory = root.join(".labflow/locks");
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("bench-{}.lock", name.replace('.', "-")));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_exclusive()
            .map_err(|_| anyhow!("benchmark `{name}` is busy"))?;
        Ok(Self { file })
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::SourceQuestion;

    #[test]
    fn hidden_knowledge_may_be_missing_or_null() {
        for input in [
            r#"{"id":"q1","Q":"question"}"#,
            r#"{"id":"q1","Q":"question","K":null}"#,
        ] {
            let question: SourceQuestion = serde_json::from_str(input).unwrap();
            assert_eq!(question.k, None);
        }
    }
}
