use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use reqwest::Client;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::Config;
use crate::plan::{AssetPath, Benchmark, Plan};

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
    status: String,
    clarifications: u8,
}

#[derive(Clone, Debug)]
struct Round {
    id: i64,
    session_id: Option<String>,
    questions: Vec<Question>,
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
        prompt: String,
        reply: String,
        clarification: bool,
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
        prompt: String,
        reply: String,
        clarification: bool,
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
    Output(Value),
    Fail(String),
}

#[derive(Clone, Debug)]
enum DeleteAfter {
    Commit(i64),
    Fail(String),
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
                Ok(vec![Effect::Output(json!({ "round": round.id }))])
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
                    return Ok(vec![Effect::Output(Value::Null)]);
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
                prompt,
                reply,
                clarification,
            } => Ok(vec![Effect::SaveAnswer {
                round_id,
                question,
                prompt,
                reply,
                clarification,
            }]),
            Self::AnswerSaved {
                question,
                reply,
                clarification,
            } => {
                let stored = current_question_mut(current_round(state)?)?;
                *stored = question.clone();
                let value = if !clarification {
                    json!({ "Q": question.q, "K": question.k, "reply": reply })
                } else {
                    json!({ "reply": reply })
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
                Ok(vec![Effect::Output(Value::Null)])
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
                Ok(vec![Effect::Output(Value::Null)])
            }
        }
    }
}

struct ContextData {
    root: PathBuf,
    name: String,
    benchmark: Benchmark,
    records: PathBuf,
    backend_url: String,
    client: Client,
}

pub async fn run(root: PathBuf, name: String, command: Command) -> Result<()> {
    let plan = Plan::load(&root)?;
    let benchmark = plan
        .benchmarks
        .get(&name)
        .cloned()
        .with_context(|| format!("unknown benchmark `{name}`"))?;
    let records = benchmark.records.resolve(&root);
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
        records,
        backend_url: format!("http://{}:{}", plan.backend.hostname, config.port),
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
                let transaction = connection.transaction()?;
                transaction.execute(
                    "INSERT INTO bench_rounds(status, respondent, started_at) VALUES ('current', ?1, ?2)",
                    params![context.benchmark.respondent, now],
                )?;
                let round_id = transaction.last_insert_rowid();
                for question in &questions {
                    transaction.execute(
                        "INSERT INTO round_questions(round_id, ordinal, question_id, q, k, status, clarification_count)
                         VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0)",
                        params![round_id, question.ordinal, question.id, question.q, question.k],
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
                        .json(&json!({
                            "title": format!("labflow:bench:{}:{round_id}", context.name),
                            "agent": context.benchmark.respondent,
                            "permission": respondent_permissions(&context.benchmark),
                        }))
                        .send()
                        .await?
                        .error_for_status()?;
                    let value: Value = response.json().await?;
                    Ok(value["id"]
                        .as_str()
                        .context("OpenCode respondent session response has no id")?
                        .to_owned())
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
                    "UPDATE bench_rounds SET session_id = ?1 WHERE id = ?2 AND status = 'current'",
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
                let response = context
                    .client
                    .post(format!(
                        "{}/session/{session_id}/message",
                        context.backend_url
                    ))
                    .query(&[("directory", context.root.to_string_lossy().as_ref())])
                    .json(&json!({
                        "agent": context.benchmark.respondent,
                        "parts": [{ "type": "text", "text": prompt }]
                    }))
                    .send()
                    .await?
                    .error_for_status()?;
                let value: Value = response.json().await?;
                let reply = response_text(&value);
                let mut question = question;
                if !clarification {
                    question.clarifications = 0;
                }
                Ok(Some(Event::Answered {
                    round_id,
                    question,
                    prompt,
                    reply,
                    clarification,
                }))
            }
            Self::SaveAnswer {
                round_id,
                question,
                prompt,
                reply,
                clarification,
            } => {
                let connection = Connection::open(&context.records)?;
                let transaction = connection.unchecked_transaction()?;
                if clarification {
                    transaction.execute(
                        "UPDATE round_questions SET clarification_count = ?1 WHERE round_id = ?2 AND question_id = ?3",
                        params![question.clarifications, round_id, question.id],
                    )?;
                } else {
                    transaction.execute(
                        "UPDATE round_questions SET status = 'current', first_reply = ?1 WHERE round_id = ?2 AND question_id = ?3",
                        params![reply, round_id, question.id],
                    )?;
                }
                transaction.execute(
                    "INSERT INTO question_turns(round_id, question_id, ordinal, speaker, kind, content, created_at)
                     VALUES (?1, ?2, COALESCE((SELECT max(ordinal) + 1 FROM question_turns WHERE round_id = ?1 AND question_id = ?2), 0), 'C', ?3, ?4, ?5)",
                    params![round_id, question.id, if clarification { "clarification" } else { "question" }, prompt, now()?],
                )?;
                transaction.execute(
                    "INSERT INTO question_turns(round_id, question_id, ordinal, speaker, kind, content, created_at)
                     VALUES (?1, ?2, COALESCE((SELECT max(ordinal) + 1 FROM question_turns WHERE round_id = ?1 AND question_id = ?2), 0), 'R', ?3, ?4, ?5)",
                    params![round_id, question.id, if clarification { "clarification-answer" } else { "answer" }, reply, now()?],
                )?;
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
                Connection::open(&context.records)?.execute(
                    "UPDATE round_questions SET status = 'archived', archived_at = ?1 WHERE round_id = ?2 AND question_id = ?3 AND status = 'current'",
                    params![now()?, round_id, question_id],
                )?;
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
                    "UPDATE bench_rounds SET status = 'committed', committed_at = ?1, session_id = NULL WHERE id = ?2 AND status = 'current'",
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
    Connection::open(path)?.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS bench_rounds (
           id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT NOT NULL,
           respondent TEXT NOT NULL, session_id TEXT, started_at INTEGER NOT NULL,
           committed_at INTEGER
         );
         CREATE UNIQUE INDEX IF NOT EXISTS one_current_round ON bench_rounds(status) WHERE status = 'current';
         CREATE TABLE IF NOT EXISTS round_questions (
           round_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, question_id TEXT NOT NULL,
           q TEXT NOT NULL, k TEXT NOT NULL, status TEXT NOT NULL,
           first_reply TEXT, clarification_count INTEGER NOT NULL, archived_at INTEGER,
           PRIMARY KEY(round_id, question_id)
         );
         CREATE TABLE IF NOT EXISTS question_turns (
           round_id INTEGER NOT NULL, question_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
           speaker TEXT NOT NULL, kind TEXT NOT NULL, content TEXT NOT NULL, created_at INTEGER NOT NULL,
           PRIMARY KEY(round_id, question_id, ordinal)
         );",
    )?;
    Ok(())
}

fn restore(path: &Path) -> Result<State> {
    let connection = Connection::open(path)?;
    let row = connection
        .query_row(
            "SELECT id, session_id FROM bench_rounds WHERE status = 'current'",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((id, session_id)) = row else {
        return Ok(State::default());
    };
    let mut statement = connection.prepare(
        "SELECT ordinal, question_id, q, k, status, clarification_count
         FROM round_questions WHERE round_id = ?1 ORDER BY ordinal",
    )?;
    let questions = statement
        .query_map([id], |row| {
            Ok(Question {
                ordinal: row.get(0)?,
                id: row.get(1)?,
                q: row.get(2)?,
                k: row.get(3)?,
                status: row.get(4)?,
                clarifications: row.get(5)?,
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
    #[serde(rename = "K")]
    k: String,
}

fn load_questions(context: &ContextData) -> Result<Vec<Question>> {
    let source = fs::read_to_string(context.benchmark.challenge.source.resolve(&context.root))?;
    let mut catalog = BTreeMap::new();
    for (index, line) in source
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
    {
        let question: SourceQuestion = serde_json::from_str(line)
            .with_context(|| format!("invalid challenge source line {}", index + 1))?;
        if catalog.insert(question.id.clone(), question).is_some() {
            bail!("duplicate question id in challenge source");
        }
    }
    let ids = fs::read_to_string(context.benchmark.challenge.questions.resolve(&context.root))?;
    let mut seen = BTreeSet::new();
    ids.lines()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .enumerate()
        .map(|(ordinal, id)| {
            if !seen.insert(id.to_owned()) {
                bail!("duplicate question id `{id}`")
            }
            let source = catalog
                .get(id)
                .with_context(|| format!("unknown question id `{id}`"))?;
            Ok(Question {
                ordinal: ordinal as i64,
                id: id.to_owned(),
                q: source.q.clone(),
                k: source.k.clone(),
                status: "pending".into(),
                clarifications: 0,
            })
        })
        .collect()
}

fn respondent_permissions(benchmark: &Benchmark) -> Vec<Value> {
    let mut rules = vec![
        rule("*", "*", "deny"),
        rule("glob", "*", "allow"),
        rule("grep", "*", "deny"),
    ];
    for path in benchmark
        .public_knowledge
        .iter()
        .chain(&benchmark.respondent_access.read)
        .chain(&benchmark.respondent_access.write)
    {
        for pattern in path_patterns(path) {
            rules.push(rule("read", &pattern, "allow"));
        }
    }
    for path in &benchmark.respondent_access.write {
        for pattern in path_patterns(path) {
            rules.push(rule("edit", &pattern, "allow"));
        }
    }
    for command in &benchmark.respondent_access.commands {
        rules.push(rule("bash", command, "allow"));
    }
    rules
}

fn rule(permission: &str, pattern: &str, action: &str) -> Value {
    json!({ "permission": permission, "pattern": pattern, "action": action })
}

fn path_patterns(path: &AssetPath) -> Vec<String> {
    let value = path.as_str().trim_end_matches('/').to_owned();
    if path.is_directory() {
        vec![value.clone(), format!("{value}/*")]
    } else {
        vec![value]
    }
}

fn response_text(value: &Value) -> String {
    value["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|part| part["type"] == "text")
        .filter_map(|part| part["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
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
