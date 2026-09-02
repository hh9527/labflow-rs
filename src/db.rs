use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::artifact::ArtifactName;
use crate::domain::{Session, State, Task, Timestamp};
use crate::plan::Plan;

pub const STATES_DB: &str = ".labflow/states.sqlite";
pub const TIMELINE_DB: &str = ".labflow/timeline.sqlite";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostTasks {
    pub tasks: Vec<ArtifactName>,
    pub opt: Vec<ArtifactName>,
}

#[derive(Clone, Debug)]
pub struct Databases {
    pub states: PathBuf,
    pub timeline: PathBuf,
}

#[derive(Clone, Debug)]
pub struct TimelineAction {
    pub kind: String,
    pub subject: Option<String>,
    pub started_at: Timestamp,
    pub finished_at: Option<Timestamp>,
    pub result: Option<String>,
}

impl Databases {
    pub fn initialize(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root.join(".labflow"))?;
        let databases = Self {
            states: root.join(STATES_DB),
            timeline: root.join(TIMELINE_DB),
        };
        let states = Connection::open(&databases.states)?;
        states.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS artifacts (
                name TEXT PRIMARY KEY,
                modified INTEGER
            );
            CREATE TABLE IF NOT EXISTS virtual_artifacts (
                name TEXT PRIMARY KEY,
                modified INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                role TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                artifact TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS host_tasks (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                value TEXT NOT NULL
            );
            ",
        )?;
        let timeline = Connection::open(&databases.timeline)?;
        let version: i64 = timeline.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != 2 {
            timeline.execute_batch(
                "DROP TABLE IF EXISTS records;
                 DROP TABLE IF EXISTS artifact_turn;
                 DROP TABLE IF EXISTS turn_action;",
            )?;
        }
        timeline.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS artifact_turn (
               id INTEGER PRIMARY KEY,
               artifact TEXT NOT NULL,
               iteration INTEGER NOT NULL,
               started_at INTEGER NOT NULL,
               finished_at INTEGER,
               result TEXT,
               reply_prefix TEXT,
               upstream_session_id TEXT NOT NULL,
               upstream_turn_id TEXT
             );
             CREATE INDEX IF NOT EXISTS artifact_turn_artifact ON artifact_turn(artifact, iteration);
             CREATE TABLE IF NOT EXISTS turn_action (
               artifact_turn_id INTEGER NOT NULL,
               action_index INTEGER NOT NULL,
               kind TEXT NOT NULL,
               subject TEXT,
               started_at INTEGER NOT NULL,
               finished_at INTEGER,
               result TEXT,
               PRIMARY KEY(artifact_turn_id, action_index)
             );
             PRAGMA user_version = 2;",
        )?;
        Ok(databases)
    }

    pub fn restore(&self, plan: Arc<Plan>) -> Result<State> {
        let connection = Connection::open(&self.states)?;
        let mut state = State::new(plan);
        {
            let mut statement = connection.prepare("SELECT name, modified FROM artifacts")?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<Timestamp>>(1)?,
                ))
            })?;
            for row in rows {
                let (name, modified) = row?;
                state
                    .artifacts
                    .insert(ArtifactName::parse(&name)?, modified);
            }
        }
        {
            let mut statement =
                connection.prepare("SELECT name, modified FROM virtual_artifacts")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Timestamp>(1)?))
            })?;
            for row in rows {
                let (name, modified) = row?;
                state
                    .virtual_artifacts
                    .insert(ArtifactName::parse(&name)?, modified);
            }
        }
        {
            let mut statement = connection.prepare("SELECT role, value FROM sessions")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (role, value) = row?;
                if state.plan.roles.contains_key(role.as_str()) {
                    state.sessions.insert(role, serde_json::from_str(&value)?);
                }
            }
        }
        {
            let mut statement = connection.prepare("SELECT artifact, value FROM tasks")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (artifact, value) = row?;
                let artifact = ArtifactName::parse(&artifact)?;
                if state.plan.artifacts.contains_key(&artifact) {
                    state.tasks.insert(artifact, serde_json::from_str(&value)?);
                }
            }
        }
        state.next_request_id = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'next_request_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        state.observer_session = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'observer_session'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        state.host_tasks = connection
            .query_row(
                "SELECT value FROM host_tasks WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value))
            .transpose()?
            .unwrap_or_default();
        Ok(state)
    }

    pub fn persist_artifact(&self, name: &ArtifactName, modified: Option<Timestamp>) -> Result<()> {
        Connection::open(&self.states)?.execute(
            "INSERT INTO artifacts(name, modified) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET modified = excluded.modified",
            params![name.as_str(), modified],
        )?;
        Ok(())
    }

    pub fn persist_virtual(&self, name: &ArtifactName, modified: Timestamp) -> Result<()> {
        Connection::open(&self.states)?.execute(
            "INSERT INTO virtual_artifacts(name, modified) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET modified = excluded.modified",
            params![name.as_str(), modified],
        )?;
        Ok(())
    }

    pub fn persist_session(&self, role: &str, session: &Session) -> Result<()> {
        let value = serde_json::to_string(session)?;
        Connection::open(&self.states)?.execute(
            "INSERT INTO sessions(role, value) VALUES (?1, ?2)
             ON CONFLICT(role) DO UPDATE SET value = excluded.value",
            params![role, value],
        )?;
        Ok(())
    }

    pub fn persist_task(&self, artifact: &ArtifactName, task: Option<&Task>) -> Result<()> {
        let connection = Connection::open(&self.states)?;
        if let Some(task) = task {
            let value = serde_json::to_string(task)?;
            connection.execute(
                "INSERT INTO tasks(artifact, value) VALUES (?1, ?2)
                 ON CONFLICT(artifact) DO UPDATE SET value = excluded.value",
                params![artifact.as_str(), value],
            )?;
        } else {
            connection.execute(
                "DELETE FROM tasks WHERE artifact = ?1",
                params![artifact.as_str()],
            )?;
        }
        Ok(())
    }

    pub fn clear_tasks(&self) -> Result<()> {
        Connection::open(&self.states)?.execute("DELETE FROM tasks", [])?;
        Ok(())
    }

    pub fn persist_next_request_id(&self, value: u64) -> Result<()> {
        Connection::open(&self.states)?.execute(
            "INSERT INTO meta(key, value) VALUES ('next_request_id', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![value.to_string()],
        )?;
        Ok(())
    }

    pub fn persist_observer_session(&self, session_id: &str) -> Result<()> {
        Connection::open(&self.states)?.execute(
            "INSERT INTO meta(key, value) VALUES ('observer_session', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![session_id],
        )?;
        Ok(())
    }

    pub fn persist_host_tasks(&self, tasks: &HostTasks) -> Result<()> {
        Connection::open(&self.states)?.execute(
            "INSERT INTO host_tasks(singleton, value) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET value = excluded.value",
            params![serde_json::to_string(tasks)?],
        )?;
        Ok(())
    }

    pub fn begin_timeline_turn(
        &self,
        id: u64,
        artifact: &ArtifactName,
        iteration: u8,
        started_at: Timestamp,
        upstream_session_id: &str,
    ) -> Result<()> {
        Connection::open(&self.timeline)
            .with_context(|| format!("failed to open `{}`", self.timeline.display()))?
            .execute(
                "INSERT INTO artifact_turn(id, artifact, iteration, started_at, upstream_session_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![id, artifact.as_str(), iteration, started_at, upstream_session_id],
            )?;
        Ok(())
    }

    pub fn finish_timeline_turn(
        &self,
        id: u64,
        finished_at: Timestamp,
        result: &str,
        reply_prefix: Option<&str>,
        upstream_turn_id: Option<&str>,
        actions: &[TimelineAction],
    ) -> Result<()> {
        let mut connection = Connection::open(&self.timeline)?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE artifact_turn SET finished_at = ?1, result = ?2,
             reply_prefix = COALESCE(?3, reply_prefix), upstream_turn_id = COALESCE(?4, upstream_turn_id)
             WHERE id = ?5",
            params![finished_at, result, reply_prefix, upstream_turn_id, id],
        )?;
        for (index, action) in actions.iter().enumerate() {
            transaction.execute(
                "INSERT INTO turn_action(artifact_turn_id, action_index, kind, subject, started_at, finished_at, result)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![id, index as i64, action.kind, action.subject, action.started_at, action.finished_at, action.result],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_timeline_turn_result(&self, id: u64, result: &str) -> Result<()> {
        Connection::open(&self.timeline)?.execute(
            "UPDATE artifact_turn SET result = ?1 WHERE id = ?2",
            params![result, id],
        )?;
        Ok(())
    }

    pub fn timeline_turn_started_at(&self, id: u64) -> Result<Option<Timestamp>> {
        Connection::open(&self.timeline)?
            .query_row(
                "SELECT started_at FROM artifact_turn WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }
}

pub fn read_host_tasks(root: &Path) -> Result<HostTasks> {
    let path = root.join(STATES_DB);
    if !path.is_file() {
        return Ok(HostTasks::default());
    }
    let value = Connection::open(path)?
        .query_row(
            "SELECT value FROM host_tasks WHERE singleton = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    value
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .unwrap_or_else(|| Ok(HostTasks::default()))
}

pub fn read_virtual_timestamp(root: &Path, name: &ArtifactName) -> Result<Option<Timestamp>> {
    let path = root.join(STATES_DB);
    if !path.is_file() {
        return Ok(None);
    }
    Connection::open(path)?
        .query_row(
            "SELECT modified FROM virtual_artifacts WHERE name = ?1",
            params![name.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::EXAMPLE_PLAN;

    #[test]
    fn restores_persisted_projection() {
        let directory = tempfile::tempdir().unwrap();
        let databases = Databases::initialize(directory.path()).unwrap();
        let artifact = ArtifactName::parse("query-request").unwrap();
        databases.persist_artifact(&artifact, Some(42)).unwrap();
        let blocked = ArtifactName::parse("_blocked").unwrap();
        databases.persist_virtual(&blocked, 43).unwrap();
        let state = databases
            .restore(Arc::new(Plan::parse(EXAMPLE_PLAN).unwrap()))
            .unwrap();
        assert_eq!(state.timestamp(&artifact), Some(42));
        assert_eq!(
            read_virtual_timestamp(directory.path(), &blocked).unwrap(),
            Some(43)
        );
        let host_tasks = HostTasks {
            tasks: vec![artifact],
            opt: Vec::new(),
        };
        databases.persist_host_tasks(&host_tasks).unwrap();
        assert_eq!(read_host_tasks(directory.path()).unwrap(), host_tasks);
    }
}
