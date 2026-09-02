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
        timeline.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at INTEGER NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS records_observed_at ON records(observed_at);
            ",
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
                if state.plan.roles.contains_key(&role) {
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

    pub fn append_timeline(
        &self,
        observed_at: Timestamp,
        source: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        Connection::open(&self.timeline)
            .with_context(|| format!("failed to open `{}`", self.timeline.display()))?
            .execute(
                "INSERT INTO records(observed_at, source, kind, payload) VALUES (?1, ?2, ?3, ?4)",
                params![observed_at, source, kind, serde_json::to_string(payload)?],
            )?;
        Ok(())
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
