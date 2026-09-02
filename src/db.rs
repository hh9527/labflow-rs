use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::artifact::ArtifactName;
use crate::domain::{Session, State, Task, Timestamp};
use crate::plan::Plan;

pub const STATES_DB: &str = ".labflow/states.sqlite";
pub const TIMELINE_DB: &str = ".labflow/timeline.sqlite";

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
                state.sessions.insert(role, serde_json::from_str(&value)?);
            }
        }
        {
            let mut statement = connection.prepare("SELECT artifact, value FROM tasks")?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (artifact, value) = row?;
                state.tasks.insert(
                    ArtifactName::parse(&artifact)?,
                    serde_json::from_str(&value)?,
                );
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
        let state = databases
            .restore(Arc::new(Plan::parse(EXAMPLE_PLAN).unwrap()))
            .unwrap();
        assert_eq!(state.timestamp(&artifact), Some(42));
    }
}
