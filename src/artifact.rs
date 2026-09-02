use std::fmt;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use filetime::{FileTime, set_file_mtime};
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Deserialize, Serialize};

pub const ARTIFACTS_DIR: &str = ".labflow/artifacts";

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ArtifactName(String);

impl<'de> Deserialize<'de> for ArtifactName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl ArtifactName {
    pub fn parse(value: &str) -> Result<Self> {
        static PATTERN: OnceCell<Regex> = OnceCell::new();
        let pattern = PATTERN.get_or_init(|| {
            let part = r"[a-z][0-9a-z]*(?:-[0-9a-z]+)*";
            Regex::new(&format!(
                r"^(?:{part}|{part}\.{part}|_{part}(?:\.{part})?)$"
            ))
            .expect("valid artifact regex")
        });
        if !pattern.is_match(value) {
            bail!("invalid artifact name `{value}`");
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_supervisor(&self) -> bool {
        self.0.starts_with('_')
    }

    pub fn role(&self) -> Option<&str> {
        self.0.rsplit_once('.').map(|(_, role)| role)
    }

    pub fn path(&self, root: &Path) -> PathBuf {
        root.join(ARTIFACTS_DIR).join(&self.0)
    }

    pub fn require_host_accessible(&self) -> Result<()> {
        if self.is_supervisor() {
            bail!("supervisor artifact `{self}` cannot be controlled by Host");
        }
        Ok(())
    }
}

impl fmt::Display for ArtifactName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ArtifactName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

pub fn publish(root: &Path, name: &ArtifactName) -> Result<()> {
    name.require_host_accessible()?;
    let path = name.path(root);
    fs::create_dir_all(path.parent().expect("artifact path has parent"))?;
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to publish `{name}`"))?;
    let now = FileTime::now();
    let previous = FileTime::from_last_modification_time(&fs::metadata(&path)?);
    let modified = if now > previous {
        now
    } else if previous.nanoseconds() == 999_999_999 {
        FileTime::from_unix_time(previous.unix_seconds().saturating_add(1), 0)
    } else {
        FileTime::from_unix_time(previous.unix_seconds(), previous.nanoseconds() + 1)
    };
    set_file_mtime(&path, modified)
        .with_context(|| format!("failed to touch `{}`", path.display()))?;
    Ok(())
}

pub fn unpublish(root: &Path, name: &ArtifactName) -> Result<bool> {
    name.require_host_accessible()?;
    let path = name.path(root);
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to unpublish `{name}`")),
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    #[test]
    fn validates_names_and_role_suffixes() {
        for valid in [
            "request",
            "query-request",
            "answer.researcher",
            "_blocked",
            "_ready.researcher",
        ] {
            assert!(ArtifactName::parse(valid).is_ok(), "{valid}");
        }
        for invalid in ["A", "a_b", "-a", "a.", "a.b.c", "_", "ready?", "a.B"] {
            assert!(ArtifactName::parse(invalid).is_err(), "{invalid}");
        }
        assert_eq!(
            ArtifactName::parse("answer.researcher").unwrap().role(),
            Some("researcher")
        );
    }

    #[test]
    fn repeated_publish_moves_mtime_forward() {
        let directory = tempfile::tempdir().unwrap();
        let name = ArtifactName::parse("request").unwrap();
        publish(directory.path(), &name).unwrap();
        let first = fs::metadata(name.path(directory.path()))
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        publish(directory.path(), &name).unwrap();
        let second = fs::metadata(name.path(directory.path()))
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        assert!(second > first);
    }
}
