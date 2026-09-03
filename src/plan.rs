use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::artifact::ArtifactName;

pub const PLAN_FILE: &str = "lab-plan.toml";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub version: u32,
    #[serde(default)]
    pub backend: Backend,
    #[serde(default)]
    pub roles: BTreeMap<RoleName, Role>,
    #[serde(default)]
    pub artifacts: BTreeMap<ArtifactName, Artifact>,
    #[serde(default, rename = "benchmark")]
    pub benchmarks: BTreeMap<BenchmarkName, Benchmark>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Backend {
    pub command: Vec<String>,
    pub hostname: String,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            command: vec!["opencode".into(), "serve".into()],
            hostname: "127.0.0.1".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    pub kind: RoleKind,
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RoleKind {
    LabWorker,
    Evaluator,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    #[serde(default, rename = "requires")]
    pub requires: Vec<Dependency>,
    #[serde(default)]
    pub goal: Option<FilePath>,
    #[serde(default)]
    pub assets: Vec<AssetPath>,
    pub inputs: Option<Vec<AssetPath>>,
    #[serde(default)]
    pub check: Vec<FilePath>,
    pub permissions: Option<Vec<String>>,
    #[serde(skip)]
    pub benchmark: Option<BenchmarkName>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Benchmark {
    #[serde(skip)]
    pub respondent: String,
    pub records: FilePath,
    #[serde(default, rename = "requires")]
    pub requires: Vec<Dependency>,
    #[serde(default, rename = "public-knowledge")]
    pub public_knowledge: Vec<AssetPath>,
    pub challenge: Challenge,
    #[serde(default, rename = "respondent")]
    pub respondent_access: RespondentAccess,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Challenge {
    pub source: FilePath,
    pub questions: FilePath,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RespondentAccess {
    pub read: Vec<AssetPath>,
    pub write: Vec<AssetPath>,
    pub commands: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dependency {
    pub name: ArtifactName,
    pub optional: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RoleName(String);

impl RoleName {
    fn parse(value: &str) -> Result<Self> {
        validate_part(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for RoleName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl Borrow<str> for RoleName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RoleName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BenchmarkName(String);

impl BenchmarkName {
    fn parse(value: &str) -> Result<Self> {
        let parsed = ArtifactName::parse(value)?;
        if parsed.role().is_none() {
            bail!("benchmark name `{value}` must be `<respondent>.<role>`");
        }
        Ok(Self(value.to_owned()))
    }

    fn parts(&self) -> (&str, &str) {
        self.0.split_once('.').expect("validated benchmark name")
    }
}

impl<'de> Deserialize<'de> for BenchmarkName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl Borrow<str> for BenchmarkName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BenchmarkName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AssetPath {
    value: String,
    directory: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct FilePath(AssetPath);

impl FilePath {
    pub fn parse(value: &str) -> Result<Self> {
        AssetPath::parse(value, false).map(Self)
    }
}

impl<'de> Deserialize<'de> for FilePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl Deref for FilePath {
    type Target = AssetPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for AssetPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> Deserialize<'de> for AssetPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?, true).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Dependency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        parse_dependency(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl AssetPath {
    pub fn parse(value: &str, allow_directory: bool) -> Result<Self> {
        if value.is_empty() {
            bail!("asset path cannot be empty");
        }
        let directory = value.ends_with('/');
        if directory && !allow_directory {
            bail!("`{value}` must name a file");
        }
        let path = Path::new(value.trim_end_matches('/'));
        if path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir
                        | Component::CurDir
                        | Component::RootDir
                        | Component::Prefix(_)
                )
            })
        {
            bail!("`{value}` must be a normalized relative path");
        }
        Ok(Self {
            value: value.to_owned(),
            directory,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn is_directory(&self) -> bool {
        self.directory
    }

    pub fn resolve(&self, root: &Path) -> PathBuf {
        root.join(self.value.trim_end_matches('/'))
    }
}

impl Plan {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(PLAN_FILE);
        let source = fs::read_to_string(&path)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        Self::parse(&source).with_context(|| format!("invalid `{}`", path.display()))
    }

    pub fn parse(source: &str) -> Result<Self> {
        toml::from_str::<Self>(source)
            .context("invalid TOML")?
            .normalize()
    }

    pub fn normalize(mut self) -> Result<Self> {
        self.artifacts
            .retain(|_, artifact| artifact.benchmark.is_none());
        if self.version != 1 {
            bail!("unsupported plan version {}; expected 1", self.version);
        }
        if self.backend.command.is_empty() {
            bail!("backend command cannot be empty");
        }
        if self.backend.hostname.is_empty() {
            bail!("backend hostname cannot be empty");
        }

        for (role, definition) in &self.roles {
            validate_non_file_permissions(&definition.permissions)
                .with_context(|| format!("invalid permissions for role `{role}`"))?;
        }

        for (name, artifact) in &self.artifacts {
            if name.is_supervisor() {
                bail!("supervisor artifact `{name}` is built in and cannot be declared");
            }
            if let Some(role) = name.role() {
                if !self.roles.contains_key(role) {
                    bail!("artifact `{name}` references unknown role `{role}`");
                }
                if artifact.goal.is_none() {
                    bail!("worker artifact `{name}` must declare `goal`");
                }
                if let Some(permissions) = &artifact.permissions {
                    validate_non_file_permissions(permissions)
                        .with_context(|| format!("invalid permissions for artifact `{name}`"))?;
                }
            } else if artifact.permissions.is_some() {
                bail!("host artifact `{name}` cannot declare `permissions`");
            }
        }

        for (name, benchmark) in &mut self.benchmarks {
            let (respondent, challenger_role) = name.parts();
            let respondent = respondent.to_owned();
            let challenger_role = challenger_role.to_owned();
            if !self.roles.contains_key(challenger_role.as_str()) {
                bail!("benchmark `{name}` references unknown role `{challenger_role}`");
            }
            let artifact_name =
                ArtifactName::parse(&format!("bench-{respondent}.{challenger_role}"))?;
            if self.artifacts.contains_key(&artifact_name) {
                bail!("benchmark `{name}` conflicts with artifact `{artifact_name}`");
            }
            if benchmark
                .respondent_access
                .commands
                .iter()
                .any(|command| command.trim().is_empty())
            {
                bail!("benchmark `{name}` has an empty respondent command");
            }
            let mut inputs = BTreeSet::new();
            inputs.extend(benchmark.public_knowledge.iter().cloned());
            inputs.insert(benchmark.challenge.source.0.clone());
            inputs.insert(benchmark.challenge.questions.0.clone());
            benchmark.respondent = respondent;
            self.artifacts.insert(
                artifact_name.clone(),
                Artifact {
                    requires: benchmark.requires.clone(),
                    goal: None,
                    assets: vec![benchmark.records.0.clone()],
                    inputs: Some(inputs.into_iter().collect()),
                    check: vec![benchmark.records.clone()],
                    permissions: Some(vec!["bash".into()]),
                    benchmark: Some(name.clone()),
                },
            );
        }

        validate_dependencies(&self.artifacts, &self.roles)?;
        validate_acyclic(&self.artifacts)?;
        Ok(self)
    }

    pub fn artifact_inputs(&self, name: &ArtifactName) -> Vec<AssetPath> {
        let artifact = &self.artifacts[name];
        if let Some(inputs) = &artifact.inputs {
            return inputs.clone();
        }
        artifact
            .requires
            .iter()
            .filter_map(|dependency| self.artifacts.get(&dependency.name))
            .flat_map(|dependency| dependency.assets.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

fn validate_non_file_permissions(permissions: &[String]) -> Result<()> {
    for permission in permissions {
        if matches!(permission.as_str(), "read" | "edit" | "glob" | "grep") {
            bail!("`{permission}` is derived from artifact assets and inputs");
        }
    }
    Ok(())
}

fn validate_part(value: &str) -> Result<()> {
    let synthetic = ArtifactName::parse(value)?;
    if synthetic.role().is_some() || synthetic.is_supervisor() {
        bail!("expected a single part");
    }
    Ok(())
}

fn parse_dependency(value: &str) -> Result<Dependency> {
    let (name, optional) = match value.strip_suffix('?') {
        Some(name) => (name, true),
        None => (value, false),
    };
    Ok(Dependency {
        name: ArtifactName::parse(name)?,
        optional,
    })
}

fn validate_dependencies(
    artifacts: &BTreeMap<ArtifactName, Artifact>,
    roles: &BTreeMap<RoleName, Role>,
) -> Result<()> {
    for (name, artifact) in artifacts {
        let mut seen = BTreeSet::new();
        for dependency in &artifact.requires {
            if !seen.insert(&dependency.name) {
                bail!(
                    "artifact `{name}` has duplicate dependency `{}`",
                    dependency.name
                );
            }
            let built_in = matches!(
                dependency.name.as_str(),
                "system-active"
                    | "system-supervisor"
                    | "system-backend"
                    | "system-plan"
                    | "_blocked"
            ) || dependency
                .name
                .as_str()
                .strip_prefix("_ready.")
                .is_some_and(|role| roles.contains_key(role));
            if !built_in && !artifacts.contains_key(&dependency.name) {
                bail!(
                    "artifact `{name}` has unknown dependency `{}`",
                    dependency.name
                );
            }
        }
    }
    Ok(())
}

fn validate_acyclic(artifacts: &BTreeMap<ArtifactName, Artifact>) -> Result<()> {
    fn visit<'a>(
        name: &'a ArtifactName,
        artifacts: &'a BTreeMap<ArtifactName, Artifact>,
        visiting: &mut BTreeSet<&'a ArtifactName>,
        visited: &mut BTreeSet<&'a ArtifactName>,
    ) -> Result<()> {
        if visited.contains(name) {
            return Ok(());
        }
        if !visiting.insert(name) {
            bail!("artifact dependency cycle contains `{name}`");
        }
        for dependency in &artifacts[name].requires {
            if artifacts.contains_key(&dependency.name) {
                visit(&dependency.name, artifacts, visiting, visited)?;
            }
        }
        visiting.remove(name);
        visited.insert(name);
        Ok(())
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for name in artifacts.keys() {
        visit(name, artifacts, &mut visiting, &mut visited)?;
    }
    Ok(())
}

pub const EXAMPLE_PLAN: &str = r#"version = 1

[backend]
command = ["opencode", "serve"]
hostname = "127.0.0.1"

[roles.researcher]
kind = "lab-worker"
permissions = []

[artifacts.query-request]
assets = ["goal.md"]

[artifacts."answer.researcher"]
requires = ["system-active", "_ready.researcher", "query-request"]
goal = "goal.md"
assets = ["answer.md"]
check = ["answer.md"]
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example() {
        let plan = Plan::parse(EXAMPLE_PLAN).unwrap();
        assert_eq!(
            plan.clone().normalize().unwrap().artifacts.len(),
            plan.artifacts.len()
        );
        let artifact = &plan.artifacts[&ArtifactName::parse("answer.researcher").unwrap()];
        assert_eq!(artifact.inputs, None);
        assert_eq!(artifact.requires.len(), 3);
        assert_eq!(
            plan.artifact_inputs(&ArtifactName::parse("answer.researcher").unwrap()),
            vec![AssetPath::parse("goal.md", true).unwrap()]
        );
    }

    #[test]
    fn rejects_cycles_and_bad_paths() {
        let cycle = r#"
version = 1
[roles.r]
kind = "lab-worker"
[artifacts."a.r"]
goal = "goal.md"
requires = ["b.r"]
[artifacts."b.r"]
goal = "goal.md"
requires = ["a.r"]
"#;
        assert!(
            Plan::parse(cycle)
                .unwrap_err()
                .to_string()
                .contains("cycle")
        );
        assert!(AssetPath::parse("../secret", true).is_err());
        assert!(AssetPath::parse("output/", false).is_err());
    }

    #[test]
    fn rejects_manually_declared_file_permissions() {
        let error = Plan::parse(
            r#"version = 1
[roles.r]
kind = "lab-worker"
permissions = ["read"]
[artifacts."a.r"]
goal = "goal.md"
"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("derived from artifact"));
    }

    #[test]
    fn deserialization_enforces_newtype_invariants_before_normalization() {
        assert!(toml::from_str::<Plan>("version = 1\n[artifacts.BAD]").is_err());
        assert!(
            toml::from_str::<Plan>("version = 1\n[roles.BAD]\nkind = 'lab-worker'\n",).is_err()
        );
        assert!(
            toml::from_str::<Plan>(
                "version = 1\n[roles.r]\nkind = 'lab-worker'\n[artifacts.'a.r']\ngoal = 'goals/'\n",
            )
            .is_err()
        );

        let decoded = toml::from_str::<Plan>(
            "version = 1\n[roles.r]\nkind = 'lab-worker'\n[artifacts.'a.r']\ngoal = 'goal.md'\nrequires = ['missing?']\n",
        )
        .unwrap();
        assert!(decoded.normalize().is_err());
    }

    #[test]
    fn requires_replaces_depends_on_without_compatibility_alias() {
        let plan = Plan::parse(
            "version = 1\n[artifacts.source]\n[artifacts.result]\nrequires = ['source']\n",
        )
        .unwrap();
        assert_eq!(
            plan.artifacts[&ArtifactName::parse("result").unwrap()]
                .requires
                .len(),
            1
        );

        let error = Plan::parse(
            "version = 1\n[artifacts.source]\n[artifacts.result]\ndepends-on = ['source']\n",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("unknown field `depends-on`"));
    }
}
