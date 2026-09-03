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
    pub roles: BTreeMap<RoleName, Role>,
    #[serde(default)]
    pub artifacts: BTreeMap<ArtifactName, Artifact>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    #[serde(default)]
    pub kind: ArtifactKind,
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
    pub bench: Option<Bench>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactKind {
    #[default]
    Task,
    Learn,
    Bench,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bench {
    pub name: BenchName,
    pub source: FilePath,
    pub qlist: FilePath,
    #[serde(default, rename = "public-knowledge")]
    pub public_knowledge: Vec<AssetPath>,
    #[serde(default)]
    pub permissions: BenchPermissions,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BenchName(String);

impl BenchName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for BenchName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        validate_part(&value).map_err(serde::de::Error::custom)?;
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BenchPermissions {
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
        if self.version != 1 {
            bail!("unsupported plan version {}; expected 1", self.version);
        }
        for (role, definition) in &self.roles {
            validate_non_file_permissions(&definition.permissions)
                .with_context(|| format!("invalid permissions for role `{role}`"))?;
        }

        let mut bench_names = BTreeMap::new();
        for (name, artifact) in &mut self.artifacts {
            if name.is_supervisor() {
                bail!("supervisor artifact `{name}` is built in and cannot be declared");
            }
            if let Some(role) = name.role() {
                if !self.roles.contains_key(role) {
                    bail!("artifact `{name}` references unknown role `{role}`");
                }
                if let Some(permissions) = &artifact.permissions {
                    validate_non_file_permissions(permissions)
                        .with_context(|| format!("invalid permissions for artifact `{name}`"))?;
                }
            } else if artifact.permissions.is_some() {
                bail!("host artifact `{name}` cannot declare `permissions`");
            }

            match artifact.kind {
                ArtifactKind::Task => {
                    if name.role().is_some() && artifact.goal.is_none() {
                        bail!("task artifact `{name}` must declare `goal`");
                    }
                    if artifact.bench.is_some() {
                        bail!("task artifact `{name}` cannot declare `bench`");
                    }
                }
                ArtifactKind::Learn => {
                    if name.role().is_none() {
                        bail!("learn artifact `{name}` must belong to a role");
                    }
                    if artifact.goal.is_none() {
                        bail!("learn artifact `{name}` must declare `goal`");
                    }
                    if !artifact.assets.is_empty() || !artifact.check.is_empty() {
                        bail!("learn artifact `{name}` cannot declare assets or check");
                    }
                    if artifact.bench.is_some() {
                        bail!("learn artifact `{name}` cannot declare `bench`");
                    }
                }
                ArtifactKind::Bench => {
                    if name.role().is_none() {
                        bail!("bench artifact `{name}` must belong to a role");
                    }
                    if artifact.goal.is_some() {
                        bail!("bench artifact `{name}` cannot declare `goal`");
                    }
                    if !artifact.assets.is_empty() || !artifact.check.is_empty() {
                        bail!("bench artifact `{name}` cannot declare assets or check");
                    }
                    let bench = artifact
                        .bench
                        .as_ref()
                        .with_context(|| format!("bench artifact `{name}` must declare `bench`"))?;
                    if bench
                        .permissions
                        .commands
                        .iter()
                        .any(|command| command.trim().is_empty())
                    {
                        bail!("bench artifact `{name}` has an empty command");
                    }
                    if let Some(previous) = bench_names.insert(bench.name.clone(), name.clone()) {
                        bail!(
                            "bench artifacts `{previous}` and `{name}` use duplicate bench name `{}`",
                            bench.name.as_str()
                        );
                    }
                }
            }
        }

        validate_dependencies(&self.artifacts, &self.roles)?;
        validate_acyclic(&self.artifacts)?;
        Ok(self)
    }

    pub fn artifact_inputs(&self, name: &ArtifactName) -> Vec<AssetPath> {
        let artifact = &self.artifacts[name];
        let mut inputs = artifact.inputs.clone().unwrap_or_else(|| {
            artifact
                .requires
                .iter()
                .filter_map(|dependency| self.artifacts.get(&dependency.name))
                .flat_map(|dependency| dependency.assets.iter().cloned())
                .collect()
        });
        if let Some(goal) = &artifact.goal {
            inputs.push(goal.0.clone());
        }
        if let Some(bench) = &artifact.bench {
            inputs.push(bench.source.0.clone());
            inputs.push(bench.qlist.0.clone());
            inputs.extend(bench.public_knowledge.iter().cloned());
        }
        inputs
            .into_iter()
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
    _roles: &BTreeMap<RoleName, Role>,
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
            );
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

[roles.researcher]
permissions = []

[artifacts.query-request]
assets = ["goal.md"]

[artifacts."answer.researcher"]
requires = ["system-active", "query-request"]
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
        assert_eq!(artifact.requires.len(), 2);
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
        assert!(toml::from_str::<Plan>("version = 1\n[roles.BAD]\n",).is_err());
        assert!(
            toml::from_str::<Plan>("version = 1\n[roles.r]\n[artifacts.'a.r']\ngoal = 'goals/'\n",)
                .is_err()
        );

        let decoded = toml::from_str::<Plan>(
            "version = 1\n[roles.r]\n[artifacts.'a.r']\ngoal = 'goal.md'\nrequires = ['missing?']\n",
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

    #[test]
    fn validates_and_normalizes_artifact_kinds() {
        let plan = Plan::parse(
            r#"version = 1
[roles.r]
[artifacts."learn-domain.r"]
kind = "learn"
goal = "learn.md"
inputs = ["knowledge/"]
[artifacts."bench-solver.r"]
kind = "bench"
[artifacts."bench-solver.r".bench]
name = "solver"
source = "questions.jsonl"
qlist = "questions.ids"
"#,
        )
        .unwrap();
        let bench = &plan.artifacts[&ArtifactName::parse("bench-solver.r").unwrap()];
        assert!(bench.check.is_empty());
        assert_eq!(
            plan.artifact_inputs(&ArtifactName::parse("learn-domain.r").unwrap()),
            vec![
                AssetPath::parse("knowledge/", true).unwrap(),
                AssetPath::parse("learn.md", true).unwrap(),
            ]
        );

        assert!(Plan::parse(
            "version = 1\n[roles.r]\n[artifacts.'bad.r']\nkind = 'learn'\ngoal = 'g.md'\nassets = ['out.md']\n"
        )
        .is_err());
        assert!(Plan::parse(
            "version = 1\n[roles.r]\n[artifacts.'bad.r']\nkind = 'bench'\nassets = ['a.db']\n[artifacts.'bad.r'.bench]\nname = 'bad'\nsource = 'q.jsonl'\nqlist = 'q.ids'\n"
        )
        .is_err());
        let duplicate = r#"version = 1
[roles.r]
[artifacts."bench-one.r"]
kind = "bench"
[artifacts."bench-one.r".bench]
name = "shared"
source = "q.jsonl"
qlist = "q.ids"
[artifacts."bench-two.r"]
kind = "bench"
[artifacts."bench-two.r".bench]
name = "shared"
source = "q.jsonl"
qlist = "q.ids"
"#;
        assert!(
            format!("{:#}", Plan::parse(duplicate).unwrap_err()).contains("duplicate bench name")
        );
    }

    #[test]
    fn rejects_removed_plan_surfaces() {
        for source in [
            "version = 1\n[backend]\nhostname = '127.0.0.1'\n",
            "version = 1\n[roles.r]\nkind = 'lab-worker'\n",
            "version = 1\n[benchmark.old]\nrecords = 'old.sqlite'\n",
        ] {
            let error = Plan::parse(source).unwrap_err();
            assert!(format!("{error:#}").contains("unknown field"));
        }
    }
}
