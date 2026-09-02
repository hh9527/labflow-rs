use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::artifact::ArtifactName;

pub const PLAN_FILE: &str = "lab-plan.toml";

#[derive(Clone, Debug)]
pub struct Plan {
    pub version: u32,
    pub backend: Backend,
    pub roles: BTreeMap<String, Role>,
    pub artifacts: BTreeMap<ArtifactName, Artifact>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Backend {
    pub command: Vec<String>,
    pub hostname: String,
    pub port: u16,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            command: vec!["opencode".into(), "serve".into()],
            hostname: "127.0.0.1".into(),
            port: 4096,
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

#[derive(Clone, Debug)]
pub struct Artifact {
    pub dependencies: Vec<Dependency>,
    pub goal: Option<AssetPath>,
    pub assets: Vec<AssetPath>,
    pub inputs: Option<Vec<AssetPath>>,
    pub check: Vec<AssetPath>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dependency {
    pub name: ArtifactName,
    pub optional: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AssetPath {
    value: String,
    directory: bool,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlan {
    version: u32,
    #[serde(default)]
    backend: Backend,
    #[serde(default)]
    roles: BTreeMap<String, Role>,
    #[serde(default)]
    artifacts: BTreeMap<String, RawArtifact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArtifact {
    #[serde(default, rename = "depends-on")]
    dependencies: Vec<String>,
    goal: Option<String>,
    #[serde(default)]
    assets: Vec<String>,
    inputs: Option<Vec<String>>,
    #[serde(default)]
    check: Vec<String>,
}

impl Plan {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(PLAN_FILE);
        let source = fs::read_to_string(&path)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        Self::parse(&source).with_context(|| format!("invalid `{}`", path.display()))
    }

    pub fn parse(source: &str) -> Result<Self> {
        let raw: RawPlan = toml::from_str(source).context("invalid TOML")?;
        if raw.version != 1 {
            bail!("unsupported plan version {}; expected 1", raw.version);
        }

        for role in raw.roles.keys() {
            validate_part(role).with_context(|| format!("invalid role `{role}`"))?;
        }

        let mut artifacts = BTreeMap::new();
        for (raw_name, raw_artifact) in raw.artifacts {
            let name = ArtifactName::parse(&raw_name)?;
            if name.is_supervisor() {
                bail!("supervisor artifact `{name}` is built in and cannot be declared");
            }
            let dependencies = raw_artifact
                .dependencies
                .iter()
                .map(|value| parse_dependency(value))
                .collect::<Result<Vec<_>>>()?;
            let goal = raw_artifact
                .goal
                .as_deref()
                .map(|value| AssetPath::parse(value, false))
                .transpose()?;
            let assets = parse_paths(&raw_artifact.assets, true)?;
            let inputs = raw_artifact
                .inputs
                .as_ref()
                .map(|values| parse_paths(values, true))
                .transpose()?;
            let check = parse_paths(&raw_artifact.check, false)?;

            if let Some(role) = name.role() {
                if !raw.roles.contains_key(role) {
                    bail!("artifact `{name}` references unknown role `{role}`");
                }
                if goal.is_none() {
                    bail!("worker artifact `{name}` must declare `goal`");
                }
            }
            artifacts.insert(
                name,
                Artifact {
                    dependencies,
                    goal,
                    assets,
                    inputs,
                    check,
                },
            );
        }

        validate_dependencies(&artifacts, &raw.roles)?;
        validate_acyclic(&artifacts)?;
        Ok(Self {
            version: raw.version,
            backend: raw.backend,
            roles: raw.roles,
            artifacts,
        })
    }
}

fn parse_paths(values: &[String], allow_directory: bool) -> Result<Vec<AssetPath>> {
    values
        .iter()
        .map(|value| AssetPath::parse(value, allow_directory))
        .collect()
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
    roles: &BTreeMap<String, Role>,
) -> Result<()> {
    for (name, artifact) in artifacts {
        let mut seen = BTreeSet::new();
        for dependency in &artifact.dependencies {
            if !seen.insert(&dependency.name) {
                bail!(
                    "artifact `{name}` has duplicate dependency `{}`",
                    dependency.name
                );
            }
            let built_in = matches!(
                dependency.name.as_str(),
                "system-active" | "system-supervisor" | "system-backend" | "_blocked"
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
        for dependency in &artifacts[name].dependencies {
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
port = 4096

[roles.researcher]
kind = "lab-worker"
permissions = []

[artifacts.query-request]
assets = ["goal.md"]

[artifacts."answer.researcher"]
depends-on = ["system-active", "_ready.researcher", "query-request"]
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
        let artifact = &plan.artifacts[&ArtifactName::parse("answer.researcher").unwrap()];
        assert_eq!(artifact.inputs, None);
        assert_eq!(artifact.dependencies.len(), 3);
    }

    #[test]
    fn rejects_cycles_and_bad_paths() {
        let cycle = r#"
version = 1
[roles.r]
kind = "lab-worker"
[artifacts."a.r"]
goal = "goal.md"
depends-on = ["b.r"]
[artifacts."b.r"]
goal = "goal.md"
depends-on = ["a.r"]
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
}
