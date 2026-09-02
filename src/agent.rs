use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::artifact::ArtifactName;
use crate::plan::{AssetPath, Plan, RoleKind};

const TEMPLATE_VERSION: u32 = 1;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentProfile {
    pub id: String,
    pub content: String,
}

pub fn profile(plan: &Plan, artifact: &ArtifactName) -> AgentProfile {
    let definition = &plan.artifacts[artifact];
    let role_name = artifact.role().expect("agent profiles belong to roles");
    let role = &plan.roles[role_name];
    let permissions = definition.permissions.as_ref().unwrap_or(&role.permissions);
    let role_kind = match role.kind {
        RoleKind::LabWorker => "实验员",
        RoleKind::Evaluator => "评估员",
    };

    let mut permission = Map::new();
    permission.insert("*".into(), Value::String("deny".into()));
    for name in permissions {
        permission.insert(name.clone(), Value::String("allow".into()));
    }
    permission.insert("glob".into(), Value::String("allow".into()));
    permission.insert("grep".into(), Value::String("deny".into()));

    let mut readable = BTreeSet::new();
    if let Some(goal) = &definition.goal {
        add_path_patterns(&mut readable, goal);
    }
    for path in plan
        .artifact_inputs(artifact)
        .iter()
        .chain(&definition.assets)
    {
        add_path_patterns(&mut readable, path);
    }
    permission.insert("read".into(), path_rules(readable));

    let mut writable = BTreeSet::new();
    for path in &definition.assets {
        add_path_patterns(&mut writable, path);
    }
    permission.insert("edit".into(), path_rules(writable));

    let permission = serde_json::to_string(&permission).expect("JSON map is serializable");
    let content = format!(
        "---\ndescription: \"Labflow/v{TEMPLATE_VERSION} {role_kind} {role_name}\"\nmode: subagent\npermission: {permission}\n---\n\n你是{role_kind} {role_name}，请按照指令要求完成任务。\n"
    );
    let hash = format!("{:x}", Sha256::digest(content.as_bytes()));
    AgentProfile {
        id: format!("{role_name}.{hash}"),
        content,
    }
}

pub fn profiles(plan: &Plan) -> BTreeMap<ArtifactName, AgentProfile> {
    plan.artifacts
        .keys()
        .filter(|artifact| artifact.role().is_some())
        .map(|artifact| (artifact.clone(), profile(plan, artifact)))
        .collect()
}

fn add_path_patterns(patterns: &mut BTreeSet<String>, path: &AssetPath) {
    let value = path.as_str().trim_end_matches('/');
    patterns.insert(value.to_owned());
    if path.is_directory() {
        patterns.insert(format!("{value}/*"));
    }
}

fn path_rules(patterns: BTreeSet<String>) -> Value {
    let mut rules = Map::new();
    rules.insert("*".into(), Value::String("deny".into()));
    for pattern in patterns {
        rules.insert(pattern, Value::String("allow".into()));
    }
    Value::Object(rules)
}

pub fn materialize(root: &Path, profiles: &BTreeMap<ArtifactName, AgentProfile>) -> Result<()> {
    let directory = root.join(".opencode/agents");
    fs::create_dir_all(&directory)?;
    for profile in profiles.values() {
        materialize_one(&directory, profile)?;
    }
    Ok(())
}

fn materialize_one(directory: &Path, profile: &AgentProfile) -> Result<()> {
    let destination = directory.join(format!("{}.md", profile.id));
    if destination.exists() {
        return verify(&destination, profile);
    }

    let temporary = temporary_path(directory, &profile.id);
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(profile.content.as_bytes())?;
        file.sync_all()?;
        match fs::hard_link(&temporary, &destination) {
            Ok(()) => Ok(()),
            Err(_error) if destination.exists() => verify(&destination, profile),
            Err(error) => Err(error.into()),
        }
    })();
    let _ = fs::remove_file(&temporary);
    result.with_context(|| format!("failed to materialize agent `{}`", profile.id))
}

fn verify(path: &Path, profile: &AgentProfile) -> Result<()> {
    let existing = fs::read(path)?;
    if existing != profile.content.as_bytes() {
        bail!("immutable agent `{}` has unexpected content", profile.id);
    }
    Ok(())
}

fn temporary_path(directory: &Path, id: &str) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(".{id}.{}.{}.tmp", std::process::id(), sequence))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn artifact_permissions_replace_role_permissions_and_materialization_is_immutable() {
        let plan = Plan::parse(
            r#"version = 1
[roles.a1]
kind = "lab-worker"
permissions = ["webfetch"]
[artifacts."first.a1"]
goal = "goal.md"
inputs = ["input.md", "references/"]
assets = ["output.md", "generated/"]
[artifacts."second.a1"]
goal = "goal.md"
permissions = ["bash"]
"#,
        )
        .unwrap();
        let first = profile(&plan, &ArtifactName::parse("first.a1").unwrap());
        let second = profile(&plan, &ArtifactName::parse("second.a1").unwrap());
        assert!(first.content.contains(r#""glob":"allow""#));
        assert!(first.content.contains(r#""grep":"deny""#));
        assert!(first.content.contains(r#""webfetch":"allow""#));
        assert!(first.content.contains(
            r#""read":{"*":"deny","generated":"allow","generated/*":"allow","goal.md":"allow","input.md":"allow","output.md":"allow","references":"allow","references/*":"allow"}"#
        ));
        assert!(first.content.contains(
            r#""edit":{"*":"deny","generated":"allow","generated/*":"allow","output.md":"allow"}"#
        ));
        assert!(second.content.contains(r#""bash":"allow""#));
        assert!(!second.content.contains(r#""webfetch":"allow""#));
        assert_ne!(first.id, second.id);

        let root = tempdir().unwrap();
        let generated = profiles(&plan);
        materialize(root.path(), &generated).unwrap();
        materialize(root.path(), &generated).unwrap();
        let path = root
            .path()
            .join(".opencode/agents")
            .join(format!("{}.md", first.id));
        fs::write(&path, "corrupt").unwrap();
        assert!(materialize(root.path(), &generated).is_err());
    }
}
