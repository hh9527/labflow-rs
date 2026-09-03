use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::artifact::ArtifactName;
use crate::plan::{ArtifactKind, AssetPath, Bench, Plan};

const TEMPLATE_VERSION: u32 = 1;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentProfile {
    pub id: String,
    pub content: String,
}

pub const OPENCODE_ENV_DIR: &str = ".labflow/oc-env";
pub const PLAN_ENV_ARTIFACT: &str = "_plan-env";
pub const CURRENT_ENV_ARTIFACT: &str = "_current-env";

pub fn profile(plan: &Plan, artifact: &ArtifactName) -> AgentProfile {
    let definition = &plan.artifacts[artifact];
    let role_name = artifact.role().expect("agent profiles belong to roles");
    let role = &plan.roles[role_name];
    let permissions = definition.permissions.as_ref().unwrap_or(&role.permissions);

    let mut permission = Map::new();
    permission.insert("*".into(), Value::String("deny".into()));
    for name in permissions {
        permission.insert(name.clone(), Value::String("allow".into()));
    }
    let commands = if definition.kind == ArtifactKind::Bench {
        bench_commands(artifact)
            .into_iter()
            .chain(definition.commands.iter().cloned())
            .collect()
    } else {
        definition.commands.clone()
    };
    if !commands.is_empty() {
        permission.insert("bash".into(), command_rules(commands));
    }
    permission.insert("glob".into(), Value::String("allow".into()));
    permission.insert("grep".into(), Value::String("deny".into()));

    let mut readable = BTreeSet::new();
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
        "---\ndescription: \"Labflow/v{TEMPLATE_VERSION} 实验员 {role_name}\"\nmode: subagent\npermission: {permission}\n---\n\n你是实验员 {role_name}，请按照指令要求完成任务。\n"
    );
    let hash = format!("{:x}", Sha256::digest(content.as_bytes()));
    AgentProfile {
        id: format!("{role_name}.{hash}"),
        content,
    }
}

fn bench_commands(artifact: &ArtifactName) -> Vec<String> {
    let executable = ".labflow/bin/labflow";
    vec![
        format!("{executable} bench start {artifact}"),
        format!("{executable} bench finish {artifact}"),
        format!("{executable} challenge next {artifact}"),
        format!("{executable} challenge clarify {artifact} *"),
        format!("{executable} challenge archive {artifact}"),
    ]
}

fn command_rules(commands: impl IntoIterator<Item = String>) -> Value {
    let mut rules = Map::new();
    rules.insert("*".into(), Value::String("deny".into()));
    for command in commands {
        rules.insert(command, Value::String("allow".into()));
    }
    Value::Object(rules)
}

pub fn respondent_profile(artifact: &ArtifactName, bench: &Bench) -> AgentProfile {
    let mut permission = Map::new();
    permission.insert("*".into(), Value::String("deny".into()));
    permission.insert("glob".into(), Value::String("allow".into()));
    permission.insert("grep".into(), Value::String("deny".into()));

    let mut readable = BTreeSet::new();
    for path in bench
        .public_knowledge
        .iter()
        .chain(&bench.permissions.read)
        .chain(&bench.permissions.write)
    {
        add_path_patterns(&mut readable, path);
    }
    permission.insert("read".into(), path_rules(readable));

    let mut writable = BTreeSet::new();
    for path in &bench.permissions.write {
        add_path_patterns(&mut writable, path);
    }
    permission.insert("edit".into(), path_rules(writable));
    let mut bash = Map::new();
    bash.insert("*".into(), Value::String("deny".into()));
    for command in &bench.permissions.commands {
        bash.insert(command.clone(), Value::String("allow".into()));
    }
    permission.insert("bash".into(), Value::Object(bash));

    let permission = serde_json::to_string(&permission).expect("JSON map is serializable");
    let content = format!(
        "---\ndescription: \"Labflow/v{TEMPLATE_VERSION} benchmark respondent {artifact}\"\nmode: primary\npermission: {permission}\n---\n\n你是评测中的被测 Agent。请只依据公开背景、对话中收到的问题和允许使用的工具完成解题。\n"
    );
    let hash = format!("{:x}", Sha256::digest(content.as_bytes()));
    AgentProfile {
        id: format!("{}.{}", artifact.as_str().replace('.', "-"), hash),
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

pub fn environment_profiles(plan: &Plan) -> Vec<AgentProfile> {
    let mut profiles: Vec<_> = profiles(plan).into_values().collect();
    profiles.extend(plan.artifacts.iter().filter_map(|(name, artifact)| {
        (artifact.kind == ArtifactKind::Bench)
            .then(|| respondent_profile(name, artifact.bench.as_ref().expect("normalized bench")))
    }));
    profiles.sort_by(|left, right| left.id.cmp(&right.id));
    profiles
}

pub fn materialize_environment(root: &Path, plan: &Plan) -> Result<String> {
    let profiles = environment_profiles(plan);
    let mut digest = Sha256::new();
    for profile in &profiles {
        digest.update(profile.id.as_bytes());
        digest.update([0]);
        digest.update(profile.content.as_bytes());
        digest.update([0]);
    }
    let revision = format!("{:x}", digest.finalize());
    let environment = root.join(OPENCODE_ENV_DIR).join(&revision);
    let agents = environment.join("agents");
    fs::create_dir_all(&agents)?;
    for profile in &profiles {
        materialize_one(&agents, profile)?;
    }
    write_revision(
        &ArtifactName::parse(PLAN_ENV_ARTIFACT)?.path(root),
        &revision,
    )?;
    Ok(revision)
}

pub fn verify_current_profile(root: &Path, profile: &AgentProfile) -> Result<()> {
    let plan = read_revision(&ArtifactName::parse(PLAN_ENV_ARTIFACT)?.path(root))?;
    let current = read_revision(&ArtifactName::parse(CURRENT_ENV_ARTIFACT)?.path(root))?;
    if plan.is_none() || plan != current {
        bail!("OpenCode environment is not current");
    }
    verify(
        &root
            .join(".labflow/opencode/agents")
            .join(format!("{}.md", profile.id)),
        profile,
    )
}

pub fn read_revision(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_revision(path: &Path, revision: &str) -> Result<()> {
    fs::create_dir_all(path.parent().expect("revision path has parent"))?;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    fs::write(&temporary, format!("{revision}\n"))?;
    fs::rename(&temporary, path)?;
    Ok(())
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
    let directory = root.join(".labflow/opencode/agents");
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
permissions = ["webfetch"]
[artifacts."first.a1"]
goal = "goal.md"
inputs = ["input.md", "references/"]
assets = ["output.md", "generated/"]
[artifacts."second.a1"]
goal = "goal.md"
permissions = []
commands = ["./bin/check second"]
"#,
        )
        .unwrap();
        let first = profile(&plan, &ArtifactName::parse("first.a1").unwrap());
        let second = profile(&plan, &ArtifactName::parse("second.a1").unwrap());
        assert!(first.content.contains(r#""glob":"allow""#));
        assert!(first.content.contains(r#""grep":"deny""#));
        assert!(first.content.contains(r#""webfetch":"allow""#));
        assert!(first.content.contains(r#""goal.md":"allow""#));
        assert!(first.content.contains(
            r#""edit":{"*":"deny","generated":"allow","generated/*":"allow","output.md":"allow"}"#
        ));
        assert!(
            second
                .content
                .contains(r#""bash":{"*":"deny","./bin/check second":"allow"}"#)
        );
        assert!(!second.content.contains(r#""webfetch":"allow""#));
        assert_ne!(first.id, second.id);

        let root = tempdir().unwrap();
        let generated = profiles(&plan);
        materialize(root.path(), &generated).unwrap();
        materialize(root.path(), &generated).unwrap();
        let path = root
            .path()
            .join(".labflow/opencode/agents")
            .join(format!("{}.md", first.id));
        fs::write(&path, "corrupt").unwrap();
        assert!(materialize(root.path(), &generated).is_err());
    }

    #[test]
    fn bench_only_receives_its_protocol_commands() {
        let plan = Plan::parse(
            r#"version = 1
[roles.evaluator]
permissions = []
[artifacts."score.evaluator"]
kind = "bench"
requires = ["system-active"]
inputs = ["questions.jsonl"]
[artifacts."score.evaluator".bench]
name = "score"
source = "questions.jsonl"
"#,
        )
        .unwrap();
        let profile = profile(&plan, &ArtifactName::parse("score.evaluator").unwrap());
        assert!(!profile.content.contains(r#""bash":"allow""#));
        assert!(profile.content.contains(r#""bash":{"*":"deny""#));
        for command in [
            ".labflow/bin/labflow bench start score.evaluator",
            ".labflow/bin/labflow bench finish score.evaluator",
            ".labflow/bin/labflow challenge next score.evaluator",
            ".labflow/bin/labflow challenge clarify score.evaluator *",
            ".labflow/bin/labflow challenge archive score.evaluator",
        ] {
            assert!(profile.content.contains(&format!(r#""{command}":"allow""#)));
        }

        let root = tempdir().unwrap();
        let revision = materialize_environment(root.path(), &plan).unwrap();
        let agents = root
            .path()
            .join(OPENCODE_ENV_DIR)
            .join(revision)
            .join("agents");
        let names: Vec<_> = fs::read_dir(agents)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.iter().any(|name| name.starts_with("evaluator.")));
        assert!(
            names
                .iter()
                .any(|name| name.starts_with("score-evaluator."))
        );
    }
}
