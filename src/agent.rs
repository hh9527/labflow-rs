use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::artifact::ArtifactName;
use crate::plan::{Plan, RoleKind};

const TEMPLATE_VERSION: u32 = 1;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentProfile {
    pub id: String,
    pub content: String,
}

pub fn profiles(plan: &Plan) -> BTreeMap<ArtifactName, AgentProfile> {
    plan.artifacts
        .iter()
        .filter_map(|(artifact, definition)| {
            let role_name = artifact.role()?;
            let role = &plan.roles[role_name];
            let permissions = definition.permissions.as_ref().unwrap_or(&role.permissions);
            let role_kind = match role.kind {
                RoleKind::LabWorker => "实验员",
                RoleKind::Evaluator => "评估员",
            };
            let mut permission = serde_json::Map::new();
            permission.insert("*".into(), serde_json::Value::String("deny".into()));
            for name in permissions {
                permission.insert(name.clone(), serde_json::Value::String("allow".into()));
            }
            let permission = serde_json::to_string(&permission).expect("JSON map is serializable");
            let content = format!(
                "---\ndescription: \"Labflow/v{TEMPLATE_VERSION} {role_kind} {role_name}\"\nmode: subagent\npermission: {permission}\n---\n\n你是{role_kind} {role_name}，请按照指令要求完成任务。\n"
            );
            let hash = format!("{:x}", Sha256::digest(content.as_bytes()));
            Some((
                artifact.clone(),
                AgentProfile {
                    id: format!("{role_name}.{hash}"),
                    content,
                },
            ))
        })
        .collect()
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
permissions = ["read"]
[artifacts."first.a1"]
goal = "goal.md"
[artifacts."second.a1"]
goal = "goal.md"
permissions = ["edit"]
"#,
        )
        .unwrap();
        let generated = profiles(&plan);
        let first = &generated[&ArtifactName::parse("first.a1").unwrap()];
        let second = &generated[&ArtifactName::parse("second.a1").unwrap()];
        assert!(
            first
                .content
                .contains(r#"permission: {"*":"deny","read":"allow"}"#)
        );
        assert!(
            second
                .content
                .contains(r#"permission: {"*":"deny","edit":"allow"}"#)
        );
        assert_ne!(first.id, second.id);

        let root = tempdir().unwrap();
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
