use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

use crate::artifact::ArtifactName;
use crate::domain::Timestamp;
use crate::plan::{Artifact, AssetPath, Plan};

pub fn build_task_prompt(
    root: &Path,
    plan: &Plan,
    name: &ArtifactName,
    failures: &[String],
) -> Result<String> {
    let artifact = plan
        .artifacts
        .get(name)
        .with_context(|| format!("unknown artifact `{name}`"))?;
    if let Some(benchmark) = &artifact.benchmark {
        return Ok(format!(
            "任务: {name}\n\n要求:\n- 执行 `labflow bench start {benchmark}` 开始本轮评测\n- 反复执行 `labflow challenge next {benchmark}`，根据返回的 Q、K 和 reply 决定是否使用 `labflow challenge clarify {benchmark} '<澄清文本>'`\n- 每道题完成后执行 `labflow challenge archive {benchmark}`\n- next 返回 null 后执行 `labflow bench finish {benchmark}`\n- 完成后必须先严格回答“完成任务。”，然后再做其他解释\n- 确实无法完成，则必须先严格回答“无法完成任务。”，然后再做其他解释\n"
        ));
    }
    let inputs = effective_inputs(root, plan, artifact)?;
    build_task_prompt_with_inputs(root, plan, name, failures, &inputs)
}

pub fn build_task_prompt_with_inputs(
    root: &Path,
    plan: &Plan,
    name: &ArtifactName,
    failures: &[String],
    inputs: &[AssetPath],
) -> Result<String> {
    let artifact = plan
        .artifacts
        .get(name)
        .with_context(|| format!("unknown artifact `{name}`"))?;
    let goal = artifact
        .goal
        .as_ref()
        .with_context(|| format!("artifact `{name}` has no goal"))?;
    let output_time = file_timestamp(&name.path(root))?;
    let mut prompt = format!(
        "任务: {name}\n\n要求:\n- 按照 {} 的要求完成任务\n- 完成后必须先严格回答“完成任务。”，然后再做其他解释\n- 确实无法完成，则必须先严格回答“无法完成任务。”，然后再做其他解释\n\n本次任务的前序工件:\n",
        goal.as_str()
    );

    for dependency in artifact
        .requires
        .iter()
        .filter(|dependency| !dependency.name.is_supervisor())
    {
        let timestamp = file_timestamp(&dependency.name.path(root))?;
        prompt.push_str(&format!(
            "- {}: {}\n",
            dependency.name,
            status(output_time, timestamp)
        ));
    }

    prompt.push_str("\n本次任务依赖的文件:\n");
    let mut files = BTreeSet::new();
    files.insert(goal.as_str().to_owned());
    for input in inputs {
        if input.is_directory() {
            let directory = input.resolve(root);
            if directory.exists() {
                for entry in WalkDir::new(&directory).follow_links(false) {
                    let entry = entry?;
                    if entry.file_type().is_file() {
                        let relative = entry.path().strip_prefix(root)?.to_string_lossy();
                        files.insert(relative.replace('\\', "/"));
                    }
                }
            }
        } else {
            files.insert(input.as_str().to_owned());
        }
    }
    for file in files {
        let timestamp = file_timestamp(&root.join(&file))?;
        prompt.push_str(&format!("- {file}: {}\n", status(output_time, timestamp)));
    }

    if !failures.is_empty() {
        prompt.push_str("\n你上次发布任务结果不成功的原因是:\n");
        for failure in failures {
            prompt.push_str(&format!("- {failure}\n"));
        }
    }
    Ok(prompt)
}

pub fn check_task(root: &Path, artifact: &Artifact) -> Vec<String> {
    artifact
        .check
        .iter()
        .filter(|path| !path.resolve(root).is_file())
        .map(|path| path.as_str().to_owned())
        .collect()
}

fn effective_inputs(root: &Path, plan: &Plan, artifact: &Artifact) -> Result<Vec<AssetPath>> {
    if let Some(inputs) = &artifact.inputs {
        return Ok(inputs.clone());
    }
    let mut result = BTreeSet::new();
    for dependency in &artifact.requires {
        if dependency.name.is_supervisor() {
            continue;
        }
        if dependency.optional && !dependency.name.path(root).exists() {
            continue;
        }
        if let Some(source) = plan.artifacts.get(&dependency.name) {
            result.extend(source.assets.iter().cloned());
        }
    }
    Ok(result.into_iter().collect())
}

fn file_timestamp(path: &Path) -> Result<Option<Timestamp>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let duration = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .context("file mtime predates Unix epoch")?;
            let micros: i64 = duration
                .as_micros()
                .try_into()
                .context("file mtime is out of range")?;
            Ok(Some(micros))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to stat `{}`", path.display())),
    }
}

fn status(output: Option<Timestamp>, input: Option<Timestamp>) -> &'static str {
    match (output, input) {
        (_, None) => "尚不存在",
        (None, Some(_)) => "刚更新",
        (Some(output), Some(input)) if input > output => "刚更新",
        (Some(_), Some(_)) => "未改变",
    }
}

pub fn timestamp(path: &Path) -> Result<Timestamp> {
    file_timestamp(path)?.ok_or_else(|| anyhow::anyhow!("`{}` does not exist", path.display()))
}

pub fn ensure_goal_exists(root: &Path, artifact: &Artifact) -> Result<()> {
    let Some(goal) = &artifact.goal else {
        bail!("worker artifact has no goal");
    };
    if !goal.resolve(root).is_file() {
        bail!("goal file `{}` does not exist", goal.as_str());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::publish;
    use crate::plan::EXAMPLE_PLAN;

    #[test]
    fn expands_inputs_and_failure_section() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("goal.md"), "goal").unwrap();
        let plan = Plan::parse(EXAMPLE_PLAN).unwrap();
        publish(
            directory.path(),
            &ArtifactName::parse("query-request").unwrap(),
        )
        .unwrap();
        let prompt = build_task_prompt(
            directory.path(),
            &plan,
            &ArtifactName::parse("answer.researcher").unwrap(),
            &["answer.md 文件不存在".into()],
        )
        .unwrap();
        assert!(prompt.contains("- query-request: 刚更新"));
        assert!(prompt.contains("- goal.md: 刚更新"));
        assert!(prompt.contains("你上次发布任务结果不成功的原因是:"));
    }

    #[test]
    fn recursively_expands_directories_and_skips_unpublished_optional_inputs() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir_all(directory.path().join("materials/nested")).unwrap();
        fs::write(directory.path().join("goal.md"), "goal").unwrap();
        fs::write(directory.path().join("materials/nested/data.txt"), "data").unwrap();
        fs::write(directory.path().join("optional.txt"), "stale optional data").unwrap();
        let plan = Plan::parse(
            r#"
version = 1
[roles.r]
kind = "lab-worker"
[artifacts.source]
assets = ["materials/"]
[artifacts.feedback]
assets = ["optional.txt"]
[artifacts."result.r"]
requires = ["source", "feedback?"]
goal = "goal.md"
"#,
        )
        .unwrap();
        publish(directory.path(), &ArtifactName::parse("source").unwrap()).unwrap();
        let prompt = build_task_prompt(
            directory.path(),
            &plan,
            &ArtifactName::parse("result.r").unwrap(),
            &[],
        )
        .unwrap();
        assert!(prompt.contains("- materials/nested/data.txt: 刚更新"));
        assert!(!prompt.contains("optional.txt"));
    }
}
