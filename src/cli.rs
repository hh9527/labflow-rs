use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use crate::artifact::{ARTIFACTS_DIR, ArtifactName, publish, unpublish};
use crate::config::{CONFIG_FILE, Config};
use crate::db::{read_host_tasks, read_virtual_timestamp};
use crate::plan::{EXAMPLE_PLAN, PLAN_FILE, Plan};

#[derive(Debug, Parser)]
#[command(version, about = "Artifact-driven laboratory supervisor")]
struct Cli {
    #[arg(long, global = true, default_value = ".")]
    root: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        port: u16,
    },
    Publish {
        #[arg(required = true, allow_hyphen_values = true)]
        artifacts: Vec<String>,
    },
    Status {
        artifact: Option<ArtifactName>,
    },
    Plan {
        #[command(subcommand)]
        command: PlanCommand,
    },
    Supervisor,
    Run,
    HostTasks {
        #[arg(long, value_name = "SECONDS")]
        poll: Option<u64>,
    },
}

#[derive(Debug, Subcommand)]
enum PlanCommand {
    Check,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let root = normalize_root(&cli.root)?;
    match cli.command {
        Command::Init { port } => init(&root, port),
        Command::Publish { artifacts } => publish_many(&root, &artifacts),
        Command::Status { artifact } => status(&root, artifact.as_ref()),
        Command::Plan {
            command: PlanCommand::Check,
        } => {
            let plan = Plan::load(&root)?;
            println!(
                "plan is valid: {} roles, {} artifacts",
                plan.roles.len(),
                plan.artifacts.len()
            );
            Ok(())
        }
        Command::Supervisor => crate::runtime::run(root).await,
        Command::Run => crate::runner::run(root).await,
        Command::HostTasks { poll } => host_tasks(&root, poll).await,
    }
}

fn normalize_root(root: &Path) -> Result<PathBuf> {
    if root.is_absolute() {
        Ok(root.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(root))
    }
}

fn init(root: &Path, port: u16) -> Result<()> {
    if port == 0 {
        bail!("port must be non-zero");
    }
    fs::create_dir_all(root.join(ARTIFACTS_DIR))?;
    let plan_path = root.join(PLAN_FILE);
    if plan_path.exists() {
        bail!("`{}` already exists", plan_path.display());
    }
    fs::write(&plan_path, EXAMPLE_PLAN)?;
    let goal = root.join("goal.md");
    if !goal.exists() {
        fs::write(&goal, "# 实验目标\n")?;
    }
    let config = root.join(CONFIG_FILE);
    fs::write(&config, toml::to_string(&Config { port })?)?;
    let script = root.join(".labflow/run");
    fs::write(
        &script,
        "#!/bin/sh\nset -eu\nROOT=$(CDPATH= cd -- \"$(dirname -- \"$0\")/..\" && pwd)\nexec labflow --root \"$ROOT\" run\n",
    )?;
    set_executable(&script)?;
    println!("initialized {}", root.display());
    Ok(())
}

fn publish_many(root: &Path, operations: &[String]) -> Result<()> {
    for operation in operations {
        if let Some(raw_name) = operation.strip_prefix('!') {
            if raw_name.is_empty() {
                bail!("missing artifact name after `!`");
            }
            let artifact = ArtifactName::parse(raw_name)?;
            let removed = unpublish(root, &artifact)?;
            println!(
                "{} {artifact}",
                if removed {
                    "unpublished"
                } else {
                    "not published"
                }
            );
        } else {
            let artifact = ArtifactName::parse(operation)?;
            publish(root, &artifact)?;
            println!("published {artifact}");
        }
    }
    Ok(())
}

async fn host_tasks(root: &Path, poll: Option<u64>) -> Result<()> {
    let deadline = poll.map(|seconds| tokio::time::Instant::now() + Duration::from_secs(seconds));
    loop {
        let tasks = read_host_tasks(root)?;
        if !tasks.tasks.is_empty()
            || deadline.is_none_or(|deadline| tokio::time::Instant::now() >= deadline)
        {
            println!("{}", serde_json::to_string(&tasks)?);
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}

fn status(root: &Path, filter: Option<&ArtifactName>) -> Result<()> {
    let plan = Plan::load(root)?;
    let names: Vec<_> = match filter {
        Some(name) => vec![name.clone()],
        None => plan
            .artifacts
            .keys()
            .cloned()
            .chain(
                [
                    "system-active",
                    "system-supervisor",
                    "system-backend",
                    "system-plan",
                ]
                .into_iter()
                .map(ArtifactName::parse)
                .collect::<Result<Vec<_>>>()?,
            )
            .chain(std::iter::once(ArtifactName::parse("_blocked")?))
            .chain(
                plan.roles
                    .keys()
                    .map(|role| ArtifactName::parse(&format!("_ready.{role}")))
                    .collect::<Result<Vec<_>>>()?,
            )
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
    };
    for name in names {
        if name.is_supervisor() {
            match read_virtual_timestamp(root, &name)? {
                Some(modified) => println!("{name}\tpublished\t{modified}"),
                None => println!("{name}\tnot-published"),
            }
            continue;
        }
        let path = name.path(root);
        match fs::metadata(&path) {
            Ok(metadata) => {
                let modified = metadata
                    .modified()?
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis();
                println!("{name}\tpublished\t{modified}");
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                println!("{name}\tnot-published");
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to stat `{}`", path.display()));
            }
        }
    }
    Ok(())
}
