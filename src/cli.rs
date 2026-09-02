use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use crate::artifact::{ARTIFACTS_DIR, ArtifactName, publish, unpublish};
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
    Init,
    Publish {
        artifact: ArtifactName,
    },
    Unpublish {
        artifact: ArtifactName,
    },
    Status {
        artifact: Option<ArtifactName>,
    },
    Plan {
        #[command(subcommand)]
        command: PlanCommand,
    },
    Supervisor,
}

#[derive(Debug, Subcommand)]
enum PlanCommand {
    Check,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let root = normalize_root(&cli.root)?;
    match cli.command {
        Command::Init => init(&root),
        Command::Publish { artifact } => {
            publish(&root, &artifact)?;
            println!("published {artifact}");
            Ok(())
        }
        Command::Unpublish { artifact } => {
            let removed = unpublish(&root, &artifact)?;
            println!(
                "{} {artifact}",
                if removed {
                    "unpublished"
                } else {
                    "not published"
                }
            );
            Ok(())
        }
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
        Command::Supervisor => bail!("supervisor runtime is not available yet"),
    }
}

fn normalize_root(root: &Path) -> Result<PathBuf> {
    if root.is_absolute() {
        Ok(root.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(root))
    }
}

fn init(root: &Path) -> Result<()> {
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
    let script = root.join(".labflow/run-supervisor");
    fs::write(
        &script,
        "#!/bin/sh\nwhile true; do\n  labflow supervisor --root \"$(pwd)\"\n  sleep 1\ndone\n",
    )?;
    set_executable(&script)?;
    println!("initialized {}", root.display());
    Ok(())
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
        None => plan.artifacts.keys().cloned().collect(),
    };
    for name in names {
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
