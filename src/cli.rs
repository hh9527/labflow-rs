use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand};

use crate::artifact::{ARTIFACTS_DIR, ArtifactName, publish, unpublish};
use crate::benchmark;
use crate::config::{CONFIG_FILE, Config};
use crate::db::{read_host_tasks, read_virtual_timestamp};
use crate::plan::{ArtifactKind, BenchName, EXAMPLE_PLAN, PLAN_FILE, Plan};

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
    Config {
        #[arg(long)]
        port: Option<u16>,
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
    Supervisor {
        #[arg(long, hide = true)]
        generation: Option<i64>,
    },
    HostTasks {
        #[arg(long, value_name = "SECONDS")]
        poll: Option<u64>,
    },
    Query(QueryArgs),
    QueryBench(QueryBenchArgs),
    Bench {
        #[command(subcommand)]
        command: BenchCommand,
    },
    Challenge {
        #[command(subcommand)]
        command: ChallengeCommand,
    },
}

#[derive(Debug, Args)]
struct QueryBenchArgs {
    name: BenchName,
    #[command(flatten)]
    query: QueryArgs,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("source")
        .required(true)
        .args(["execute", "file"])
))]
struct QueryArgs {
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,
    #[arg(short = 'f', long = "file", value_name = "SQL_FILE")]
    file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum BenchCommand {
    Start { name: String },
    Finish { name: String },
}

#[derive(Debug, Subcommand)]
enum ChallengeCommand {
    Next { name: String },
    PollReply { name: String },
    Clarify { name: String, text: String },
    Archive { name: String },
}

#[derive(Debug, Subcommand)]
enum PlanCommand {
    Check,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let root = normalize_root(&cli.root)?;
    match cli.command {
        Command::Config { port } => configure(&root, port),
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
        Command::Supervisor { generation } => {
            let generation = match generation {
                Some(generation) => Some(generation),
                None => crate::runtime::artifact_timestamp(
                    &ArtifactName::parse("system-supervisor")?.path(&root),
                )?,
            };
            crate::runtime::run(root, generation).await
        }
        Command::HostTasks { poll } => host_tasks(&root, poll).await,
        Command::Query(arguments) => {
            let sql = read_query(&root, arguments)?;
            let output = crate::query::query_system(&root, &sql)?;
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
        Command::QueryBench(arguments) => {
            let sql = read_query(&root, arguments.query)?;
            let output = benchmark::query(&root, &arguments.name, &sql)?;
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
        Command::Bench { command } => match command {
            BenchCommand::Start { name } => {
                benchmark::run(root, name, benchmark::Command::Start).await
            }
            BenchCommand::Finish { name } => {
                benchmark::run(root, name, benchmark::Command::Finish).await
            }
        },
        Command::Challenge { command } => match command {
            ChallengeCommand::Next { name } => {
                benchmark::run(root, name, benchmark::Command::Next).await
            }
            ChallengeCommand::PollReply { name } => {
                benchmark::run(root, name, benchmark::Command::PollReply).await
            }
            ChallengeCommand::Clarify { name, text } => {
                benchmark::run(root, name, benchmark::Command::Clarify(text)).await
            }
            ChallengeCommand::Archive { name } => {
                benchmark::run(root, name, benchmark::Command::Archive).await
            }
        },
    }
}

fn read_query(root: &Path, arguments: QueryArgs) -> Result<String> {
    match (arguments.execute, arguments.file) {
        (Some(sql), None) => Ok(sql),
        (None, Some(path)) if path == Path::new("-") => {
            let mut sql = String::new();
            std::io::stdin()
                .read_to_string(&mut sql)
                .context("failed to read SQL from stdin")?;
            Ok(sql)
        }
        (None, Some(path)) => {
            let path = if path.is_absolute() {
                path
            } else {
                root.join(path)
            };
            fs::read_to_string(&path)
                .with_context(|| format!("failed to read `{}`", path.display()))
        }
        _ => unreachable!("clap requires exactly one query source"),
    }
}

fn normalize_root(root: &Path) -> Result<PathBuf> {
    if root.is_absolute() {
        Ok(root.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(root))
    }
}

const DEFAULT_PORT: u16 = 4096;

fn configure(root: &Path, port: Option<u16>) -> Result<()> {
    let config_exists = root.join(CONFIG_FILE).exists();
    let (port, write_config) = match port {
        Some(port) => (port, true),
        None if config_exists => (Config::load(root)?.port, false),
        None => (DEFAULT_PORT, true),
    };
    if port == 0 {
        bail!("port must be non-zero");
    }
    fs::create_dir_all(root.join(ARTIFACTS_DIR))?;
    fs::create_dir_all(root.join(".labflow/benchmarks"))?;
    fs::create_dir_all(root.join(".labflow/locks"))?;
    fs::create_dir_all(root.join(".labflow/oc-env"))?;
    fs::create_dir_all(root.join(".labflow/bin"))?;
    let plan_path = root.join(PLAN_FILE);
    if !plan_path.exists() {
        fs::write(&plan_path, EXAMPLE_PLAN)?;
    }
    let goal = root.join("goal.md");
    if !goal.exists() {
        fs::write(&goal, "# 实验目标\n")?;
    }
    let config = root.join(CONFIG_FILE);
    if write_config {
        fs::write(&config, toml::to_string(&Config { port })?)?;
    }
    let script = root.join(".labflow/bin/run");
    fs::write(&script, RUN_SCRIPT)?;
    set_executable(&script)?;
    set_labflow_link(&root.join(".labflow/bin/labflow"))?;
    println!("configured {}", root.display());
    Ok(())
}

#[cfg(unix)]
fn set_labflow_link(path: &Path) -> Result<()> {
    use std::os::unix::fs::symlink;

    let executable = fs::canonicalize(std::env::current_exe()?)?;
    if fs::symlink_metadata(path).is_ok() {
        fs::remove_file(path)?;
    }
    symlink(executable, path)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_labflow_link(_path: &Path) -> Result<()> {
    Ok(())
}

const RUN_SCRIPT: &str = r#"#!/usr/bin/env bash
set -uo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS="$ROOT/.labflow/artifacts"
CONFIG="$ROOT/.labflow/config"
POLL_INTERVAL="${LABFLOW_POLL_INTERVAL:-0.1}"
OPENCODE="${OPENCODE:-opencode}"
LABFLOW_LINK="$ROOT/.labflow/bin/labflow"

if [[ ! -x "$LABFLOW_LINK" ]]; then
    echo "labflow executable is missing: $LABFLOW_LINK; run labflow config" >&2
    exit 1
fi

mkdir -p -- "$ARTIFACTS"
cd -- "$ROOT"

read_port() {
    local line=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*port[[:space:]]*=[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then
            printf '%s\n' "${BASH_REMATCH[1]}"
            return 0
        fi
    done < "$CONFIG"
    return 1
}

PORT="$(read_port)" || {
    echo "cannot read port from $CONFIG" >&2
    exit 1
}

mtime() {
    stat -Lc '%y' -- "$1" 2>/dev/null
}

supervisor_loop() {
    local control="$ARTIFACTS/system-supervisor"
    local generation="" previous="" pid="" failures=0 status=0
    trap '[[ -z "$pid" ]] || kill -TERM "$pid" 2>/dev/null || true; exit 0' TERM
    while :; do
        if [[ ! -e "$control" ]]; then
            previous=""
            failures=0
            sleep "$POLL_INTERVAL"
            continue
        fi
        generation="$(mtime "$control")" || continue
        if [[ "$generation" != "$previous" ]]; then
            previous="$generation"
            failures=0
        fi
        "$LABFLOW_LINK" --root "$ROOT" supervisor &
        pid=$!
        wait "$pid"
        status=$?
        pid=""
        if [[ -e "$control" ]] && [[ "$(mtime "$control")" == "$generation" ]]; then
            failures=$((failures + 1))
            if (( failures >= 3 )); then
                echo "supervisor exited three times for one generation; disabling system-supervisor" >&2
                rm -f -- "$control"
            else
                echo "supervisor exited with $status; restarting" >&2
            fi
        fi
    done
}

backend_loop() {
    local control="$ARTIFACTS/system-backend"
    local plan_env="$ARTIFACTS/_plan-env"
    local current_env="$ARTIFACTS/_current-env"
    local config_dir="$ROOT/.labflow/opencode"
    local generation="" previous="" current="" revision="" source="" staging=""
    local pid="" failures=0 status=0
    trap 'rm -f -- "$current_env"; [[ -z "$pid" ]] || kill -TERM "$pid" 2>/dev/null || true; exit 0' TERM
    while :; do
        if [[ ! -e "$control" ]] || [[ ! -s "$plan_env" ]]; then
            rm -f -- "$current_env"
            previous=""
            failures=0
            sleep "$POLL_INTERVAL"
            continue
        fi
        generation="$(mtime "$control")" || continue
        if [[ "$generation" != "$previous" ]]; then
            previous="$generation"
            failures=0
        fi
        IFS= read -r revision < "$plan_env" || revision=""
        if [[ ! "$revision" =~ ^[0-9a-f]{64}$ ]]; then
            echo "invalid plan environment revision: $revision" >&2
            sleep "$POLL_INTERVAL"
            continue
        fi
        source="$ROOT/.labflow/oc-env/$revision"
        if [[ ! -d "$source/agents" ]]; then
            echo "plan environment does not exist: $source" >&2
            sleep "$POLL_INTERVAL"
            continue
        fi
        staging="$ROOT/.labflow/.opencode.$$.tmp"
        rm -rf -- "$staging"
        cp -a -- "$source" "$staging"
        rm -rf -- "$config_dir"
        mv -- "$staging" "$config_dir"
        printf '%s\n' "$revision" > "$current_env.tmp"
        mv -- "$current_env.tmp" "$current_env"
        OPENCODE_CONFIG_DIR="$config_dir" \
        OPENCODE_DISABLE_PROJECT_CONFIG=1 \
        "$OPENCODE" serve --hostname 127.0.0.1 --port "$PORT" &
        pid=$!
        while kill -0 "$pid" 2>/dev/null; do
            if [[ ! -e "$control" ]]; then
                kill -TERM "$pid" 2>/dev/null || true
                break
            fi
            current="$(mtime "$control")" || current=""
            if [[ "$current" != "$generation" ]]; then
                kill -TERM "$pid" 2>/dev/null || true
                break
            fi
            sleep "$POLL_INTERVAL"
        done
        wait "$pid"
        status=$?
        pid=""
        rm -f -- "$current_env"
        if [[ -e "$control" ]] && [[ "$(mtime "$control")" == "$generation" ]]; then
            failures=$((failures + 1))
            if (( failures >= 3 )); then
                echo "backend exited three times for one generation; disabling system-backend" >&2
                rm -f -- "$control"
            else
                echo "backend exited with $status; restarting" >&2
            fi
        fi
    done
}

supervisor_loop_pid=""
backend_loop_pid=""
shutdown() {
    trap - TERM
    rm -f -- "$ARTIFACTS/system-supervisor" "$ARTIFACTS/system-backend"
    [[ -z "$supervisor_loop_pid" ]] || kill -TERM "$supervisor_loop_pid" 2>/dev/null || true
    [[ -z "$backend_loop_pid" ]] || kill -TERM "$backend_loop_pid" 2>/dev/null || true
}
trap shutdown TERM

supervisor_loop &
supervisor_loop_pid=$!
backend_loop &
backend_loop_pid=$!
wait "$supervisor_loop_pid" "$backend_loop_pid"
exit 0
"#;

fn publish_many(root: &Path, operations: &[String]) -> Result<()> {
    let plan = Plan::load(root).ok();
    for operation in operations {
        if let Some(raw_name) = operation.strip_prefix('!') {
            if raw_name.is_empty() {
                bail!("missing artifact name after `!`");
            }
            let artifact = ArtifactName::parse(raw_name)?;
            reject_learn_publish(plan.as_ref(), &artifact)?;
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
            reject_learn_publish(plan.as_ref(), &artifact)?;
            publish(root, &artifact)?;
            println!("published {artifact}");
        }
    }
    Ok(())
}

fn reject_learn_publish(plan: Option<&Plan>, artifact: &ArtifactName) -> Result<()> {
    if plan
        .and_then(|plan| plan.artifacts.get(artifact))
        .is_some_and(|definition| definition.kind == ArtifactKind::Learn)
    {
        bail!("learn artifact `{artifact}` is controlled by supervisor");
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
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
    };
    for name in names {
        if name.is_supervisor()
            || plan
                .artifacts
                .get(&name)
                .is_some_and(|artifact| artifact.kind == ArtifactKind::Learn)
        {
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
