use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::cargo::cargo_bin;
use labflow::artifact::ArtifactName;
use labflow::config::Config;
use labflow::db::{Databases, HostTasks, read_host_tasks};
use rusqlite::Connection;

fn labflow() -> Command {
    Command::new(cargo_bin("labflow"))
}

#[test]
fn host_can_initialize_publish_and_unpublish() {
    let directory = tempfile::tempdir().unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "init",
            "--port",
            "4096",
        ])
        .status()
        .unwrap()
        .success()
        .then_some(())
        .unwrap();
    assert!(directory.path().join("lab-plan.toml").is_file());
    assert_eq!(Config::load(directory.path()).unwrap().port, 4096);
    let run = directory.path().join(".labflow/run");
    assert!(run.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(fs::metadata(run).unwrap().permissions().mode() & 0o111, 0);
    }
    assert!(
        labflow()
            .args([
                "--root",
                directory.path().to_str().unwrap(),
                "plan",
                "check",
            ])
            .status()
            .unwrap()
            .success()
    );

    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "answer.researcher",
            "query-request",
            "!query-request",
        ])
        .status()
        .unwrap()
        .success()
        .then_some(())
        .unwrap();
    assert!(
        directory
            .path()
            .join(".labflow/artifacts/answer.researcher")
            .is_file()
    );

    assert!(
        !labflow()
            .args([
                "--root",
                directory.path().to_str().unwrap(),
                "publish",
                "_blocked",
            ])
            .status()
            .unwrap()
            .success()
    );
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "!answer.researcher",
        ])
        .status()
        .unwrap();
    assert!(
        !directory
            .path()
            .join(".labflow/artifacts/answer.researcher")
            .exists()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn supervisor_runs_task_and_exits_on_control_artifact() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join(".labflow/artifacts")).unwrap();
    let port = available_port();
    fs::write(
        directory.path().join(".labflow/config"),
        format!("port = {port}\n"),
    )
    .unwrap();
    fs::write(directory.path().join("goal.md"), "write answer.md").unwrap();
    let fake =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_opencode.py");
    let plan = format!(
        r#"version = 1
[backend]
command = ["python3", "{}"]
hostname = "127.0.0.1"
[roles.researcher]
kind = "lab-worker"
permissions = []
[artifacts.query-request]
assets = ["goal.md"]
[artifacts."answer.researcher"]
depends-on = ["query-request"]
goal = "goal.md"
assets = ["answer.md"]
check = ["answer.md"]
"#,
        fake.display()
    );
    fs::write(directory.path().join("lab-plan.toml"), plan).unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-backend",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    let supervisor_output = directory.path().join("supervisor-output.log");
    let mut supervisor = labflow()
        .args(["--root", directory.path().to_str().unwrap(), "supervisor"])
        .stdout(fs::File::create(&supervisor_output).unwrap())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();

    wait_until(Duration::from_secs(10), || {
        let database = directory.path().join(".labflow/states.sqlite");
        if !database.is_file() {
            return false;
        }
        Connection::open(database)
            .and_then(|connection| {
                connection.query_row(
                    "SELECT count(*) FROM artifacts WHERE name = 'system-supervisor'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .is_ok_and(|count| count == 1)
    });
    wait_until(Duration::from_secs(10), || {
        fs::read_to_string(directory.path().join("backend-starts"))
            .is_ok_and(|content| !content.is_empty())
    });
    let backend_starts = fs::read_to_string(directory.path().join("backend-starts"))
        .unwrap()
        .lines()
        .count();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-backend",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        fs::read_to_string(directory.path().join("backend-starts"))
            .is_ok_and(|content| content.lines().count() > backend_starts)
    });
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "query-request",
        ])
        .status()
        .unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-active",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(15), || {
        directory.path().join("answer.md").is_file()
            && directory
                .path()
                .join(".labflow/artifacts/answer.researcher")
                .is_file()
    });
    let agents = fs::read_dir(directory.path().join(".opencode/agents"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(agents.len(), 1);
    let agent_name = agents[0].file_name().into_string().unwrap();
    assert!(agent_name.starts_with("researcher."));
    assert!(agent_name.ends_with(".md"));
    let agent = fs::read_to_string(agents[0].path()).unwrap();
    assert!(agent.contains(r#""glob":"allow""#));
    assert!(agent.contains(r#""grep":"deny""#));
    assert!(agent.contains(r#""read":{"*":"deny","answer.md":"allow","goal.md":"allow"}"#));
    assert!(agent.contains(r#""edit":{"*":"deny","answer.md":"allow"}"#));
    wait_until(Duration::from_secs(10), || {
        Connection::open(directory.path().join(".labflow/timeline.sqlite"))
            .and_then(|connection| {
                connection.query_row("SELECT count(*) FROM artifact_turn", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .is_ok_and(|records| records > 0)
    });
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "!system-backend",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        child_pids(supervisor.id()).is_empty()
    });

    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    let start = Instant::now();
    loop {
        if supervisor.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "supervisor did not exit"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let timeline = Connection::open(directory.path().join(".labflow/timeline.sqlite")).unwrap();
    let records: i64 = timeline
        .query_row("SELECT count(*) FROM artifact_turn", [], |row| row.get(0))
        .unwrap();
    assert!(records > 0);
    let turn: (String, i64, String, String, String) = timeline
        .query_row(
            "SELECT artifact, iteration, result, reply_prefix, upstream_turn_id FROM artifact_turn",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        turn,
        (
            "answer.researcher".into(),
            1,
            "succeeded".into(),
            "完成任务。".into(),
            "msg_fake".into(),
        )
    );
    let actions = timeline
        .prepare("SELECT kind, subject, result FROM turn_action ORDER BY action_index")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        actions,
        vec![
            ("reasoning".into(), None, "succeeded".into()),
            ("read".into(), Some("goal.md".into()), "succeeded".into()),
            ("text".into(), None, "succeeded".into()),
        ]
    );
    let titles = fs::read_to_string(directory.path().join("session-titles.jsonl")).unwrap();
    let titles = titles
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["title"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert!(titles.contains(&"[researcher] 刷新 answer.researcher".into()));
    assert!(titles.contains(&"[researcher] 等待任务".into()));
    let output = fs::read_to_string(supervisor_output).unwrap();
    assert!(output.contains("answer.researcher 已经启动刷新"));
    assert!(output.contains("answer.researcher 完成刷新（耗时 "));
    assert!(output.contains("，最长思考 "));
}

#[test]
fn host_tasks_poll_waits_for_required_decision() {
    let directory = tempfile::tempdir().unwrap();
    let databases = Databases::initialize(directory.path()).unwrap();
    let mut poll = labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "host-tasks",
            "--poll",
            "5",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(250));
    assert!(poll.try_wait().unwrap().is_none());

    databases
        .persist_host_tasks(&HostTasks {
            tasks: Vec::new(),
            opt: vec![ArtifactName::parse("query-feedback").unwrap()],
        })
        .unwrap();
    thread::sleep(Duration::from_millis(250));
    assert!(poll.try_wait().unwrap().is_none());

    databases
        .persist_host_tasks(&HostTasks {
            tasks: vec![ArtifactName::parse("query-request").unwrap()],
            opt: vec![ArtifactName::parse("query-feedback").unwrap()],
        })
        .unwrap();
    let output = poll.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<HostTasks>(&output.stdout).unwrap(),
        HostTasks {
            tasks: vec![ArtifactName::parse("query-request").unwrap()],
            opt: vec![ArtifactName::parse("query-feedback").unwrap()],
        }
    );
}

#[test]
fn benchmark_cli_runs_isolated_respondent_round() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join(".labflow")).unwrap();
    let port = available_port();
    fs::write(
        directory.path().join(".labflow/config"),
        format!("port = {port}\n"),
    )
    .unwrap();
    fs::write(
        directory.path().join("lab-plan.toml"),
        r#"version = 1
[backend]
hostname = "127.0.0.1"
[roles.challenger]
kind = "evaluator"
[benchmark."solver.challenger"]
records = "bench/results.sqlite"
public-knowledge = ["public/"]
challenge.source = "questions.jsonl"
challenge.questions = "questions.ids"
[benchmark."solver.challenger".respondent]
write = ["workspace/result.json"]
commands = ["just verify"]
"#,
    )
    .unwrap();
    fs::write(
        directory.path().join("questions.jsonl"),
        "{\"id\":\"q1\",\"Q\":\"solve it\",\"K\":\"hidden hint\"}\n",
    )
    .unwrap();
    fs::write(directory.path().join("questions.ids"), "q1\n").unwrap();
    let fake = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_opencode.py");
    let mut backend = Command::new("python3")
        .args([fake.to_str().unwrap(), "--port", &port.to_string()])
        .current_dir(directory.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    wait_until(Duration::from_secs(5), || {
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
    });

    let invoke = |arguments: &[&str]| {
        labflow()
            .arg("--root")
            .arg(directory.path())
            .args(arguments)
            .output()
            .unwrap()
    };
    let output = invoke(&["bench", "start", "solver.challenger"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["round"].is_i64());

    let output = invoke(&["challenge", "next", "solver.challenger"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let answer: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(answer["Q"], "solve it");
    assert_eq!(answer["K"], "hidden hint");
    assert_eq!(answer["reply"], "respondent reply");

    let output = invoke(&[
        "challenge",
        "clarify",
        "solver.challenger",
        "use the public rule",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["reply"],
        "respondent reply"
    );
    assert!(
        invoke(&["challenge", "archive", "solver.challenger"])
            .status
            .success()
    );
    let output = invoke(&["challenge", "next", "solver.challenger"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::Value::Null
    );
    assert!(
        invoke(&["bench", "finish", "solver.challenger"])
            .status
            .success()
    );

    let session: serde_json::Value =
        serde_json::from_slice(&fs::read(directory.path().join("benchmark-session.json")).unwrap())
            .unwrap();
    assert!(session.get("parentID").is_none());
    assert_eq!(session["agent"], "solver");
    assert!(directory.path().join("benchmark-deleted").is_file());
    let records = Connection::open(directory.path().join("bench/results.sqlite")).unwrap();
    assert_eq!(
        records
            .query_row("SELECT status FROM bench_round", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "committed"
    );
    let turns = records
        .prepare("SELECT turn_index, is_last_turn, q, a FROM turn ORDER BY turn_index")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        turns,
        vec![
            (0, false, "solve it".into(), "respondent reply".into()),
            (
                1,
                true,
                "use the public rule".into(),
                "respondent reply".into()
            ),
        ]
    );
    let action_count: i64 = records
        .query_row("SELECT count(*) FROM action", [], |row| row.get(0))
        .unwrap();
    assert_eq!(action_count, 6);
    let command: String = records
        .query_row(
            "SELECT subject FROM action WHERE kind = 'bash' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(command, "just verify");
    backend.kill().unwrap();
    backend.wait().unwrap();
}

#[test]
fn invalid_plan_requests_system_plan_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join(".labflow/artifacts")).unwrap();
    fs::write(directory.path().join(".labflow/config"), "port = 4096\n").unwrap();
    fs::write(directory.path().join("lab-plan.toml"), "not toml").unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    let mut supervisor = labflow()
        .args(["--root", directory.path().to_str().unwrap(), "supervisor"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_until(Duration::from_secs(10), || {
        read_host_tasks(directory.path())
            .is_ok_and(|tasks| tasks.tasks == vec![ArtifactName::parse("system-plan").unwrap()])
    });
    fs::write(
        directory.path().join("lab-plan.toml"),
        labflow::plan::EXAMPLE_PLAN,
    )
    .unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-plan",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        read_host_tasks(directory.path()).is_ok_and(|tasks| {
            !tasks
                .tasks
                .contains(&ArtifactName::parse("system-plan").unwrap())
        })
    });
    supervisor.kill().unwrap();
    supervisor.wait().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn run_tracks_supervisor_artifact_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "init",
            "--port",
            "4096",
        ])
        .status()
        .unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    let mut runner = labflow()
        .args(["--root", directory.path().to_str().unwrap(), "run"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let runner_pid = runner.id();
    let mut first = String::new();
    wait_until(Duration::from_secs(10), || {
        first = child_pids(runner_pid);
        !first.is_empty()
    });
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        let current = child_pids(runner_pid);
        !current.is_empty() && current != first
    });
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "!system-supervisor",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        child_pids(runner_pid).is_empty()
    });
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        !child_pids(runner_pid).is_empty()
    });
    Command::new("kill")
        .args(["-TERM", &runner_pid.to_string()])
        .status()
        .unwrap();
    assert!(runner.wait().unwrap().success());
    assert!(
        !directory
            .path()
            .join(".labflow/artifacts/system-supervisor")
            .exists()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn run_unpublishes_crashing_supervisor_generation() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join(".labflow/artifacts")).unwrap();
    fs::write(directory.path().join(".labflow/config"), "invalid").unwrap();
    fs::write(
        directory.path().join("lab-plan.toml"),
        labflow::plan::EXAMPLE_PLAN,
    )
    .unwrap();
    labflow()
        .args([
            "--root",
            directory.path().to_str().unwrap(),
            "publish",
            "system-supervisor",
        ])
        .status()
        .unwrap();
    let mut runner = labflow()
        .args(["--root", directory.path().to_str().unwrap(), "run"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_until(Duration::from_secs(10), || {
        !directory
            .path()
            .join(".labflow/artifacts/system-supervisor")
            .exists()
    });
    Command::new("kill")
        .args(["-TERM", &runner.id().to_string()])
        .status()
        .unwrap();
    assert!(runner.wait().unwrap().success());
}

#[cfg(target_os = "linux")]
fn child_pids(pid: u32) -> String {
    fs::read_to_string(format!("/proc/{pid}/task/{pid}/children"))
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn available_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = Instant::now();
    while !predicate() {
        assert!(
            start.elapsed() < timeout,
            "condition was not met before timeout"
        );
        thread::sleep(Duration::from_millis(50));
    }
}
