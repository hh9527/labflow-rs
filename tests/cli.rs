use std::fs;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::cargo::cargo_bin;
use rusqlite::Connection;

fn labflow() -> Command {
    Command::new(cargo_bin("labflow"))
}

#[test]
fn host_can_initialize_publish_and_unpublish() {
    let directory = tempfile::tempdir().unwrap();
    labflow()
        .args(["--root", directory.path().to_str().unwrap(), "init"])
        .status()
        .unwrap()
        .success()
        .then_some(())
        .unwrap();
    assert!(directory.path().join("lab-plan.toml").is_file());
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
            "unpublish",
            "answer.researcher",
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

#[test]
fn supervisor_runs_task_and_exits_on_control_artifact() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir_all(directory.path().join(".labflow/artifacts")).unwrap();
    fs::write(directory.path().join("goal.md"), "write answer.md").unwrap();
    let port = available_port();
    let fake =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_opencode.py");
    let plan = format!(
        r#"version = 1
[backend]
command = ["python3", "{}"]
hostname = "127.0.0.1"
port = {port}
[roles.researcher]
kind = "lab-worker"
permissions = ["read"]
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
    let mut supervisor = labflow()
        .args(["--root", directory.path().to_str().unwrap(), "supervisor"])
        .stdout(Stdio::null())
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
            .is_ok_and(|content| content.lines().count() >= 2)
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
        .query_row("SELECT count(*) FROM records", [], |row| row.get(0))
        .unwrap();
    assert!(records > 0);
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
