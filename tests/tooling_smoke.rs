use serde_json::Value;
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "onyxdb-tooling-smoke-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn choose_port() -> u16 {
    for base in 50_000..60_000 {
        let listeners = [base, base + 1, base + 1000]
            .into_iter()
            .map(|port| TcpListener::bind(("127.0.0.1", port)).ok())
            .collect::<Option<Vec<_>>>();
        if listeners.is_some() {
            return base;
        }
    }
    panic!("Unable to reserve ports for the tooling smoke test");
}

fn start_server(directory: &Path, port: u16) -> ServerProcess {
    let mut child = Command::new(env!("CARGO_BIN_EXE_onyxdb"))
        .current_dir(directory)
        .args(["--port", &port.to_string(), "--appendfsync", "no"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    for _ in 0..240 {
        if TcpStream::connect_timeout(&address, Duration::from_millis(25)).is_ok() {
            return ServerProcess(child);
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("OnyxDB exited before accepting benchmark connections: {status}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("OnyxDB did not start within the tooling smoke-test timeout");
}

#[test]
fn benchmark_runs_against_the_real_server_and_emits_machine_readable_results() {
    let directory = TestDirectory::new();
    let port = choose_port();
    let _server = start_server(&directory.0, port);
    let output = Command::new(env!("CARGO_BIN_EXE_onyx-bench"))
        .args([
            "--address",
            &format!("127.0.0.1:{port}"),
            "--workload",
            "mixed",
            "--requests",
            "40",
            "--warmup",
            "8",
            "--concurrency",
            "2",
            "--pipeline",
            "4",
            "--keyspace",
            "8",
            "--payload-size",
            "16",
            "--repeats",
            "1",
            "--output",
            "json",
            "--key-prefix",
            "tooling-smoke",
            "--metrics-address",
            &format!("127.0.0.1:{}", port + 1000),
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["methodology_version"], 2);
    assert_eq!(report["configuration"]["workload"], "mixed");
    assert_eq!(report["runs"][0]["requested"], 40);
    assert_eq!(report["runs"][0]["completed"], 40);
    assert_eq!(report["runs"][0]["errors"], 0);
    assert!(report["runs"][0]["operations_per_second"].as_f64().unwrap() > 0.0);
    assert!(
        report["runs"][0]["latency_microseconds"]["p99"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert!(
        report["runs"][0]["server_metrics"]["delta"]["onyxdb_commit_groups_total"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert!(
        report["runs"][0]["server_metrics"]["delta"]["onyxdb_binlog_append_accepted_total"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert!(
        report["runs"][0]["server_metrics"]["delta"]
            .get("onyxdb_keys_total")
            .is_none()
    );
}
