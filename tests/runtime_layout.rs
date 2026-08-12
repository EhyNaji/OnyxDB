use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static RUNTIME_LAYOUT_TEST_LOCK: Mutex<()> = Mutex::new(());

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
            "onyxdb-runtime-layout-{}-{nonce}",
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
    for base in 61_000..64_000 {
        let listeners = [base, base + 1, base + 1000]
            .into_iter()
            .map(|port| TcpListener::bind(("127.0.0.1", port)).ok())
            .collect::<Option<Vec<_>>>();
        if listeners.is_some() {
            return base;
        }
    }
    panic!("Unable to reserve ports for the runtime-layout test");
}

fn start_server(working_directory: &Path, data_directory: &Path, port: u16) -> ServerProcess {
    let child = Command::new(env!("CARGO_BIN_EXE_onyxdb"))
        .current_dir(working_directory)
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--data-dir",
            data_directory.to_str().unwrap(),
            "--appendfsync",
            "no",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut server = ServerProcess(child);
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    for _ in 0..240 {
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(25)) {
            stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap();
            if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut response = [0_u8; 7];
                if stream.read_exact(&mut response).is_ok() && response == *b"+PONG\r\n" {
                    return server;
                }
            }
        }
        if let Some(status) = server.0.try_wait().unwrap() {
            panic!("OnyxDB exited before accepting runtime-layout connections: {status}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("OnyxDB did not start within the runtime-layout test timeout");
}

#[test]
fn explicit_data_directory_isolated_and_exclusively_owned() {
    let _test_guard = RUNTIME_LAYOUT_TEST_LOCK.lock().unwrap();
    let directory = TestDirectory::new();
    let data_directory = directory.0.join("data");
    let port = choose_port();
    let _server = start_server(&directory.0, &data_directory, port);

    assert!(data_directory.join("onyx.binlog").exists());
    assert!(data_directory.join("onyx.lock").exists());
    assert!(!directory.0.join("onyx.binlog").exists());
    assert!(!directory.0.join("onyx.snapshot").exists());

    let second = Command::new(env!("CARGO_BIN_EXE_onyxdb"))
        .current_dir(&directory.0)
        .args([
            "--port",
            &port.to_string(),
            "--data-dir",
            data_directory.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!second.status.success());
    let error = String::from_utf8_lossy(&second.stderr);
    assert!(
        error.contains("already owned by another OnyxDB process"),
        "unexpected startup error: {error}"
    );
}

#[test]
fn startup_fails_if_any_required_listener_is_unavailable() {
    let _test_guard = RUNTIME_LAYOUT_TEST_LOCK.lock().unwrap();
    let directory = TestDirectory::new();
    let data_directory = directory.0.join("data");
    let port = choose_port();
    let _metrics_reservation = TcpListener::bind(("127.0.0.1", port + 1000)).unwrap();

    let server = Command::new(env!("CARGO_BIN_EXE_onyxdb"))
        .current_dir(&directory.0)
        .args([
            "--port",
            &port.to_string(),
            "--data-dir",
            data_directory.to_str().unwrap(),
            "--appendfsync",
            "no",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();

    assert!(!server.status.success());
}
