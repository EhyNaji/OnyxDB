use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct ServerProcess {
    child: Child,
}

impl ServerProcess {
    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
            "onyxdb-process-restart-{}-{}",
            std::process::id(),
            nonce
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

fn choose_ports() -> u16 {
    for base in 20_000..50_000 {
        let addresses = [base, base + 1, base + 1000];
        let listeners: Option<Vec<TcpListener>> = addresses
            .into_iter()
            .map(|port| TcpListener::bind(("127.0.0.1", port)).ok())
            .collect();
        if listeners.is_some() {
            return base;
        }
    }
    panic!("Unable to reserve ports for the restart test");
}

fn start_server(directory: &Path, port: u16) -> ServerProcess {
    let executable = env!("CARGO_BIN_EXE_onyxdb");
    let mut child = Command::new(executable)
        .current_dir(directory)
        .args(["--port", &port.to_string(), "--appendfsync", "always"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    for _ in 0..200 {
        if TcpStream::connect_timeout(&address, Duration::from_millis(25)).is_ok() {
            return ServerProcess { child };
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("OnyxDB exited before accepting connections: {status}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("OnyxDB did not start within the test timeout");
}

fn send_command(port: u16, command: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(command.as_bytes()).unwrap();
    stream.write_all(b"\r\n").unwrap();
    stream.flush().unwrap();

    let mut reader = BufReader::new(stream);
    let mut header = String::new();
    reader.read_line(&mut header).unwrap();
    if let Some(length) = header
        .strip_prefix('$')
        .and_then(|value| value.trim().parse::<isize>().ok())
        && length >= 0
    {
        let mut payload = vec![0u8; length as usize];
        reader.read_exact(&mut payload).unwrap();
        let mut terminator = [0u8; 2];
        reader.read_exact(&mut terminator).unwrap();
        assert_eq!(&terminator, b"\r\n");
        return String::from_utf8(payload).unwrap();
    }
    header.trim_end_matches(['\r', '\n']).to_string()
}

#[test]
fn abrupt_restart_replays_only_committed_effects() {
    let directory = TestDirectory::new();
    let port = choose_ports();
    let server = start_server(&directory.0, port);

    assert_eq!(send_command(port, "SET key original"), "+OK");
    assert_eq!(send_command(port, "SETNX key replacement"), ":0");
    assert_eq!(send_command(port, "SET counter 10"), "+OK");
    assert_eq!(send_command(port, "DECRBY counter -2"), ":12");
    assert_eq!(send_command(port, "APPEND text alpha"), ":5");
    assert_eq!(send_command(port, "APPEND text -beta"), ":10");
    assert_eq!(
        send_command(port, r#"JSON.SET document $ {"visits":0}"#),
        "+OK"
    );
    assert_eq!(
        send_command(port, "JSON.NUMINCRBY document $.visits 3"),
        "3"
    );
    server.stop();

    let server = start_server(&directory.0, port);
    assert_eq!(send_command(port, "GET key"), "original");
    assert_eq!(send_command(port, "GET counter"), "12");
    assert_eq!(send_command(port, "GET text"), "alpha-beta");
    assert_eq!(send_command(port, "JSON.GET document $.visits"), "3");
    server.stop();
}
