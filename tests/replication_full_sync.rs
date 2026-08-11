use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static NEXT_TEST_PORT: AtomicU16 = AtomicU16::new(30_000);

#[derive(Debug, PartialEq, Eq)]
enum Resp {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Resp>),
}

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
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "onyxdb-replication-{label}-{}-{nonce}",
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
    for _ in 0..3_000 {
        let base = NEXT_TEST_PORT.fetch_add(3, Ordering::SeqCst);
        if base > 49_000 {
            NEXT_TEST_PORT.store(30_000, Ordering::SeqCst);
            continue;
        }
        let addresses = [base, base + 1, base + 1000];
        let listeners: Option<Vec<TcpListener>> = addresses
            .into_iter()
            .map(|port| TcpListener::bind(("127.0.0.1", port)).ok())
            .collect();
        if listeners.is_some() {
            return base;
        }
    }
    panic!("Unable to reserve ports for a replication test");
}

fn start_server(directory: &Path, port: u16, extra_args: &[String]) -> ServerProcess {
    let executable = env!("CARGO_BIN_EXE_onyxdb");
    let mut command = Command::new(executable);
    command
        .current_dir(directory)
        .args(["--port", &port.to_string(), "--appendfsync", "always"])
        .args(extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = command.spawn().unwrap();

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    for _ in 0..240 {
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

fn replica_args(master_port: u16) -> Vec<String> {
    vec![
        "--replica-of".to_string(),
        format!("127.0.0.1:{master_port}"),
    ]
}

fn maxmemory_args(limit: usize, policy: &str) -> Vec<String> {
    vec![
        "--maxmemory".to_string(),
        limit.to_string(),
        "--maxmemory-policy".to_string(),
        policy.to_string(),
    ]
}

fn assert_oom(response: Resp) {
    assert!(
        matches!(response, Resp::Error(ref message) if message.starts_with("OOM")),
        "expected an OOM response, got {response:?}"
    );
}

fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut encoded = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        encoded.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        encoded.extend_from_slice(arg);
        encoded.extend_from_slice(b"\r\n");
    }
    encoded
}

fn encode_obp_frame(cmd: u8, correlation_id: u32, args: &[&[u8]]) -> Vec<u8> {
    encode_obp_frame_with_payload(cmd, correlation_id, args, None)
}

fn encode_obp_frame_with_payload(
    cmd: u8,
    correlation_id: u32,
    args: &[&[u8]],
    payload: Option<&[u8]>,
) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.push(0x4f);
    encoded.push(0x01);
    encoded.push(cmd);
    encoded.extend_from_slice(&0u16.to_be_bytes());
    encoded.extend_from_slice(&correlation_id.to_be_bytes());
    encoded.extend_from_slice(&(args.len() as u16).to_be_bytes());
    for argument in args {
        encoded.extend_from_slice(&(argument.len() as u32).to_be_bytes());
        encoded.extend_from_slice(argument);
    }
    let payload = payload.unwrap_or_default();
    encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    encoded.extend_from_slice(payload);
    encoded
}

fn read_obp_response(reader: &mut BufReader<TcpStream>) -> (u32, Vec<u8>) {
    let mut header = [0u8; 11];
    reader.read_exact(&mut header).unwrap();
    assert_eq!(header[0], 0x4f);
    assert_eq!(header[1], 0x01);
    let correlation_id = u32::from_be_bytes(header[5..9].try_into().unwrap());
    let argument_count = u16::from_be_bytes(header[9..11].try_into().unwrap());
    for _ in 0..argument_count {
        let mut length = [0u8; 4];
        reader.read_exact(&mut length).unwrap();
        let mut argument = vec![0u8; u32::from_be_bytes(length) as usize];
        reader.read_exact(&mut argument).unwrap();
    }
    let mut payload_length = [0u8; 4];
    reader.read_exact(&mut payload_length).unwrap();
    let mut payload = vec![0u8; u32::from_be_bytes(payload_length) as usize];
    reader.read_exact(&mut payload).unwrap();
    (correlation_id, payload)
}

fn send_obp_frame(port: u16, cmd: u8, args: &[&[u8]]) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port + 1)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(&encode_obp_frame(cmd, 1, args)).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    response
}

fn read_line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.ends_with("\r\n"), "malformed RESP line: {line:?}");
    line.truncate(line.len() - 2);
    line
}

fn read_resp(reader: &mut BufReader<TcpStream>) -> Resp {
    let header = read_line(reader);
    let (prefix, value) = header.split_at(1);
    match prefix {
        "+" => Resp::Simple(value.to_string()),
        "-" => Resp::Error(value.to_string()),
        ":" => Resp::Integer(value.parse().unwrap()),
        "$" => {
            let length: isize = value.parse().unwrap();
            if length < 0 {
                return Resp::Bulk(None);
            }
            let mut payload = vec![0u8; length as usize];
            reader.read_exact(&mut payload).unwrap();
            let mut terminator = [0u8; 2];
            reader.read_exact(&mut terminator).unwrap();
            assert_eq!(&terminator, b"\r\n");
            Resp::Bulk(Some(payload))
        }
        "*" => {
            let count: usize = value.parse().unwrap();
            Resp::Array((0..count).map(|_| read_resp(reader)).collect())
        }
        _ => panic!("unexpected RESP prefix: {prefix}"),
    }
}

fn send_command(port: u16, args: &[&[u8]]) -> Resp {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(&encode_command(args)).unwrap();
    stream.flush().unwrap();
    read_resp(&mut BufReader::new(stream))
}

fn assert_protocol_error_and_closed(port: u16, request: &[u8]) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut reader = BufReader::new(stream);
    let response = read_resp(&mut reader);
    assert!(
        matches!(response, Resp::Error(ref message) if message.starts_with("ERR Protocol error:")),
        "expected a protocol error, got {response:?}"
    );
    let mut trailing = Vec::new();
    reader.read_to_end(&mut trailing).unwrap();
    assert!(trailing.is_empty(), "connection returned trailing data");
}

fn wait_for_response(port: u16, args: &[&[u8]], expected: &Resp) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut stream = stream;
            if stream.write_all(&encode_command(args)).is_ok()
                && stream.flush().is_ok()
                && read_resp(&mut BufReader::new(stream)) == *expected
            {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {expected:?} from {args:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn resp_bulk(value: &str) -> Resp {
    Resp::Bulk(Some(value.as_bytes().to_vec()))
}

fn snapshot_blob_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&(key.len() as u32).to_be_bytes());
    entry.extend_from_slice(key);
    entry.extend_from_slice(&0u64.to_be_bytes());
    entry.push(1);
    entry.extend_from_slice(&(value.len() as u32).to_be_bytes());
    entry.extend_from_slice(value);
    entry
}

fn hex_encode(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize]);
        encoded.push(HEX[(byte & 0x0f) as usize]);
    }
    encoded
}

#[test]
fn full_sync_replaces_stale_state_replicates_mutations_and_survives_restart() {
    let master_directory = TestDirectory::new("master");
    let replica_directory = TestDirectory::new("replica");
    let master_port = choose_port();
    let replica_port = choose_port();

    let stale_server = start_server(&replica_directory.0, replica_port, &[]);
    assert_eq!(
        send_command(replica_port, &[b"SET", b"stale", b"old"]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(
        send_command(replica_port, &[b"SET", b"overwritten", b"old"]),
        Resp::Simple("OK".to_string())
    );
    stale_server.stop();

    let master = start_server(&master_directory.0, master_port, &[]);
    assert_eq!(
        send_command(master_port, &[b"SET", b"overwritten", b"master"]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(
        send_command(master_port, &[b"SET", b"text", b"alpha"]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(
        send_command(master_port, &[b"SET", b"counter", b"5"]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(
        send_command(master_port, &[b"RPUSH", b"items", b"one"]),
        Resp::Integer(1)
    );
    assert_eq!(
        send_command(master_port, &[b"RPUSH", b"items", b"two"]),
        Resp::Integer(2)
    );
    assert_eq!(
        send_command(master_port, &[b"HSET", b"hash", b"field", b"value"]),
        Resp::Integer(1)
    );
    assert_eq!(
        send_command(master_port, &[b"SADD", b"set", b"member"]),
        Resp::Integer(1)
    );
    assert_eq!(
        send_command(
            master_port,
            &[b"JSON.SET", b"document", b"$", br#"{"visits":2}"#],
        ),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(
        send_command(master_port, &[b"SET", b"expiring", b"alive", b"EX", b"600"],),
        Resp::Simple("OK".to_string())
    );

    let replica = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );
    wait_for_response(
        replica_port,
        &[b"GET", b"overwritten"],
        &resp_bulk("master"),
    );
    wait_for_response(replica_port, &[b"GET", b"stale"], &Resp::Bulk(None));
    assert_eq!(
        send_command(replica_port, &[b"GET", b"text"]),
        resp_bulk("alpha")
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"counter"]),
        resp_bulk("5")
    );
    assert_eq!(
        send_command(replica_port, &[b"LRANGE", b"items", b"0", b"-1"]),
        Resp::Array(vec![resp_bulk("one"), resp_bulk("two")])
    );
    assert_eq!(
        send_command(replica_port, &[b"HGET", b"hash", b"field"]),
        resp_bulk("value")
    );
    assert_eq!(
        send_command(replica_port, &[b"SMEMBERS", b"set"]),
        Resp::Array(vec![resp_bulk("member")])
    );
    assert_eq!(
        send_command(replica_port, &[b"JSON.GET", b"document", b"$.visits"]),
        resp_bulk("2")
    );
    assert!(matches!(
        send_command(replica_port, &[b"TTL", b"expiring"]),
        Resp::Integer(ttl) if ttl > 0 && ttl <= 600
    ));

    assert_eq!(
        send_command(master_port, &[b"INCR", b"counter"]),
        Resp::Integer(6)
    );
    assert_eq!(
        send_command(master_port, &[b"APPEND", b"text", b"-beta"]),
        Resp::Integer(10)
    );
    assert_eq!(
        send_command(master_port, &[b"LPUSH", b"items", b"zero"]),
        Resp::Integer(3)
    );
    assert_eq!(
        send_command(master_port, &[b"HSET", b"hash", b"second", b"two"]),
        Resp::Integer(1)
    );
    assert_eq!(
        send_command(master_port, &[b"SADD", b"set", b"second"]),
        Resp::Integer(1)
    );
    assert_eq!(
        send_command(
            master_port,
            &[b"JSON.NUMINCRBY", b"document", b"$.visits", b"3"],
        ),
        resp_bulk("5")
    );

    wait_for_response(replica_port, &[b"GET", b"counter"], &resp_bulk("6"));
    wait_for_response(replica_port, &[b"GET", b"text"], &resp_bulk("alpha-beta"));
    wait_for_response(
        replica_port,
        &[b"HGET", b"hash", b"second"],
        &resp_bulk("two"),
    );
    wait_for_response(
        replica_port,
        &[b"JSON.GET", b"document", b"$.visits"],
        &resp_bulk("5"),
    );

    master.stop();
    replica.stop();

    let restarted_replica = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"stale"]),
        Resp::Bulk(None)
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"counter"]),
        resp_bulk("6")
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"text"]),
        resp_bulk("alpha-beta")
    );
    assert_eq!(
        send_command(replica_port, &[b"HGET", b"hash", b"second"]),
        resp_bulk("two")
    );
    assert_eq!(
        send_command(replica_port, &[b"JSON.GET", b"document", b"$.visits"]),
        resp_bulk("5")
    );
    restarted_replica.stop();
}

#[test]
fn obp_cannot_mutate_a_read_only_replica() {
    let master_directory = TestDirectory::new("obp-read-only-master");
    let replica_directory = TestDirectory::new("obp-read-only-replica");
    let master_port = choose_port();
    let replica_port = choose_port();
    let master = start_server(&master_directory.0, master_port, &[]);
    let replica = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );

    let response = send_obp_frame(replica_port, 0x02, &[b"divergent", b"local"]);
    assert!(
        response
            .windows(b"READONLY".len())
            .any(|window| window == b"READONLY"),
        "OBP replica write was not rejected: {response:?}"
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"divergent"]),
        Resp::Bulk(None)
    );

    replica.stop();
    master.stop();
}

#[test]
fn obp_payload_frames_pipeline_and_flush_without_client_disconnect() {
    let directory = TestDirectory::new("obp-framing");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);
    let mut stream = TcpStream::connect(("127.0.0.1", port + 1)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut request = encode_obp_frame_with_payload(0xf0, 11, &[], Some(b"ignored-payload"));
    request.extend_from_slice(&encode_obp_frame(0xf0, 12, &[]));
    stream.write_all(&request).unwrap();
    stream.flush().unwrap();

    let mut reader = BufReader::new(stream);
    let first = read_obp_response(&mut reader);
    let second = read_obp_response(&mut reader);
    assert_eq!(first.0, 11);
    assert_eq!(second.0, 12);
    assert!(first.1.windows(4).any(|window| window == b"PONG"));
    assert!(second.1.windows(4).any(|window| window == b"PONG"));

    server.stop();
}

#[test]
fn maxmemory_noeviction_rejects_new_and_existing_value_growth() {
    let directory = TestDirectory::new("maxmemory-noeviction-values");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(128, "noeviction"));

    assert_eq!(
        send_command(port, &[b"SET", b"key", b"a"]),
        Resp::Simple("OK".to_string())
    );
    assert_oom(send_command(port, &[b"APPEND", b"key", &[b'x'; 128]]));
    assert_eq!(send_command(port, &[b"GET", b"key"]), resp_bulk("a"));

    assert_oom(send_command(port, &[b"SET", b"oversized", &[b'y'; 256]]));
    assert_eq!(
        send_command(port, &[b"GET", b"oversized"]),
        Resp::Bulk(None)
    );

    server.stop();
    let restarted = start_server(&directory.0, port, &maxmemory_args(128, "noeviction"));
    assert_eq!(send_command(port, &[b"GET", b"key"]), resp_bulk("a"));
    assert_eq!(
        send_command(port, &[b"GET", b"oversized"]),
        Resp::Bulk(None)
    );
    restarted.stop();
}

#[test]
fn maxmemory_noeviction_rolls_back_collection_and_multi_key_growth() {
    let directory = TestDirectory::new("maxmemory-noeviction-aggregate");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(192, "noeviction"));

    assert_eq!(
        send_command(port, &[b"RPUSH", b"items", b"first"]),
        Resp::Integer(1)
    );
    assert_oom(send_command(port, &[b"RPUSH", b"items", &[b'z'; 192]]));
    assert_eq!(
        send_command(port, &[b"LRANGE", b"items", b"0", b"-1"]),
        Resp::Array(vec![resp_bulk("first")])
    );

    assert_oom(send_command(
        port,
        &[b"MSET", b"first", &[b'a'; 128], b"second", &[b'b'; 128]],
    ));
    assert_eq!(send_command(port, &[b"GET", b"first"]), Resp::Bulk(None));
    assert_eq!(send_command(port, &[b"GET", b"second"]), Resp::Bulk(None));

    server.stop();
}

#[test]
fn maxmemory_noeviction_rolls_back_json_growth() {
    let directory = TestDirectory::new("maxmemory-noeviction-json");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(192, "noeviction"));

    assert_eq!(
        send_command(port, &[b"JSON.SET", b"doc", b"$", br#"{"items":[]}"#]),
        Resp::Simple("OK".to_string())
    );
    let large_json_string = format!("\"{}\"", "x".repeat(192));
    assert_oom(send_command(
        port,
        &[
            b"JSON.ARRAPPEND",
            b"doc",
            b"$.items",
            large_json_string.as_bytes(),
        ],
    ));
    assert_eq!(
        send_command(port, &[b"JSON.GET", b"doc", b"$.items"]),
        resp_bulk("[]")
    );

    server.stop();
}

#[test]
fn maxmemory_eviction_uses_projected_usage_and_preserves_the_written_key() {
    let directory = TestDirectory::new("maxmemory-projected-eviction");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(160, "allkeys-lru"));

    assert_eq!(
        send_command(port, &[b"SET", b"first", &[b'a'; 64]]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(
        send_command(port, &[b"SET", b"second", &[b'b'; 64]]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(send_command(port, &[b"GET", b"first"]), Resp::Bulk(None));
    assert_eq!(
        send_command(port, &[b"GET", b"second"]),
        Resp::Bulk(Some(vec![b'b'; 64]))
    );

    server.stop();
    let restarted = start_server(&directory.0, port, &maxmemory_args(160, "allkeys-lru"));
    assert_eq!(send_command(port, &[b"GET", b"first"]), Resp::Bulk(None));
    assert_eq!(
        send_command(port, &[b"GET", b"second"]),
        Resp::Bulk(Some(vec![b'b'; 64]))
    );
    restarted.stop();
}

#[test]
fn maxmemory_admission_is_identical_for_obp_writes() {
    let directory = TestDirectory::new("maxmemory-obp");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(128, "noeviction"));

    let response = send_obp_frame(port, 0x02, &[b"oversized", &[b'x'; 256]]);
    assert!(
        response.windows(3).any(|window| window == b"OOM"),
        "expected an OOM response, got {response:?}"
    );
    assert_eq!(
        send_command(port, &[b"GET", b"oversized"]),
        Resp::Bulk(None)
    );

    server.stop();
}

#[test]
fn maxmemory_failed_eviction_restores_victims_and_rejects_an_unfit_value() {
    let directory = TestDirectory::new("maxmemory-eviction-rollback");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(200, "allkeys-lru"));

    assert_eq!(
        send_command(port, &[b"SET", b"survivor", &[b's'; 64]]),
        Resp::Simple("OK".to_string())
    );
    assert_oom(send_command(port, &[b"SET", b"unfit", &[b'u'; 256]]));
    assert_eq!(
        send_command(port, &[b"GET", b"survivor"]),
        Resp::Bulk(Some(vec![b's'; 64]))
    );
    assert_eq!(send_command(port, &[b"GET", b"unfit"]), Resp::Bulk(None));

    server.stop();
}

#[test]
fn maxmemory_volatile_policy_requires_an_eligible_victim() {
    let directory = TestDirectory::new("maxmemory-volatile-policy");
    let port = choose_port();
    let server = start_server(&directory.0, port, &maxmemory_args(160, "volatile-lru"));

    assert_eq!(
        send_command(port, &[b"SET", b"first", &[b'a'; 64]]),
        Resp::Simple("OK".to_string())
    );
    assert_oom(send_command(port, &[b"SET", b"second", &[b'b'; 64]]));
    assert_eq!(
        send_command(port, &[b"GET", b"first"]),
        Resp::Bulk(Some(vec![b'a'; 64]))
    );
    assert_eq!(
        send_command(port, &[b"EXPIRE", b"first", b"600"]),
        Resp::Integer(1)
    );
    assert_eq!(
        send_command(port, &[b"SET", b"second", &[b'b'; 64]]),
        Resp::Simple("OK".to_string())
    );
    assert_eq!(send_command(port, &[b"GET", b"first"]), Resp::Bulk(None));
    assert_eq!(
        send_command(port, &[b"GET", b"second"]),
        Resp::Bulk(Some(vec![b'b'; 64]))
    );

    server.stop();
}

#[test]
fn maxmemory_recovery_allows_non_growing_mutations_above_the_limit() {
    let directory = TestDirectory::new("maxmemory-recovered-over-limit");
    let port = choose_port();
    let initial = start_server(&directory.0, port, &[]);
    assert_eq!(
        send_command(port, &[b"SET", b"key", &[b'a'; 256]]),
        Resp::Simple("OK".to_string())
    );
    initial.stop();

    let limited = start_server(&directory.0, port, &maxmemory_args(128, "noeviction"));
    assert_eq!(
        send_command(port, &[b"SET", b"key", &[b'b'; 192]]),
        Resp::Simple("OK".to_string())
    );
    assert_oom(send_command(port, &[b"APPEND", b"key", b"growth"]));
    assert_eq!(send_command(port, &[b"DEL", b"key"]), Resp::Integer(1));
    assert_eq!(send_command(port, &[b"GET", b"key"]), Resp::Bulk(None));

    limited.stop();
}

#[test]
fn replica_applies_authoritative_state_even_when_it_exceeds_its_local_maxmemory() {
    let master_directory = TestDirectory::new("maxmemory-replica-master");
    let replica_directory = TestDirectory::new("maxmemory-replica");
    let master_port = choose_port();
    let replica_port = choose_port();
    let master = start_server(&master_directory.0, master_port, &[]);
    assert_eq!(
        send_command(master_port, &[b"SET", b"large", &[b'a'; 256]]),
        Resp::Simple("OK".to_string())
    );

    let mut replica_configuration = replica_args(master_port);
    replica_configuration.extend(maxmemory_args(128, "noeviction"));
    let replica = start_server(&replica_directory.0, replica_port, &replica_configuration);
    wait_for_response(
        replica_port,
        &[b"GET", b"large"],
        &Resp::Bulk(Some(vec![b'a'; 256])),
    );
    assert_eq!(
        send_command(master_port, &[b"APPEND", b"large", b"b"]),
        Resp::Integer(257)
    );
    wait_for_response(
        replica_port,
        &[b"GET", b"large"],
        &Resp::Bulk(Some([vec![b'a'; 256], vec![b'b']].concat())),
    );

    replica.stop();
    master.stop();
}

#[test]
fn resp_malformed_array_header_is_fail_closed() {
    let directory = TestDirectory::new("resp-malformed-array");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);
    let mut request = b"*invalid\r\n".to_vec();
    request.extend_from_slice(&encode_command(&[b"SET", b"smuggled", b"value"]));

    assert_protocol_error_and_closed(port, &request);
    assert_eq!(send_command(port, &[b"GET", b"smuggled"]), Resp::Bulk(None));

    server.stop();
}

#[test]
fn resp_oversized_array_header_is_fail_closed() {
    let directory = TestDirectory::new("resp-oversized-array");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);
    let mut request = b"*1025\r\n".to_vec();
    request.extend_from_slice(&encode_command(&[b"SET", b"smuggled", b"value"]));

    assert_protocol_error_and_closed(port, &request);
    assert_eq!(send_command(port, &[b"GET", b"smuggled"]), Resp::Bulk(None));

    server.stop();
}

#[test]
fn resp_requires_crlf_for_array_and_bulk_headers() {
    let directory = TestDirectory::new("resp-header-crlf");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);
    let request = b"*3\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    assert_protocol_error_and_closed(port, request);
    assert_eq!(send_command(port, &[b"GET", b"key"]), Resp::Bulk(None));

    server.stop();
}

#[test]
fn resp_rejects_invalid_utf8_instead_of_mutating_lossily() {
    let directory = TestDirectory::new("resp-invalid-utf8");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);
    let request = encode_command(&[b"SET", b"key", &[0xff, 0xfe]]);

    assert_protocol_error_and_closed(port, &request);
    assert_eq!(send_command(port, &[b"GET", b"key"]), Resp::Bulk(None));

    server.stop();
}

#[test]
fn resp_oversized_bulk_is_rejected_before_its_payload() {
    let directory = TestDirectory::new("resp-oversized-bulk");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);

    assert_protocol_error_and_closed(port, b"*1\r\n$8388609\r\n");

    server.stop();
}

#[test]
fn resp_overlong_header_is_fail_closed() {
    let directory = TestDirectory::new("resp-overlong-header");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);
    let request = format!("*{}\r\n", "1".repeat(64));

    assert_protocol_error_and_closed(port, request.as_bytes());

    server.stop();
}

#[test]
fn resp_truncated_bulk_is_reported_and_closed() {
    let directory = TestDirectory::new("resp-truncated-bulk");
    let port = choose_port();
    let server = start_server(&directory.0, port, &[]);

    assert_protocol_error_and_closed(port, b"*1\r\n$4\r\nPI");

    server.stop();
}

#[test]
fn restarting_a_replica_as_master_detaches_its_upstream_history() {
    let master_directory = TestDirectory::new("role-history-master");
    let replica_directory = TestDirectory::new("role-history-replica");
    let master_port = choose_port();
    let replica_port = choose_port();

    let master = start_server(&master_directory.0, master_port, &[]);
    assert_eq!(
        send_command(master_port, &[b"SET", b"baseline", b"master"]),
        Resp::Simple("OK".to_string())
    );
    let replica = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );
    wait_for_response(replica_port, &[b"GET", b"baseline"], &resp_bulk("master"));
    replica.stop();

    let detached_master = start_server(&replica_directory.0, replica_port, &[]);
    assert_eq!(
        send_command(replica_port, &[b"SET", b"divergent", b"local"]),
        Resp::Simple("OK".to_string())
    );
    detached_master.stop();

    assert_eq!(
        send_command(master_port, &[b"SET", b"authoritative", b"upstream"]),
        Resp::Simple("OK".to_string())
    );
    let reattached_replica = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );
    wait_for_response(
        replica_port,
        &[b"GET", b"authoritative"],
        &resp_bulk("upstream"),
    );
    wait_for_response(replica_port, &[b"GET", b"divergent"], &Resp::Bulk(None));

    reattached_replica.stop();
    master.stop();
}

#[test]
fn master_snapshot_boundary_transitions_to_the_next_live_sequence() {
    let directory = TestDirectory::new("boundary");
    let port = choose_port();
    let master = start_server(&directory.0, port, &[]);
    assert_eq!(
        send_command(port, &[b"SET", b"baseline", b"value"]),
        Resp::Simple("OK".to_string())
    );

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(&encode_command(&[b"SYNC3", b"0", b"0"]))
        .unwrap();
    stream.flush().unwrap();
    let mut reader = BufReader::new(stream);

    let handshake = match read_resp(&mut reader) {
        Resp::Simple(marker) => marker,
        other => panic!("unexpected full-sync handshake: {other:?}"),
    };
    let fields: Vec<&str> = handshake.split_whitespace().collect();
    assert_eq!(fields[0], "FULLRESYNC3");
    let replid = fields[1].parse::<u64>().unwrap();
    let boundary = fields[2].parse::<u64>().unwrap();
    let entry_count = fields[3].parse::<usize>().unwrap();
    assert_ne!(replid, 0);
    assert_eq!(boundary, 1);

    assert_eq!(
        send_command(port, &[b"INCR", b"after-boundary"]),
        Resp::Integer(1)
    );

    for _ in 0..entry_count {
        let declared_length = match read_resp(&mut reader) {
            Resp::Array(frame) => {
                assert_eq!(frame.len(), 2);
                assert_eq!(frame[0], resp_bulk("FULLSYNCENTRY"));
                match &frame[1] {
                    Resp::Bulk(Some(length)) => std::str::from_utf8(length)
                        .unwrap()
                        .parse::<usize>()
                        .unwrap(),
                    other => panic!("unexpected snapshot length: {other:?}"),
                }
            }
            other => panic!("unexpected snapshot frame: {other:?}"),
        };
        let mut received = 0usize;
        while received < declared_length {
            match read_resp(&mut reader) {
                Resp::Array(frame) => {
                    assert_eq!(frame.len(), 2);
                    assert_eq!(frame[0], resp_bulk("FULLSYNCCHUNK"));
                    match &frame[1] {
                        Resp::Bulk(Some(payload)) => {
                            assert!(payload.len().is_multiple_of(2));
                            received += payload.len() / 2;
                        }
                        other => panic!("unexpected snapshot chunk: {other:?}"),
                    }
                }
                other => panic!("unexpected snapshot chunk frame: {other:?}"),
            }
        }
        assert_eq!(received, declared_length);
    }
    assert_eq!(
        read_resp(&mut reader),
        Resp::Simple(format!("SYNCDONE3 {replid} {boundary}"))
    );
    let effect_length = match read_resp(&mut reader) {
        Resp::Array(frame) => {
            assert_eq!(frame.len(), 3);
            assert_eq!(frame[0], resp_bulk("APPLYEFFECT"));
            assert_eq!(frame[1], resp_bulk(&(boundary + 1).to_string()));
            match &frame[2] {
                Resp::Bulk(Some(length)) => std::str::from_utf8(length)
                    .unwrap()
                    .parse::<usize>()
                    .unwrap(),
                other => panic!("unexpected effect length: {other:?}"),
            }
        }
        other => panic!("unexpected live replication frame: {other:?}"),
    };
    let mut received = 0usize;
    while received < effect_length {
        match read_resp(&mut reader) {
            Resp::Array(frame) => {
                assert_eq!(frame.len(), 2);
                assert_eq!(frame[0], resp_bulk("EFFECTCHUNK"));
                match &frame[1] {
                    Resp::Bulk(Some(payload)) => {
                        assert!(payload.len().is_multiple_of(2));
                        received += payload.len() / 2;
                    }
                    other => panic!("unexpected effect chunk: {other:?}"),
                }
            }
            other => panic!("unexpected effect chunk frame: {other:?}"),
        }
    }
    assert_eq!(received, effect_length);
    master.stop();
}

#[test]
fn interrupted_full_sync_is_discarded_and_a_retry_installs_atomically() {
    let replica_directory = TestDirectory::new("interrupted");
    let replica_port = choose_port();
    let master_port = choose_port();

    let standalone = start_server(&replica_directory.0, replica_port, &[]);
    assert_eq!(
        send_command(replica_port, &[b"SET", b"old", b"live"]),
        Resp::Simple("OK".to_string())
    );
    standalone.stop();

    let listener = TcpListener::bind(("127.0.0.1", master_port)).unwrap();
    let (first_closed_tx, first_closed_rx) = mpsc::channel();
    let (allow_retry_tx, allow_retry_rx) = mpsc::channel();
    let (second_closed_tx, second_closed_rx) = mpsc::channel();
    let (allow_completion_tx, allow_completion_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let fake_master = std::thread::spawn(move || {
        let (first, _) = listener.accept().unwrap();
        first
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut first = BufReader::new(first);
        let mut request = String::new();
        first.read_line(&mut request).unwrap();
        assert!(request.starts_with("SYNC3 "));
        let mut first = first.into_inner();
        let entry = snapshot_blob_entry(b"new", b"installed");
        let payload = hex_encode(&entry);
        let entry_length = entry.len().to_string();
        let incomplete_length = (entry.len() + 1).to_string();
        first.write_all(b"+FULLRESYNC3 123 7 1\r\n").unwrap();
        first
            .write_all(&encode_command(&[
                b"FULLSYNCENTRY",
                incomplete_length.as_bytes(),
            ]))
            .unwrap();
        first
            .write_all(&encode_command(&[b"FULLSYNCCHUNK", &payload]))
            .unwrap();
        first.flush().unwrap();
        drop(first);
        first_closed_tx.send(()).unwrap();

        allow_retry_rx.recv().unwrap();
        let (second, _) = listener.accept().unwrap();
        second
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut second = BufReader::new(second);
        let mut request = String::new();
        second.read_line(&mut request).unwrap();
        assert!(request.starts_with("SYNC3 "));
        let mut second = second.into_inner();
        second.write_all(b"+FULLRESYNC3 123 7 1\r\n").unwrap();
        second
            .write_all(&encode_command(&[
                b"FULLSYNCENTRY",
                entry_length.as_bytes(),
            ]))
            .unwrap();
        second
            .write_all(&encode_command(&[b"FULLSYNCCHUNK", &payload]))
            .unwrap();
        second.flush().unwrap();
        drop(second);
        second_closed_tx.send(()).unwrap();

        allow_completion_rx.recv().unwrap();
        let (third, _) = listener.accept().unwrap();
        third
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut third = BufReader::new(third);
        let mut request = String::new();
        third.read_line(&mut request).unwrap();
        assert!(request.starts_with("SYNC3 "));
        let mut third = third.into_inner();
        third.write_all(b"+FULLRESYNC3 123 7 1\r\n").unwrap();
        third
            .write_all(&encode_command(&[
                b"FULLSYNCENTRY",
                entry_length.as_bytes(),
            ]))
            .unwrap();
        third
            .write_all(&encode_command(&[b"FULLSYNCCHUNK", &payload]))
            .unwrap();
        third.write_all(b"+SYNCDONE3 123 7\r\n").unwrap();
        third.flush().unwrap();
        completed_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_secs(1));
    });

    let replica = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );
    first_closed_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        send_command(replica_port, &[b"GET", b"old"]),
        resp_bulk("live")
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"new"]),
        Resp::Bulk(None)
    );

    allow_retry_tx.send(()).unwrap();
    second_closed_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        send_command(replica_port, &[b"GET", b"old"]),
        resp_bulk("live")
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"new"]),
        Resp::Bulk(None)
    );

    allow_completion_tx.send(()).unwrap();
    completed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    wait_for_response(replica_port, &[b"GET", b"new"], &resp_bulk("installed"));
    wait_for_response(replica_port, &[b"GET", b"old"], &Resp::Bulk(None));
    replica.stop();
    fake_master.join().unwrap();

    let restarted = start_server(
        &replica_directory.0,
        replica_port,
        &replica_args(master_port),
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"new"]),
        resp_bulk("installed")
    );
    assert_eq!(
        send_command(replica_port, &[b"GET", b"old"]),
        Resp::Bulk(None)
    );
    restarted.stop();
}
