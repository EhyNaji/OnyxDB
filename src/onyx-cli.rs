use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

fn encode_command(input: &str) -> String {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let mut out = format!("*{}\r\n", parts.len());
    for p in &parts {
        out.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    out
}

async fn read_reply(reader: &mut BufReader<OwnedReadHalf>) -> std::io::Result<String> {
    let mut header = String::new();
    let n = reader.read_line(&mut header).await?;
    if n == 0 {
        return Ok("(connection closed)".to_string());
    }
    let header = header.trim_end();
    if header.is_empty() {
        return Ok(String::new());
    }
    let prefix = header.chars().next().unwrap();
    let rest = &header[1..];
    match prefix {
        '+' => Ok(rest.to_string()),
        '-' => Ok(format!("(error) {}", rest)),
        ':' => Ok(rest.to_string()),
        '$' => {
            let len: i64 = rest.parse().unwrap_or(-1);
            if len < 0 {
                Ok("(nil)".to_string())
            } else {
                let mut buf = vec![0u8; len as usize + 2];
                reader.read_exact(&mut buf).await?;
                buf.truncate(len as usize);
                Ok(String::from_utf8_lossy(&buf).to_string())
            }
        }
        '*' => {
            let count: i64 = rest.parse().unwrap_or(0);
            if count <= 0 {
                return Ok("(empty array)".to_string());
            }
            let mut items = Vec::new();
            for _ in 0..count {
                items.push(Box::pin(read_reply(reader)).await?);
            }
            Ok(format!("[{}]", items.join(", ")))
        }
        _ => Ok(header.to_string()),
    }
}

// Comandi che leggono soltanto: possono essere instradati verso una Replica.
// Tutto il resto (scritture, PING, INFO, SAVE, SYNC) va sempre al Master.
fn is_read_command(cmd: &str) -> bool {
    matches!(
        cmd.to_ascii_uppercase().as_str(),
        "GET"
            | "MGET"
            | "LRANGE"
            | "LLEN"
            | "HGET"
            | "HGETALL"
            | "SMEMBERS"
            | "SISMEMBER"
            | "EXISTS"
            | "TYPE"
            | "TTL"
            | "KEYS"
            | "STRLEN"
    )
}

struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

async fn connect(addr: &str) -> Option<Connection> {
    match TcpStream::connect(addr).await {
        Ok(stream) => {
            let (r, w) = stream.into_split();
            Some(Connection {
                reader: BufReader::new(r),
                writer: w,
            })
        }
        Err(e) => {
            println!("Unable to connect to {}: {}", addr, e);
            None
        }
    }
}

async fn send_and_read(conn: &mut Connection, command: &str) -> String {
    let encoded = encode_command(command);
    if conn.writer.write_all(encoded.as_bytes()).await.is_err() {
        return "(write error: connection lost)".to_string();
    }
    read_reply(&mut conn.reader)
        .await
        .unwrap_or_else(|e| format!("(read error: {})", e))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut master_addr = "127.0.0.1:6380".to_string();
    let mut replica_addrs: Vec<String> = Vec::new();

    for i in 0..args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            master_addr = format!("127.0.0.1:{}", args[i + 1]);
        }
        if args[i] == "--master" && i + 1 < args.len() {
            master_addr = args[i + 1].clone();
        }
        if args[i] == "--replicas" && i + 1 < args.len() {
            replica_addrs = args[i + 1]
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        }
    }

    println!("OnyxDB CLI - Master: {}", master_addr);
    if !replica_addrs.is_empty() {
        println!("Read replicas: {:?}", replica_addrs);
    }

    let mut master_conn = match connect(&master_addr).await {
        Some(c) => c,
        None => return,
    };

    // Teniamo indirizzo e connessione ACCOPPIATI: se una Replica fallisce la
    // connessione all'avvio viene scartata insieme al suo indirizzo, cosi'
    // gli indici restano sempre allineati (niente piu' mismatch tra "quale
    // connessione uso" e "quale indirizzo stampo nel log").
    let mut replica_conns: Vec<(String, Connection)> = Vec::new();
    for addr in &replica_addrs {
        if let Some(c) = connect(addr).await {
            replica_conns.push((addr.clone(), c));
        }
    }
    let mut replica_index = 0usize;

    println!("Connected! Type 'exit' to quit.\n");

    let stdin = io::stdin();
    loop {
        print!("onyx> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap() == 0 {
            break;
        }
        let command = input.trim();
        if command.is_empty() {
            continue;
        }
        if command.eq_ignore_ascii_case("exit") {
            println!("Goodbye!");
            break;
        }

        let cmd_name = command.split_whitespace().next().unwrap_or("");

        let reply = if is_read_command(cmd_name) && !replica_conns.is_empty() {
            let idx = replica_index % replica_conns.len();
            replica_index += 1;
            let (target, conn) = &mut replica_conns[idx];
            let reply = send_and_read(conn, command).await;
            println!("[reading from replica {}]", target);
            reply
        } else {
            send_and_read(&mut master_conn, command).await
        };

        println!("{}", reply);
    }
}
