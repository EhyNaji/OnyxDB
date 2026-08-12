use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task;

const NUM_THREADS: usize = 50;
const OPS_PER_THREAD: usize = 500;
const PIPELINE_BATCH: usize = 50;

fn encode_command(parts: &[String]) -> String {
    let mut out = format!("*{}\r\n", parts.len());
    for p in parts {
        out.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    out
}

// Reads and discards one RESP response.
async fn skip_reply(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> std::io::Result<()> {
    let mut header = String::new();
    reader.read_line(&mut header).await?;
    let header = header.trim_end();
    if header.is_empty() {
        return Ok(());
    }

    let prefix = header.chars().next().unwrap();
    let rest = &header[1..];

    match prefix {
        '$' => {
            let len: i64 = rest.parse().unwrap_or(-1);
            if len >= 0 {
                let mut buf = vec![0u8; len as usize + 2];
                reader.read_exact(&mut buf).await?;
            }
        }
        '*' => {
            let count: i64 = rest.parse().unwrap_or(0);
            for _ in 0..count.max(0) {
                Box::pin(skip_reply(reader)).await?;
            }
        }
        _ => {} // Simple strings, errors, and integers end at the first line.
    }
    Ok(())
}

async fn run_worker_sync(thread_id: usize) -> u128 {
    let stream = TcpStream::connect("127.0.0.1:6380")
        .await
        .expect("Connection failed");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let start = Instant::now();

    for i in 0..OPS_PER_THREAD {
        let key = format!("bench_{}_{}", thread_id, i);
        let set_cmd = encode_command(&["SET".to_string(), key.clone(), format!("value_{}", i)]);
        writer.write_all(set_cmd.as_bytes()).await.unwrap();
        skip_reply(&mut reader).await.unwrap();

        let get_cmd = encode_command(&["GET".to_string(), key.clone()]);
        writer.write_all(get_cmd.as_bytes()).await.unwrap();
        skip_reply(&mut reader).await.unwrap();
    }

    start.elapsed().as_millis()
}

async fn run_worker_pipeline(thread_id: usize) -> u128 {
    let stream = TcpStream::connect("127.0.0.1:6380")
        .await
        .expect("Connection failed");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let start = Instant::now();

    let mut i = 0;
    while i < OPS_PER_THREAD {
        let batch_end = (i + PIPELINE_BATCH).min(OPS_PER_THREAD);
        let mut batch = String::new();
        let mut expected_responses = 0;

        for j in i..batch_end {
            let key = format!("bench_{}_{}", thread_id, j);
            let value = format!("value_{}", j);
            batch.push_str(&encode_command(&["SET".to_string(), key.clone(), value]));
            batch.push_str(&encode_command(&["GET".to_string(), key]));
            expected_responses += 2;
        }

        writer.write_all(batch.as_bytes()).await.unwrap();

        for _ in 0..expected_responses {
            skip_reply(&mut reader).await.unwrap();
        }

        i = batch_end;
    }

    start.elapsed().as_millis()
}

async fn run_benchmark(
    label: &str,
    worker: fn(usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = u128> + Send>>,
) {
    println!("\n=== {} ===", label);
    println!(
        "Threads: {} | Operations per thread: {}",
        NUM_THREADS, OPS_PER_THREAD
    );

    let total_start = Instant::now();
    let mut handles = vec![];

    for t in 0..NUM_THREADS {
        let handle = task::spawn(worker(t));
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total_elapsed = total_start.elapsed();
    let total_ops = NUM_THREADS * OPS_PER_THREAD * 2;

    println!("Total time: {} ms", total_elapsed.as_millis());
    println!("Total operations: {}", total_ops);

    let ops_per_sec = (total_ops as f64) / (total_elapsed.as_secs_f64());
    println!("Operations per second: {:.0} ops/sec", ops_per_sec);
}

#[tokio::main]
async fn main() {
    println!("OnyxDB Benchmark - Sync vs Pipeline comparison (RESP protocol)");
    println!("Connecting to server at 127.0.0.1:6380...");

    run_benchmark("SYNC mode (one request at a time)", |t| {
        Box::pin(run_worker_sync(t))
    })
    .await;
    run_benchmark("PIPELINE mode (batch of commands)", |t| {
        Box::pin(run_worker_pipeline(t))
    })
    .await;
}
