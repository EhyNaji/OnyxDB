use onyxdb::client::{MAX_PIPELINE_BYTES, MAX_PIPELINE_COMMANDS, RESPResponse, RespClient};
use serde_json::json;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_REQUESTS: usize = 100_000;
const DEFAULT_WARMUP_REQUESTS: usize = 10_000;
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_PIPELINE: usize = 1;
const DEFAULT_KEYSPACE: usize = 10_000;
const DEFAULT_PAYLOAD_SIZE: usize = 64;
const DEFAULT_REPEATS: usize = 3;
const MAX_REQUESTS: usize = 10_000_000;
const MAX_CONCURRENCY: usize = 1024;
const MAX_KEYSPACE: usize = 1_000_000;
const MAX_PAYLOAD_SIZE: usize = 8 * 1024 * 1024 - 1024;
const MAX_REPEATS: usize = 100;
const SETUP_PIPELINE: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Get,
    Set,
    Mixed,
    JsonGet,
    JsonSet,
}

impl Workload {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "get" => Some(Self::Get),
            "set" => Some(Self::Set),
            "mixed" => Some(Self::Mixed),
            "json-get" => Some(Self::JsonGet),
            "json-set" => Some(Self::JsonSet),
            _ => None,
        }
    }

    fn requires_setup(self) -> bool {
        matches!(self, Self::Get | Self::Mixed | Self::JsonGet)
    }

    fn is_redis_comparable(self) -> bool {
        matches!(self, Self::Get | Self::Set | Self::Mixed)
    }

    fn requires_changing_string_value(self) -> bool {
        matches!(self, Self::Set | Self::Mixed)
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Get => "get",
            Self::Set => "set",
            Self::Mixed => "mixed",
            Self::JsonGet => "json-get",
            Self::JsonSet => "json-set",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BenchmarkConfig {
    address: String,
    server_label: String,
    workload: Workload,
    requests: usize,
    warmup_requests: usize,
    concurrency: usize,
    pipeline: usize,
    keyspace: usize,
    payload_size: usize,
    repeats: usize,
    output: OutputFormat,
    key_prefix: String,
    keep_data: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            address: "127.0.0.1:6380".into(),
            server_label: "onyxdb".into(),
            workload: Workload::Mixed,
            requests: DEFAULT_REQUESTS,
            warmup_requests: DEFAULT_WARMUP_REQUESTS,
            concurrency: DEFAULT_CONCURRENCY,
            pipeline: DEFAULT_PIPELINE,
            keyspace: DEFAULT_KEYSPACE,
            payload_size: DEFAULT_PAYLOAD_SIZE,
            repeats: DEFAULT_REPEATS,
            output: OutputFormat::Human,
            key_prefix: format!("onyxbench:{}:{timestamp}", std::process::id()),
            keep_data: false,
        }
    }
}

impl BenchmarkConfig {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let mut config = Self::default();
        let mut index = 1;
        while index < arguments.len() {
            let option = arguments[index].as_str();
            let value = || {
                arguments
                    .get(index + 1)
                    .ok_or_else(|| format!("{option} requires a value"))
            };
            match option {
                "--address" => config.address = value()?.clone(),
                "--label" => config.server_label = value()?.clone(),
                "--workload" => {
                    let workload = value()?;
                    config.workload = Workload::parse(workload)
                        .ok_or_else(|| format!("unsupported workload: {workload}"))?;
                }
                "--requests" => config.requests = parse_usize(option, value()?)?,
                "--warmup" => config.warmup_requests = parse_usize(option, value()?)?,
                "--concurrency" => config.concurrency = parse_usize(option, value()?)?,
                "--pipeline" => config.pipeline = parse_usize(option, value()?)?,
                "--keyspace" => config.keyspace = parse_usize(option, value()?)?,
                "--payload-size" => config.payload_size = parse_usize(option, value()?)?,
                "--repeats" => config.repeats = parse_usize(option, value()?)?,
                "--output" => {
                    config.output = match value()?.as_str() {
                        "human" => OutputFormat::Human,
                        "json" => OutputFormat::Json,
                        value => return Err(format!("unsupported output format: {value}")),
                    };
                }
                "--key-prefix" => config.key_prefix = value()?.clone(),
                "--keep-data" => {
                    config.keep_data = true;
                    index += 1;
                    continue;
                }
                "--help" | "-h" => return Err(String::new()),
                _ => return Err(format!("unknown option: {option}")),
            }
            index += 2;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        validate_range("requests", self.requests, 1, MAX_REQUESTS)?;
        if self.warmup_requests > MAX_REQUESTS {
            return Err(format!("warmup must not exceed {MAX_REQUESTS}"));
        }
        validate_range("concurrency", self.concurrency, 1, MAX_CONCURRENCY)?;
        validate_range("pipeline", self.pipeline, 1, MAX_PIPELINE_COMMANDS)?;
        validate_range("keyspace", self.keyspace, 1, MAX_KEYSPACE)?;
        validate_range("payload-size", self.payload_size, 0, MAX_PAYLOAD_SIZE)?;
        if self.payload_size == 0 && self.workload.requires_changing_string_value() {
            return Err("payload-size must be at least 1 for set and mixed workloads".into());
        }
        validate_range("repeats", self.repeats, 1, MAX_REPEATS)?;
        if self.server_label.trim().is_empty() {
            return Err("label must not be empty".into());
        }
        if self.server_label.len() > 256 {
            return Err("label must not exceed 256 bytes".into());
        }
        if self.address.len() > 1024 {
            return Err("address must not exceed 1024 bytes".into());
        }
        if self.key_prefix.is_empty() {
            return Err("key-prefix must not be empty".into());
        }
        if self.key_prefix.len() > 1024 {
            return Err("key-prefix must not exceed 1024 bytes".into());
        }
        let projected_batch_bytes = self
            .estimated_command_bytes()
            .checked_mul(self.pipeline)
            .ok_or_else(|| "pipeline byte projection overflow".to_string())?;
        if projected_batch_bytes > MAX_PIPELINE_BYTES {
            return Err(format!(
                "pipeline and payload project to more than {} buffered bytes",
                MAX_PIPELINE_BYTES
            ));
        }
        Ok(())
    }

    fn estimated_command_bytes(&self) -> usize {
        self.payload_size
            .saturating_add(self.key_prefix.len())
            .saturating_add(512)
    }

    fn setup_pipeline(&self) -> usize {
        SETUP_PIPELINE
            .min(MAX_PIPELINE_BYTES / self.estimated_command_bytes().max(1))
            .max(1)
    }
}

fn parse_usize(option: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("{option} requires a non-negative integer"))
}

fn validate_range(name: &str, value: usize, minimum: usize, maximum: usize) -> Result<(), String> {
    if (minimum..=maximum).contains(&value) {
        Ok(())
    } else {
        Err(format!("{name} must be between {minimum} and {maximum}"))
    }
}

#[derive(Clone)]
struct Authentication {
    username: Option<String>,
    password: String,
}

fn authentication_from_environment() -> Result<Option<Authentication>, String> {
    let username = std::env::var("ONYXDB_BENCH_USER").ok();
    let password = std::env::var("ONYXDB_BENCH_PASSWORD").ok();
    match (username, password) {
        (Some(_), None) => Err("ONYXDB_BENCH_USER requires ONYXDB_BENCH_PASSWORD".into()),
        (username, Some(password)) => Ok(Some(Authentication { username, password })),
        (None, None) => Ok(None),
    }
}

async fn connect_client(
    address: &str,
    authentication: Option<&Authentication>,
) -> Result<RespClient, Box<dyn Error + Send + Sync>> {
    let mut client = RespClient::connect(address).await?;
    if let Some(authentication) = authentication {
        let mut command = vec!["AUTH".to_string()];
        if let Some(username) = &authentication.username {
            command.push(username.clone());
        }
        command.push(authentication.password.clone());
        let response = client.send(&command).await?;
        if response.is_error() {
            return Err(format!("benchmark authentication failed: {response}").into());
        }
    }
    Ok(client)
}

#[derive(Clone)]
struct WorkloadData {
    config: Arc<BenchmarkConfig>,
    value: Arc<String>,
    json_value: Arc<String>,
}

impl WorkloadData {
    fn new(config: Arc<BenchmarkConfig>) -> Self {
        let value = "x".repeat(config.payload_size);
        let json_value = json!({"payload": value, "counter": 0}).to_string();
        Self {
            config,
            value: Arc::new(value),
            json_value: Arc::new(json_value),
        }
    }

    fn key(&self, operation: usize) -> String {
        format!(
            "{}:{}",
            self.config.key_prefix,
            operation % self.config.keyspace
        )
    }

    fn string_value(&self, operation: usize) -> String {
        if self.config.payload_size == 0 {
            return String::new();
        }
        let generation = operation / self.config.keyspace;
        let marker = format!("{generation:016x}");
        let marker_length = marker.len().min(self.config.payload_size);
        let marker_start = marker.len() - marker_length;
        let mut value = (*self.value).clone();
        value.replace_range(..marker_length, &marker[marker_start..]);
        value
    }

    fn json_value(&self, operation: usize) -> String {
        json!({
            "payload": self.value.as_str(),
            "counter": operation,
        })
        .to_string()
    }

    fn command(&self, operation: usize) -> Vec<String> {
        let key = self.key(operation);
        match self.config.workload {
            Workload::Get => vec!["GET".into(), key],
            Workload::Set => vec!["SET".into(), key, self.string_value(operation)],
            Workload::Mixed if operation.is_multiple_of(2) => {
                vec!["SET".into(), key, self.string_value(operation)]
            }
            Workload::Mixed => vec!["GET".into(), key],
            Workload::JsonGet => vec!["JSON.GET".into(), key, "$.payload".into()],
            Workload::JsonSet => vec![
                "JSON.SET".into(),
                key,
                "$".into(),
                self.json_value(operation),
            ],
        }
    }

    fn setup_command(&self, key_index: usize) -> Vec<String> {
        let key = self.key(key_index);
        match self.config.workload {
            Workload::JsonGet => vec![
                "JSON.SET".into(),
                key,
                "$".into(),
                (*self.json_value).clone(),
            ],
            _ => vec!["SET".into(), key, (*self.value).clone()],
        }
    }

    fn response_is_valid(&self, operation: usize, response: &RESPResponse) -> bool {
        match self.config.workload {
            Workload::Get | Workload::JsonGet => {
                matches!(response, RESPResponse::BulkString(Some(_)))
            }
            Workload::Set | Workload::JsonSet => is_ok(response),
            Workload::Mixed if operation.is_multiple_of(2) => is_ok(response),
            Workload::Mixed => matches!(response, RESPResponse::BulkString(Some(_))),
        }
    }
}

fn is_ok(response: &RESPResponse) -> bool {
    matches!(response, RESPResponse::SimpleString(value) if value.as_ref() == b"OK")
}

async fn prepare_dataset(
    data: &WorkloadData,
    authentication: Option<&Authentication>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if !data.config.workload.requires_setup() {
        return Ok(());
    }
    let mut client = connect_client(&data.config.address, authentication).await?;
    let mut start = 0;
    while start < data.config.keyspace {
        let end = (start + data.config.setup_pipeline()).min(data.config.keyspace);
        let commands = (start..end)
            .map(|index| data.setup_command(index))
            .collect::<Vec<_>>();
        client.write_pipeline(&commands).await?;
        for _ in start..end {
            let response = client.read().await?;
            if !is_ok(&response) {
                return Err(format!("dataset setup failed: {response}").into());
            }
        }
        start = end;
    }
    Ok(())
}

async fn cleanup_dataset(
    data: &WorkloadData,
    authentication: Option<&Authentication>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if data.config.keep_data {
        return Ok(());
    }
    let mut client = connect_client(&data.config.address, authentication).await?;
    let mut start = 0;
    while start < data.config.keyspace {
        let end = (start + SETUP_PIPELINE).min(data.config.keyspace);
        let commands = (start..end)
            .map(|index| vec!["DEL".to_string(), data.key(index)])
            .collect::<Vec<_>>();
        client.write_pipeline(&commands).await?;
        for _ in start..end {
            let response = client.read().await?;
            if response.is_error() {
                return Err(format!("dataset cleanup failed: {response}").into());
            }
        }
        start = end;
    }
    Ok(())
}

#[derive(Default)]
struct WorkerResult {
    completed: usize,
    errors: usize,
    transport_errors: usize,
    latency_nanoseconds: Vec<u64>,
}

async fn run_worker(
    mut client: RespClient,
    data: WorkloadData,
    first_operation: usize,
    request_count: usize,
    collect_latency: bool,
) -> WorkerResult {
    let mut result = WorkerResult {
        latency_nanoseconds: if collect_latency {
            Vec::with_capacity(request_count)
        } else {
            Vec::new()
        },
        ..WorkerResult::default()
    };
    let mut completed = 0;
    'requests: while completed < request_count {
        let batch_size = data
            .config
            .pipeline
            .min(request_count.saturating_sub(completed));
        let batch_start_index = first_operation + completed;
        let commands = (0..batch_size)
            .map(|offset| data.command(batch_start_index + offset))
            .collect::<Vec<_>>();
        let started = Instant::now();
        if client.write_pipeline(&commands).await.is_err() {
            result.errors += batch_size;
            result.transport_errors += batch_size;
            break;
        }
        for offset in 0..batch_size {
            let response = match client.read().await {
                Ok(response) => response,
                Err(_) => {
                    let unread = batch_size - offset;
                    result.errors += unread;
                    result.transport_errors += unread;
                    break 'requests;
                }
            };
            result.completed += 1;
            if collect_latency {
                result
                    .latency_nanoseconds
                    .push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
            if !data.response_is_valid(batch_start_index + offset, &response) {
                result.errors += 1;
            }
        }
        completed += batch_size;
    }
    result
}

#[derive(Clone, Debug)]
struct RunResult {
    elapsed: Duration,
    requested: usize,
    completed: usize,
    errors: usize,
    transport_errors: usize,
    latency_nanoseconds: Vec<u64>,
}

impl RunResult {
    fn operations_per_second(&self) -> f64 {
        self.completed as f64 / self.elapsed.as_secs_f64()
    }

    fn percentile_microseconds(&self, percentile: f64) -> f64 {
        percentile_nanoseconds(&self.latency_nanoseconds, percentile) as f64 / 1_000.0
    }
}

async fn run_phase(
    data: &WorkloadData,
    authentication: Option<&Authentication>,
    requests: usize,
    operation_offset: usize,
    collect_latency: bool,
) -> Result<RunResult, Box<dyn Error + Send + Sync>> {
    if requests == 0 {
        return Ok(RunResult {
            elapsed: Duration::ZERO,
            requested: 0,
            completed: 0,
            errors: 0,
            transport_errors: 0,
            latency_nanoseconds: Vec::new(),
        });
    }
    let worker_count = data.config.concurrency.min(requests);
    let mut clients = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        clients.push(connect_client(&data.config.address, authentication).await?);
    }

    let base_requests = requests / worker_count;
    let extra_requests = requests % worker_count;
    let started = Instant::now();
    let mut handles = Vec::with_capacity(worker_count);
    let mut first_operation = operation_offset;
    for (worker, client) in clients.into_iter().enumerate() {
        let request_count = base_requests + usize::from(worker < extra_requests);
        let worker_first_operation = first_operation;
        first_operation += request_count;
        handles.push(tokio::spawn(run_worker(
            client,
            data.clone(),
            worker_first_operation,
            request_count,
            collect_latency,
        )));
    }

    let mut result = RunResult {
        elapsed: Duration::ZERO,
        requested: requests,
        completed: 0,
        errors: 0,
        transport_errors: 0,
        latency_nanoseconds: if collect_latency {
            Vec::with_capacity(requests)
        } else {
            Vec::new()
        },
    };
    for handle in handles {
        let worker = handle.await?;
        result.completed += worker.completed;
        result.errors += worker.errors;
        result.transport_errors += worker.transport_errors;
        result
            .latency_nanoseconds
            .extend(worker.latency_nanoseconds);
    }
    result.elapsed = started.elapsed();
    result.latency_nanoseconds.sort_unstable();
    Ok(result)
}

fn percentile_nanoseconds(sorted_values: &[u64], percentile: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let rank = ((percentile / 100.0) * sorted_values.len() as f64).ceil() as usize;
    sorted_values[rank.saturating_sub(1).min(sorted_values.len() - 1)]
}

fn print_usage() {
    println!(
        "Usage: onyx-bench [options]\n\
         \n\
         --address <host:port>       Target server (default 127.0.0.1:6380)\n\
         --label <name>              Server label in reports (default onyxdb)\n\
         --workload <name>           get, set, mixed, json-get, or json-set\n\
         --requests <count>          Measured operations per run\n\
         --warmup <count>            Unmeasured warmup operations\n\
         --concurrency <count>       Concurrent connections\n\
         --pipeline <count>          Commands submitted per connection batch\n\
         --keyspace <count>          Number of benchmark keys\n\
         --payload-size <bytes>      String payload bytes\n\
         --repeats <count>           Number of measured runs\n\
         --output <human|json>       Report format\n\
         --key-prefix <prefix>       Explicit dataset prefix\n\
         --keep-data                 Do not delete benchmark keys\n\
         \n\
         Authentication uses ONYXDB_BENCH_PASSWORD and optional ONYXDB_BENCH_USER."
    );
}

fn report_human(config: &BenchmarkConfig, authenticated: bool, results: &[RunResult]) {
    println!("OnyxDB benchmark methodology v1");
    println!("Target: {} ({})", config.server_label, config.address);
    println!(
        "Environment: {} {} | logical CPUs: {} | benchmark version: {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism().map_or(1, usize::from),
        env!("CARGO_PKG_VERSION")
    );
    println!(
        "Workload: {} | RESP-comparable: {} | requests/run: {} | warmup: {}",
        config.workload,
        config.workload.is_redis_comparable(),
        config.requests,
        config.warmup_requests
    );
    println!(
        "Concurrency: {} | pipeline: {} | keyspace: {} | payload: {} bytes | auth: {}",
        config.concurrency, config.pipeline, config.keyspace, config.payload_size, authenticated
    );
    println!(
        "Latency definition: client-observed response completion from pipeline batch submission"
    );
    for (index, result) in results.iter().enumerate() {
        println!(
            "Run {}: {:.0} ops/s | completed {}/{} | errors {} (transport {}) | p50 {:.3} us | p95 {:.3} us | p99 {:.3} us | p99.9 {:.3} us",
            index + 1,
            result.operations_per_second(),
            result.completed,
            result.requested,
            result.errors,
            result.transport_errors,
            result.percentile_microseconds(50.0),
            result.percentile_microseconds(95.0),
            result.percentile_microseconds(99.0),
            result.percentile_microseconds(99.9),
        );
    }
}

fn report_json(config: &BenchmarkConfig, authenticated: bool, results: &[RunResult]) {
    let runs = results
        .iter()
        .enumerate()
        .map(|(index, result)| {
            json!({
                "run": index + 1,
                "elapsed_seconds": result.elapsed.as_secs_f64(),
                "requested": result.requested,
                "completed": result.completed,
                "errors": result.errors,
                "transport_errors": result.transport_errors,
                "operations_per_second": result.operations_per_second(),
                "latency_microseconds": {
                    "p50": result.percentile_microseconds(50.0),
                    "p95": result.percentile_microseconds(95.0),
                    "p99": result.percentile_microseconds(99.0),
                    "p99_9": result.percentile_microseconds(99.9),
                }
            })
        })
        .collect::<Vec<_>>();
    let report = json!({
        "methodology_version": 1,
        "target": {"label": config.server_label, "address": config.address},
        "environment": {
            "os": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism().map_or(1, usize::from),
            "benchmark_version": env!("CARGO_PKG_VERSION"),
        },
        "configuration": {
            "workload": config.workload.to_string(),
            "redis_comparable": config.workload.is_redis_comparable(),
            "requests_per_run": config.requests,
            "warmup_requests": config.warmup_requests,
            "concurrency": config.concurrency,
            "pipeline": config.pipeline,
            "keyspace": config.keyspace,
            "payload_size_bytes": config.payload_size,
            "repeats": config.repeats,
            "authenticated": authenticated,
            "key_prefix": config.key_prefix,
            "keep_data": config.keep_data,
            "latency_definition": "client-observed response completion from pipeline batch submission",
        },
        "runs": runs,
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let config = match BenchmarkConfig::parse(&arguments) {
        Ok(config) => Arc::new(config),
        Err(error) => {
            if !error.is_empty() {
                eprintln!("Configuration error: {error}");
            }
            print_usage();
            return Ok(());
        }
    };
    let authentication = authentication_from_environment()?;
    let data = WorkloadData::new(Arc::clone(&config));

    prepare_dataset(&data, authentication.as_ref()).await?;
    let warmup = run_phase(
        &data,
        authentication.as_ref(),
        config.warmup_requests,
        0,
        false,
    )
    .await?;
    if warmup.errors > 0 || warmup.completed != warmup.requested {
        cleanup_dataset(&data, authentication.as_ref()).await?;
        return Err(format!(
            "warmup failed: completed {}/{}, errors {}",
            warmup.completed, warmup.requested, warmup.errors
        )
        .into());
    }

    let mut results = Vec::with_capacity(config.repeats);
    for repeat in 0..config.repeats {
        results.push(
            run_phase(
                &data,
                authentication.as_ref(),
                config.requests,
                repeat.saturating_mul(config.requests),
                true,
            )
            .await?,
        );
    }
    cleanup_dataset(&data, authentication.as_ref()).await?;

    match config.output {
        OutputFormat::Human => report_human(&config, authentication.is_some(), &results),
        OutputFormat::Json => report_json(&config, authentication.is_some(), &results),
    }
    if results
        .iter()
        .any(|result| result.errors > 0 || result.completed != result.requested)
    {
        return Err("one or more benchmark runs completed with errors".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn benchmark_configuration_is_strict_and_bounded() {
        let config = BenchmarkConfig::parse(&arguments(&[
            "onyx-bench",
            "--address",
            "server:6379",
            "--label",
            "redis",
            "--workload",
            "get",
            "--requests",
            "1000",
            "--warmup",
            "100",
            "--concurrency",
            "4",
            "--pipeline",
            "16",
            "--keyspace",
            "250",
            "--payload-size",
            "32",
            "--repeats",
            "2",
            "--output",
            "json",
            "--key-prefix",
            "controlled",
            "--keep-data",
        ]))
        .unwrap();
        assert_eq!(config.workload, Workload::Get);
        assert_eq!(config.pipeline, 16);
        assert_eq!(config.output, OutputFormat::Json);
        assert!(config.keep_data);

        for invalid in [
            arguments(&["onyx-bench", "--requests", "0"]),
            arguments(&["onyx-bench", "--pipeline", "4097"]),
            arguments(&[
                "onyx-bench",
                "--pipeline",
                "4096",
                "--payload-size",
                "8387584",
            ]),
            arguments(&["onyx-bench", "--workload", "unknown"]),
            arguments(&["onyx-bench", "--output", "csv"]),
            arguments(&["onyx-bench", "--unknown"]),
        ] {
            assert!(BenchmarkConfig::parse(&invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn workload_generation_is_deterministic_and_validates_response_types() {
        let config = Arc::new(BenchmarkConfig {
            workload: Workload::Mixed,
            key_prefix: "bench".into(),
            keyspace: 2,
            payload_size: 3,
            ..BenchmarkConfig::default()
        });
        let data = WorkloadData::new(config);
        assert_eq!(data.command(0), ["SET", "bench:0", "000"]);
        assert_eq!(data.command(1), ["GET", "bench:1"]);
        assert_eq!(data.command(2), ["SET", "bench:0", "001"]);
        assert_ne!(data.command(0), data.command(2));
        assert!(data.response_is_valid(0, &RESPResponse::SimpleString(Bytes::from_static(b"OK"))));
        assert!(data.response_is_valid(
            1,
            &RESPResponse::BulkString(Some(Bytes::from_static(b"xxx")))
        ));
        assert!(!data.response_is_valid(1, &RESPResponse::BulkString(None)));
    }

    #[test]
    fn percentile_uses_nearest_rank_and_handles_empty_samples() {
        let samples = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile_nanoseconds(&samples, 50.0), 5);
        assert_eq!(percentile_nanoseconds(&samples, 95.0), 10);
        assert_eq!(percentile_nanoseconds(&samples, 99.9), 10);
        assert_eq!(percentile_nanoseconds(&[], 99.0), 0);
    }

    #[test]
    fn string_write_workloads_reject_zero_sized_payloads() {
        let error = BenchmarkConfig::parse(&arguments(&[
            "onyx-bench",
            "--workload",
            "set",
            "--payload-size",
            "0",
        ]))
        .unwrap_err();
        assert_eq!(
            error,
            "payload-size must be at least 1 for set and mixed workloads"
        );
    }
}
