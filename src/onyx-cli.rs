use onyxdb::client::{RespClient, parse_command_line};
use onyxdb::command::is_replica_routable_read;
use std::io::{self, Write};

#[derive(Debug, PartialEq, Eq)]
struct CliConfig {
    master_address: String,
    replica_addresses: Vec<String>,
}

impl CliConfig {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let mut master_address = "127.0.0.1:6380".to_string();
        let mut replica_addresses = Vec::new();
        let mut index = 1;
        while index < arguments.len() {
            match arguments[index].as_str() {
                "--port" => {
                    let port = arguments
                        .get(index + 1)
                        .ok_or_else(|| "--port requires a value".to_string())?;
                    port.parse::<u16>()
                        .map_err(|_| format!("invalid port: {port}"))?;
                    master_address = format!("127.0.0.1:{port}");
                    index += 2;
                }
                "--master" => {
                    master_address = arguments
                        .get(index + 1)
                        .ok_or_else(|| "--master requires an address".to_string())?
                        .clone();
                    index += 2;
                }
                "--replicas" => {
                    replica_addresses = arguments
                        .get(index + 1)
                        .ok_or_else(|| {
                            "--replicas requires a comma-separated address list".to_string()
                        })?
                        .split(',')
                        .map(str::trim)
                        .filter(|address| !address.is_empty())
                        .map(str::to_string)
                        .collect();
                    index += 2;
                }
                "--help" | "-h" => return Err(String::new()),
                option => return Err(format!("unknown option: {option}")),
            }
        }
        Ok(Self {
            master_address,
            replica_addresses,
        })
    }
}

fn print_usage() {
    println!("Usage: onyx-cli [--port <port> | --master <host:port>] [--replicas <host:port,...>]");
    println!("Quoted and escaped command arguments are preserved.");
}

#[tokio::main]
async fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    let config = match CliConfig::parse(&arguments) {
        Ok(config) => config,
        Err(error) => {
            if !error.is_empty() {
                eprintln!("Configuration error: {error}");
            }
            print_usage();
            return;
        }
    };

    println!("OnyxDB CLI - master: {}", config.master_address);
    let mut master = match RespClient::connect(&config.master_address).await {
        Ok(connection) => connection,
        Err(error) => {
            eprintln!(
                "Unable to connect to master {}: {}",
                config.master_address, error
            );
            return;
        }
    };

    let mut replicas = Vec::new();
    for address in config.replica_addresses {
        match RespClient::connect(&address).await {
            Ok(connection) => replicas.push((address, connection)),
            Err(error) => eprintln!("Unable to connect to replica {address}: {error}"),
        }
    }
    if !replicas.is_empty() {
        println!("Connected read replicas: {}", replicas.len());
    }
    println!("Type 'exit' to quit.");

    let stdin = io::stdin();
    let mut replica_index = 0usize;
    loop {
        print!("onyx> ");
        if io::stdout().flush().is_err() {
            break;
        }

        let mut input = String::new();
        match stdin.read_line(&mut input) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let arguments = match parse_command_line(input.trim_end()) {
            Ok(arguments) if arguments.is_empty() => continue,
            Ok(arguments) => arguments,
            Err(error) => {
                eprintln!("Parse error: {error}");
                continue;
            }
        };
        if arguments.len() == 1 && arguments[0].eq_ignore_ascii_case("exit") {
            println!("Goodbye!");
            break;
        }

        let command = arguments[0].to_ascii_uppercase();
        let result = if is_replica_routable_read(&command) && !replicas.is_empty() {
            let index = replica_index % replicas.len();
            replica_index = replica_index.wrapping_add(1);
            let (address, connection) = &mut replicas[index];
            println!("[reading from replica {address}]");
            connection.send(&arguments).await
        } else {
            master.send(&arguments).await
        };

        match result {
            Ok(response) => println!("{response}"),
            Err(error) => eprintln!("Connection error: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn cli_configuration_is_strict_and_deterministic() {
        let config = CliConfig::parse(&arguments(&[
            "onyx-cli",
            "--port",
            "7000",
            "--master",
            "db.example:7001",
            "--replicas",
            "replica-a:7001, replica-b:7001",
        ]))
        .unwrap();
        assert_eq!(config.master_address, "db.example:7001");
        assert_eq!(
            config.replica_addresses,
            ["replica-a:7001", "replica-b:7001"]
        );
        assert!(CliConfig::parse(&arguments(&["onyx-cli", "--port"])).is_err());
        assert!(CliConfig::parse(&arguments(&["onyx-cli", "--unknown"])).is_err());
    }
}
