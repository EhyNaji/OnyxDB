use onyxdb::engine::EvictionPolicy;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;

const DEFAULT_PORT: u16 = 6380;
const DEFAULT_FAILOVER_TIMEOUT_SECS: u64 = 30;
const METRICS_PORT_OFFSET: u16 = 1000;
const MAX_SERVER_PORT: u16 = u16::MAX - METRICS_PORT_OFFSET;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "everysec" => Some(Self::EverySec),
            "no" => Some(Self::No),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct UpstreamCredentials {
    pub(crate) username: String,
    pub(crate) password: String,
}

pub(crate) struct ServerConfig {
    pub(crate) master_addr: Option<String>,
    pub(crate) upstream_credentials: Option<UpstreamCredentials>,
    pub(crate) users: HashMap<String, String>,
    pub(crate) fsync_policy: FsyncPolicy,
    pub(crate) maxmemory_bytes: usize,
    pub(crate) maxmemory_policy: EvictionPolicy,
    pub(crate) auto_failover: bool,
    pub(crate) failover_timeout_secs: u64,
    pub(crate) port: u16,
    pub(crate) warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConfigError {
    UpstreamUsernameWithoutPassword,
    UpstreamCredentialsWithoutReplica,
    InvalidPort(String),
}

impl fmt::Debug for UpstreamCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("master_addr", &self.master_addr)
            .field("upstream_credentials", &self.upstream_credentials)
            .field("user_count", &self.users.len())
            .field("fsync_policy", &self.fsync_policy)
            .field("maxmemory_bytes", &self.maxmemory_bytes)
            .field("maxmemory_policy", &self.maxmemory_policy)
            .field("auto_failover", &self.auto_failover)
            .field("failover_timeout_secs", &self.failover_timeout_secs)
            .field("port", &self.port)
            .field("warnings", &self.warnings)
            .finish()
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UpstreamUsernameWithoutPassword => formatter.write_str(
                "upstream replication username requires --masterauth or ONYXDB_MASTER_PASSWORD",
            ),
            Self::UpstreamCredentialsWithoutReplica => {
                formatter.write_str("upstream replication credentials require --replica-of")
            }
            Self::InvalidPort(value) => write!(
                formatter,
                "invalid server port '{}'; expected a value between 1 and {}",
                value, MAX_SERVER_PORT
            ),
        }
    }
}

impl Error for ConfigError {}

impl ServerConfig {
    pub(crate) fn from_process() -> Result<Self, ConfigError> {
        let args: Vec<String> = env::args().collect();
        Self::parse(&args, |name| env::var(name).ok())
    }

    fn parse<F>(args: &[String], get_env: F) -> Result<Self, ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut master_addr = None;
        let mut master_user = None;
        let mut master_password = None;
        let mut password = None;
        let mut appendfsync = None;
        let mut maxmemory = None;
        let mut maxmemory_policy = None;
        let mut users = HashMap::new();
        let mut warnings = Vec::new();
        let mut auto_failover = false;
        let mut failover_timeout_secs = DEFAULT_FAILOVER_TIMEOUT_SECS;
        let mut port = DEFAULT_PORT.to_string();

        for index in 0..args.len() {
            let next = || args.get(index + 1).cloned();
            match args[index].as_str() {
                "--replica-of" => master_addr = next().or(master_addr),
                "--masteruser" => master_user = next().or(master_user),
                "--masterauth" => master_password = next().or(master_password),
                "--requirepass" => password = next().or(password),
                "--appendfsync" => appendfsync = next().or(appendfsync),
                "--maxmemory" => maxmemory = next().or(maxmemory),
                "--maxmemory-policy" => maxmemory_policy = next().or(maxmemory_policy),
                "--user" => {
                    if let Some(value) = next() {
                        match value.split_once(':') {
                            Some((name, user_password)) => {
                                users.insert(name.to_string(), user_password.to_string());
                            }
                            None => warnings
                                .push("Invalid format for --user; expected name:password".into()),
                        }
                    }
                }
                "--auto-failover" => auto_failover = true,
                "--failover-timeout" => {
                    if let Some(value) = next() {
                        failover_timeout_secs = value
                            .parse::<u64>()
                            .unwrap_or(DEFAULT_FAILOVER_TIMEOUT_SECS);
                    }
                }
                "--port" => port = next().unwrap_or(port),
                _ => {}
            }
        }

        password = password.or_else(|| get_env("ONYXDB_PASSWORD"));
        master_user = master_user.or_else(|| get_env("ONYXDB_MASTER_USER"));
        master_password = master_password.or_else(|| get_env("ONYXDB_MASTER_PASSWORD"));

        if master_user.is_some() && master_password.is_none() {
            return Err(ConfigError::UpstreamUsernameWithoutPassword);
        }
        if master_addr.is_none() && (master_user.is_some() || master_password.is_some()) {
            return Err(ConfigError::UpstreamCredentialsWithoutReplica);
        }

        let upstream_credentials = master_password.map(|password| UpstreamCredentials {
            username: master_user.unwrap_or_else(|| "default".to_string()),
            password,
        });
        if let Some(password) = password {
            users.insert("default".to_string(), password);
        }

        let fsync_policy = match appendfsync.as_deref() {
            Some(value) => FsyncPolicy::parse(value).unwrap_or_else(|| {
                warnings.push(format!(
                    "Invalid value for --appendfsync ('{}'), using 'everysec' as default",
                    value
                ));
                FsyncPolicy::EverySec
            }),
            None => FsyncPolicy::EverySec,
        };
        let maxmemory_bytes = match maxmemory.as_deref() {
            Some(value) => parse_memory_size(value).unwrap_or_else(|| {
                warnings.push(format!(
                    "Invalid value for --maxmemory ('{}'); memory limiting is disabled",
                    value
                ));
                0
            }),
            None => 0,
        };
        let maxmemory_policy = match maxmemory_policy.as_deref() {
            Some(value) => EvictionPolicy::parse(value).unwrap_or_else(|| {
                warnings.push(format!(
                    "Invalid value for --maxmemory-policy ('{}'); using 'noeviction'",
                    value
                ));
                EvictionPolicy::NoEviction
            }),
            None => EvictionPolicy::NoEviction,
        };
        let port = port
            .parse::<u16>()
            .ok()
            .filter(|port| (1..=MAX_SERVER_PORT).contains(port))
            .ok_or_else(|| ConfigError::InvalidPort(port.clone()))?;

        Ok(Self {
            master_addr,
            upstream_credentials,
            users,
            fsync_policy,
            maxmemory_bytes,
            maxmemory_policy,
            auto_failover,
            failover_timeout_secs,
            port,
            warnings,
        })
    }
}

fn parse_memory_size(value: &str) -> Option<usize> {
    let value = value.trim().to_ascii_lowercase();
    let (number, multiplier) = if let Some(number) = value.strip_suffix("gb") {
        (number, 1024 * 1024 * 1024)
    } else if let Some(number) = value.strip_suffix("mb") {
        (number, 1024 * 1024)
    } else if let Some(number) = value.strip_suffix("kb") {
        (number, 1024)
    } else if let Some(number) = value.strip_suffix('b') {
        (number, 1)
    } else {
        (value.as_str(), 1)
    };
    number
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn parse(values: &[&str]) -> Result<ServerConfig, ConfigError> {
        ServerConfig::parse(&arguments(values), |_| None)
    }

    #[test]
    fn defaults_are_stable() {
        let config = parse(&["onyxdb"]).unwrap();
        assert_eq!(config.port, DEFAULT_PORT);
        assert_eq!(config.fsync_policy, FsyncPolicy::EverySec);
        assert_eq!(config.maxmemory_bytes, 0);
        assert_eq!(config.maxmemory_policy, EvictionPolicy::NoEviction);
        assert_eq!(config.failover_timeout_secs, DEFAULT_FAILOVER_TIMEOUT_SECS);
        assert!(config.users.is_empty());
        assert!(config.warnings.is_empty());
    }

    #[test]
    fn command_line_values_override_environment_credentials() {
        let args = arguments(&[
            "onyxdb",
            "--replica-of",
            "127.0.0.1:6380",
            "--masteruser",
            "cli-user",
            "--masterauth",
            "cli-secret",
            "--requirepass",
            "client-secret",
        ]);
        let config = ServerConfig::parse(&args, |name| match name {
            "ONYXDB_PASSWORD" => Some("environment-client-secret".into()),
            "ONYXDB_MASTER_USER" => Some("environment-user".into()),
            "ONYXDB_MASTER_PASSWORD" => Some("environment-master-secret".into()),
            _ => None,
        })
        .unwrap();

        assert_eq!(config.users.get("default").unwrap(), "client-secret");
        assert_eq!(
            config.upstream_credentials,
            Some(UpstreamCredentials {
                username: "cli-user".into(),
                password: "cli-secret".into(),
            })
        );
    }

    #[test]
    fn repeated_options_preserve_the_last_complete_value() {
        let config = parse(&[
            "onyxdb",
            "--port",
            "7000",
            "--port",
            "7001",
            "--user",
            "reader:first",
            "--user",
            "reader:second",
        ])
        .unwrap();
        assert_eq!(config.port, 7001);
        assert_eq!(config.users.get("reader").unwrap(), "second");
    }

    #[test]
    fn upstream_credentials_require_a_replica_and_complete_authentication() {
        assert_eq!(
            parse(&["onyxdb", "--masterauth", "secret"]).unwrap_err(),
            ConfigError::UpstreamCredentialsWithoutReplica
        );
        assert_eq!(
            parse(&[
                "onyxdb",
                "--replica-of",
                "127.0.0.1:6380",
                "--masteruser",
                "replica"
            ])
            .unwrap_err(),
            ConfigError::UpstreamUsernameWithoutPassword
        );
    }

    #[test]
    fn server_port_reserves_obp_and_metrics_ports() {
        assert_eq!(
            parse(&["onyxdb", "--port", "64536"]).unwrap_err(),
            ConfigError::InvalidPort("64536".into())
        );
        assert_eq!(
            parse(&["onyxdb", "--port", "invalid"]).unwrap_err(),
            ConfigError::InvalidPort("invalid".into())
        );
        assert_eq!(parse(&["onyxdb", "--port", "64535"]).unwrap().port, 64535);
    }

    #[test]
    fn debug_output_redacts_all_passwords() {
        let config = parse(&[
            "onyxdb",
            "--replica-of",
            "127.0.0.1:6380",
            "--masteruser",
            "replica",
            "--masterauth",
            "upstream-secret",
            "--requirepass",
            "client-secret",
        ])
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("upstream-secret"));
        assert!(!debug.contains("client-secret"));
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("user_count: 1"));
    }

    #[test]
    fn invalid_non_secret_values_fall_back_with_warnings() {
        let config = parse(&[
            "onyxdb",
            "--appendfsync",
            "sometimes",
            "--maxmemory",
            "too-large",
            "--maxmemory-policy",
            "unknown",
            "--failover-timeout",
            "never",
            "--user",
            "malformed",
        ])
        .unwrap();
        assert_eq!(config.fsync_policy, FsyncPolicy::EverySec);
        assert_eq!(config.maxmemory_bytes, 0);
        assert_eq!(config.maxmemory_policy, EvictionPolicy::NoEviction);
        assert_eq!(config.failover_timeout_secs, DEFAULT_FAILOVER_TIMEOUT_SECS);
        assert_eq!(config.warnings.len(), 4);
        assert!(
            config
                .warnings
                .iter()
                .all(|warning| !warning.contains("secret"))
        );
    }

    #[test]
    fn memory_sizes_are_bounded_and_case_insensitive() {
        assert_eq!(parse_memory_size("2KB"), Some(2 * 1024));
        assert_eq!(parse_memory_size("3 mb"), Some(3 * 1024 * 1024));
        assert_eq!(parse_memory_size("19"), Some(19));
        assert_eq!(parse_memory_size(&format!("{}gb", usize::MAX)), None);
    }
}
