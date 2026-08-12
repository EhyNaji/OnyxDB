/// Returns whether a command can mutate authoritative database state.
///
/// Callers must normalize command names to uppercase before classification.
pub fn is_write_command(command: &str) -> bool {
    matches!(
        command,
        "SET"
            | "GETSET"
            | "SETNX"
            | "MSET"
            | "DEL"
            | "EXPIRE"
            | "EXPIREAT"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "HSET"
            | "SADD"
            | "RENAME"
            | "INCR"
            | "INCRBY"
            | "DECRBY"
            | "APPEND"
            | "HDEL"
            | "SREM"
            | "COPY"
            | "JSON.SET"
            | "JSON.DEL"
            | "JSON.NUMINCRBY"
            | "JSON.ARRAPPEND"
    )
}

/// Returns whether the bundled CLI may route a command to a read-only replica.
pub fn is_replica_routable_read(command: &str) -> bool {
    matches!(
        command,
        "GET"
            | "MGET"
            | "LRANGE"
            | "LLEN"
            | "HGET"
            | "HGETALL"
            | "HKEYS"
            | "HVALS"
            | "SMEMBERS"
            | "SISMEMBER"
            | "EXISTS"
            | "TYPE"
            | "TTL"
            | "KEYS"
            | "STRLEN"
            | "JSON.GET"
            | "JSON.TYPE"
            | "JSON.ARRLEN"
            | "JSON.OBJKEYS"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_replica_routable_command_is_read_only() {
        for command in [
            "GET",
            "MGET",
            "LRANGE",
            "LLEN",
            "HGET",
            "HGETALL",
            "HKEYS",
            "HVALS",
            "SMEMBERS",
            "SISMEMBER",
            "EXISTS",
            "TYPE",
            "TTL",
            "KEYS",
            "STRLEN",
            "JSON.GET",
            "JSON.TYPE",
            "JSON.ARRLEN",
            "JSON.OBJKEYS",
        ] {
            assert!(!is_write_command(command), "{command}");
            assert!(is_replica_routable_read(command), "{command}");
        }
    }

    #[test]
    fn mutation_classification_is_case_explicit() {
        assert!(is_write_command("SET"));
        assert!(!is_write_command("set"));
        assert!(!is_replica_routable_read("get"));
    }
}
