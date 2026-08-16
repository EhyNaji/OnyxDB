//! Typed execution of data commands against the in-memory store.

use crate::clock::unix_seconds as now;
use crate::command::is_write_command;
use crate::engine::OnyxValue;
use crate::resp::RESPValue;
use crate::store::ShardedStore;
use bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationState {
    /// The command is not classified as an authoritative data mutation.
    NotRequested,
    /// A mutation command was rejected or completed without changing state.
    NoChange,
    /// Store semantics ran and any resulting state still requires admission
    /// and durability before it may become authoritative.
    Tentative,
    /// The server admitted and durably ordered the resulting committed effects.
    Committed,
}

#[derive(Debug)]
pub struct CommandOutcome {
    pub response: RESPValue,
    pub mutation: MutationState,
}

impl CommandOutcome {
    pub fn read(response: RESPValue) -> Self {
        Self {
            response,
            mutation: MutationState::NotRequested,
        }
    }

    pub fn into_response(self) -> RESPValue {
        self.response
    }
}

/// Returns the unique keys whose persistent entries may be changed by a
/// mutation command. The server captures these entries before execution so
/// committed effects are derived from authoritative before/after state.
pub fn affected_keys(args: &[String]) -> Vec<Bytes> {
    let command = args.first().map(String::as_str).unwrap_or("");
    let mut keys = Vec::new();
    match command {
        "MSET" => {
            let mut index = 1;
            while index + 1 < args.len() {
                keys.push(Bytes::copy_from_slice(args[index].as_bytes()));
                index += 2;
            }
        }
        "RENAME" | "COPY" => {
            if let Some(key) = args.get(1) {
                keys.push(Bytes::copy_from_slice(key.as_bytes()));
            }
            if let Some(key) = args.get(2) {
                keys.push(Bytes::copy_from_slice(key.as_bytes()));
            }
        }
        _ if is_write_command(command) => {
            if let Some(key) = args.get(1) {
                keys.push(Bytes::copy_from_slice(key.as_bytes()));
            }
        }
        _ => {}
    }

    let mut unique = std::collections::HashSet::new();
    keys.retain(|key| unique.insert(key.clone()));
    keys
}

pub fn execute_command(store: &ShardedStore, args: &[String]) -> CommandOutcome {
    let cmd = args.first().map(|s| s.as_str()).unwrap_or("");
    let key = args.get(1).map(|s| s.as_str()).unwrap_or("");
    let arg = args.get(2).map(|s| s.as_str()).unwrap_or("");

    let (response, reported_mutation) = match cmd {
        "SET" if args.len() >= 3 => {
            // Accept Redis-compatible EX/PX/NX/XX options plus internal EXAT,
            // which makes expiration deterministic during replay.
            let mut expires_at: Option<u64> = None;
            let mut condition: Option<bool> = None; // Some(true)=NX, Some(false)=XX
            let mut i = 3;
            let mut valid = true;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "EX" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(secs) if secs > 0 => {
                            expires_at = Some(now().saturating_add(secs));
                            i += 2;
                        }
                        Some(_) | None => {
                            valid = false;
                            break;
                        }
                    },
                    "PX" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(millis) if millis > 0 => {
                            let seconds = millis.saturating_add(999) / 1000;
                            expires_at = Some(now().saturating_add(seconds));
                            i += 2;
                        }
                        Some(_) | None => {
                            valid = false;
                            break;
                        }
                    },
                    "EXAT" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(ts) => {
                            expires_at = Some(ts);
                            i += 2;
                        }
                        None => {
                            valid = false;
                            break;
                        }
                    },
                    "NX" => {
                        condition = Some(true);
                        i += 1;
                    }
                    "XX" => {
                        condition = Some(false);
                        i += 1;
                    }
                    _ => {
                        valid = false;
                        break;
                    }
                }
            }
            if !valid {
                (RESPValue::Error("ERR syntax error".to_string()), false)
            } else {
                let ok = store.set_conditional_value(
                    Bytes::from(key.to_string()),
                    OnyxValue::Blob(Bytes::from(arg.to_string())),
                    expires_at,
                    condition,
                );
                if ok {
                    (RESPValue::SimpleString("OK".to_string()), true)
                } else {
                    // A failed NX/XX condition performs no write and returns nil.
                    (RESPValue::BulkString(None), false)
                }
            }
        }
        "GET" if args.len() >= 2 => match store.get(key) {
            Ok(Some(value)) => (RESPValue::BulkString(Some(value)), false),
            Ok(None) => (RESPValue::BulkString(None), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "DEL" if args.len() >= 2 => {
            let deleted = store.delete(key);
            (RESPValue::Integer(if deleted { 1 } else { 0 }), deleted)
        }
        "INCR" if args.len() >= 2 => match store.incr(key) {
            Ok(value) => (RESPValue::Integer(value), true),
            Err(message) => (RESPValue::Error(message.to_string()), false),
        },
        "LPUSH" if args.len() >= 3 => match store.lpush(key, arg.to_string()) {
            Ok(length) => (RESPValue::Integer(length as i64), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "RPUSH" if args.len() >= 3 => match store.rpush(key, arg.to_string()) {
            Ok(length) => (RESPValue::Integer(length as i64), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "LPOP" if args.len() >= 2 => match store.lpop(key) {
            Ok(Some(value)) => (RESPValue::BulkString(Some(value)), true),
            Ok(None) => (RESPValue::BulkString(None), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "RPOP" if args.len() >= 2 => match store.rpop(key) {
            Ok(Some(value)) => (RESPValue::BulkString(Some(value)), true),
            Ok(None) => (RESPValue::BulkString(None), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "LRANGE" if args.len() >= 2 => {
            let start = args.get(2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let stop = args
                .get(3)
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(-1);
            match store.lrange(key, start, stop) {
                Ok(list) => (
                    RESPValue::Array(
                        list.into_iter()
                            .map(|s| RESPValue::BulkString(Some(s)))
                            .collect(),
                    ),
                    false,
                ),
                Err(error) => (RESPValue::Error(error.message().to_string()), false),
            }
        }

        "EXPIREAT" if args.len() >= 3 => {
            if let Ok(t) = arg.parse::<u64>() {
                let updated = store.expire_at(key, t);
                (RESPValue::Integer(if updated { 1 } else { 0 }), updated)
            } else {
                (RESPValue::Error("ERR invalid timestamp".to_string()), false)
            }
        }
        "TTL" if args.len() >= 2 => (RESPValue::Integer(store.ttl(key)), false),
        "EXISTS" if args.len() >= 2 => (
            RESPValue::Integer(if store.exists(key) { 1 } else { 0 }),
            false,
        ),
        "TYPE" if args.len() >= 2 => match store.value_type(key) {
            Some(t) => (RESPValue::SimpleString(t.to_string()), false),
            None => (RESPValue::SimpleString("none".to_string()), false),
        },
        "JSON.SET" => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let raw_value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            if args.len() < 4 || path.is_empty() {
                (
                    RESPValue::Error("ERR usage: JSON.SET key path json-value".to_string()),
                    false,
                )
            } else {
                match serde_json::from_str::<serde_json::Value>(raw_value) {
                    Ok(parsed) => match store.json_set(key, path, parsed) {
                        Ok(()) => (RESPValue::SimpleString("OK".to_string()), true),
                        Err(e) => (RESPValue::Error(e.to_string()), false),
                    },
                    Err(_) => (
                        RESPValue::Error("ERR value is not valid JSON".to_string()),
                        false,
                    ),
                }
            }
        }
        "JSON.GET" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_get(key, path) {
                Ok(Some(s)) => (RESPValue::BulkString(Some(s)), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.DEL" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_del(key, path) {
                Ok(deleted) => (RESPValue::Integer(if deleted { 1 } else { 0 }), deleted),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.TYPE" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_type(key, path) {
                Ok(Some(t)) => (RESPValue::SimpleString(t.to_string()), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.NUMINCRBY" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let delta_str = args.get(3).map(|s| s.as_str()).unwrap_or("");
            match delta_str.parse::<f64>() {
                Ok(delta) => match store.json_numincrby(key, path, delta) {
                    Ok(new_val) => (RESPValue::BulkString(Some(new_val.to_string())), true),
                    Err(e) => (RESPValue::Error(e), false),
                },
                Err(_) => (
                    RESPValue::Error("ERR delta is not a valid number".to_string()),
                    false,
                ),
            }
        }
        "JSON.ARRAPPEND" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let raw_value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            match serde_json::from_str::<serde_json::Value>(raw_value) {
                Ok(parsed) => match store.json_arrappend(key, path, parsed) {
                    Ok(new_len) => (RESPValue::Integer(new_len as i64), true),
                    Err(e) => (RESPValue::Error(e), false),
                },
                Err(_) => (
                    RESPValue::Error("ERR value is not valid JSON".to_string()),
                    false,
                ),
            }
        }
        "JSON.ARRLEN" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_arrlen(key, path) {
                Ok(Some(len)) => (RESPValue::Integer(len as i64), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e), false),
            }
        }
        "JSON.OBJKEYS" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_objkeys(key, path) {
                Ok(Some(keys)) => (
                    RESPValue::Array(
                        keys.into_iter()
                            .map(|k| RESPValue::BulkString(Some(k)))
                            .collect(),
                    ),
                    false,
                ),
                Ok(None) => (RESPValue::Array(Vec::new()), false),
                Err(e) => (RESPValue::Error(e), false),
            }
        }
        "SADD" if args.len() >= 3 => match store.sadd(key, arg) {
            Ok(added) => (RESPValue::Integer(if added { 1 } else { 0 }), added),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SMEMBERS" if args.len() >= 2 => match store.smembers(key) {
            Ok(members) => (
                RESPValue::Array(
                    members
                        .into_iter()
                        .map(|m| RESPValue::BulkString(Some(m)))
                        .collect(),
                ),
                false,
            ),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SREM" if args.len() >= 3 => match store.srem(key, arg) {
            Ok(removed) => (RESPValue::Integer(if removed { 1 } else { 0 }), removed),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SISMEMBER" if args.len() >= 3 => match store.sismember(key, arg) {
            Ok(present) => (RESPValue::Integer(if present { 1 } else { 0 }), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "LLEN" if args.len() >= 2 => match store.llen(key) {
            Ok(length) => (RESPValue::Integer(length as i64), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "RENAME" if args.len() >= 3 => {
            if store.rename(key, arg) {
                (RESPValue::SimpleString("OK".to_string()), true)
            } else {
                (RESPValue::Error("ERR no such key".to_string()), false)
            }
        }
        "MSET" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                (
                    RESPValue::Error("ERR wrong number of arguments for 'mset'".to_string()),
                    false,
                )
            } else {
                let mut i = 1;
                while i + 1 < args.len() {
                    store.set(args[i].clone(), args[i + 1].clone());
                    i += 2;
                }
                (RESPValue::SimpleString("OK".to_string()), true)
            }
        }
        "MGET" => {
            let results = args[1..]
                .iter()
                .map(|key| store.get(key))
                .collect::<Result<Vec<_>, _>>();
            match results {
                Ok(values) => (
                    RESPValue::Array(values.into_iter().map(RESPValue::BulkString).collect()),
                    false,
                ),
                Err(error) => (RESPValue::Error(error.message().to_string()), false),
            }
        }
        "KEYS" => {
            let pattern = key;
            let keys = store.keys_matching(pattern);
            (
                RESPValue::Array(
                    keys.into_iter()
                        .map(|k| RESPValue::BulkString(Some(k)))
                        .collect(),
                ),
                false,
            )
        }
        "HSET" if args.len() >= 3 => {
            let field = arg;
            let value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            if args.len() < 4 {
                (
                    RESPValue::Error("ERR wrong number of arguments for 'hset'".to_string()),
                    false,
                )
            } else {
                match store.hset(key, field, value) {
                    Ok(is_new) => (RESPValue::Integer(if is_new { 1 } else { 0 }), true),
                    Err(error) => (RESPValue::Error(error.message().to_string()), false),
                }
            }
        }
        "HGET" if args.len() >= 3 => {
            let field = arg;
            (
                match store.hget(key, field) {
                    Ok(value) => RESPValue::BulkString(value),
                    Err(error) => RESPValue::Error(error.message().to_string()),
                },
                false,
            )
        }
        "HGETALL" if args.len() >= 2 => match store.hgetall(key) {
            Ok(pairs) => {
                let mut flat = Vec::with_capacity(pairs.len() * 2);
                for (f, v) in pairs {
                    flat.push(RESPValue::BulkString(Some(f)));
                    flat.push(RESPValue::BulkString(Some(v)));
                }
                (RESPValue::Array(flat), false)
            }
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "HDEL" if args.len() >= 3 => {
            let field = arg;
            match store.hdel(key, field) {
                Ok(removed) => (RESPValue::Integer(if removed { 1 } else { 0 }), removed),
                Err(error) => (RESPValue::Error(error.message().to_string()), false),
            }
        }
        "REPLICAOF" if key.eq_ignore_ascii_case("no") && arg.eq_ignore_ascii_case("one") => {
            (RESPValue::SimpleString("OK".to_string()), false)
        }
        "INCRBY" if args.len() >= 3 => match arg.parse::<i64>() {
            Ok(delta) => match store.incrby(key, delta) {
                Ok(value) => (RESPValue::Integer(value), true),
                Err(message) => (RESPValue::Error(message.to_string()), false),
            },
            Err(_) => (
                RESPValue::Error("ERR value is not an integer".to_string()),
                false,
            ),
        },
        "DECRBY" if args.len() >= 3 => match arg.parse::<i64>() {
            Ok(delta) => match delta.checked_neg() {
                Some(negated) => match store.incrby(key, negated) {
                    Ok(value) => (RESPValue::Integer(value), true),
                    Err(message) => (RESPValue::Error(message.to_string()), false),
                },
                None => (
                    RESPValue::Error("ERR increment or decrement would overflow".to_string()),
                    false,
                ),
            },
            Err(_) => (
                RESPValue::Error("ERR value is not an integer".to_string()),
                false,
            ),
        },
        "APPEND" if args.len() >= 3 => match store.append(key, arg) {
            Ok(length) => (RESPValue::Integer(length as i64), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "STRLEN" if args.len() >= 2 => match store.strlen(key) {
            Ok(length) => (RESPValue::Integer(length as i64), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "GETSET" if args.len() >= 3 => match store.getset(key, arg) {
            Ok(old) => (RESPValue::BulkString(old), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SETNX" if args.len() >= 3 => {
            let inserted = store.setnx(key, arg);
            (RESPValue::Integer(if inserted { 1 } else { 0 }), inserted)
        }
        "HKEYS" if args.len() >= 2 => match store.hkeys(key) {
            Ok(fields) => (
                RESPValue::Array(
                    fields
                        .into_iter()
                        .map(|f| RESPValue::BulkString(Some(f)))
                        .collect(),
                ),
                false,
            ),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "HVALS" if args.len() >= 2 => match store.hvals(key) {
            Ok(vals) => (
                RESPValue::Array(
                    vals.into_iter()
                        .map(|v| RESPValue::BulkString(Some(v)))
                        .collect(),
                ),
                false,
            ),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "COPY" if args.len() >= 3 => {
            let copied = store.copy(key, arg);
            (RESPValue::Integer(if copied { 1 } else { 0 }), copied)
        }
        "EXPIRE" if args.len() >= 3 => {
            let condition = args.get(3).map(|s| s.to_uppercase());
            match arg.parse::<u64>() {
                Ok(s) => {
                    if condition
                        .as_deref()
                        .is_some_and(|value| !matches!(value, "NX" | "XX"))
                    {
                        (RESPValue::Error("ERR syntax error".to_string()), false)
                    } else {
                        let ok = match &condition {
                            Some(c) => store.expire_conditional(key, s, c),
                            None => store.expire(key, s),
                        };
                        (RESPValue::Integer(if ok { 1 } else { 0 }), ok)
                    }
                }
                Err(_) => (
                    RESPValue::Error("ERR invalid expire time".to_string()),
                    false,
                ),
            }
        }
        "PING" => (RESPValue::SimpleString("PONG".to_string()), false),
        _ => (
            RESPValue::Error("ERR unknown command or wrong syntax".to_string()),
            false,
        ),
    };

    let mutation = if !is_write_command(cmd) {
        MutationState::NotRequested
    } else if reported_mutation {
        MutationState::Tentative
    } else {
        MutationState::NoChange
    };
    CommandOutcome { response, mutation }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn read_and_mutation_outcomes_are_explicit() {
        let store = ShardedStore::new();

        let read = execute_command(&store, &args(&["GET", "key"]));
        assert_eq!(read.mutation, MutationState::NotRequested);

        let mutation = execute_command(&store, &args(&["SET", "key", "value"]));
        assert_eq!(mutation.mutation, MutationState::Tentative);
        assert!(matches!(
            mutation.response,
            RESPValue::SimpleString(value) if value == "OK"
        ));
    }

    #[test]
    fn semantic_no_ops_do_not_claim_a_tentative_mutation() {
        let store = ShardedStore::new();
        execute_command(&store, &args(&["SET", "key", "value"]));

        for command in [
            args(&["SETNX", "key", "replacement"]),
            args(&["DEL", "missing"]),
            args(&["SADD", "set", "member"]),
        ] {
            let first = execute_command(&store, &command);
            if command[0] == "SADD" {
                assert_eq!(first.mutation, MutationState::Tentative);
                let repeated = execute_command(&store, &command);
                assert_eq!(repeated.mutation, MutationState::NoChange);
            } else {
                assert_eq!(first.mutation, MutationState::NoChange);
            }
        }
    }

    #[test]
    fn rejected_mutation_is_distinct_from_a_read() {
        let store = ShardedStore::new();
        let rejected = execute_command(&store, &args(&["SET", "key", "value", "PX", "0"]));

        assert!(matches!(rejected.response, RESPValue::Error(_)));
        assert_eq!(rejected.mutation, MutationState::NoChange);
        assert_eq!(store.get("key"), Ok(None));
    }

    #[test]
    fn affected_keys_cover_multi_key_mutations_without_duplicates() {
        assert_eq!(
            affected_keys(&args(&["MSET", "first", "1", "second", "2"])),
            vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
        );
        assert_eq!(
            affected_keys(&args(&["RENAME", "same", "same"])),
            vec![Bytes::from_static(b"same")]
        );
        assert!(affected_keys(&args(&["GET", "key"])).is_empty());
    }
}
