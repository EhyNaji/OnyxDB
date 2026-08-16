//! Typed storage facade over the sharded in-memory engine.

use crate::clock::unix_seconds;
use crate::engine::{DataEntry, EngineStats, EntryMutation, EvictionPolicy, OnyxEngine, OnyxValue};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

pub const MAX_KEYS: usize = 1_000_000;

#[path = "store/json_path.rs"]
mod json_path;
use json_path::{
    arrappend_json_path, delete_json_path, get_json_path, numincrby_json_path, parse_json_path,
    set_json_path,
};

pub struct ShardedStore {
    engine: OnyxEngine,
    maxmemory_bytes: usize,
    maxmemory_policy: EvictionPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    WrongType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    KeyLimit,
    Maxmemory,
}

impl AdmissionError {
    pub fn message(self) -> &'static str {
        match self {
            Self::KeyLimit => "ERR database key limit reached",
            Self::Maxmemory => {
                "OOM command not allowed because the projected memory usage exceeds maxmemory"
            }
        }
    }
}

/// Captures the exact pre-command state required for admission and rollback.
///
/// The caller owns higher-level write and visibility serialization. This type
/// deliberately has no persistence or replication dependency: after admission,
/// the server derives canonical committed effects from `before_entries` and the
/// store's current state before deciding whether to keep or roll back the change.
pub struct MutationAttempt<'a> {
    store: &'a ShardedStore,
    before_entries: HashMap<Bytes, Option<DataEntry>>,
    memory_before: usize,
    key_count_before: usize,
    evicted_entries: Vec<(Bytes, DataEntry)>,
    active: bool,
}

impl MutationAttempt<'_> {
    pub fn before_entries(&self) -> &HashMap<Bytes, Option<DataEntry>> {
        &self.before_entries
    }

    /// Enforces projected key and logical-memory limits after tentative state
    /// mutation. Eviction victims are returned in authoritative removal order.
    pub fn evicted_entries(&self) -> &[(Bytes, DataEntry)] {
        &self.evicted_entries
    }

    pub fn admit(&mut self, protected_keys: &HashSet<Bytes>) -> Result<(), AdmissionError> {
        let key_count_after = self.store.stats().total_keys;
        if key_count_after > MAX_KEYS && key_count_after > self.key_count_before {
            return Err(AdmissionError::KeyLimit);
        }

        let limit = self.store.maxmemory_bytes();
        let memory_after = self.store.used_memory_bytes();
        if limit == 0 || memory_after <= limit || memory_after <= self.memory_before {
            return Ok(());
        }
        if self.store.maxmemory_policy() == EvictionPolicy::NoEviction {
            return Err(AdmissionError::Maxmemory);
        }

        let evicted =
            self.store
                .engine
                .evict_to_fit(limit, self.store.maxmemory_policy(), protected_keys);
        if self.store.used_memory_bytes() <= limit {
            self.evicted_entries.extend(evicted);
            return Ok(());
        }

        for (key, entry) in &evicted {
            self.store.engine.apply_entry(key.clone(), entry.clone());
        }
        Err(AdmissionError::Maxmemory)
    }

    /// Restores the pre-command state and any unrelated admission victims.
    pub fn rollback(mut self) {
        self.restore();
        self.active = false;
    }

    pub fn commit(mut self) {
        self.active = false;
    }

    fn restore(&self) {
        self.store
            .restore_entries(&self.before_entries, &self.evicted_entries);
    }
}

impl Drop for MutationAttempt<'_> {
    fn drop(&mut self) {
        if self.active {
            self.restore();
        }
    }
}

impl StoreError {
    pub fn message(self) -> &'static str {
        match self {
            Self::WrongType => "WRONGTYPE Operation against a key holding the wrong kind of value",
        }
    }
}

impl Default for ShardedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedStore {
    pub fn new() -> Self {
        Self::with_maxmemory(0, EvictionPolicy::NoEviction)
    }

    pub fn with_maxmemory(maxmemory_bytes: usize, maxmemory_policy: EvictionPolicy) -> Self {
        Self {
            engine: OnyxEngine::new(),
            maxmemory_bytes,
            maxmemory_policy,
        }
    }

    pub fn begin_mutation(&self, keys: &[Bytes]) -> MutationAttempt<'_> {
        let before_entries = keys
            .iter()
            .map(|key| (key.clone(), self.engine.peek(key)))
            .collect();
        MutationAttempt {
            store: self,
            before_entries,
            memory_before: self.used_memory_bytes(),
            key_count_before: self.stats().total_keys,
            evicted_entries: Vec::new(),
            active: true,
        }
    }

    pub fn restore_entries(
        &self,
        before: &HashMap<Bytes, Option<DataEntry>>,
        evicted_entries: &[(Bytes, DataEntry)],
    ) {
        for (key, previous) in before {
            match previous {
                Some(entry) => {
                    self.engine.apply_entry(key.clone(), entry.clone());
                }
                None => {
                    self.engine.delete(key);
                }
            }
        }
        for (key, entry) in evicted_entries {
            if !before.contains_key(key) {
                self.engine.apply_entry(key.clone(), entry.clone());
            }
        }
    }

    /// Returns the persistent entry without changing LRU metadata.
    pub fn peek_entry(&self, key: &Bytes) -> Option<DataEntry> {
        self.engine.peek(key)
    }

    /// Reads and clones an entry while updating its access metadata.
    pub fn get_entry(&self, key: &Bytes) -> Option<DataEntry> {
        self.engine.get(key)
    }

    /// Installs an authoritative entry exactly, for recovery and replication.
    pub fn apply_entry(&self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        self.engine.apply_entry(key, entry)
    }

    /// Installs a raw engine value. Command paths should prefer typed methods.
    pub fn set_value(
        &self,
        key: Bytes,
        value: OnyxValue,
        expires_at: Option<u64>,
    ) -> Option<DataEntry> {
        self.engine.set(key, value, expires_at)
    }

    pub fn delete_bytes(&self, key: &Bytes) -> bool {
        self.engine.delete(key)
    }

    pub fn set_conditional_value(
        &self,
        key: Bytes,
        value: OnyxValue,
        expires_at: Option<u64>,
        condition: Option<bool>,
    ) -> bool {
        self.engine
            .set_conditional(key, value, expires_at, condition)
    }

    /// Clones the complete non-expired dataset for a durable state boundary.
    pub fn raw_entries(&self) -> Vec<(Bytes, DataEntry)> {
        self.engine.snapshot_all()
    }

    /// Atomically replaces the complete dataset after the caller has validated
    /// the recovery or full-synchronization boundary.
    pub fn replace_all(&self, entries: Vec<(Bytes, DataEntry)>) {
        self.engine.replace_all(entries);
    }

    pub fn evict_to_fit(
        &self,
        limit: usize,
        policy: EvictionPolicy,
        protected_keys: &HashSet<Bytes>,
    ) -> Vec<(Bytes, DataEntry)> {
        self.engine.evict_to_fit(limit, policy, protected_keys)
    }

    // --- String operations ---
    pub fn set(&self, key: String, value: String) {
        self.engine
            .set(Bytes::from(key), OnyxValue::Blob(Bytes::from(value)), None);
    }

    pub fn set_raw(&self, key: String, entry: DataEntry) {
        self.engine
            .set(Bytes::from(key), entry.value, entry.expires_at);
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, StoreError> {
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Blob(b) => Ok(String::from_utf8_lossy(b).to_string()),
                OnyxValue::Int(n) => Ok(n.to_string()),
                _ => Err(StoreError::WrongType),
            });
        result.transpose()
    }

    pub fn get_raw(&self, key: &str) -> Option<DataEntry> {
        self.engine.get(&Bytes::from(key.to_string()))
    }

    pub fn delete(&self, key: &str) -> bool {
        self.engine.delete(&Bytes::from(key.to_string()))
    }

    pub fn exists(&self, key: &str) -> bool {
        self.engine.peek(&Bytes::from(key.to_string())).is_some()
    }

    pub fn expire_at(&self, key: &str, timestamp: u64) -> bool {
        // Update expiration under one engine lock without cloning the value.
        self.engine
            .set_expiry(&Bytes::from(key.to_string()), timestamp)
    }

    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        self.expire_at(key, unix_seconds().saturating_add(seconds))
    }

    pub fn ttl(&self, key: &str) -> i64 {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| {
                if let Some(exp) = entry.expires_at {
                    let remaining = exp.saturating_sub(unix_seconds());
                    if remaining == 0 {
                        -2
                    } else {
                        i64::try_from(remaining).unwrap_or(i64::MAX)
                    }
                } else {
                    -1
                }
            })
            .unwrap_or(-2)
    }

    pub fn incr(&self, key: &str) -> Result<i64, &'static str> {
        self.incrby(key, 1)
    }

    pub fn incrby(&self, key: &str, delta: i64) -> Result<i64, &'static str> {
        // Read, overflow validation, and mutation occur under one engine lock,
        // so concurrent increments cannot overwrite each other.
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Int(0),
            move |v| {
                let current = match v {
                    OnyxValue::Int(n) => *n,
                    OnyxValue::Blob(b) => std::str::from_utf8(b)
                        .ok()
                        .and_then(|value| value.parse::<i64>().ok())
                        .ok_or("ERR value is not an integer")?,
                    _ => return Err("ERR value is not an integer"),
                };
                let new_val = current
                    .checked_add(delta)
                    .ok_or("ERR increment or decrement would overflow")?;
                *v = OnyxValue::Int(new_val);
                Ok(new_val)
            },
        )
    }

    pub fn append(&self, key: &str, suffix: &str) -> Result<usize, StoreError> {
        let suffix_owned = suffix.to_string();
        // Append under one engine lock so concurrent suffixes are not lost.
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Blob(Bytes::new()),
            move |v| {
                let mut s = match v {
                    OnyxValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    OnyxValue::Int(value) => value.to_string(),
                    _ => return Err(StoreError::WrongType),
                };
                s.push_str(&suffix_owned);
                let len = s.len();
                *v = OnyxValue::Blob(Bytes::from(s));
                Ok(len)
            },
        )
    }

    pub fn strlen(&self, key: &str) -> Result<usize, StoreError> {
        self.get(key)
            .map(|value| value.map_or(0, |value| value.len()))
    }

    pub fn getset(&self, key: &str, new_value: &str) -> Result<Option<String>, StoreError> {
        let new_value_owned = new_value.to_string();
        self.engine.update_entry_or_insert_with_presence(
            Bytes::from(key.to_string()),
            || OnyxValue::Blob(Bytes::new()),
            move |entry, existed| {
                let old = match &entry.value {
                    OnyxValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    OnyxValue::Int(n) => n.to_string(),
                    _ => return Err(StoreError::WrongType),
                };
                entry.value = OnyxValue::Blob(Bytes::from(new_value_owned));
                entry.expires_at = None;
                Ok(existed.then_some(old))
            },
        )
    }

    pub fn setnx(&self, key: &str, value: &str) -> bool {
        // The engine performs the presence check and insertion atomically.
        self.engine.set_if_absent(
            Bytes::from(key.to_string()),
            OnyxValue::Blob(Bytes::from(value.to_string())),
        )
    }

    // --- List operations ---
    pub fn lpush(&self, key: &str, item: String) -> Result<usize, StoreError> {
        let item_b = Bytes::from(item);
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::List(Vec::new()),
            move |v| match v {
                OnyxValue::List(l) => {
                    l.insert(0, item_b);
                    Ok(l.len())
                }
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn rpush(&self, key: &str, item: String) -> Result<usize, StoreError> {
        let item_b = Bytes::from(item);
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::List(Vec::new()),
            move |v| match v {
                OnyxValue::List(l) => {
                    l.push(item_b);
                    Ok(l.len())
                }
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, |v| match v {
                OnyxValue::List(l) if !l.is_empty() => {
                    let item = l.remove(0);
                    let result = Ok(Some(String::from_utf8_lossy(&item).to_string()));
                    if l.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                OnyxValue::List(_) => EntryMutation::Keep(Ok(None)),
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(None))
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, |v| match v {
                OnyxValue::List(l) if !l.is_empty() => {
                    let item = l.pop().unwrap();
                    let result = Ok(Some(String::from_utf8_lossy(&item).to_string()));
                    if l.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                OnyxValue::List(_) => EntryMutation::Keep(Ok(None)),
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(None))
    }

    /// Implements inclusive Redis-style LRANGE bounds. Negative indices count
    /// from the end and out-of-range values are clamped.
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, StoreError> {
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::List(l) => {
                    let len = l.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }
                    let norm = |idx: i64| -> i64 { if idx < 0 { (len + idx).max(0) } else { idx } };
                    let s = norm(start);
                    let mut e = norm(stop);
                    if s > len - 1 || s > e {
                        return Ok(Vec::new());
                    }
                    if e > len - 1 {
                        e = len - 1;
                    }
                    Ok(l[s as usize..=e as usize]
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect())
                }
                _ => Err(StoreError::WrongType),
            });
        result.unwrap_or(Ok(Vec::new()))
    }

    pub fn llen(&self, key: &str) -> Result<usize, StoreError> {
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::List(l) => Ok(l.len()),
                _ => Err(StoreError::WrongType),
            });
        result.unwrap_or(Ok(0))
    }

    // --- Hash operations ---
    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool, StoreError> {
        let field_b = Bytes::from(field.to_string());
        let value_b = Bytes::from(value.to_string());
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Hash(std::collections::HashMap::new()),
            move |v| match v {
                OnyxValue::Hash(h) => Ok(h.insert(field_b, value_b).is_none()),
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, StoreError> {
        let field_b = Bytes::from(field.to_string());
        self.engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Hash(h) => Ok(h
                        .get(&field_b)
                        .map(|b| String::from_utf8_lossy(b).to_string())),
                    _ => Err(StoreError::WrongType),
                }
            })
            .unwrap_or(Ok(None))
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Hash(h) => Ok(h
                    .iter()
                    .map(|(k, v)| {
                        (
                            String::from_utf8_lossy(k).to_string(),
                            String::from_utf8_lossy(v).to_string(),
                        )
                    })
                    .collect()),
                _ => Err(StoreError::WrongType),
            })
            .unwrap_or(Ok(Vec::new()))
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<bool, StoreError> {
        let field_b = Bytes::from(field.to_string());
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, move |v| match v {
                OnyxValue::Hash(h) => {
                    let removed = h.remove(&field_b).is_some();
                    let result = Ok(removed);
                    if removed && h.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(false))
    }

    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.hgetall(key)
            .map(|hash| hash.into_iter().map(|(field, _)| field).collect())
    }

    pub fn hvals(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.hgetall(key)
            .map(|hash| hash.into_iter().map(|(_, value)| value).collect())
    }

    // --- Set operations ---
    pub fn sadd(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let member_b = Bytes::from(member.to_string());
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Set(std::collections::HashSet::new()),
            move |v| match v {
                OnyxValue::Set(s) => Ok(s.insert(member_b)),
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Set(s) => Ok(s
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect()),
                _ => Err(StoreError::WrongType),
            })
            .unwrap_or(Ok(Vec::new()))
    }

    pub fn srem(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let member_b = Bytes::from(member.to_string());
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, move |v| match v {
                OnyxValue::Set(s) => {
                    let removed = s.remove(&member_b);
                    let result = Ok(removed);
                    if removed && s.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(false))
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let member_b = Bytes::from(member.to_string());
        self.engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Set(s) => Ok(s.contains(&member_b)),
                    _ => Err(StoreError::WrongType),
                }
            })
            .unwrap_or(Ok(false))
    }
    // --- JSON operations ---

    /// Replaces the complete document at `$`, or updates an existing JSON
    /// document at a partial path.
    pub fn json_set(
        &self,
        key: &str,
        path: &str,
        new_value: serde_json::Value,
    ) -> Result<(), &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let key_b = Bytes::from(key.to_string());

        if segments.is_empty() {
            return self.engine.update_or_insert_with_presence(
                key_b,
                || OnyxValue::Json(serde_json::Value::Null),
                move |value, _| match value {
                    OnyxValue::Json(root) => {
                        *root = new_value;
                        Ok(())
                    }
                    _ => Err(StoreError::WrongType.message()),
                },
            );
        }

        // A partial path requires an existing JSON document.
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(set_json_path(root, &segments, new_value)),
            _ => None, // Existing non-JSON value: wrong type.
        });
        match result {
            Some(Some(true)) => Ok(()),
            Some(Some(false)) => {
                Err("ERR path not reachable (intermediate element missing or index out of bounds)")
            }
            Some(None) => Err(StoreError::WrongType.message()),
            None => Err("ERR key does not exist: use JSON.SET key $ {...} to create it"),
        }
    }

    pub fn json_get(&self, key: &str, path: &str) -> Result<Option<String>, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => Ok(if segments.is_empty() {
                        Some(root.to_string())
                    } else {
                        get_json_path(root, &segments).map(|v| v.to_string())
                    }),
                    _ => Err(StoreError::WrongType.message()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    pub fn json_del(&self, key: &str, path: &str) -> Result<bool, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        if segments.is_empty() {
            let result =
                self.engine
                    .update_if_exists_with_action(
                        &Bytes::from(key.to_string()),
                        |value| match value {
                            OnyxValue::Json(_) => EntryMutation::Delete(Ok(true)),
                            _ => EntryMutation::Keep(Err(StoreError::WrongType.message())),
                        },
                    );
            return result.unwrap_or(Ok(false));
        }
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(delete_json_path(root, &segments)),
            _ => None,
        });
        match result {
            Some(Some(deleted)) => Ok(deleted),
            Some(None) => Err(StoreError::WrongType.message()),
            None => Ok(false), // Missing key: nothing to delete.
        }
    }

    pub fn json_type(&self, key: &str, path: &str) -> Result<Option<&'static str>, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => {
                        let node = if segments.is_empty() {
                            Some(root)
                        } else {
                            get_json_path(root, &segments)
                        };
                        Ok(node.map(|v| match v {
                            serde_json::Value::Null => "null",
                            serde_json::Value::Bool(_) => "boolean",
                            serde_json::Value::Number(_) => "number",
                            serde_json::Value::String(_) => "string",
                            serde_json::Value::Array(_) => "array",
                            serde_json::Value::Object(_) => "object",
                        }))
                    }
                    _ => Err(StoreError::WrongType.message()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
    pub fn json_numincrby(&self, key: &str, path: &str, delta: f64) -> Result<f64, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(numincrby_json_path(root, &segments, delta)),
            _ => None,
        });
        match result {
            Some(Some(Ok(new_val))) => Ok(new_val),
            Some(Some(Err(e))) => Err(e.to_string()),
            Some(None) => Err(StoreError::WrongType.message().to_string()),
            None => {
                Err("ERR key does not exist: use JSON.SET key $ {...} to create it".to_string())
            }
        }
    }

    pub fn json_arrappend(
        &self,
        key: &str,
        path: &str,
        new_value: serde_json::Value,
    ) -> Result<usize, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(arrappend_json_path(root, &segments, new_value)),
            _ => None,
        });
        match result {
            Some(Some(Ok(new_len))) => Ok(new_len),
            Some(Some(Err(e))) => Err(e.to_string()),
            Some(None) => Err(StoreError::WrongType.message().to_string()),
            None => {
                Err("ERR key does not exist: use JSON.SET key $ {...} to create it".to_string())
            }
        }
    }
    pub fn json_arrlen(&self, key: &str, path: &str) -> Result<Option<usize>, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => {
                        let node = if segments.is_empty() {
                            Some(root)
                        } else {
                            get_json_path(root, &segments)
                        };
                        Ok(node.and_then(|v| v.as_array().map(|a| a.len())))
                    }
                    _ => Err(StoreError::WrongType.message().to_string()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    pub fn json_objkeys(&self, key: &str, path: &str) -> Result<Option<Vec<String>>, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => {
                        let node = if segments.is_empty() {
                            Some(root)
                        } else {
                            get_json_path(root, &segments)
                        };
                        Ok(node.and_then(|v| v.as_object().map(|o| o.keys().cloned().collect())))
                    }
                    _ => Err(StoreError::WrongType.message().to_string()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
    // --- Key operations ---
    pub fn rename(&self, old_key: &str, new_key: &str) -> bool {
        self.engine.rename(
            &Bytes::from(old_key.to_string()),
            Bytes::from(new_key.to_string()),
        )
    }

    pub fn copy(&self, src: &str, dst: &str) -> bool {
        self.engine
            .copy(&Bytes::from(src.to_string()), Bytes::from(dst.to_string()))
    }

    pub fn value_type(&self, key: &str) -> Option<&'static str> {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Blob(_) => "string",
                OnyxValue::Int(_) => "int",
                OnyxValue::Float(_) => "float",
                OnyxValue::List(_) => "list",
                OnyxValue::Hash(_) => "hash",
                OnyxValue::Set(_) => "set",
                OnyxValue::Json(_) => "json",
                OnyxValue::Vector(_) => "vector",
            })
    }

    pub fn all_keys(&self) -> Vec<String> {
        self.engine
            .all_keys()
            .into_iter()
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect()
    }

    pub fn keys_matching(&self, pattern: &str) -> Vec<String> {
        if pattern == "*" || pattern.is_empty() {
            return self.all_keys();
        }
        self.all_keys()
            .into_iter()
            .filter(|k| glob_match(pattern, k))
            .collect()
    }

    pub fn snapshot_entries(&self) -> Vec<(String, DataEntry)> {
        self.engine
            .snapshot_all()
            .into_iter()
            .map(|(k, entry)| (String::from_utf8_lossy(&k).to_string(), entry))
            .collect()
    }

    pub fn used_memory_bytes(&self) -> usize {
        self.engine.total_memory_bytes()
    }

    pub fn maxmemory_bytes(&self) -> usize {
        self.maxmemory_bytes
    }

    pub fn maxmemory_policy(&self) -> EvictionPolicy {
        self.maxmemory_policy
    }

    pub fn expire_conditional(&self, key: &str, seconds: u64, condition: &str) -> bool {
        let require_expiry = match condition {
            "NX" => Some(false),
            "XX" => Some(true),
            _ => return false,
        };
        self.engine.set_expiry_conditional(
            &Bytes::from(key.to_string()),
            unix_seconds().saturating_add(seconds),
            require_expiry,
        )
    }

    pub fn get_expiry(&self, key: &str) -> Option<u64> {
        self.engine
            .read(&Bytes::from(key.to_string()), |e| e.expires_at)
            .flatten()
    }

    pub fn stats(&self) -> EngineStats {
        self.engine.stats()
    }

    pub fn gc_expired(&self) -> usize {
        self.engine.gc_expired()
    }
}
pub fn is_expired(entry: &DataEntry) -> bool {
    if let Some(exp) = entry.expires_at {
        unix_seconds() >= exp
    } else {
        false
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p_idx = 0;
    let mut t_idx = 0;
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let mut star_idx = None;
    let mut match_idx = 0;

    while t_idx < t.len() {
        if p_idx < p.len() && p[p_idx] == t[t_idx] {
            // Literal match: advance both inputs.
            p_idx += 1;
            t_idx += 1;
        } else if p_idx < p.len() && p[p_idx] == '*' {
            // `*` is a backtracking point. First match zero characters, then
            // expand it one character at a time if later input requires it.
            star_idx = Some(p_idx);
            match_idx = t_idx;
            p_idx += 1;
        } else if let Some(star) = star_idx {
            p_idx = star + 1;
            match_idx += 1;
            t_idx = match_idx;
        } else {
            return false;
        }
    }

    while p_idx < p.len() && p[p_idx] == '*' {
        p_idx += 1;
    }
    p_idx == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn glob_matching_supports_literals_and_wildcards() {
        assert!(glob_match("user:*", "user:42"));
        assert!(glob_match("*", "any"));
        assert!(!glob_match("user:*", "product:1"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "different"));
    }

    #[test]
    fn empty_collection_deletion_holds_the_shard_lock_through_removal() {
        let store = Arc::new(ShardedStore::new());
        assert_eq!(store.lpush("list", "old".to_string()), Ok(1));
        let (mutation_started_tx, mutation_started_rx) = std::sync::mpsc::channel();
        let (release_mutation_tx, release_mutation_rx) = std::sync::mpsc::channel();
        let pop_store = Arc::clone(&store);
        let pop = std::thread::spawn(move || {
            pop_store
                .engine
                .update_if_exists_with_action(&Bytes::from_static(b"list"), |value| {
                    let OnyxValue::List(list) = value else {
                        panic!("test key changed type");
                    };
                    assert_eq!(list.pop(), Some(Bytes::from_static(b"old")));
                    mutation_started_tx.send(()).unwrap();
                    release_mutation_rx.recv().unwrap();
                    EntryMutation::Delete(())
                })
        });
        mutation_started_rx.recv().unwrap();

        let (push_started_tx, push_started_rx) = std::sync::mpsc::channel();
        let (push_complete_tx, push_complete_rx) = std::sync::mpsc::channel();
        let push_store = Arc::clone(&store);
        let push = std::thread::spawn(move || {
            push_started_tx.send(()).unwrap();
            let result = push_store.rpush("list", "new".to_string());
            push_complete_tx.send(result).unwrap();
        });
        push_started_rx.recv().unwrap();
        assert!(
            push_complete_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "a concurrent write entered the shard before deletion completed"
        );

        release_mutation_tx.send(()).unwrap();
        assert_eq!(pop.join().unwrap(), Some(()));
        assert_eq!(push_complete_rx.recv().unwrap(), Ok(1));
        push.join().unwrap();
        assert_eq!(store.lrange("list", 0, -1), Ok(vec!["new".to_string()]));
    }

    #[test]
    fn rejected_growth_is_restored_from_the_mutation_attempt() {
        let store = ShardedStore::with_maxmemory(128, EvictionPolicy::NoEviction);
        store.set("key".to_string(), "original".to_string());
        let key = Bytes::from_static(b"key");
        let mut attempt = store.begin_mutation(std::slice::from_ref(&key));
        store.append("key", &"x".repeat(256)).unwrap();

        assert_eq!(
            attempt.admit(&HashSet::from([key])),
            Err(AdmissionError::Maxmemory)
        );
        attempt.rollback();
        assert_eq!(store.get("key"), Ok(Some("original".to_string())));
    }

    #[test]
    fn abandoned_mutation_attempt_rolls_back_automatically() {
        let store = ShardedStore::new();
        store.set("key".to_string(), "original".to_string());
        let key = Bytes::from_static(b"key");
        {
            let _attempt = store.begin_mutation(std::slice::from_ref(&key));
            store.set("key".to_string(), "tentative".to_string());
        }
        assert_eq!(store.get("key"), Ok(Some("original".to_string())));
    }

    #[test]
    fn committed_mutation_attempt_is_not_rolled_back_on_drop() {
        let store = ShardedStore::new();
        store.set("key".to_string(), "original".to_string());
        let key = Bytes::from_static(b"key");
        let attempt = store.begin_mutation(std::slice::from_ref(&key));
        store.set("key".to_string(), "committed".to_string());

        attempt.commit();

        assert_eq!(store.get("key"), Ok(Some("committed".to_string())));
    }

    #[test]
    fn unwinding_mutation_attempt_restores_the_baseline() {
        let store = ShardedStore::new();
        store.set("key".to_string(), "original".to_string());
        let result = std::panic::catch_unwind(|| {
            let key = Bytes::from_static(b"key");
            let _attempt = store.begin_mutation(std::slice::from_ref(&key));
            store.set("key".to_string(), "tentative".to_string());
            panic!("injected failure");
        });

        assert!(result.is_err());
        assert_eq!(store.get("key"), Ok(Some("original".to_string())));
    }

    #[test]
    fn abandoned_admitted_mutation_restores_ttl_and_all_eviction_victims() {
        let store = ShardedStore::with_maxmemory(180, EvictionPolicy::AllKeysLru);
        let original_expiry = unix_seconds() + 600;
        store.set_value(
            Bytes::from_static(b"target"),
            OnyxValue::Blob(Bytes::from_static(b"old")),
            Some(original_expiry),
        );
        store.set("first".to_string(), "a".repeat(8));
        store.set("second".to_string(), "b".repeat(8));
        let target = Bytes::from_static(b"target");
        {
            let mut attempt = store.begin_mutation(std::slice::from_ref(&target));
            store.set("target".to_string(), "c".repeat(32));
            attempt.admit(&HashSet::from([target.clone()])).unwrap();
            assert!(!attempt.evicted_entries().is_empty());
        }

        let restored = store.peek_entry(&target).unwrap();
        assert_eq!(restored.value, OnyxValue::Blob(Bytes::from_static(b"old")));
        assert_eq!(restored.expires_at, Some(original_expiry));
        assert_eq!(store.get("first"), Ok(Some("a".repeat(8))));
        assert_eq!(store.get("second"), Ok(Some("b".repeat(8))));
    }

    #[test]
    fn persistence_rollback_restores_eviction_victims_and_target_state() {
        let store = ShardedStore::with_maxmemory(180, EvictionPolicy::AllKeysLru);
        store.set("first".to_string(), "a".repeat(8));
        store.set("second".to_string(), "b".repeat(8));
        let target = Bytes::from_static(b"target");
        let mut attempt = store.begin_mutation(std::slice::from_ref(&target));
        store.set("target".to_string(), "c".repeat(8));

        attempt.admit(&HashSet::from([target])).unwrap();
        assert!(!attempt.evicted_entries().is_empty());
        attempt.rollback();

        assert_eq!(store.get("first"), Ok(Some("a".repeat(8))));
        assert_eq!(store.get("second"), Ok(Some("b".repeat(8))));
        assert_eq!(store.get("target"), Ok(None));
    }
}
