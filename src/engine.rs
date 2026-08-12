//! OnyxDB engine — sharded in-memory storage.
//!
//! Keys are distributed across a fixed number of shards using FNV-1a hashing.
//! Each shard has an independent mutex, so contention is limited to keys that
//! map to the same shard.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};

// The shard count is a power of two so routing can use a bitmask.
pub const NUM_SHARDS: usize = 64; // 64 shard, bitmask 0x3F

/// Hashes keys with FNV-1a, which is inexpensive for short keys.
#[inline]
fn hash_key(key: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for &byte in key {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Selects a key's shard with a bitmask instead of modulo.
#[inline]
fn shard_for_key(key: &[u8]) -> usize {
    (hash_key(key) as usize) & (NUM_SHARDS - 1)
}

// ============================================================
// ONYX VALUE TYPES
// ============================================================

#[derive(Clone, Debug, PartialEq)]
pub enum OnyxValue {
    /// Opaque string or byte payload.
    Blob(Bytes),
    /// Native signed 64-bit integer.
    Int(i64),
    /// Native 64-bit floating-point value.
    Float(f64),
    /// Ordered list of byte payloads.
    List(Vec<Bytes>),
    /// Field-to-value map.
    Hash(HashMap<Bytes, Bytes>),
    /// Set of byte payloads.
    Set(std::collections::HashSet<Bytes>),
    /// Native JSON document with path access.
    Json(serde_json::Value),
    /// Floating-point vector intended for embeddings.
    Vector(Vec<f32>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataEntry {
    pub value: OnyxValue,
    pub expires_at: Option<u64>,
    pub created_at: u64,
    pub last_accessed: u64,
}

pub enum EntryMutation<R> {
    Keep(R),
    Delete(R),
}

/// Estimates dataset bytes for admission and eviction. This intentionally does
/// not model allocator and container capacity overhead, so it is a stable
/// logical accounting metric rather than a byte-exact process RSS measurement.
fn approx_entry_size(key: &Bytes, entry: &DataEntry) -> usize {
    let value_size = match &entry.value {
        OnyxValue::Blob(b) => b.len(),
        OnyxValue::Int(_) => 8,
        OnyxValue::Float(_) => 8,
        OnyxValue::List(l) => l.iter().fold(0usize, |size, value| {
            size.saturating_add(value.len().saturating_add(8))
        }),
        OnyxValue::Hash(h) => h.iter().fold(0usize, |size, (field, value)| {
            size.saturating_add(field.len().saturating_add(value.len()).saturating_add(16))
        }),
        OnyxValue::Set(s) => s.iter().fold(0usize, |size, value| {
            size.saturating_add(value.len().saturating_add(8))
        }),
        OnyxValue::Json(j) => j.to_string().len(),
        OnyxValue::Vector(v) => v.len().saturating_mul(4),
    };
    key.len().saturating_add(value_size).saturating_add(64)
}

/// Eviction policy used when the dataset exceeds `--maxmemory`.
///
/// Candidate selection is intentionally approximate. Each shard proposes its
/// best local candidate and the engine selects the best of those candidates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysRandom,
    VolatileRandom,
}

impl EvictionPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Some(EvictionPolicy::NoEviction),
            "allkeys-lru" => Some(EvictionPolicy::AllKeysLru),
            "volatile-lru" => Some(EvictionPolicy::VolatileLru),
            "allkeys-random" => Some(EvictionPolicy::AllKeysRandom),
            "volatile-random" => Some(EvictionPolicy::VolatileRandom),
            _ => None,
        }
    }
}

fn cheap_random_index(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as usize;
    // Knuth's multiplier provides adequate dispersion without a full RNG.
    nanos.wrapping_mul(2654435761) % len
}

// ============================================================
// SHARD — exclusively owned while its mutex guard is held
// ============================================================

pub struct Shard {
    data: HashMap<Bytes, DataEntry>,
    /// Operation counter retained for engine statistics.
    op_count: u64,
    /// Timestamp of the most recent modification.
    last_modified: u64,
    /// Approximate logical bytes occupied by entries in this shard.
    mem_bytes: usize,
}

impl Shard {
    fn new() -> Self {
        Self {
            data: HashMap::with_capacity(1024),
            op_count: 0,
            last_modified: 0,
            mem_bytes: 0,
        }
    }

    fn purge_if_expired(&mut self, key: &Bytes, timestamp: u64) -> bool {
        let expired = self
            .data
            .get(key)
            .is_some_and(|entry| entry.expires_at.is_some_and(|expiry| timestamp >= expiry));
        if !expired {
            return false;
        }
        if let Some(entry) = self.data.remove(key) {
            self.mem_bytes = self
                .mem_bytes
                .saturating_sub(approx_entry_size(key, &entry));
        }
        true
    }

    /// Reads an entry and updates `last_accessed` for LRU candidate selection.
    /// The mutable receiver is safe because callers already hold the shard mutex.
    #[inline]
    fn get(&mut self, key: &Bytes) -> Option<&DataEntry> {
        let ts = now();
        self.purge_if_expired(key, ts);
        let entry = self.data.get_mut(key)?;
        entry.last_accessed = ts;
        Some(&*entry)
    }

    #[inline]
    fn insert(&mut self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        self.op_count += 1;
        self.last_modified = now();
        let new_size = approx_entry_size(&key, &entry);
        let old = self.data.insert(key.clone(), entry);
        if let Some(ref old_entry) = old {
            let old_size = approx_entry_size(&key, old_entry);
            self.mem_bytes = self.mem_bytes.saturating_sub(old_size);
        }
        self.mem_bytes = self.mem_bytes.saturating_add(new_size);
        old
    }

    #[inline]
    fn remove(&mut self, key: &Bytes) -> Option<DataEntry> {
        if self.purge_if_expired(key, now()) {
            return None;
        }
        self.op_count += 1;
        self.last_modified = now();
        let removed = self.data.remove(key);
        if let Some(ref e) = removed {
            let size = approx_entry_size(key, e);
            self.mem_bytes = self.mem_bytes.saturating_sub(size);
        }
        removed
    }

    /// Removes expired keys and returns the number removed.
    fn expire_keys(&mut self) -> usize {
        let now = now();
        let expired: Vec<Bytes> = self
            .data
            .iter()
            .filter(|(_, entry)| entry.expires_at.is_some_and(|exp| now >= exp))
            .map(|(k, _)| k.clone())
            .collect();

        let count = expired.len();
        for key in expired {
            if let Some(e) = self.data.remove(&key) {
                let size = approx_entry_size(&key, &e);
                self.mem_bytes = self.mem_bytes.saturating_sub(size);
            }
        }
        count
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    /// Reads an entry by reference for the duration of the closure.
    fn read<F, R>(&mut self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&DataEntry) -> R,
    {
        self.get(key).map(f)
    }

    /// Mutates an existing value under one shard lock and returns `None` when absent.
    fn update_if_exists<F, R>(&mut self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        self.update_if_exists_with_action(key, |value| EntryMutation::Keep(f(value)))
    }

    fn update_if_exists_with_action<F, R>(&mut self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&mut OnyxValue) -> EntryMutation<R>,
    {
        let ts = now();
        self.purge_if_expired(key, ts);
        match self.data.get_mut(key) {
            Some(entry) => {
                self.op_count += 1;
                self.last_modified = ts;
                let old_size = approx_entry_size(key, entry);
                entry.last_accessed = ts;
                let (result, delete) = match f(&mut entry.value) {
                    EntryMutation::Keep(result) => (result, false),
                    EntryMutation::Delete(result) => (result, true),
                };
                if delete {
                    self.data.remove(key);
                    self.mem_bytes = self.mem_bytes.saturating_sub(old_size);
                } else {
                    let new_size = self
                        .data
                        .get(key)
                        .map(|entry| approx_entry_size(key, entry))
                        .unwrap_or(0);
                    self.mem_bytes = self
                        .mem_bytes
                        .saturating_sub(old_size)
                        .saturating_add(new_size);
                }
                Some(result)
            }
            None => None,
        }
    }

    /// Mutates a value in place, inserting `default()` when the key is absent.
    fn update_or_insert<F, R>(&mut self, key: Bytes, default: impl FnOnce() -> OnyxValue, f: F) -> R
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        self.update_or_insert_with_presence(key, default, |value, _| f(value))
    }

    fn update_or_insert_with_presence<F, R>(
        &mut self,
        key: Bytes,
        default: impl FnOnce() -> OnyxValue,
        f: F,
    ) -> R
    where
        F: FnOnce(&mut OnyxValue, bool) -> R,
    {
        self.update_entry_or_insert_with_presence(key, default, |entry, existed| {
            f(&mut entry.value, existed)
        })
    }

    fn update_entry_or_insert_with_presence<F, R>(
        &mut self,
        key: Bytes,
        default: impl FnOnce() -> OnyxValue,
        f: F,
    ) -> R
    where
        F: FnOnce(&mut DataEntry, bool) -> R,
    {
        let ts = now();
        self.purge_if_expired(&key, ts);
        self.op_count += 1;
        self.last_modified = ts;
        let existed = self.data.contains_key(&key);
        let entry = self.data.entry(key.clone()).or_insert_with(|| DataEntry {
            value: default(),
            expires_at: None,
            created_at: ts,
            last_accessed: ts,
        });
        let old_size = if existed {
            approx_entry_size(&key, entry)
        } else {
            0
        };
        entry.last_accessed = ts;
        let result = f(entry, existed);
        let new_size = approx_entry_size(&key, entry);
        self.mem_bytes = self
            .mem_bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);
        result
    }

    /// Updates only the expiration of an existing key without cloning its value.
    fn set_expiry(&mut self, key: &Bytes, timestamp: u64) -> bool {
        self.set_expiry_conditional(key, timestamp, None)
    }

    fn set_expiry_conditional(
        &mut self,
        key: &Bytes,
        timestamp: u64,
        require_expiry: Option<bool>,
    ) -> bool {
        let current = now();
        if self.purge_if_expired(key, current) {
            return false;
        }
        let Some(has_expiry) = self.data.get(key).map(|entry| entry.expires_at.is_some()) else {
            return false;
        };
        if require_expiry.is_some_and(|required| required != has_expiry) {
            return false;
        }
        if timestamp <= current {
            return self.remove(key).is_some();
        }
        match self.data.get_mut(key) {
            Some(entry) => {
                entry.expires_at = Some(timestamp);
                self.op_count += 1;
                self.last_modified = now();
                true
            }
            None => false,
        }
    }

    /// Inserts only when the key is absent, atomically under the shard lock.
    fn insert_if_absent(&mut self, key: Bytes, entry: DataEntry) -> bool {
        self.purge_if_expired(&key, now());
        if self.data.contains_key(&key) {
            false
        } else {
            self.op_count += 1;
            self.last_modified = now();
            let size = approx_entry_size(&key, &entry);
            self.data.insert(key, entry);
            self.mem_bytes = self.mem_bytes.saturating_add(size);
            true
        }
    }

    /// Selects the best local eviction candidate while excluding keys whose
    /// post-command values must be preserved. Volatile policies consider only
    /// entries with an expiration.
    fn eviction_candidate(
        &self,
        policy: EvictionPolicy,
        protected_keys: &HashSet<Bytes>,
    ) -> Option<(Bytes, u64)> {
        let only_volatile = matches!(
            policy,
            EvictionPolicy::VolatileLru | EvictionPolicy::VolatileRandom
        );
        let is_random = matches!(
            policy,
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom
        );

        if is_random {
            let matching_count = self
                .data
                .iter()
                .filter(|(key, entry)| {
                    !protected_keys.contains(*key) && (!only_volatile || entry.expires_at.is_some())
                })
                .count();
            let index = cheap_random_index(matching_count);
            self.data
                .iter()
                .filter(|(key, entry)| {
                    !protected_keys.contains(*key) && (!only_volatile || entry.expires_at.is_some())
                })
                .nth(index)
                .map(|(key, _)| (key.clone(), 0))
        } else {
            self.data
                .iter()
                .filter(|(key, entry)| {
                    !protected_keys.contains(*key) && (!only_volatile || entry.expires_at.is_some())
                })
                .min_by_key(|(_, e)| e.last_accessed)
                .map(|(k, e)| (k.clone(), e.last_accessed))
        }
    }
}

// ============================================================
// ENGINE — shard coordination and cross-shard operations
// ============================================================

pub struct OnyxEngine {
    shards: Vec<std::sync::Mutex<Shard>>,
}

impl Default for OnyxEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl OnyxEngine {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(std::sync::Mutex::new(Shard::new()));
        }
        Self { shards }
    }

    /// Reads one entry from a single shard.
    #[inline]
    pub fn get(&self, key: &Bytes) -> Option<DataEntry> {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.get(key).cloned()
    }

    /// Returns the persistent entry without updating access metadata.
    /// This is used while deriving the canonical effect of a write.
    pub fn peek(&self, key: &Bytes) -> Option<DataEntry> {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.purge_if_expired(key, now());
        shard.data.get(key).cloned()
    }

    /// Installs an entry exactly as described by a persistent committed effect.
    pub fn apply_entry(&self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.purge_if_expired(&key, now());
        if entry.expires_at.is_some_and(|expiry| now() >= expiry) {
            return shard.remove(&key);
        }
        shard.insert(key, entry)
    }

    /// Replaces the complete dataset after locking every shard in index order.
    /// Callers must serialize multi-shard observations across the replacement
    /// boundary because those operations may otherwise release one shard
    /// before acquiring the next.
    pub fn replace_all(&self, entries: Vec<(Bytes, DataEntry)>) {
        let mut shards: Vec<std::sync::MutexGuard<'_, Shard>> = self
            .shards
            .iter()
            .map(|shard| shard.lock().unwrap())
            .collect();
        for shard in &mut shards {
            shard.data.clear();
            shard.mem_bytes = 0;
            shard.op_count += 1;
            shard.last_modified = now();
        }
        for (key, entry) in entries {
            let shard_idx = shard_for_key(&key);
            shards[shard_idx].insert(key, entry);
        }
    }

    /// Sets one entry in a single shard.
    #[inline]
    pub fn set(&self, key: Bytes, value: OnyxValue, expires: Option<u64>) -> Option<DataEntry> {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.purge_if_expired(&key, now());
        if expires.is_some_and(|expiry| now() >= expiry) {
            return shard.remove(&key);
        }
        let entry = DataEntry {
            value,
            expires_at: expires,
            created_at: now(),
            last_accessed: now(),
        };
        shard.insert(key, entry)
    }

    /// Deletes one entry from a single shard.
    #[inline]
    pub fn delete(&self, key: &Bytes) -> bool {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.remove(key).is_some()
    }

    /// Reads an entry without cloning it while the closure holds the shard lock.
    pub fn read<F, R>(&self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&DataEntry) -> R,
    {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.read(key, f)
    }

    /// Mutates an existing value under one shard lock.
    pub fn update_if_exists<F, R>(&self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_if_exists(key, f)
    }

    pub fn update_if_exists_with_action<F, R>(&self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&mut OnyxValue) -> EntryMutation<R>,
    {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_if_exists_with_action(key, f)
    }

    /// Mutates a value in place, inserting a default value when absent.
    pub fn update_or_insert<F, R>(&self, key: Bytes, default: impl FnOnce() -> OnyxValue, f: F) -> R
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_or_insert(key, default, f)
    }

    pub fn update_or_insert_with_presence<F, R>(
        &self,
        key: Bytes,
        default: impl FnOnce() -> OnyxValue,
        f: F,
    ) -> R
    where
        F: FnOnce(&mut OnyxValue, bool) -> R,
    {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_or_insert_with_presence(key, default, f)
    }

    pub fn update_entry_or_insert_with_presence<F, R>(
        &self,
        key: Bytes,
        default: impl FnOnce() -> OnyxValue,
        f: F,
    ) -> R
    where
        F: FnOnce(&mut DataEntry, bool) -> R,
    {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_entry_or_insert_with_presence(key, default, f)
    }

    /// Updates only the expiration without cloning the value.
    pub fn set_expiry(&self, key: &Bytes, timestamp: u64) -> bool {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.set_expiry(key, timestamp)
    }

    pub fn set_expiry_conditional(
        &self,
        key: &Bytes,
        timestamp: u64,
        require_expiry: Option<bool>,
    ) -> bool {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.set_expiry_conditional(key, timestamp, require_expiry)
    }

    /// Applies a conditional set atomically under one shard lock.
    /// `Some(true)` means NX, `Some(false)` means XX, and `None` is unconditional.
    pub fn set_conditional(
        &self,
        key: Bytes,
        value: OnyxValue,
        expires_at: Option<u64>,
        condition: Option<bool>,
    ) -> bool {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.purge_if_expired(&key, now());
        let exists = shard.data.contains_key(&key);
        let allowed = match condition {
            Some(true) => !exists, // NX
            Some(false) => exists, // XX
            None => true,
        };
        if allowed {
            let ts = now();
            shard.insert(
                key,
                DataEntry {
                    value,
                    expires_at,
                    created_at: ts,
                    last_accessed: ts,
                },
            );
        }
        allowed
    }

    /// Inserts only when absent, atomically under one shard lock.
    pub fn set_if_absent(&self, key: Bytes, value: OnyxValue) -> bool {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        let ts = now();
        shard.insert_if_absent(
            key,
            DataEntry {
                value,
                expires_at: None,
                created_at: ts,
                last_accessed: ts,
            },
        )
    }

    /// Renames across shards while locking shard indices in ascending order.
    pub fn rename(&self, from: &Bytes, to: Bytes) -> bool {
        let from_shard = shard_for_key(from);
        let to_shard = shard_for_key(&to);

        if from_shard == to_shard {
            let mut shard = self.shards[from_shard].lock().unwrap();
            if let Some(entry) = shard.remove(from) {
                shard.insert(to, entry);
                return true;
            }
            return false;
        }

        let (lower, higher) = if from_shard < to_shard {
            (from_shard, to_shard)
        } else {
            (to_shard, from_shard)
        };
        let mut lower_lock = self.shards[lower].lock().unwrap();
        let mut higher_lock = self.shards[higher].lock().unwrap();

        let entry = if from_shard == lower {
            lower_lock.remove(from)
        } else {
            higher_lock.remove(from)
        };

        match entry {
            Some(e) => {
                if to_shard == lower {
                    lower_lock.insert(to, e);
                } else {
                    higher_lock.insert(to, e);
                }
                true
            }
            None => false,
        }
    }

    pub fn copy(&self, from: &Bytes, to: Bytes) -> bool {
        let from_shard = shard_for_key(from);
        let to_shard = shard_for_key(&to);
        let timestamp = now();

        if from_shard == to_shard {
            let mut shard = self.shards[from_shard].lock().unwrap();
            shard.purge_if_expired(from, timestamp);
            let Some(source) = shard.data.get(from).cloned() else {
                return false;
            };
            if from == &to {
                return true;
            }
            shard.insert(
                to,
                DataEntry {
                    value: source.value,
                    expires_at: source.expires_at,
                    created_at: timestamp,
                    last_accessed: timestamp,
                },
            );
            return true;
        }

        let (lower, higher) = if from_shard < to_shard {
            (from_shard, to_shard)
        } else {
            (to_shard, from_shard)
        };
        let mut lower_lock = self.shards[lower].lock().unwrap();
        let mut higher_lock = self.shards[higher].lock().unwrap();
        let source_shard = if from_shard == lower {
            &mut lower_lock
        } else {
            &mut higher_lock
        };
        source_shard.purge_if_expired(from, timestamp);
        let Some(source) = source_shard.data.get(from).cloned() else {
            return false;
        };
        let destination_shard = if to_shard == lower {
            &mut lower_lock
        } else {
            &mut higher_lock
        };
        destination_shard.insert(
            to,
            DataEntry {
                value: source.value,
                expires_at: source.expires_at,
                created_at: timestamp,
                last_accessed: timestamp,
            },
        );
        true
    }

    /// Reads multiple keys while locking at most one shard at a time.
    pub fn mget(&self, keys: &[Bytes]) -> Vec<Option<DataEntry>> {
        let mut by_shard: Vec<Vec<(usize, Bytes)>> = vec![Vec::new(); NUM_SHARDS];
        for (idx, key) in keys.iter().enumerate() {
            let s = shard_for_key(key);
            by_shard[s].push((idx, key.clone()));
        }

        let mut results: Vec<Option<DataEntry>> = vec![None; keys.len()];

        for (shard_idx, shard_keys) in by_shard.into_iter().enumerate() {
            if shard_keys.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_idx].lock().unwrap();
            for (orig_idx, key) in shard_keys {
                results[orig_idx] = shard.get(&key).cloned();
            }
        }

        results
    }

    /// Returns all non-expired keys without cloning their values.
    pub fn all_keys(&self) -> Vec<Bytes> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire_keys();
            for k in s.data.keys() {
                out.push(k.clone());
            }
        }
        out
    }

    /// Clones every non-expired entry for snapshots and full synchronization.
    pub fn snapshot_all(&self) -> Vec<(Bytes, DataEntry)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire_keys();
            for (k, entry) in s.data.iter() {
                out.push((k.clone(), entry.clone()));
            }
        }
        out
    }

    /// Returns aggregate engine statistics.
    pub fn stats(&self) -> EngineStats {
        let mut total_keys = 0;
        let mut total_ops = 0;
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire_keys();
            total_keys += s.len();
            total_ops += s.op_count;
        }
        EngineStats {
            total_keys,
            total_ops,
            num_shards: NUM_SHARDS,
        }
    }

    /// Returns the saturating sum of estimated dataset bytes across all shards.
    pub fn total_memory_bytes(&self) -> usize {
        self.shards.iter().fold(0usize, |total, shard| {
            let mut shard = shard.lock().unwrap();
            shard.expire_keys();
            total.saturating_add(shard.mem_bytes)
        })
    }

    /// Frees memory until usage is at or below `maxmemory_bytes` and returns
    /// the exact entries removed, in authoritative eviction order.
    ///
    /// Candidate selection remains approximate by design: each shard proposes
    /// one local candidate and the best candidate is selected globally.
    pub fn evict_to_fit(
        &self,
        maxmemory_bytes: usize,
        policy: EvictionPolicy,
        protected_keys: &HashSet<Bytes>,
    ) -> Vec<(Bytes, DataEntry)> {
        if policy == EvictionPolicy::NoEviction || maxmemory_bytes == 0 {
            return Vec::new();
        }
        let mut evicted = Vec::new();
        loop {
            if self.total_memory_bytes() <= maxmemory_bytes {
                break;
            }
            let mut best: Option<(usize, Bytes, u64)> = None;
            for (idx, shard_lock) in self.shards.iter().enumerate() {
                let shard = shard_lock.lock().unwrap();
                if let Some((key, score)) = shard.eviction_candidate(policy, protected_keys) {
                    let better = match &best {
                        None => true,
                        Some((_, _, best_score)) => score < *best_score,
                    };
                    if better {
                        best = Some((idx, key, score));
                    }
                }
            }
            match best {
                Some((idx, key, _)) => {
                    let mut shard = self.shards[idx].lock().unwrap();
                    if let Some(entry) = shard.remove(&key) {
                        evicted.push((key, entry));
                    }
                }
                None => break,
            }
        }
        evicted
    }

    /// Removes expired keys from every shard.
    pub fn gc_expired(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            total += s.expire_keys();
        }
        total
    }
}

#[derive(Clone, Debug)]
pub struct EngineStats {
    pub total_keys: usize,
    pub total_ops: u64,
    pub num_shards: usize,
}

// ============================================================
// UTILS
// ============================================================

#[inline]
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
