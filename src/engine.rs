//! OnyxDB engine — sharded in-memory storage.
//!
//! Keys are distributed across a fixed number of shards using FNV-1a hashing.
//! Each shard has an independent mutex, so contention is limited to keys that
//! map to the same shard.

use crate::clock::unix_seconds;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
    data: ShardData,
    /// Operation counter retained for engine statistics.
    op_count: u64,
    /// Timestamp of the most recent modification.
    last_modified: u64,
    /// Approximate logical bytes occupied by entries in this shard.
    mem_bytes: usize,
    /// Logical key count across the flat map or snapshot base plus changes.
    key_count: usize,
    total_keys: Arc<AtomicUsize>,
}

/// A snapshot epoch keeps the authoritative base immutable while live writes
/// record only the keys they change. Once the snapshot view is dropped, the
/// delta is folded back into the original map without cloning untouched data.
enum ShardData {
    Flat(HashMap<Bytes, DataEntry>),
    Snapshot {
        base: Arc<HashMap<Bytes, DataEntry>>,
        changes: HashMap<Bytes, Option<DataEntry>>,
    },
}

impl ShardData {
    fn get(&self, key: &Bytes) -> Option<&DataEntry> {
        match self {
            Self::Flat(data) => data.get(key),
            Self::Snapshot { base, changes } => match changes.get(key) {
                Some(Some(entry)) => Some(entry),
                Some(None) => None,
                None => base.get(key),
            },
        }
    }

    fn get_mut(&mut self, key: &Bytes) -> Option<&mut DataEntry> {
        match self {
            Self::Flat(data) => data.get_mut(key),
            Self::Snapshot { base, changes } => {
                if !changes.contains_key(key) {
                    changes.insert(key.clone(), base.get(key).cloned());
                }
                changes.get_mut(key).and_then(Option::as_mut)
            }
        }
    }

    fn insert(&mut self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        match self {
            Self::Flat(data) => data.insert(key, entry),
            Self::Snapshot { base, changes } => {
                let previous = match changes.get(&key) {
                    Some(previous) => previous.clone(),
                    None => base.get(&key).cloned(),
                };
                changes.insert(key, Some(entry));
                previous
            }
        }
    }

    fn remove(&mut self, key: &Bytes) -> Option<DataEntry> {
        match self {
            Self::Flat(data) => data.remove(key),
            Self::Snapshot { base, changes } => {
                let previous = match changes.get(key) {
                    Some(previous) => previous.clone(),
                    None => base.get(key).cloned(),
                };
                if previous.is_some() {
                    changes.insert(key.clone(), None);
                }
                previous
            }
        }
    }

    fn visit(&self, mut visitor: impl FnMut(&Bytes, &DataEntry)) {
        match self {
            Self::Flat(data) => {
                for (key, entry) in data {
                    visitor(key, entry);
                }
            }
            Self::Snapshot { base, changes } => {
                for (key, change) in changes {
                    if let Some(entry) = change {
                        visitor(key, entry);
                    }
                }
                for (key, entry) in base.iter() {
                    if !changes.contains_key(key) {
                        visitor(key, entry);
                    }
                }
            }
        }
    }

    fn begin_snapshot(&mut self) -> Arc<HashMap<Bytes, DataEntry>> {
        let current = std::mem::replace(self, Self::Flat(HashMap::new()));
        match current {
            Self::Flat(data) => {
                let base = Arc::new(data);
                *self = Self::Snapshot {
                    base: Arc::clone(&base),
                    changes: HashMap::new(),
                };
                base
            }
            snapshot @ Self::Snapshot { .. } => {
                *self = snapshot;
                panic!("an engine snapshot epoch is already active");
            }
        }
    }

    fn finish_snapshot(&mut self) -> bool {
        let current = std::mem::replace(self, Self::Flat(HashMap::new()));
        let Self::Snapshot { base, changes } = current else {
            *self = current;
            return false;
        };
        let mut data = match Arc::try_unwrap(base) {
            Ok(data) => data,
            Err(shared) => (*shared).clone(),
        };
        for (key, change) in changes {
            match change {
                Some(entry) => {
                    data.insert(key, entry);
                }
                None => {
                    data.remove(&key);
                }
            }
        }
        *self = Self::Flat(data);
        true
    }
}

impl Shard {
    fn new(total_keys: Arc<AtomicUsize>) -> Self {
        Self {
            data: ShardData::Flat(HashMap::with_capacity(1024)),
            op_count: 0,
            last_modified: 0,
            mem_bytes: 0,
            key_count: 0,
            total_keys,
        }
    }

    fn record_key_removal(&self) {
        let result = self
            .total_keys
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            });
        debug_assert!(result.is_ok(), "physical key count underflow");
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
            self.key_count = self.key_count.saturating_sub(1);
            self.record_key_removal();
        }
        true
    }

    /// Reads an entry and updates `last_accessed` for LRU candidate selection.
    /// The mutable receiver is safe because callers already hold the shard mutex.
    #[inline]
    fn get(&mut self, key: &Bytes) -> Option<&DataEntry> {
        let ts = unix_seconds();
        self.purge_if_expired(key, ts);
        let entry = self.data.get_mut(key)?;
        entry.last_accessed = ts;
        Some(&*entry)
    }

    #[inline]
    fn insert(&mut self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        self.op_count += 1;
        self.last_modified = unix_seconds();
        let new_size = approx_entry_size(&key, &entry);
        let old = self.data.insert(key.clone(), entry);
        if old.is_none() {
            self.key_count = self.key_count.saturating_add(1);
            self.total_keys.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(ref old_entry) = old {
            let old_size = approx_entry_size(&key, old_entry);
            self.mem_bytes = self.mem_bytes.saturating_sub(old_size);
        }
        self.mem_bytes = self.mem_bytes.saturating_add(new_size);
        old
    }

    #[inline]
    fn remove(&mut self, key: &Bytes) -> Option<DataEntry> {
        if self.purge_if_expired(key, unix_seconds()) {
            return None;
        }
        self.op_count += 1;
        self.last_modified = unix_seconds();
        let removed = self.data.remove(key);
        if let Some(ref e) = removed {
            let size = approx_entry_size(key, e);
            self.mem_bytes = self.mem_bytes.saturating_sub(size);
            self.key_count = self.key_count.saturating_sub(1);
            self.record_key_removal();
        }
        removed
    }

    /// Removes expired keys and returns the number removed.
    fn expire_keys(&mut self) -> usize {
        let now = unix_seconds();
        let mut expired = Vec::new();
        self.data.visit(|key, entry| {
            if entry.expires_at.is_some_and(|expiry| now >= expiry) {
                expired.push(key.clone());
            }
        });

        let count = expired.len();
        for key in expired {
            if let Some(e) = self.data.remove(&key) {
                let size = approx_entry_size(&key, &e);
                self.mem_bytes = self.mem_bytes.saturating_sub(size);
                self.key_count = self.key_count.saturating_sub(1);
                self.record_key_removal();
            }
        }
        count
    }

    fn len(&self) -> usize {
        self.key_count
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
        let ts = unix_seconds();
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
                    self.key_count = self.key_count.saturating_sub(1);
                    self.record_key_removal();
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
        let ts = unix_seconds();
        self.purge_if_expired(&key, ts);
        self.op_count += 1;
        self.last_modified = ts;
        let existed = self.data.get(&key).is_some();
        if !existed {
            self.data.insert(
                key.clone(),
                DataEntry {
                    value: default(),
                    expires_at: None,
                    created_at: ts,
                    last_accessed: ts,
                },
            );
            self.key_count = self.key_count.saturating_add(1);
            self.total_keys.fetch_add(1, Ordering::SeqCst);
        }
        let entry = self
            .data
            .get_mut(&key)
            .expect("an inserted or existing entry must be mutable");
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
        let current = unix_seconds();
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
                self.last_modified = unix_seconds();
                true
            }
            None => false,
        }
    }

    /// Inserts only when the key is absent, atomically under the shard lock.
    fn insert_if_absent(&mut self, key: Bytes, entry: DataEntry) -> bool {
        self.purge_if_expired(&key, unix_seconds());
        if self.data.get(&key).is_some() {
            false
        } else {
            self.op_count += 1;
            self.last_modified = unix_seconds();
            let size = approx_entry_size(&key, &entry);
            self.data.insert(key, entry);
            self.mem_bytes = self.mem_bytes.saturating_add(size);
            self.key_count = self.key_count.saturating_add(1);
            self.total_keys.fetch_add(1, Ordering::SeqCst);
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

        let eligible = |key: &Bytes, entry: &DataEntry| {
            !protected_keys.contains(key) && (!only_volatile || entry.expires_at.is_some())
        };
        if is_random {
            let mut matching_count = 0usize;
            self.data.visit(|key, entry| {
                if eligible(key, entry) {
                    matching_count = matching_count.saturating_add(1);
                }
            });
            let selected_index = cheap_random_index(matching_count);
            let mut current_index = 0usize;
            let mut selected = None;
            self.data.visit(|key, entry| {
                if selected.is_none() && eligible(key, entry) {
                    if current_index == selected_index {
                        selected = Some((key.clone(), 0));
                    }
                    current_index = current_index.saturating_add(1);
                }
            });
            selected
        } else {
            let mut selected: Option<(Bytes, u64)> = None;
            self.data.visit(|key, entry| {
                if !eligible(key, entry) {
                    return;
                }
                if selected
                    .as_ref()
                    .is_none_or(|(_, score)| entry.last_accessed < *score)
                {
                    selected = Some((key.clone(), entry.last_accessed));
                }
            });
            selected
        }
    }
}

// ============================================================
// ENGINE — shard coordination and cross-shard operations
// ============================================================

pub struct OnyxEngine {
    shards: Vec<std::sync::Mutex<Shard>>,
    total_keys: Arc<AtomicUsize>,
    snapshot_active: AtomicBool,
}

/// Immutable point-in-time engine state. Expiration is evaluated once at the
/// capture boundary so entries that expire while the file is being written do
/// not change the snapshot's logical contents.
pub struct EngineSnapshot {
    shards: Vec<Option<Arc<HashMap<Bytes, DataEntry>>>>,
    captured_at: u64,
    entry_capacity: usize,
}

impl EngineSnapshot {
    pub fn entries(&self) -> impl Iterator<Item = (&Bytes, &DataEntry)> {
        let captured_at = self.captured_at;
        self.shards
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(|shard| shard.iter())
            .filter(move |(_, entry)| entry.expires_at.is_none_or(|expiry| captured_at < expiry))
    }

    pub fn len(&self) -> usize {
        self.entries().count()
    }

    pub fn is_empty(&self) -> bool {
        self.entries().next().is_none()
    }

    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub(crate) fn entry_capacity(&self) -> usize {
        self.entry_capacity
    }

    pub(crate) fn append_shard_entries(
        &self,
        shard_index: usize,
        entries: &mut Vec<(Bytes, DataEntry)>,
    ) {
        let captured_at = self.captured_at;
        let shard = self.shards[shard_index]
            .as_ref()
            .expect("an unreleased snapshot shard must retain its immutable view");
        entries.extend(
            shard
                .iter()
                .filter(|(_, entry)| entry.expires_at.is_none_or(|expiry| captured_at < expiry))
                .map(|(key, entry)| (key.clone(), entry.clone())),
        );
    }

    pub(crate) fn release_shard(&mut self, shard_index: usize) {
        assert!(
            self.shards[shard_index].take().is_some(),
            "a snapshot shard cannot be released twice"
        );
    }
}

impl Default for OnyxEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl OnyxEngine {
    pub fn new() -> Self {
        let total_keys = Arc::new(AtomicUsize::new(0));
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(std::sync::Mutex::new(Shard::new(Arc::clone(&total_keys))));
        }
        Self {
            shards,
            total_keys,
            snapshot_active: AtomicBool::new(false),
        }
    }

    /// Returns the number of physically present keys without sweeping expiry.
    /// Callers that are near a hard key limit must perform an exact expiry-aware
    /// scan before rejecting growth.
    pub fn physical_key_count(&self) -> usize {
        self.total_keys.load(Ordering::SeqCst)
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
        shard.purge_if_expired(key, unix_seconds());
        shard.data.get(key).cloned()
    }

    /// Starts one copy-on-write snapshot epoch in O(shard-count) time.
    /// Snapshot installation is serialized above the engine, so nesting is an
    /// architectural violation and fails closed.
    pub fn begin_snapshot(&self) -> EngineSnapshot {
        assert!(
            self.snapshot_active
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "an engine snapshot epoch is already active"
        );
        let captured_at = unix_seconds();
        let shards = self
            .shards
            .iter()
            .map(|shard| Some(shard.lock().unwrap().data.begin_snapshot()))
            .collect();
        EngineSnapshot {
            shards,
            captured_at,
            entry_capacity: self.physical_key_count(),
        }
    }

    pub(crate) fn finish_snapshot_shard(&self, shard_index: usize) {
        assert!(
            self.shards[shard_index]
                .lock()
                .unwrap()
                .data
                .finish_snapshot(),
            "an engine snapshot shard cannot be finished twice"
        );
    }

    /// Folds live snapshot deltas back into their original shard maps after the
    /// immutable view has been dropped.
    pub fn finish_snapshot(&self) {
        assert!(
            self.snapshot_active.load(Ordering::SeqCst),
            "no engine snapshot epoch is active"
        );
        for shard in &self.shards {
            shard.lock().unwrap().data.finish_snapshot();
        }
        self.snapshot_active.store(false, Ordering::SeqCst);
    }

    /// Installs an entry exactly as described by a persistent committed effect.
    pub fn apply_entry(&self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.purge_if_expired(&key, unix_seconds());
        if entry
            .expires_at
            .is_some_and(|expiry| unix_seconds() >= expiry)
        {
            return shard.remove(&key);
        }
        shard.insert(key, entry)
    }

    /// Replaces the complete dataset after locking every shard in index order.
    /// Callers must serialize multi-shard observations across the replacement
    /// boundary because those operations may otherwise release one shard
    /// before acquiring the next.
    pub fn replace_all(&self, entries: Vec<(Bytes, DataEntry)>) {
        assert!(
            !self.snapshot_active.load(Ordering::SeqCst),
            "complete dataset replacement cannot overlap a snapshot epoch"
        );
        let mut shards: Vec<std::sync::MutexGuard<'_, Shard>> = self
            .shards
            .iter()
            .map(|shard| shard.lock().unwrap())
            .collect();
        for shard in &mut shards {
            shard.data = ShardData::Flat(HashMap::new());
            shard.mem_bytes = 0;
            shard.key_count = 0;
            shard.op_count += 1;
            shard.last_modified = unix_seconds();
        }
        self.total_keys.store(0, Ordering::SeqCst);
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
        shard.purge_if_expired(&key, unix_seconds());
        if expires.is_some_and(|expiry| unix_seconds() >= expiry) {
            return shard.remove(&key);
        }
        let entry = DataEntry {
            value,
            expires_at: expires,
            created_at: unix_seconds(),
            last_accessed: unix_seconds(),
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
        shard.purge_if_expired(&key, unix_seconds());
        let exists = shard.data.get(&key).is_some();
        let allowed = match condition {
            Some(true) => !exists, // NX
            Some(false) => exists, // XX
            None => true,
        };
        if allowed {
            let ts = unix_seconds();
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
        let ts = unix_seconds();
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
        let timestamp = unix_seconds();

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
            s.data.visit(|key, _| out.push(key.clone()));
        }
        out
    }

    /// Clones every non-expired entry for snapshots and full synchronization.
    pub fn snapshot_all(&self) -> Vec<(Bytes, DataEntry)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire_keys();
            s.data
                .visit(|key, entry| out.push((key.clone(), entry.clone())));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(value: &'static [u8], expires_at: Option<u64>) -> DataEntry {
        DataEntry {
            value: OnyxValue::Blob(Bytes::from_static(value)),
            expires_at,
            created_at: unix_seconds(),
            last_accessed: unix_seconds(),
        }
    }

    #[test]
    fn physical_key_count_tracks_single_shard_mutations() {
        let engine = OnyxEngine::new();
        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");

        engine.set(first.clone(), OnyxValue::Int(1), None);
        assert_eq!(engine.physical_key_count(), 1);
        engine.set(first.clone(), OnyxValue::Int(2), None);
        assert_eq!(engine.physical_key_count(), 1);

        assert!(engine.set_if_absent(second.clone(), OnyxValue::Int(3)));
        assert!(!engine.set_if_absent(second.clone(), OnyxValue::Int(4)));
        assert_eq!(engine.physical_key_count(), 2);

        assert!(engine.set_expiry(&second, 1));
        assert_eq!(engine.physical_key_count(), 1);
        assert!(engine.delete(&first));
        assert_eq!(engine.physical_key_count(), 0);
    }

    #[test]
    fn physical_key_count_tracks_replacement_expiry_and_action_deletion() {
        let engine = OnyxEngine::new();
        let first = Bytes::from_static(b"replacement:first");
        let second = Bytes::from_static(b"replacement:second");
        engine.replace_all(vec![
            (first.clone(), entry(b"one", None)),
            (second.clone(), entry(b"two", Some(1))),
        ]);
        assert_eq!(engine.physical_key_count(), 2);
        assert_eq!(engine.gc_expired(), 1);
        assert_eq!(engine.physical_key_count(), 1);

        let deleted = engine.update_if_exists_with_action(&first, |_| EntryMutation::Delete(()));
        assert_eq!(deleted, Some(()));
        assert_eq!(engine.physical_key_count(), 0);

        engine.replace_all(vec![(second, entry(b"replacement", None))]);
        assert_eq!(engine.physical_key_count(), 1);
        assert_eq!(engine.stats().total_keys, engine.physical_key_count());
    }

    #[test]
    fn physical_key_count_remains_consistent_when_insert_mutation_unwinds() {
        let engine = OnyxEngine::new();
        let key = Bytes::from_static(b"unwind");
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            engine.update_or_insert(
                key.clone(),
                || OnyxValue::Int(0),
                |_| {
                    panic!("injected mutation failure");
                },
            );
        }));

        assert!(unwind.is_err());
        assert_eq!(engine.physical_key_count(), 1);
    }

    #[test]
    fn physical_key_count_converges_after_concurrent_shard_mutations() {
        let engine = Arc::new(OnyxEngine::new());
        let mut writers = Vec::new();
        for worker in 0..4 {
            let engine = Arc::clone(&engine);
            writers.push(std::thread::spawn(move || {
                for key in 0..256 {
                    engine.set(
                        Bytes::from(format!("worker:{worker}:key:{key}")),
                        OnyxValue::Int(key),
                        None,
                    );
                }
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }

        assert_eq!(engine.physical_key_count(), 1024);
        assert_eq!(engine.stats().total_keys, 1024);
    }

    #[test]
    fn snapshot_epoch_preserves_the_boundary_while_live_state_changes() {
        let engine = OnyxEngine::new();
        let first = Bytes::from_static(b"snapshot:first");
        let second = Bytes::from_static(b"snapshot:second");
        let third = Bytes::from_static(b"snapshot:third");
        engine.set(first.clone(), OnyxValue::Int(1), None);
        engine.set(second.clone(), OnyxValue::Int(2), None);

        let snapshot = engine.begin_snapshot();
        engine.set(first.clone(), OnyxValue::Int(10), None);
        assert!(engine.delete(&second));
        engine.set(third.clone(), OnyxValue::Int(3), None);

        let captured = snapshot
            .entries()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            captured.get(&first).map(|entry| &entry.value),
            Some(&OnyxValue::Int(1))
        );
        assert_eq!(
            captured.get(&second).map(|entry| &entry.value),
            Some(&OnyxValue::Int(2))
        );
        assert!(!captured.contains_key(&third));
        assert_eq!(
            engine.peek(&first).map(|entry| entry.value),
            Some(OnyxValue::Int(10))
        );
        assert_eq!(engine.peek(&second), None);
        assert_eq!(
            engine.peek(&third).map(|entry| entry.value),
            Some(OnyxValue::Int(3))
        );

        drop(snapshot);
        engine.finish_snapshot();
        assert_eq!(engine.stats().total_keys, 2);
        assert_eq!(engine.physical_key_count(), 2);
        assert_eq!(
            engine.peek(&first).map(|entry| entry.value),
            Some(OnyxValue::Int(10))
        );
        assert_eq!(
            engine.peek(&third).map(|entry| entry.value),
            Some(OnyxValue::Int(3))
        );
    }

    #[test]
    fn snapshot_epoch_copies_only_changed_entries_and_filters_expiry_at_capture() {
        let engine = OnyxEngine::new();
        let live = Bytes::from_static(b"snapshot:live");
        let expired = Bytes::from_static(b"snapshot:expired");
        engine.replace_all(vec![
            (live.clone(), entry(b"old", None)),
            (expired, entry(b"expired", Some(1))),
        ]);

        let snapshot = engine.begin_snapshot();
        engine.set(
            live.clone(),
            OnyxValue::Blob(Bytes::from_static(b"new")),
            None,
        );

        let shard = engine.shards[shard_for_key(&live)].lock().unwrap();
        let ShardData::Snapshot { base, changes } = &shard.data else {
            panic!("snapshot capture did not install a copy-on-write shard");
        };
        assert!(base.contains_key(&live));
        assert_eq!(changes.len(), 1);
        assert!(changes.contains_key(&live));
        drop(shard);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.entries().next().map(|(key, _)| key), Some(&live));

        drop(snapshot);
        engine.finish_snapshot();
        assert_eq!(
            engine.peek(&live).map(|entry| entry.value),
            Some(OnyxValue::Blob(Bytes::from_static(b"new")))
        );
    }

    #[test]
    fn snapshot_epoch_remains_exact_while_shards_return_to_flat_storage() {
        fn key_in_shard(shard_index: usize) -> Bytes {
            (0usize..)
                .map(|candidate| Bytes::from(format!("snapshot:shard:{candidate}")))
                .find(|key| shard_for_key(key) == shard_index)
                .expect("a key must map to every shard")
        }

        let engine = OnyxEngine::new();
        let released_key = key_in_shard(0);
        let pending_key = key_in_shard(1);
        engine.set(released_key.clone(), OnyxValue::Int(1), None);
        engine.set(pending_key.clone(), OnyxValue::Int(2), None);

        let mut snapshot = engine.begin_snapshot();
        let mut captured = Vec::new();
        snapshot.append_shard_entries(0, &mut captured);
        snapshot.release_shard(0);
        engine.finish_snapshot_shard(0);

        engine.set(released_key.clone(), OnyxValue::Int(10), None);
        engine.set(pending_key.clone(), OnyxValue::Int(20), None);
        captured.extend(
            snapshot
                .entries()
                .map(|(key, entry)| (key.clone(), entry.clone())),
        );
        drop(snapshot);
        engine.finish_snapshot();

        let captured = captured.into_iter().collect::<HashMap<_, _>>();
        assert_eq!(
            captured.get(&released_key).map(|entry| &entry.value),
            Some(&OnyxValue::Int(1))
        );
        assert_eq!(
            captured.get(&pending_key).map(|entry| &entry.value),
            Some(&OnyxValue::Int(2))
        );
        assert_eq!(
            engine.peek(&released_key).map(|entry| entry.value),
            Some(OnyxValue::Int(10))
        );
        assert_eq!(
            engine.peek(&pending_key).map(|entry| entry.value),
            Some(OnyxValue::Int(20))
        );
    }
}
