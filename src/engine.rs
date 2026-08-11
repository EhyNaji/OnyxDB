//! OnyxDB Engine — Lock-free, shard-per-core storage
//!
//! Ogni core logico ha il proprio shard. Le chiavi vengono distribuite
//! tramite consistent hashing sui shard. Ogni shard è protetto da un
//! Mutex indipendente, quindi la contesa è limitata alle chiavi che
//! cadono sullo stesso shard.

use bytes::Bytes;
use std::collections::HashMap;

// Numero di shard: potenza di 2 per hashing veloce con bitmask
pub const NUM_SHARDS: usize = 64; // 64 shard, bitmask 0x3F

/// Hash veloce per distribuire le chiavi (FNV-1a, ottimizzato per stringhe corte)
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

/// Determina lo shard per una chiave (bitmask invece di modulo, più veloce)
#[inline]
fn shard_for_key(key: &[u8]) -> usize {
    (hash_key(key) as usize) & (NUM_SHARDS - 1)
}

// ============================================================
// TIPI DI DATI ONYX (più ricchi di Redis)
// ============================================================

#[derive(Clone, Debug, PartialEq)]
pub enum OnyxValue {
    /// Blob opaco (stringa/bytes)
    Blob(Bytes),
    /// Intero nativo (64-bit signed)
    Int(i64),
    /// Float nativo (64-bit)
    Float(f64),
    /// Lista di blob
    List(Vec<Bytes>),
    /// Hash map campo->valore
    Hash(HashMap<Bytes, Bytes>),
    /// Set di blob
    Set(std::collections::HashSet<Bytes>),
    /// JSON nativo (con path query)
    Json(serde_json::Value),
    /// Vector per AI/ML (embeddings)
    Vector(Vec<f32>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataEntry {
    pub value: OnyxValue,
    pub expires_at: Option<u64>,
    pub created_at: u64,
    pub last_accessed: u64,
}

/// Stima approssimativa (non esatta: non tiene conto dell'overhead reale di
/// allocatore/HashMap) dei byte occupati da una entry. Basta per tenere la
/// memoria complessiva sotto controllo con `--maxmemory`, non è pensata per
/// essere precisa al byte.
fn approx_entry_size(key: &Bytes, entry: &DataEntry) -> usize {
    let value_size = match &entry.value {
        OnyxValue::Blob(b) => b.len(),
        OnyxValue::Int(_) => 8,
        OnyxValue::Float(_) => 8,
        OnyxValue::List(l) => l.iter().map(|b| b.len() + 8).sum(),
        OnyxValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 16).sum(),
        OnyxValue::Set(s) => s.iter().map(|b| b.len() + 8).sum(),
        OnyxValue::Json(j) => j.to_string().len(),
        OnyxValue::Vector(v) => v.len() * 4,
    };
    // Overhead fisso per la entry stessa (struct DataEntry, bucket dell'HashMap, ecc.)
    key.len() + value_size + 64
}

/// Politica di eviction quando si supera `--maxmemory`. Stesso vocabolario
/// di Redis, con lo stesso livello di approssimazione: anche Redis non fa
/// un vero minimo globale per LRU/random, campiona un sottoinsieme di
/// chiavi per restare economico. Qui il "campione" è per-shard: quando
/// serve liberare spazio, ogni shard propone il suo miglior candidato
/// locale e si sceglie il migliore tra quelli.
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
    // Moltiplicatore di Knuth: mescola bene senza bisogno di un vero RNG
    // (non serve crittograficamente sicuro, solo "abbastanza sparso").
    nanos.wrapping_mul(2654435761) % len
}

// ============================================================
// SHARD — Owned da un singolo thread alla volta (via Mutex)
// ============================================================

pub struct Shard {
    data: HashMap<Bytes, DataEntry>,
    /// Contatore operazioni per trigger snapshot incrementale
    op_count: u64,
    /// Timestamp ultima modifica
    last_modified: u64,
    /// Somma approssimativa dei byte occupati dalle entry di questo shard.
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

    /// Legge un'entry aggiornando last_accessed (necessario per un LRU vero:
    /// senza questo, "last accessed" sarebbe in realtà "last written").
    /// Prende `&mut self` apposta: lo shard è comunque dietro un Mutex, quindi
    /// anche una "lettura" ha già l'esclusività necessaria per aggiornare lo
    /// stato — non costa nulla in più rispetto a prima.
    #[inline]
    fn get(&mut self, key: &Bytes) -> Option<&DataEntry> {
        let ts = now();
        let entry = self.data.get_mut(key)?;
        if let Some(exp) = entry.expires_at
            && ts >= exp
        {
            return None; // scaduto, ma non rimosso qui (lazy)
        }
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
        self.mem_bytes += new_size;
        old
    }

    #[inline]
    fn remove(&mut self, key: &Bytes) -> Option<DataEntry> {
        self.op_count += 1;
        self.last_modified = now();
        let removed = self.data.remove(key);
        if let Some(ref e) = removed {
            let size = approx_entry_size(key, e);
            self.mem_bytes = self.mem_bytes.saturating_sub(size);
        }
        removed
    }

    /// Rimuove le chiavi scadute, ritorna quante ne ha pulite
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

    /// Legge l'entry SENZA clonarla (solo riferimento, per la durata della chiusura).
    fn read<F, R>(&mut self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&DataEntry) -> R,
    {
        self.get(key).map(f)
    }

    /// Modifica in-place il valore di una chiave se esiste già; se non esiste,
    /// non fa nulla e ritorna None. Un solo lock, nessun clone della collezione.
    fn update_if_exists<F, R>(&mut self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        let ts = now();
        match self.data.get_mut(key) {
            Some(entry) => {
                self.op_count += 1;
                self.last_modified = ts;
                let old_size = approx_entry_size(key, entry);
                entry.last_accessed = ts;
                let result = f(&mut entry.value);
                let new_size = approx_entry_size(key, entry);
                self.mem_bytes = self.mem_bytes.saturating_sub(old_size) + new_size;
                Some(result)
            }
            None => None,
        }
    }

    /// Modifica in-place il valore di una chiave, creandola con `default()` se
    /// non esiste ancora. Un solo lock, nessun clone della collezione: usa
    /// l'API `entry()` di HashMap per get-or-insert in un colpo solo.
    fn update_or_insert<F, R>(&mut self, key: Bytes, default: impl FnOnce() -> OnyxValue, f: F) -> R
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        let ts = now();
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
        let result = f(&mut entry.value);
        let new_size = approx_entry_size(&key, entry);
        self.mem_bytes = self.mem_bytes.saturating_sub(old_size) + new_size;
        result
    }

    /// Imposta solo la scadenza di una chiave esistente, senza toccare (né
    /// clonare) il valore. Ritorna false se la chiave non esiste.
    fn set_expiry(&mut self, key: &Bytes, timestamp: u64) -> bool {
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

    /// Inserisce solo se la chiave non esiste già — atomico rispetto ad altri
    /// insert_if_absent/insert/remove sullo stesso shard, perché tutto avviene
    /// sotto lo stesso lock (a differenza di un get()+set() separati).
    fn insert_if_absent(&mut self, key: Bytes, entry: DataEntry) -> bool {
        if self.data.contains_key(&key) {
            false
        } else {
            self.op_count += 1;
            self.last_modified = now();
            let size = approx_entry_size(&key, &entry);
            self.data.insert(key, entry);
            self.mem_bytes += size;
            true
        }
    }

    /// Propone il miglior candidato locale all'eviction secondo la policy:
    /// per LRU, la entry con last_accessed più vecchio; per random, una
    /// entry presa a caso. Per le policy `volatile-*` considera solo le
    /// chiavi che hanno un TTL impostato (esattamente come in Redis).
    fn eviction_candidate(&self, policy: EvictionPolicy) -> Option<(Bytes, u64)> {
        let only_volatile = matches!(
            policy,
            EvictionPolicy::VolatileLru | EvictionPolicy::VolatileRandom
        );
        let is_random = matches!(
            policy,
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom
        );

        let matching: Vec<(&Bytes, &DataEntry)> = self
            .data
            .iter()
            .filter(|(_, e)| !only_volatile || e.expires_at.is_some())
            .collect();
        if matching.is_empty() {
            return None;
        }

        if is_random {
            let idx = cheap_random_index(matching.len());
            let (k, _) = matching[idx];
            Some((k.clone(), 0))
        } else {
            matching
                .into_iter()
                .min_by_key(|(_, e)| e.last_accessed)
                .map(|(k, e)| (k.clone(), e.last_accessed))
        }
    }
}

// ============================================================
// ENGINE — Coordina gli shard, dispatch cross-shard
// ============================================================

pub struct OnyxEngine {
    shards: Vec<std::sync::Mutex<Shard>>,
}

impl OnyxEngine {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(std::sync::Mutex::new(Shard::new()));
        }
        Self { shards }
    }

    /// GET — single shard, no cross-shard needed
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
        let shard = self.shards[shard_idx].lock().unwrap();
        let entry = shard.data.get(key)?;
        if entry.expires_at.is_some_and(|expiry| now() >= expiry) {
            return None;
        }
        Some(entry.clone())
    }

    /// Installs an entry exactly as described by a persistent committed effect.
    pub fn apply_entry(&self, key: Bytes, entry: DataEntry) -> Option<DataEntry> {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
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

    /// SET — single shard
    #[inline]
    pub fn set(&self, key: Bytes, value: OnyxValue, expires: Option<u64>) -> Option<DataEntry> {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        let entry = DataEntry {
            value,
            expires_at: expires,
            created_at: now(),
            last_accessed: now(),
        };
        shard.insert(key, entry)
    }

    /// DEL — single shard
    #[inline]
    pub fn delete(&self, key: &Bytes) -> bool {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.remove(key).is_some()
    }

    /// Legge l'entry senza clonarla: il lock resta preso solo per la durata
    /// della chiusura `f`. Da preferire a `get()` ogni volta che serve solo
    /// leggere un pezzo dell'entry (un campo di un hash, la lunghezza di una
    /// lista, ecc.) invece di tutta la collezione.
    pub fn read<F, R>(&self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&DataEntry) -> R,
    {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.read(key, f)
    }

    /// Modifica in-place una chiave esistente (no-op se non esiste). Un solo
    /// lock, nessun clone: sostituisce il pattern "get() + modifica + set()"
    /// che non era atomico (due scritture concorrenti potevano pestarsi i
    /// piedi, perdendo un aggiornamento).
    pub fn update_if_exists<F, R>(&self, key: &Bytes, f: F) -> Option<R>
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_if_exists(key, f)
    }

    /// Modifica in-place una chiave, creandola se non esiste. Stesso discorso
    /// di `update_if_exists`, ma con upsert (usato da HSET, SADD, LPUSH, INCR...).
    pub fn update_or_insert<F, R>(&self, key: Bytes, default: impl FnOnce() -> OnyxValue, f: F) -> R
    where
        F: FnOnce(&mut OnyxValue) -> R,
    {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.update_or_insert(key, default, f)
    }

    /// Imposta solo la scadenza, senza clonare il valore.
    pub fn set_expiry(&self, key: &Bytes, timestamp: u64) -> bool {
        let shard_idx = shard_for_key(key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
        shard.set_expiry(key, timestamp)
    }

    /// SET condizionale, tutto sotto lo stesso lock (niente finestra
    /// exists()+set() come nell'implementazione precedente di NX/XX).
    /// `condition`: Some(true) = NX (solo se assente), Some(false) = XX
    /// (solo se già presente), None = incondizionato. Ritorna true se ha
    /// scritto.
    pub fn set_conditional(
        &self,
        key: Bytes,
        value: OnyxValue,
        expires_at: Option<u64>,
        condition: Option<bool>,
    ) -> bool {
        let shard_idx = shard_for_key(&key);
        let mut shard = self.shards[shard_idx].lock().unwrap();
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

    /// Inserisce solo se assente, in modo atomico (per SETNX usato come lock
    /// distribuito: prima non lo era davvero, perché get()+set() lasciava una
    /// finestra in cui due SETNX concorrenti potevano credere entrambi di
    /// aver vinto la corsa).
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

    /// RENAME — puo' toccare due shard diversi. Per evitare deadlock quando
    /// due RENAME concorrenti si "incrociano" tra gli stessi due shard,
    /// blocchiamo SEMPRE nello stesso ordine (indice di shard crescente),
    /// indipendentemente da chi e' 'from' e chi e' 'to'.
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

    /// MGET — può toccare multipli shard. Blocca uno shard alla volta
    /// (mai due contemporaneamente), quindi non c'è rischio di deadlock.
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

    /// Ritorna solo le chiavi non scadute, su tutti gli shard. Più leggero
    /// di `snapshot_all` (non clona i valori), usato dal comando KEYS.
    pub fn all_keys(&self) -> Vec<Bytes> {
        let current = now();
        let mut out = Vec::new();
        for shard in &self.shards {
            let s = shard.lock().unwrap();
            for (k, entry) in s.data.iter() {
                let expired = entry.expires_at.is_some_and(|exp| current >= exp);
                if !expired {
                    out.push(k.clone());
                }
            }
        }
        out
    }

    /// Ritorna una copia di tutte le entry non scadute, su tutti gli shard.
    /// Costoso (clona tutto): va usato solo per operazioni "rare" come
    /// snapshot su disco e dump iniziale verso una Replica in SYNC, mai
    /// sul percorso caldo di un singolo comando.
    pub fn snapshot_all(&self) -> Vec<(Bytes, DataEntry)> {
        let current = now();
        let mut out = Vec::new();
        for shard in &self.shards {
            let s = shard.lock().unwrap();
            for (k, entry) in s.data.iter() {
                let expired = entry.expires_at.is_some_and(|exp| current >= exp);
                if !expired {
                    out.push((k.clone(), entry.clone()));
                }
            }
        }
        out
    }

    /// Stats aggregate
    pub fn stats(&self) -> EngineStats {
        let mut total_keys = 0;
        let mut total_ops = 0;
        for shard in &self.shards {
            let s = shard.lock().unwrap();
            total_keys += s.len();
            total_ops += s.op_count;
        }
        EngineStats {
            total_keys,
            total_ops,
            num_shards: NUM_SHARDS,
        }
    }

    /// Somma approssimativa dei byte occupati da tutte le entry, su tutti
    /// gli shard. Usata da `--maxmemory` per decidere quando fare eviction.
    pub fn total_memory_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().mem_bytes)
            .sum()
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
    ) -> Vec<(Bytes, DataEntry)> {
        if policy == EvictionPolicy::NoEviction || maxmemory_bytes == 0 {
            return Vec::new();
        }
        let mut evicted = Vec::new();
        // Do not loop indefinitely when the selected policy cannot evict any
        // remaining entry, such as volatile policies with no expiring keys.
        for _ in 0..10_000 {
            if self.total_memory_bytes() <= maxmemory_bytes {
                break;
            }
            let mut best: Option<(usize, Bytes, u64)> = None;
            for (idx, shard_lock) in self.shards.iter().enumerate() {
                let shard = shard_lock.lock().unwrap();
                if let Some((key, score)) = shard.eviction_candidate(policy) {
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

    /// GC: pulisce chiavi scadute su tutti gli shard
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
