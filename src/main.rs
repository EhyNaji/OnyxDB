mod storage;
mod resp;
mod engine;
mod protocol;
use engine::{OnyxEngine, OnyxValue, DataEntry, EvictionPolicy};
use protocol::OBPFrame;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use tracing::{info, warn, error};
use std::env;
use resp::{RESPValue, read_command};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader as StdBufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::io::{BufReader as TokioBufReader, BufWriter as TokioBufWriter, AsyncWriteExt, AsyncReadExt};use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

// 1. ALLOCATORE DI MEMORIA AD ALTE PRESTAZIONI
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SNAPSHOT_PATH: &str = "onyx.snapshot";
const LOG_PATH: &str = "onyx.log";
const COMPACTION_THRESHOLD: usize = 100000;
const MAX_KEYS: usize = 1_000_000;
/// Utenti autorizzati (nome -> password). Nessuna granularità per comando
/// (quello sarebbe un'altra feature a parte) — qui
/// è "chi ha una password valida può fare tutto", ma con utenti multipli
/// invece di un'unica password condivisa. `--requirepass`/`ONYXDB_PASSWORD`
/// restano supportati per compatibilità: diventano l'utente "default".
static USERS: std::sync::OnceLock<std::collections::HashMap<String, String>> = std::sync::OnceLock::new();

fn auth_required() -> bool {
    USERS.get().map(|u| !u.is_empty()).unwrap_or(false)
}

fn check_credentials(username: &str, password: &str) -> bool {
    match USERS.get() {
        Some(users) => users.get(username).map(|p| p == password).unwrap_or(false),
        None => false,
    }
}

// ============================================================
// FSYNC POLICY — quanto spesso forzare la scrittura fisica su disco del
// binlog. 
// - Always:    fsync dopo ogni batch di scritture. Massima durabilità,
//              più latenza per comando.
// - EverySec:  fsync una volta al secondo in background (default, come Redis).
//              Nel peggiore dei casi si perde fino a ~1s di scritture se il
//              sistema operativo/hardware crasha (non il solo processo).
// - No:        nessun fsync esplicito, solo il flush dei buffer userspace.
//              Più veloce, ma affidato completamente al SO per la durabilità.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Some(FsyncPolicy::Always),
            "everysec" => Some(FsyncPolicy::EverySec),
            "no" => Some(FsyncPolicy::No),
            _ => None,
        }
    }
}

static FSYNC_POLICY: std::sync::OnceLock<FsyncPolicy> = std::sync::OnceLock::new();

fn fsync_policy() -> FsyncPolicy {
    *FSYNC_POLICY.get().unwrap_or(&FsyncPolicy::EverySec)
}

/// Apre (o crea) il binlog in append mode, ritentando ogni 3s in caso di
/// errore I/O invece di far crashare il processo (disco pieno, permessi,
/// file bloccato da un antivirus, ecc.). Bloccante di proposito: viene
/// chiamata solo all'avvio e nei rari momenti di compattazione, mai sul
/// percorso caldo di un comando.
fn open_binlog_file(path: &str) -> File {
    loop {
        match OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => return f,
            Err(e) => {
                error!("Impossibile aprire {} ({}). Ritento tra 3s...", path, e);
                std::thread::sleep(Duration::from_secs(3));
            }
        }
    }
}

// ============================================================
// MEMORY EVICTION — limite di memoria configurabile
// ============================================================
// `--maxmemory <bytes>` (0 = nessun limite, default) + `--maxmemory-policy`
// (default noeviction). Quando configurato, prima di ogni scrittura che
// creerebbe una nuova chiave si controlla se si è sopra la soglia e, se sì,
// si libera spazio secondo la policy (o si rifiuta il comando, con
// noeviction).
static MAXMEMORY_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static MAXMEMORY_POLICY: std::sync::OnceLock<EvictionPolicy> = std::sync::OnceLock::new();

fn maxmemory_bytes() -> usize {
    *MAXMEMORY_BYTES.get().unwrap_or(&0)
}

fn maxmemory_policy() -> EvictionPolicy {
    *MAXMEMORY_POLICY.get().unwrap_or(&EvictionPolicy::NoEviction)
}

/// Converte una stringa tipo "100mb", "1gb", "500kb" o un numero puro di
/// byte nel corrispondente valore in byte. 
/// (`--maxmemory 100mb`), case-insensitive.
fn parse_memory_size(s: &str) -> Option<usize> {
    let s = s.trim().to_ascii_lowercase();
    let (number_part, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    number_part.trim().parse::<usize>().ok().map(|n| n * multiplier)
}

// 2. FAST TIME (Orologio di Sistema Cachato)
static CURRENT_TIME: AtomicU64 = AtomicU64::new(0);
static START_TIME: AtomicU64 = AtomicU64::new(0);
static IS_REPLICA: AtomicBool = AtomicBool::new(false);
// Replication ID: generato una volta ad ogni avvio del Master (casuale,
// derivato dal tempo di avvio + PID). Serve a distinguere "sono lo stesso
// processo Master di prima" da "sono ripartito da zero" — senza questo,
// una Replica che si riconnette con un vecchio offset dopo un riavvio del
// Master rischia di credersi "già allineata" quando in realtà il nuovo
// processo non ha alcuna memoria di quell'offset. 0 è riservato per
// "replication ID sconosciuto" lato Replica (prima connessione mai fatta).
static REPL_ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

fn repl_id() -> u64 {
    *REPL_ID.get().unwrap_or(&0)
}
static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_COMMANDS: AtomicUsize = AtomicUsize::new(0);
static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);
fn now() -> u64 {
    CURRENT_TIME.load(Ordering::Relaxed)
}

pub struct ShardedStore {
    engine: OnyxEngine,
}

impl ShardedStore {
    pub fn new() -> Self {
        Self {
            engine: OnyxEngine::new(),
        }
    }

    // --- String operations ---
    pub fn set(&self, key: String, value: String) {
        self.engine.set(Bytes::from(key), OnyxValue::Blob(Bytes::from(value)), None);
    }

    pub fn set_raw(&self, key: String, entry: DataEntry) {
        self.engine.set(Bytes::from(key), entry.value, entry.expires_at);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.engine.read(&Bytes::from(key.to_string()), |entry| {
            match &entry.value {
                OnyxValue::Blob(b) => Some(String::from_utf8_lossy(b).to_string()),
                OnyxValue::Int(n) => Some(n.to_string()),
                OnyxValue::List(l) => Some(format!("{:?}", l)),
                OnyxValue::Hash(h) => Some(format!("{:?}", h)),
                OnyxValue::Set(s) => Some(format!("{:?}", s)),
                _ => None,
            }
        }).flatten()
    }

    pub fn get_raw(&self, key: &str) -> Option<DataEntry> {
        self.engine.get(&Bytes::from(key.to_string()))
    }

    pub fn delete(&self, key: &str) -> bool {
        self.engine.delete(&Bytes::from(key.to_string()))
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn expire_at(&self, key: &str, timestamp: u64) -> bool {
        // Un solo lock: imposta solo la scadenza, senza clonare il valore
        // (prima: get() dell'intera entry + set() di rimpiazzo).
        self.engine.set_expiry(&Bytes::from(key.to_string()), timestamp)
    }

    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        let now = now();
        self.expire_at(key, now + seconds)
    }

    pub fn ttl(&self, key: &str) -> i64 {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| {
                if let Some(exp) = entry.expires_at {
                    let remaining = exp.saturating_sub(now());
                    if remaining == 0 { -2 } else { remaining as i64 }
                } else {
                    -1
                }
            })
            .unwrap_or(-2)
    }

    pub fn incr(&self, key: &str) -> i64 {
        self.incrby(key, 1)
    }

    pub fn incrby(&self, key: &str, delta: i64) -> i64 {
        // Un solo lock per leggi-e-scrivi: prima era get()+set() separati,
        // quindi due INCR concorrenti sulla stessa chiave potevano leggere lo
        // stesso valore di partenza e uno dei due incrementi andava perso.
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Int(0),
            move |v| {
                let new_val = match v {
                    OnyxValue::Int(n) => *n + delta,
                    OnyxValue::Blob(b) => {
                        String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0) + delta
                    }
                    _ => delta,
                };
                *v = OnyxValue::Int(new_val);
                new_val
            },
        )
    }

    pub fn append(&self, key: &str, suffix: &str) -> usize {
        let suffix_owned = suffix.to_string();
        // Stesso discorso di incrby: un solo lock, niente APPEND persi sotto
        // concorrenza.
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Blob(Bytes::new()),
            move |v| {
                let mut s = match v {
                    OnyxValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    _ => String::new(),
                };
                s.push_str(&suffix_owned);
                let len = s.len();
                *v = OnyxValue::Blob(Bytes::from(s));
                len
            },
        )
    }

    pub fn strlen(&self, key: &str) -> usize {
        self.get(key).map(|s| s.len()).unwrap_or(0)
    }

    pub fn getset(&self, key: &str, new_value: &str) -> Option<String> {
        let new_value_owned = new_value.to_string();
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Blob(Bytes::new()),
            move |v| {
                let old = match v {
                    OnyxValue::Blob(b) => Some(String::from_utf8_lossy(b).to_string()),
                    OnyxValue::Int(n) => Some(n.to_string()),
                    OnyxValue::List(l) => Some(format!("{:?}", l)),
                    OnyxValue::Hash(h) => Some(format!("{:?}", h)),
                    OnyxValue::Set(s) => Some(format!("{:?}", s)),
                    _ => None,
                };
                *v = OnyxValue::Blob(Bytes::from(new_value_owned));
                old
            },
        )
    }

    pub fn setnx(&self, key: &str, value: &str) -> bool {
        //prima era exists()+set(), con una
        // finestra in cui due SETNX concorrenti potevano credere entrambi
        // di aver "vinto" — il che vanificava il suo uso come lock.
        self.engine.set_if_absent(
            Bytes::from(key.to_string()),
            OnyxValue::Blob(Bytes::from(value.to_string())),
        )
    }

    // --- List operations ---
    pub fn lpush(&self, key: &str, item: String) -> usize {
        let item_b = Bytes::from(item);
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::List(Vec::new()),
            move |v| match v {
                OnyxValue::List(l) => {
                    l.insert(0, item_b);
                    l.len()
                }
                _ => {
                    *v = OnyxValue::List(vec![item_b]);
                    1
                }
            },
        )
    }

    pub fn rpush(&self, key: &str, item: String) -> usize {
        let item_b = Bytes::from(item);
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::List(Vec::new()),
            move |v| match v {
                OnyxValue::List(l) => {
                    l.push(item_b);
                    l.len()
                }
                _ => {
                    *v = OnyxValue::List(vec![item_b]);
                    1
                }
            },
        )
    }

    pub fn lpop(&self, key: &str) -> Option<String> {
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, |v| match v {
            OnyxValue::List(l) if !l.is_empty() => {
                let item = l.remove(0);
                (Some(String::from_utf8_lossy(&item).to_string()), l.is_empty())
            }
            _ => (None, false),
        });
        match result {
            Some((Some(item), true)) => { self.engine.delete(&key_b); Some(item) }
            Some((Some(item), false)) => Some(item),
            _ => None,
        }
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, |v| match v {
            OnyxValue::List(l) if !l.is_empty() => {
                let item = l.pop().unwrap();
                (Some(String::from_utf8_lossy(&item).to_string()), l.is_empty())
            }
            _ => (None, false),
        });
        match result {
            Some((Some(item), true)) => { self.engine.delete(&key_b); Some(item) }
            Some((Some(item), false)) => Some(item),
            _ => None,
        }
    }

    /// LRANGE con start/stop stile Redis: indici 0-based inclusivi su
    /// entrambi gli estremi, indici negativi contano dalla fine (-1 =
    /// ultimo elemento), fuori range vengono "clampati" invece di dare
    /// errore. `LRANGE chiave` (senza indici, dal vecchio comportamento)
    /// continua a funzionare passando start=0, stop=-1 dal chiamante.
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Option<Vec<String>> {
        self.engine.read(&Bytes::from(key.to_string()), |entry| {
            match &entry.value {
                OnyxValue::List(l) => {
                    let len = l.len() as i64;
                    if len == 0 { return Some(Vec::new()); }
                    let norm = |idx: i64| -> i64 { if idx < 0 { (len + idx).max(0) } else { idx } };
                    let s = norm(start);
                    let mut e = norm(stop);
                    if s > len - 1 || s > e { return Some(Vec::new()); }
                    if e > len - 1 { e = len - 1; }
                    Some(l[s as usize..=e as usize].iter().map(|b| String::from_utf8_lossy(b).to_string()).collect())
                }
                _ => None,
            }
        }).flatten()
    }

    pub fn llen(&self, key: &str) -> Option<usize> {
        self.engine.read(&Bytes::from(key.to_string()), |entry| {
            match &entry.value {
                OnyxValue::List(l) => Some(l.len()),
                _ => None,
            }
        }).flatten()
    }

    // --- Hash operations ---
    pub fn hset(&self, key: &str, field: &str, value: &str) -> bool {
        let field_b = Bytes::from(field.to_string());
        let value_b = Bytes::from(value.to_string());
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Hash(std::collections::HashMap::new()),
            move |v| match v {
                OnyxValue::Hash(h) => h.insert(field_b, value_b).is_none(),
                _ => {
                    let mut h = std::collections::HashMap::new();
                    h.insert(field_b, value_b);
                    *v = OnyxValue::Hash(h);
                    true
                }
            },
        )
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<String> {
        let field_b = Bytes::from(field.to_string());
        self.engine.read(&Bytes::from(key.to_string()), move |entry| {
            match &entry.value {
                OnyxValue::Hash(h) => h.get(&field_b).map(|b| String::from_utf8_lossy(b).to_string()),
                _ => None,
            }
        }).flatten()
    }

    pub fn hgetall(&self, key: &str) -> Option<Vec<(String, String)>> {
        self.engine.read(&Bytes::from(key.to_string()), |entry| {
            match &entry.value {
                OnyxValue::Hash(h) => {
                    Some(h.iter().map(|(k, v)| {
                        (
                            String::from_utf8_lossy(k).to_string(),
                            String::from_utf8_lossy(v).to_string(),
                        )
                    }).collect())
                }
                _ => None,
            }
        }).flatten()
    }

    pub fn hdel(&self, key: &str, field: &str) -> bool {
        let field_b = Bytes::from(field.to_string());
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Hash(h) => {
                let removed = h.remove(&field_b).is_some();
                (removed, h.is_empty())
            }
            _ => (false, false),
        });
        match result {
            Some((removed, true)) => { self.engine.delete(&key_b); removed }
            Some((removed, false)) => removed,
            None => false,
        }
    }

    pub fn hkeys(&self, key: &str) -> Option<Vec<String>> {
        self.hgetall(key).map(|h| h.into_iter().map(|(k, _)| k).collect())
    }

    pub fn hvals(&self, key: &str) -> Option<Vec<String>> {
        self.hgetall(key).map(|h| h.into_iter().map(|(_, v)| v).collect())
    }

    // --- Set operations ---
    pub fn sadd(&self, key: &str, member: &str) -> bool {
        let member_b = Bytes::from(member.to_string());
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Set(std::collections::HashSet::new()),
            move |v| match v {
                OnyxValue::Set(s) => s.insert(member_b),
                _ => {
                    let mut s = std::collections::HashSet::new();
                    s.insert(member_b);
                    *v = OnyxValue::Set(s);
                    true
                }
            },
        )
    }

    pub fn smembers(&self, key: &str) -> Option<Vec<String>> {
        self.engine.read(&Bytes::from(key.to_string()), |entry| {
            match &entry.value {
                OnyxValue::Set(s) => {
                    Some(s.iter().map(|b| String::from_utf8_lossy(b).to_string()).collect())
                }
                _ => None,
            }
        }).flatten()
    }

    pub fn srem(&self, key: &str, member: &str) -> bool {
        let member_b = Bytes::from(member.to_string());
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Set(s) => {
                let removed = s.remove(&member_b);
                (removed, s.is_empty())
            }
            _ => (false, false),
        });
        match result {
            Some((removed, true)) => { self.engine.delete(&key_b); removed }
            Some((removed, false)) => removed,
            None => false,
        }
    }

    pub fn sismember(&self, key: &str, member: &str) -> bool {
        let member_b = Bytes::from(member.to_string());
        self.engine.read(&Bytes::from(key.to_string()), move |entry| {
            match &entry.value {
                OnyxValue::Set(s) => s.contains(&member_b),
                _ => false,
            }
        }).unwrap_or(false)
    }
    // --- JSON operations ---

    /// JSON.SET: se path == "$", sostituisce l'intero documento (creandolo
    /// se la chiave non esiste). Con un path parziale, la chiave deve già
    /// esistere e contenere un valore JSON.
    pub fn json_set(&self, key: &str, path: &str, new_value: serde_json::Value) -> Result<(), &'static str> {
        let segments = parse_json_path(path).ok_or("ERR path JSON non valido")?;
        let key_b = Bytes::from(key.to_string());

        if segments.is_empty() {
            // "$": crea o sovrascrive l'intero documento.
            self.engine.set(key_b, OnyxValue::Json(new_value), None);
            return Ok(());
        }

        // Path parziale: la chiave deve già esistere con un valore JSON.
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(set_json_path(root, &segments, new_value)),
            _ => None, // esiste ma non è JSON: tipo sbagliato
        });
        match result {
            Some(Some(true)) => Ok(()),
            Some(Some(false)) => Err("ERR path non raggiungibile (elemento intermedio assente o indice fuori range)"),
            Some(None) => Err("WRONGTYPE la chiave esiste ma non contiene un valore JSON"),
            None => Err("ERR chiave inesistente: usa JSON.SET chiave $ {...} per crearla"),
        }
    }

    pub fn json_get(&self, key: &str, path: &str) -> Result<Option<String>, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR path JSON non valido")?;
        let result = self.engine.read(&Bytes::from(key.to_string()), move |entry| match &entry.value {
            OnyxValue::Json(root) => {
                if segments.is_empty() {
                    Some(root.to_string())
                } else {
                    get_json_path(root, &segments).map(|v| v.to_string())
                }
            }
            _ => None,
        });
        match result {
            Some(Some(s)) => Ok(Some(s)),
            Some(None) => Ok(None), // chiave JSON esiste ma il path non trova nulla
            None => Ok(None),        // chiave inesistente
        }
    }

    pub fn json_del(&self, key: &str, path: &str) -> Result<bool, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR path JSON non valido")?;
        if segments.is_empty() {
            // DEL sul documento intero: stessa semantica del DEL normale.
            return Ok(self.delete(key));
        }
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(delete_json_path(root, &segments)),
            _ => None,
        });
        match result {
            Some(Some(deleted)) => Ok(deleted),
            Some(None) => Err("WRONGTYPE la chiave esiste ma non contiene un valore JSON"),
            None => Ok(false), // chiave inesistente: nulla da cancellare
        }
    }

    pub fn json_type(&self, key: &str, path: &str) -> Result<Option<&'static str>, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR path JSON non valido")?;
        let result = self.engine.read(&Bytes::from(key.to_string()), move |entry| match &entry.value {
            OnyxValue::Json(root) => {
                let node = if segments.is_empty() { Some(root) } else { get_json_path(root, &segments) };
                node.map(|v| match v {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                })
            }
            _ => None,
        });
        Ok(result.flatten())
    }
    // --- Key operations ---
    pub fn rename(&self, old_key: &str, new_key: &str) -> bool {
        self.engine.rename(&Bytes::from(old_key.to_string()), Bytes::from(new_key.to_string()))
    }

    pub fn copy(&self, src: &str, dst: &str) -> bool {
        if let Some(entry) = self.engine.get(&Bytes::from(src.to_string())) {
            self.engine.set(Bytes::from(dst.to_string()), entry.value, entry.expires_at);
            true
        } else {
            false
        }
    }

    pub fn value_type(&self, key: &str) -> Option<&'static str> {
        self.engine.read(&Bytes::from(key.to_string()), |entry| match &entry.value {
            OnyxValue::Blob(_) => "string",
            OnyxValue::Int(_) | OnyxValue::Float(_) => "int",
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
        self.all_keys().into_iter().filter(|k| glob_match(pattern, k)).collect()
    }

    pub fn snapshot_entries(&self) -> Vec<(String, DataEntry)> {
        self.engine
            .snapshot_all()
            .into_iter()
            .map(|(k, entry)| (String::from_utf8_lossy(&k).to_string(), entry))
            .collect()
    }

    pub fn is_full(&self) -> bool {
        self.engine.stats().total_keys >= MAX_KEYS
    }

    pub fn used_memory_bytes(&self) -> usize {
        self.engine.total_memory_bytes()
    }

    /// Se è configurato un `--maxmemory`, prova a liberare spazio secondo la
    /// policy scelta prima di accettare una scrittura che creerebbe una
    /// nuova chiave. Ritorna false solo quando la policy è `noeviction` e
    /// siamo sopra il limite (il comando va rifiutato); true in ogni altro
    /// caso, incluso "nessun limite configurato".
    pub fn make_room_for_write(&self) -> bool {
        let limit = maxmemory_bytes();
        if limit == 0 {
            return true;
        }
        if self.engine.total_memory_bytes() <= limit {
            return true;
        }
        let policy = maxmemory_policy();
        if policy == EvictionPolicy::NoEviction {
            return false;
        }
        self.engine.evict_to_fit(limit, policy);
        true
    }

    pub fn expire_conditional(&self, key: &str, seconds: u64, condition: &str) -> bool {
        let has_expiry = self.get_expiry(key).is_some();
        let allowed = match condition {
            "NX" => !has_expiry,
            "XX" => has_expiry,
            _ => true,
        };
        if allowed {
            self.expire(key, seconds)
        } else {
            false
        }
    }

    pub fn get_expiry(&self, key: &str) -> Option<u64> {
        self.engine.read(&Bytes::from(key.to_string()), |e| e.expires_at).flatten()
    }

    pub fn stats(&self) -> engine::EngineStats {
        self.engine.stats()
    }

    pub fn gc_expired(&self) -> usize {
        self.engine.gc_expired()
    }
    pub fn engine(&self) -> &OnyxEngine {
        &self.engine
    }
 }
fn is_expired(entry: &DataEntry) -> bool {
    if let Some(exp) = entry.expires_at {
        now() >= exp
    } else {
        false
    }
}

// ============================================================
// JSON PATH — parser ridotto: solo campi (.nome) e indici di array ([N]).
// Niente wildcard e niente filtri.
// ============================================================

#[derive(Debug, Clone, PartialEq)]
enum JsonPathSegment {
    Field(String),
    Index(usize),
}
/// Interpreta un path stile "$.a.b[2].c" in una sequenza di passi da
/// seguire dentro un serde_json::Value. "$" da solo (documento intero)
/// ritorna un vettore vuoto. Ritorna None se il path è sintatticamente
/// malformato (non se il path "non esiste" nei dati. Quello viene scoperto
/// solo in get_json_path/set_json_path).
fn parse_json_path(path: &str) -> Option<Vec<JsonPathSegment>> {
    let path = path.trim();
    if path != "$" && !path.starts_with('$') {
        return None; // ogni path valido inizia con '$'
    }
    let rest = &path[1..]; // scarta il '$' iniziale
    if rest.is_empty() {
        return Some(Vec::new()); // "$" da solo: documento intero
    }
    if !rest.starts_with('.') && !rest.starts_with('[') {
        return None; // dopo '$' deve seguire '.' o '['
    }

    let mut segments = Vec::new();
    let chars: Vec<char> = rest.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                    i += 1;
                }
                if start == i {
                    return None; // ".." o "." finale senza nome di campo
                }
                let field: String = chars[start..i].iter().collect();
                segments.push(JsonPathSegment::Field(field));
            }
            '[' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != ']' {
                    i += 1;
                }
                if i >= chars.len() {
                    return None; // '[' senza ']' di chiusura
                }
                let idx_str: String = chars[start..i].iter().collect();
                let idx: usize = idx_str.parse().ok()?; // solo indici >= 0
                segments.push(JsonPathSegment::Index(idx));
                i += 1; // salta il ']'
            }
            _ => return None, // carattere inatteso fuori da un segmento . o [
        }
    }

    Some(segments)
}
/// Naviga `root` seguendo `segments` e ritorna un riferimento al nodo
/// finale, se esiste. None se un passo intermedio non esiste o non è del
/// tipo giusto.
fn get_json_path<'a>(root: &'a serde_json::Value, segments: &[JsonPathSegment]) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for seg in segments {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.get(f)?,
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => arr.get(*idx)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Imposta il valore al path indicato, creando l'ultimo passo se manca ma
/// SENZA creare automaticamente livelli intermedi assenti. Ritorna true se ha scritto, false se il genitore del
/// passo finale non esiste o non è del tipo compatibile.
fn set_json_path(root: &mut serde_json::Value, segments: &[JsonPathSegment], new_value: serde_json::Value) -> bool {
    if segments.is_empty() {
        *root = new_value;
        return true;
    }
    let mut current = root;
    for seg in &segments[..segments.len() - 1] {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
                match map.get_mut(f) {
                    Some(v) => v,
                    None => return false, // livello intermedio assente: niente auto-creazione
                }
            }
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                match arr.get_mut(*idx) {
                    Some(v) => v,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (&segments[segments.len() - 1], current) {
        (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
            map.insert(f.clone(), new_value);
            true
        }
        (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
            if *idx < arr.len() {
                arr[*idx] = new_value;
                true
            } else if *idx == arr.len() {
                arr.push(new_value); // append in coda, come farebbe un push naturale
                true
            } else {
                false // indice troppo avanti, buco non ammesso
            }
        }
        _ => false,
    }
}

/// Rimuove il nodo al path indicato. Ritorna true se ha rimosso qualcosa.
fn delete_json_path(root: &mut serde_json::Value, segments: &[JsonPathSegment]) -> bool {
    if segments.is_empty() {
        return false; // DEL sul documento intero non passa da qui (si usa DEL normale sulla chiave)
    }
    let mut current = root;
    for seg in &segments[..segments.len() - 1] {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
                match map.get_mut(f) {
                    Some(v) => v,
                    None => return false,
                }
            }
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                match arr.get_mut(*idx) {
                    Some(v) => v,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (&segments[segments.len() - 1], current) {
        (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.remove(f).is_some(),
        (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
            if *idx < arr.len() {
                arr.remove(*idx);
                true
            } else {
                false
            }
        }
        _ => false,
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
            // Match letterale: avanza entrambi
            p_idx += 1;
            t_idx += 1;
        } else if p_idx < p.len() && p[p_idx] == '*' {
            // Il '*' e' un punto di backtrack: prova prima a matchare zero
            // caratteri (lo si espandera' un carattere alla volta solo se
            // serve, nel ramo sotto).
            star_idx = Some(p_idx);
            match_idx = t_idx;
            p_idx += 1;
        } else if star_idx.is_some() {
            // Non ha funzionato: fai "espandere" l'ultimo '*' di un
            // carattere in piu' e riprova da li'.
            p_idx = star_idx.unwrap() + 1;
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

/// Quante voci tiene al massimo il backlog di replica (in numero di
/// comandi, (non byte) — semplificazione voluta:
/// più facile da ragionare Replica staccata a lungo
/// perde "N comandi" invece di "N byte", è comunque un buon
/// proxy dello stesso concetto).
const BACKLOG_CAPACITY: usize = 10_000;

enum LogMessage {
    Append(Vec<u8>),  // modificato il 07/26 in Vec<u8> (binario)
    Compact,
}

/// Stato di una Replica connessa, per il monitoraggio del lag.
struct ReplicaStatus {
    addr: String,
    last_ack_offset: u64,
    last_ack_time: u64,
}

struct Persistence {
    log_tx: mpsc::Sender<LogMessage>,
    write_count: AtomicUsize,
    compaction_pending: AtomicBool,
    // Canale broadcast: ogni comando di scrittura viene trasmesso a tutte le
    // Replica connesse in tempo reale (in aggiunta al log su disco), taggato
    // con l'offset di replicazione a cui corrisponde.
    replica_tx: tokio::sync::broadcast::Sender<(u64, String)>,
    // Se true, questa istanza smette di comportarsi da Replica e diventa
    // un Master indipendente (promozione manuale via REPLICAOF NO ONE).
    promote_to_master: Arc<AtomicBool>,
    // Offset di replicazione: cresce di 1 a ogni comando di scrittura
    // replicato. Non è byte-accurato ma basta per capire
    // "quanti comandi indietro" è una Replica.
    repl_offset: AtomicU64,
    // Buffer circolare degli ultimi BACKLOG_CAPACITY comandi, con il loro
    // offset. Usato per il resync parziale: una Replica che si riconnette
    // con un offset ancora presente qui riceve solo i comandi mancanti,
    // invece di un dump completo.
    backlog: std::sync::Mutex<std::collections::VecDeque<(u64, String)>>,
    next_replica_id: AtomicU64,
    replica_status: std::sync::Mutex<std::collections::HashMap<u64, ReplicaStatus>>,
    // Pub/Sub: un unico canale broadcast per tutti i canali applicativi
    // (channel_name, payload) — ogni subscriber filtra da solo i messaggi
    // dei canali a cui è iscritto, invece di avere un canale broadcast
    // dedicato per ogni nome di canale (che andrebbe creato/distrutto
    // dinamicamente, più complicato da gestire in modo sicuro).
    pubsub_tx: tokio::sync::broadcast::Sender<(String, String)>,
    next_subscriber_id: AtomicU64,
    // canale -> insieme di id di subscriber iscritti, usato solo per dare
    // a PUBLISH il numero di destinatari (Redis fa lo stesso).
    subscriptions: std::sync::Mutex<std::collections::HashMap<String, std::collections::HashSet<u64>>>,
}

// ============================================================
// LOG BINARIO - Formato compatto per operazioni di scrittura
// ============================================================
const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;
const OP_EXPIRE: u8 = 3;
const OP_L_PUSH: u8 = 4;
const OP_HSET: u8 = 5;
const OP_SADD: u8 = 6;
const OP_RENAME: u8 = 7;
const OP_INCR: u8 = 8;
const OP_DECR: u8 = 9;
const OP_APPEND: u8 = 10;
const OP_HDEL: u8 = 11;
const OP_SREM: u8 = 12;
const OP_COPY: u8 = 13;
const OP_MSET: u8 = 14;
const OP_R_PUSH: u8 = 15;
const OP_LPOP: u8 = 16;
const OP_RPOP: u8 = 17;
const OP_JSON_SET: u8 = 18;
const OP_JSON_DEL: u8 = 19;

fn write_u16_be(buf: &mut Vec<u8>, val: u16) {
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

fn write_u32_be(buf: &mut Vec<u8>, val: u32) {
    buf.push((val >> 24) as u8);
    buf.push((val >> 16) as u8);
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

fn write_u64_be(buf: &mut Vec<u8>, val: u64) {
    buf.push((val >> 56) as u8);
    buf.push((val >> 48) as u8);
    buf.push((val >> 40) as u8);
    buf.push((val >> 32) as u8);
    buf.push((val >> 24) as u8);
    buf.push((val >> 16) as u8);
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

// Versioni "checked": ritornano None invece di andare in panic se il
// binlog è troncato o corrotto (bit-flip, scrittura interrotta a metà da
// un crash) e i byte richiesti non ci sono davvero. Usate durante il
// recovery all'avvio, dove un binlog danneggiato non deve MAI far
// crashare il processo — nel peggiore dei casi si perde quel singolo
// record, non l'intero avvio.
fn read_u16_be(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    if offset.checked_add(2)? > bytes.len() { return None; }
    let val = ((bytes[*offset] as u16) << 8) | (bytes[*offset + 1] as u16);
    *offset += 2;
    Some(val)
}

fn read_u32_be(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    if offset.checked_add(4)? > bytes.len() { return None; }
    let val = ((bytes[*offset] as u32) << 24)
        | ((bytes[*offset + 1] as u32) << 16)
        | ((bytes[*offset + 2] as u32) << 8)
        | (bytes[*offset + 3] as u32);
    *offset += 4;
    Some(val)
}

fn read_u64_be(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    if offset.checked_add(8)? > bytes.len() { return None; }
    let val = ((bytes[*offset] as u64) << 56)
        | ((bytes[*offset + 1] as u64) << 48)
        | ((bytes[*offset + 2] as u64) << 40)
        | ((bytes[*offset + 3] as u64) << 32)
        | ((bytes[*offset + 4] as u64) << 24)
        | ((bytes[*offset + 5] as u64) << 16)
        | ((bytes[*offset + 6] as u64) << 8)
        | (bytes[*offset + 7] as u64);
    *offset += 8;
    Some(val)
}

/// Estrae una fetta di `len` byte a partire da `offset`, senza andare in
/// panic se non ci stanno (record troncato/corrotto).
fn safe_slice(bytes: &[u8], offset: usize, len: usize) -> Option<&[u8]> {
    let end = offset.checked_add(len)?;
    bytes.get(offset..end)
}

/// Converte un comando + entry in record binario per il log
fn command_to_binary_record(cmd: &str, args: &[String], _entry: Option<&DataEntry>) -> Option<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    let op_code = match cmd {
        "SET" | "GETSET" | "SETNX" | "MSET" => OP_SET,
        "DEL" => OP_DEL,
        "EXPIRE" | "EXPIREAT" => OP_EXPIRE,
        "LPUSH" => OP_L_PUSH,
        "RPUSH" => OP_R_PUSH,
        "LPOP" => OP_LPOP,
        "RPOP" => OP_RPOP,
        "JSON.SET" => OP_JSON_SET,
        "JSON.DEL" => OP_JSON_DEL,
        "HSET" => OP_HSET,
        "SADD" => OP_SADD,
        "RENAME" => OP_RENAME,
        "INCR" | "INCRBY" => OP_INCR,
        "DECRBY" => OP_DECR,
        "APPEND" => OP_APPEND,
        "HDEL" => OP_HDEL,
        "SREM" => OP_SREM,
        "COPY" => OP_COPY,
        _ => return None, // Comando non persistente
    };
    
    buf.push(op_code);
    
    match cmd {
        "SET" | "GETSET" | "SETNX" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let value = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            buf.push(1); // tipo: stringa
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
            // Scadenza assoluta, se presente (SET normalizzato con "EXAT
            // <timestamp>" da normalize_for_log — vedi lì per il perché):
            // senza questo, un SET con EX/PX sopravviverebbe alla
            // persistenza/replica perdendo silenziosamente la scadenza.
            let expiry: u64 = if args.len() >= 5 && args[3].eq_ignore_ascii_case("EXAT") {
                args[4].parse().unwrap_or(0)
            } else {
                0
            };
            write_u64_be(&mut buf, expiry);
        }
        "MSET" => {
            if args.len() < 3 { return None; }
            buf[0] = OP_MSET;
            let num_pairs = (args.len() - 1) / 2;
            write_u16_be(&mut buf, num_pairs as u16);
            let mut i = 1;
            while i + 1 < args.len() {
                let key = &args[i];
                let value = &args[i + 1];
                write_u16_be(&mut buf, key.len() as u16);
                buf.extend_from_slice(key.as_bytes());
                write_u32_be(&mut buf, value.len() as u32);
                buf.extend_from_slice(value.as_bytes());
                i += 2;
            }
            return Some(buf);
        }
        "DEL" => {
            if args.len() < 2 { return None; }
            let key = &args[1];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
        }
        "EXPIRE" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let seconds = args[2].parse::<u64>().unwrap_or(0);
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, seconds);
        }
        "EXPIREAT" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let timestamp = args[2].parse::<u64>().unwrap_or(0);
            buf[0] = OP_EXPIRE; // stesso codice, timestamp assoluto
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, timestamp);
        }
        "LPUSH" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let item = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, item.len() as u32);
            buf.extend_from_slice(item.as_bytes());
        }
        "RPUSH" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let item = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, item.len() as u32);
            buf.extend_from_slice(item.as_bytes());
        }
        "LPOP" | "RPOP" => {
            if args.len() < 2 { return None; }
            let key = &args[1];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
        }
        "HSET" => {
            if args.len() < 4 { return None; }
            let key = &args[1];
            let field = &args[2];
            let value = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, field.len() as u16);
            buf.extend_from_slice(field.as_bytes());
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
        "SADD" | "SREM" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let member = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, member.len() as u32);
            buf.extend_from_slice(member.as_bytes());
        }
        "RENAME" => {
            if args.len() < 3 { return None; }
            let old_key = &args[1];
            let new_key = &args[2];
            write_u16_be(&mut buf, old_key.len() as u16);
            buf.extend_from_slice(old_key.as_bytes());
            write_u16_be(&mut buf, new_key.len() as u16);
            buf.extend_from_slice(new_key.as_bytes());
        }
        "INCR" | "INCRBY" => {
            if args.len() < 2 { return None; }
            let key = &args[1];
            let delta = if cmd == "INCR" { 1 } else { args.get(2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(1) };
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, delta as u64);
        }
        "DECRBY" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let delta = args[2].parse::<i64>().unwrap_or(1);
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, delta.abs() as u64);
        }
        "APPEND" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let suffix = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, suffix.len() as u32);
            buf.extend_from_slice(suffix.as_bytes());
        }
        "HDEL" => {
            if args.len() < 3 { return None; }
            let key = &args[1];
            let field = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, field.len() as u16);
            buf.extend_from_slice(field.as_bytes());
        }
        "COPY" => {
            if args.len() < 3 { return None; }
            let src = &args[1];
            let dst = &args[2];
            write_u16_be(&mut buf, src.len() as u16);
            buf.extend_from_slice(src.as_bytes());
            write_u16_be(&mut buf, dst.len() as u16);
            buf.extend_from_slice(dst.as_bytes());
        }
        _ => return None,
    }
    
    Some(buf)
}

/// Legge un record binario e lo converte in args per execute_command
fn binary_record_to_args(record: &[u8]) -> Option<Vec<String>> {
    if record.is_empty() { return None; }
    
    let op = record[0];
    let mut offset = 1;
    
    match op {
        OP_SET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let _val_type = *record.get(offset)?; offset += 1;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            offset += val_len;
            // I record scritti prima di questa versione non hanno questi 8
            // byte finali con la scadenza: se mancano, va bene lo stesso,
            // significa che "nessuna scadenza" (comportamento invariato).
            let expiry = read_u64_be(record, &mut offset).unwrap_or(0);
            if expiry > 0 {
                Some(vec!["SET".to_string(), key, value, "EXAT".to_string(), expiry.to_string()])
            } else {
                Some(vec!["SET".to_string(), key, value])
            }
        }
        OP_MSET => {
            let num_pairs = read_u16_be(record, &mut offset)? as usize;
            let mut args = vec!["MSET".to_string()];
            for _ in 0..num_pairs {
                let key_len = read_u16_be(record, &mut offset)? as usize;
                let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
                offset += key_len;
                let val_len = read_u32_be(record, &mut offset)? as usize;
                let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
                offset += val_len;
                args.push(key);
                args.push(value);
            }
            Some(args)
        }
        OP_DEL => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            Some(vec!["DEL".to_string(), key])
        }
        OP_EXPIRE => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let timestamp = read_u64_be(record, &mut offset)?;
            Some(vec!["EXPIREAT".to_string(), key, timestamp.to_string()])
        }
        OP_L_PUSH => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let item_len = read_u32_be(record, &mut offset)? as usize;
            let item = String::from_utf8_lossy(safe_slice(record, offset, item_len)?).to_string();
            Some(vec!["LPUSH".to_string(), key, item])
        }
        OP_R_PUSH => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let item_len = read_u32_be(record, &mut offset)? as usize;
            let item = String::from_utf8_lossy(safe_slice(record, offset, item_len)?).to_string();
            Some(vec!["RPUSH".to_string(), key, item])
        }
        OP_LPOP => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            Some(vec!["LPOP".to_string(), key])
        }
        OP_RPOP => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            Some(vec!["RPOP".to_string(), key])
        }
        OP_HSET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let field_len = read_u16_be(record, &mut offset)? as usize;
            let field = String::from_utf8_lossy(safe_slice(record, offset, field_len)?).to_string();
            offset += field_len;
            let value_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, value_len)?).to_string();
            Some(vec!["HSET".to_string(), key, field, value])
        }
        OP_SADD => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let member_len = read_u32_be(record, &mut offset)? as usize;
            let member = String::from_utf8_lossy(safe_slice(record, offset, member_len)?).to_string();
            Some(vec!["SADD".to_string(), key, member])
        }
        OP_SREM => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let member_len = read_u32_be(record, &mut offset)? as usize;
            let member = String::from_utf8_lossy(safe_slice(record, offset, member_len)?).to_string();
            Some(vec!["SREM".to_string(), key, member])
        }
        OP_RENAME => {
            let old_len = read_u16_be(record, &mut offset)? as usize;
            let old_key = String::from_utf8_lossy(safe_slice(record, offset, old_len)?).to_string();
            offset += old_len;
            let new_len = read_u16_be(record, &mut offset)? as usize;
            let new_key = String::from_utf8_lossy(safe_slice(record, offset, new_len)?).to_string();
            Some(vec!["RENAME".to_string(), old_key, new_key])
        }
        OP_INCR => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let delta = read_u64_be(record, &mut offset)?;
            Some(vec!["INCRBY".to_string(), key, delta.to_string()])
        }
        OP_DECR => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let delta = read_u64_be(record, &mut offset)?;
            Some(vec!["DECRBY".to_string(), key, delta.to_string()])
        }
        OP_APPEND => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let suffix_len = read_u32_be(record, &mut offset)? as usize;
            let suffix = String::from_utf8_lossy(safe_slice(record, offset, suffix_len)?).to_string();
            Some(vec!["APPEND".to_string(), key, suffix])
        }
        OP_HDEL => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let field_len = read_u16_be(record, &mut offset)? as usize;
            let field = String::from_utf8_lossy(safe_slice(record, offset, field_len)?).to_string();
            Some(vec!["HDEL".to_string(), key, field])
        }
        OP_COPY => {
            let src_len = read_u16_be(record, &mut offset)? as usize;
            let src = String::from_utf8_lossy(safe_slice(record, offset, src_len)?).to_string();
            offset += src_len;
            let dst_len = read_u16_be(record, &mut offset)? as usize;
            let dst = String::from_utf8_lossy(safe_slice(record, offset, dst_len)?).to_string();
            Some(vec!["COPY".to_string(), src, dst])
        }
        OP_JSON_SET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            offset += path_len;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            Some(vec!["JSON.SET".to_string(), key, path, value])
        }
        OP_JSON_DEL => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            Some(vec!["JSON.DEL".to_string(), key, path])
        }
        _ => None,
    }
}

fn line_to_entry(line: &str) -> Option<(String, DataEntry)> {
    let mut parts = line.splitn(4, '\t');
    let key = parts.next()?.to_string();
    let val_type = parts.next()?;
    let exp_val = parts.next()?.parse::<u64>().unwrap_or(0);
    let val_str = parts.next()?;

    let expires_at = if exp_val == 0 { None } else { Some(exp_val) };
    let value = match val_type {
        "STR" => Some(OnyxValue::Blob(Bytes::from(val_str.to_string()))),
        "INT" => val_str.parse::<i64>().ok().map(OnyxValue::Int),
        "LIST" => {
            let items: Vec<Bytes> = if val_str.is_empty() {
                Vec::new()
            } else {
                val_str.split('|').map(|s| Bytes::from(s.to_string())).collect()
            };
            Some(OnyxValue::List(items))
        }
        "HASH" => {
            let mut map = std::collections::HashMap::new();
            if !val_str.is_empty() {
                for pair in val_str.split('|') {
                    if let Some((k, v)) = pair.split_once('=') {
                        map.insert(Bytes::from(k.to_string()), Bytes::from(v.to_string()));
                    }
                }
            }
            Some(OnyxValue::Hash(map))
        }
        "JSON" => {
            serde_json::from_str::<serde_json::Value>(val_str).ok().map(OnyxValue::Json)
        }
        "SET" => {
            let set: std::collections::HashSet<Bytes> = if val_str.is_empty() {
                std::collections::HashSet::new()
            } else {
                val_str.split('|').map(|s| Bytes::from(s.to_string())).collect()
            };
            Some(OnyxValue::Set(set))
        }
        _ => None,
    }?;

    let ts = now();
    Some((key, DataEntry { value, expires_at, created_at: ts, last_accessed: ts }))
}

fn value_to_line(key: &str, entry: &DataEntry) -> String {
    let (val_type, val_str): (&str, String) = match &entry.value {
        OnyxValue::Blob(b) => ("STR", String::from_utf8_lossy(b).to_string()),
        OnyxValue::Int(n) => ("INT", n.to_string()),
        OnyxValue::Float(f) => ("STR", f.to_string()),
        OnyxValue::List(list) => (
            "LIST",
            list.iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
                .join("|"),
        ),
        OnyxValue::Hash(map) => (
            "HASH",
            map.iter()
                .map(|(k, v)| format!("{}={}", String::from_utf8_lossy(k), String::from_utf8_lossy(v)))
                .collect::<Vec<_>>()
                .join("|"),
        ),
        OnyxValue::Set(set) => (
            "SET",
            set.iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
                .join("|"),
        ),
        OnyxValue::Json(j) => ("JSON", j.to_string()),
        // Vector: non ancora supportato nel formato snapshot testuale
        _ => ("STR", String::new()),
    };

    let exp_val = entry.expires_at.unwrap_or(0);
    format!("{}\t{}\t{}\t{}", key, val_type, exp_val, val_str)
}
/// Elenco dei comandi che scrivono (usato sia per il gate READONLY sulle
/// Replica, sia — indirettamente — coerente con quello che command_to_binary_record
/// sa mappare su un op-code per il binlog). Va prima del dispatch, quindi
/// non richiede aver già eseguito il comando per saperlo.
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET" | "GETSET" | "SETNX" | "MSET" | "DEL" | "EXPIRE" | "EXPIREAT"
            | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "HSET" | "SADD" | "RENAME"
            | "INCR" | "INCRBY" | "DECRBY" | "APPEND" | "HDEL" | "SREM" | "COPY"
            | "JSON.SET" | "JSON.DEL"
    )
}
/// Un resync parziale è ammissibile solo se il replication ID richiesto
/// dalla Replica coincide con quello attuale del Master: garantisce che
/// stiamo parlando con lo stesso identico processo Master di prima, non
/// con uno ripartito da zero (nel qual caso il vecchio offset non ha più
/// alcun significato, anche se per coincidenza "sembra" plausibile).
/// requested_replid == 0 vuol dire "la Replica non conosce ancora nessun
/// replid" (prima connessione in assoluto): mai ammesso a resync parziale.
fn replid_allows_partial(requested_replid: u64, current_replid: u64) -> bool {
    requested_replid != 0 && requested_replid == current_replid
}
/// Dato che il replication ID combacia, decide se il backlog attuale
/// permette davvero un resync parziale senza buchi.
fn partial_resync_possible(
    requested_offset: u64,
    backlog_oldest: Option<u64>,
    current_repl_offset: u64,
) -> bool {
    match backlog_oldest {
        Some(oldest) => oldest <= requested_offset + 1,
        None => requested_offset == current_repl_offset,
    }
}
fn execute_command(store: &ShardedStore, args: &[String]) -> (RESPValue, bool) {
    let cmd = args.get(0).map(|s| s.as_str()).unwrap_or("");
    let key = args.get(1).map(|s| s.as_str()).unwrap_or("");
    let arg = args.get(2).map(|s| s.as_str()).unwrap_or("");

    const CREATE_COMMANDS: &[&str] = &["SET", "LPUSH", "RPUSH", "HSET", "SADD", "MSET", "APPEND", "GETSET", "INCRBY", "DECRBY", "INCR", "JSON.SET"];
    if CREATE_COMMANDS.contains(&cmd) && !key.is_empty() && !store.exists(key) {
        if store.is_full() {
            return (RESPValue::Error("ERR database pieno: limite massimo di chiavi raggiunto".to_string()), false);
        }
        if !store.make_room_for_write() {
            return (RESPValue::Error("OOM command not allowed when used memory > 'maxmemory'".to_string()), false);
        }
    }

    match cmd {
        "SET" if !key.is_empty() && !arg.is_empty() => {
            // Opzioni extra, in qualsiasi ordine come in Redis: EX secondi |
            // PX millisecondi | EXAT timestamp_assoluto (quest'ultima solo
            // per uso interno: è così che persistenza/replica ripropongono
            // in modo deterministico un SET con scadenza, senza rivalutare
            // "adesso" al momento del replay), NX | XX.
            let mut expires_at: Option<u64> = None;
            let mut condition: Option<bool> = None; // Some(true)=NX, Some(false)=XX
            let mut i = 3;
            let mut valid = true;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "EX" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(secs) => { expires_at = Some(now() + secs); i += 2; }
                        None => { valid = false; break; }
                    },
                    "PX" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(millis) => { expires_at = Some(now() + millis / 1000); i += 2; }
                        None => { valid = false; break; }
                    },
                    "EXAT" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(ts) => { expires_at = Some(ts); i += 2; }
                        None => { valid = false; break; }
                    },
                    "NX" => { condition = Some(true); i += 1; }
                    "XX" => { condition = Some(false); i += 1; }
                    _ => { valid = false; break; }
                }
            }
            if !valid {
                (RESPValue::Error("ERR syntax error".to_string()), false)
            } else {
                let ok = store.engine.set_conditional(
                    Bytes::from(key.to_string()),
                    OnyxValue::Blob(Bytes::from(arg.to_string())),
                    expires_at,
                    condition,
                );
                if ok {
                    (RESPValue::SimpleString("OK".to_string()), true)
                } else {
                    // NX con chiave già esistente, o XX con chiave assente:
                    // nessuna scrittura avvenuta, come in Redis risponde nil.
                    (RESPValue::BulkString(None), false)
                }
            }
        }
        "GET" if !key.is_empty() => (match store.get(key) { Some(v) => RESPValue::BulkString(Some(v)), None => RESPValue::BulkString(None) }, false),
        "DEL" if !key.is_empty() => (RESPValue::Integer(if store.delete(key) { 1 } else { 0 }), true),
        "INCR" if !key.is_empty() => (RESPValue::Integer(store.incr(key)), true),
        "LPUSH" if !key.is_empty() && !arg.is_empty() => (RESPValue::Integer(store.lpush(key, arg.to_string()) as i64), true),
        "RPUSH" if !key.is_empty() && !arg.is_empty() => (RESPValue::Integer(store.rpush(key, arg.to_string()) as i64), true),
        "LPOP" if !key.is_empty() => {
            match store.lpop(key) {
                Some(v) => (RESPValue::BulkString(Some(v)), true),
                None => (RESPValue::BulkString(None), false),
            }
        }
        "RPOP" if !key.is_empty() => {
            match store.rpop(key) {
                Some(v) => (RESPValue::BulkString(Some(v)), true),
                None => (RESPValue::BulkString(None), false),
            }
        }
        "LRANGE" if !key.is_empty() => {
            let start = args.get(2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let stop = args.get(3).and_then(|s| s.parse::<i64>().ok()).unwrap_or(-1);
            match store.lrange(key, start, stop) {
                Some(list) => (RESPValue::Array(list.into_iter().map(|s| RESPValue::BulkString(Some(s))).collect()), false),
                None => (RESPValue::Array(Vec::new()), false),
            }
        }

        "EXPIREAT" if !key.is_empty() && !arg.is_empty() => {
            if let Ok(t) = arg.parse::<u64>() {
                (RESPValue::Integer(if store.expire_at(key, t) { 1 } else { 0 }), true)
            } else {
                (RESPValue::Error("ERR invalid timestamp".to_string()), false)
            }
        }
        "TTL" if !key.is_empty() => (RESPValue::Integer(store.ttl(key)), false),
        "EXISTS" if !key.is_empty() => (RESPValue::Integer(if store.exists(key) { 1 } else { 0 }), false),
        "TYPE" if !key.is_empty() => {
            match store.value_type(key) {
                Some(t) => (RESPValue::SimpleString(t.to_string()), false),
                None => (RESPValue::SimpleString("none".to_string()), false),
            }
        }
        "JSON.SET" => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let raw_value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            if key.is_empty() || path.is_empty() || raw_value.is_empty() {
                (RESPValue::Error("ERR uso: JSON.SET chiave path valore-json".to_string()), false)
            } else {
                match serde_json::from_str::<serde_json::Value>(raw_value) {
                    Ok(parsed) => match store.json_set(key, path, parsed) {
                        Ok(()) => (RESPValue::SimpleString("OK".to_string()), true),
                        Err(e) => (RESPValue::Error(e.to_string()), false),
                    },
                    Err(_) => (RESPValue::Error("ERR valore non è JSON valido".to_string()), false),
                }
            }
        }
        "JSON.GET" if !key.is_empty() => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_get(key, path) {
                Ok(Some(s)) => (RESPValue::BulkString(Some(s)), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.DEL" if !key.is_empty() => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_del(key, path) {
                Ok(deleted) => (RESPValue::Integer(if deleted { 1 } else { 0 }), deleted),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.TYPE" if !key.is_empty() => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_type(key, path) {
                Ok(Some(t)) => (RESPValue::SimpleString(t.to_string()), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "SADD" if !key.is_empty() && !arg.is_empty() => {
            (RESPValue::Integer(if store.sadd(key, arg) { 1 } else { 0 }), true)
        }
        "SMEMBERS" if !key.is_empty() => {
            match store.smembers(key) {
                Some(members) => (RESPValue::Array(members.into_iter().map(|m| RESPValue::BulkString(Some(m))).collect()), false),
                None => (RESPValue::Array(Vec::new()), false),
            }
        }
        "SREM" if !key.is_empty() && !arg.is_empty() => {
            (RESPValue::Integer(if store.srem(key, arg) { 1 } else { 0 }), true)
        }
        "SISMEMBER" if !key.is_empty() && !arg.is_empty() => {
            (RESPValue::Integer(if store.sismember(key, arg) { 1 } else { 0 }), false)
        }
        "LLEN" if !key.is_empty() => {
            (RESPValue::Integer(store.llen(key).unwrap_or(0) as i64), false)
        }
        "RENAME" if !key.is_empty() && !arg.is_empty() => {
            if store.rename(key, arg) {
                (RESPValue::SimpleString("OK".to_string()), true)
            } else {
                (RESPValue::Error("ERR no such key".to_string()), false)
            }
        }
        "MSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                (RESPValue::Error("ERR wrong number of arguments for 'mset'".to_string()), false)
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
            let results: Vec<RESPValue> = args[1..].iter()
                .map(|k| match store.get(k) {
                    Some(v) => RESPValue::BulkString(Some(v)),
                    None => RESPValue::BulkString(None),
                })
                .collect();
            (RESPValue::Array(results), false)
        }
        "KEYS" => {
            let pattern = key;
            let keys = store.keys_matching(pattern);
            (RESPValue::Array(keys.into_iter().map(|k| RESPValue::BulkString(Some(k))).collect()), false)
        }
        "HSET" if !key.is_empty() && !arg.is_empty() => {
            let field = arg;
            let value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            if value.is_empty() {
                (RESPValue::Error("ERR wrong number of arguments for 'hset'".to_string()), false)
            } else {
                let is_new = store.hset(key, field, value);
                (RESPValue::Integer(if is_new { 1 } else { 0 }), true)
            }
        }
        "HGET" if !key.is_empty() && !arg.is_empty() => {
            let field = arg;
            (match store.hget(key, field) { Some(v) => RESPValue::BulkString(Some(v)), None => RESPValue::BulkString(None) }, false)
        }
        "HGETALL" if !key.is_empty() => {
            match store.hgetall(key) {
                Some(pairs) => {
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (f, v) in pairs {
                        flat.push(RESPValue::BulkString(Some(f)));
                        flat.push(RESPValue::BulkString(Some(v)));
                    }
                    (RESPValue::Array(flat), false)
                }
                None => (RESPValue::Array(Vec::new()), false),
            }
        }
        "HDEL" if !key.is_empty() && !arg.is_empty() => {
            let field = arg;
            (RESPValue::Integer(if store.hdel(key, field) { 1 } else { 0 }), true)
        }
        "REPLICAOF" if key.eq_ignore_ascii_case("no") && arg.eq_ignore_ascii_case("one") => {
            (RESPValue::SimpleString("OK".to_string()), false)
        }
        "INCRBY" if !key.is_empty() && !arg.is_empty() => {
            match arg.parse::<i64>() {
                Ok(delta) => (RESPValue::Integer(store.incrby(key, delta)), true),
                Err(_) => (RESPValue::Error("ERR value is not an integer".to_string()), false),
            }
        }
        "DECRBY" if !key.is_empty() && !arg.is_empty() => {
            match arg.parse::<i64>() {
                Ok(delta) => (RESPValue::Integer(store.incrby(key, -delta)), true),
                Err(_) => (RESPValue::Error("ERR value is not an integer".to_string()), false),
            }
        }
        "APPEND" if !key.is_empty() && !arg.is_empty() => {
            (RESPValue::Integer(store.append(key, arg) as i64), true)
        }
        "STRLEN" if !key.is_empty() => {
            (RESPValue::Integer(store.strlen(key) as i64), false)
        }
        "GETSET" if !key.is_empty() && !arg.is_empty() => {
            let old = store.getset(key, arg);
            (RESPValue::BulkString(old), true)
        }
        "INFO" => {
            let uptime = now().saturating_sub(START_TIME.load(Ordering::Relaxed));
            let role = if IS_REPLICA.load(Ordering::Relaxed) { "replica" } else { "master" };
            let num_keys = store.stats().total_keys;
            let active_conns = ACTIVE_CONNECTIONS.load(Ordering::Relaxed);
            let total_cmds = TOTAL_COMMANDS.load(Ordering::Relaxed);
            let hits = CACHE_HITS.load(Ordering::Relaxed);
            let misses = CACHE_MISSES.load(Ordering::Relaxed);
            let hit_rate = if hits + misses > 0 {
                (hits as f64 / (hits + misses) as f64) * 100.0
            } else {
                0.0
            };
            let used_memory = store.used_memory_bytes();
            let mm_limit = maxmemory_bytes();
            let mm_policy_str = format!("{:?}", maxmemory_policy());

            let info_text = format!(
                "role:{}\nuptime_seconds:{}\nconnected_keys:{}\nmax_keys:{}\nactive_connections:{}\ntotal_commands:{}\ncache_hits:{}\ncache_misses:{}\nhit_rate_percent:{:.1}\nused_memory_bytes:{}\nmaxmemory_bytes:{}\nmaxmemory_policy:{}",
                role, uptime, num_keys, MAX_KEYS, active_conns, total_cmds, hits, misses, hit_rate, used_memory, mm_limit, mm_policy_str
            );
            (RESPValue::BulkString(Some(info_text)), false)
        }
        "SETNX" if !key.is_empty() && !arg.is_empty() => {
            (RESPValue::Integer(if store.setnx(key, arg) { 1 } else { 0 }), true)
        }
        "HKEYS" if !key.is_empty() => {
            match store.hkeys(key) {
                Some(fields) => (RESPValue::Array(fields.into_iter().map(|f| RESPValue::BulkString(Some(f))).collect()), false),
                None => (RESPValue::Array(Vec::new()), false),
            }
        }
        "HVALS" if !key.is_empty() => {
            match store.hvals(key) {
                Some(vals) => (RESPValue::Array(vals.into_iter().map(|v| RESPValue::BulkString(Some(v))).collect()), false),
                None => (RESPValue::Array(Vec::new()), false),
            }
        }
        "COPY" if !key.is_empty() && !arg.is_empty() => {
            (RESPValue::Integer(if store.copy(key, arg) { 1 } else { 0 }), true)
        }
        "EXPIRE" if !key.is_empty() && !arg.is_empty() => {
            let condition = args.get(3).map(|s| s.to_uppercase());
            match arg.parse::<u64>() {
                Ok(s) => {
                    let ok = match &condition {
                        Some(c) => store.expire_conditional(key, s, c),
                        None => store.expire(key, s),
                    };
                    (RESPValue::Integer(if ok { 1 } else { 0 }), true)
                }
                Err(_) => (RESPValue::Error("ERR invalid expire time".to_string()), false),
            }
        }
        "PING" => (RESPValue::SimpleString("PONG".to_string()), false),
        _ => (RESPValue::Error("ERR comando non riconosciuto o sintassi errata".to_string()), false),
    }
}

fn normalize_for_log(store: &ShardedStore, args: &[String]) -> String {
    let cmd = args.get(0).map(|s| s.as_str()).unwrap_or("");
    let key = args.get(1).map(|s| s.as_str()).unwrap_or("");

    if cmd == "EXPIRE" {
        if let Some(exp) = store.get_expiry(key) {
            return format!("EXPIREAT {} {}", key, exp);
        }
    }
    if cmd == "SET" && args.len() > 3 {
        // SET con opzioni (EX/PX/NX/XX): NX/XX sono a posto così come sono
        // (existence-based, replay deterministico), ma EX/PX sono relativi
        // ad "adesso" — se li rigiocassimo alla lettera in fase di replay
        // (binlog o Replica), "adesso" sarebbe un istante diverso da quello
        // originale. Normalizziamo quindi in "SET chiave valore EXAT
        // <timestamp_assoluto>" quando è risultata una scadenza, così il
        // replay riproduce esattamente la stessa scadenza, non una nuova.
        if let Some(exp) = store.get_expiry(key) {
            let value = args.get(2).map(|s| s.as_str()).unwrap_or("");
            return format!("SET {} {} EXAT {}", key, value, exp);
        }
    }
    args.join(" ")
}
   
fn load_data(store: &ShardedStore) {
    CURRENT_TIME.store(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), Ordering::SeqCst);

    if Path::new(SNAPSHOT_PATH).exists() {
        if let Ok(file) = File::open(SNAPSHOT_PATH) {
            let decoder = GzDecoder::new(file);
            let mut count = 0;
            let mut skipped = 0;
            for line_result in StdBufReader::new(decoder).lines() {
                let line = match line_result {
                    Ok(l) => l,
                    Err(_) => { skipped += 1; continue; } // riga non UTF-8 valida
                };
                match line_to_entry(&line) {
                    Some((key, entry)) => {
                        if !is_expired(&entry) {
                            store.set_raw(key, entry);
                            count += 1;
                        }
                    }
                    None => skipped += 1, // riga malformata nello snapshot
                }
            }
            if skipped > 0 {
                warn!("Snapshot: {} righe scartate perché malformate o illeggibili", skipped);
            }
            info!("Snapshot caricato: {} elementi attivi", count);
        }
    }

    const BINLOG_PATH: &str = "onyx.binlog";
    if Path::new(BINLOG_PATH).exists() {
        if let Ok(data) = fs::read(BINLOG_PATH) {
            let mut offset = 0;
            let mut count = 0;
            let mut corrupt_records = 0;
            while offset < data.len() {
                if offset + 4 > data.len() {
                    warn!(
                        "Binlog troncato: avanzo {} byte in coda non formano un record completo, scartati",
                        data.len() - offset
                    );
                    break;
                }
                let record_len = ((data[offset] as u32) << 24)
                    | ((data[offset + 1] as u32) << 16)
                    | ((data[offset + 2] as u32) << 8)
                    | (data[offset + 3] as u32);
                offset += 4;

                if offset + record_len as usize > data.len() {
                    warn!(
                        "Binlog troncato: un record dichiara {} byte ma ne restano solo {}, scartato",
                        record_len, data.len() - offset
                    );
                    break;
                }
                let record = &data[offset..offset + record_len as usize];
                offset += record_len as usize;

                match binary_record_to_args(record) {
                    Some(args) => {
                        execute_command(store, &args);
                        count += 1;
                    }
                    None => corrupt_records += 1, // lunghezza ok ma contenuto non decodificabile
                }
            }
            if corrupt_records > 0 {
                warn!("Binlog: {} record scartati perché corrotti o non riconosciuti", corrupt_records);
            }
            info!("Binlog riprodotto: {} comandi", count);
        }
    }
}
async fn handle_client(stream: TcpStream, store: Arc<ShardedStore>, persistence: Arc<Persistence>) {
    let _ = stream.set_nodelay(true);
    let peer_addr = stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "sconosciuto".to_string());
    ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    struct ConnGuard;
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let _guard = ConnGuard;
    let (reader, writer) = stream.into_split();
    let mut buf_reader = TokioBufReader::with_capacity(65536, reader);
    let mut buf_writer = TokioBufWriter::with_capacity (65536, writer); 
    let mut scratch = String::with_capacity(256);
    let mut resp_buf = String::with_capacity(256);
    let mut authenticated = !auth_required();
    let mut in_transaction = false;
    let mut queued_commands: Vec<Vec<String>> = Vec::new();

    loop {
        let mut args = match read_command(&mut buf_reader, &mut scratch).await {
            Ok(Some(args)) if !args.is_empty() => args,
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => break,
        };

        // Normalizza il nome del comando in maiuscolo UNA VOLTA sola, in-place
        // (make_ascii_uppercase non alloca): prima il match in execute_command
        // era case-sensitive, quindi "set" minuscolo non funzionava — un vero
        // bug di compatibilità, non solo una questione di stile. Farlo qui,
        // prima di ogni uso, garantisce che dispatch, coda di EXEC, e stream
        // di replica vedano tutti la stessa versione normalizzata.
        args[0].make_ascii_uppercase();
        let cmd = args[0].as_str();

        // AUTH: sia `AUTH password` (utente "default") sia `AUTH utente password`
        if cmd.eq_ignore_ascii_case("AUTH") {
            let (username, provided_password) = if args.len() >= 3 {
                (args[1].clone(), args[2].clone())
            } else {
                ("default".to_string(), args.get(1).cloned().unwrap_or_default())
            };
            let response = if !auth_required() {
                RESPValue::Error("ERR nessuna password configurata su questo server".to_string())
            } else if check_credentials(&username, &provided_password) {
                authenticated = true;
                RESPValue::SimpleString("OK".to_string())
            } else {
                RESPValue::Error("WRONGPASS nome utente o password non validi".to_string())
            };
            resp_buf.clear();
            response.encode_into(&mut resp_buf);
           let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
           let _ = buf_writer.flush().await;
            continue;
        }
        // Autenticazione: da qui in poi, se serve login e non è stato fatto,
        // nessun altro comando è ammesso (nemmeno MULTI/EXEC/SYNC/SUBSCRIBE).
        if !authenticated {
            resp_buf.clear();
            RESPValue::Error("NOAUTH autenticazione richiesta. Usa AUTH password".to_string())
                .encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }
        // MULTI


        if cmd.eq_ignore_ascii_case("MULTI") {
            in_transaction = true;
            queued_commands.clear();
            resp_buf.clear();
            RESPValue::SimpleString("OK".to_string()).encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // DISCARD
        if cmd.eq_ignore_ascii_case("DISCARD") {
            let response = if in_transaction {
                in_transaction = false;
                queued_commands.clear();
                RESPValue::SimpleString("OK".to_string())
            } else {
                RESPValue::Error("ERR DISCARD senza una transazione attiva (usa prima MULTI)".to_string())
            };
            resp_buf.clear();
            response.encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // EXEC
        if cmd.eq_ignore_ascii_case("EXEC") {
            let response = if !in_transaction {
                RESPValue::Error("ERR EXEC senza una transazione attiva (usa prima MULTI)".to_string())
            } else {
                in_transaction = false;
                let mut results = Vec::with_capacity(queued_commands.len());
                for queued_args in queued_commands.drain(..) {
                    let queued_cmd = queued_args.get(0).map(|s| s.as_str()).unwrap_or("");
                    if IS_REPLICA.load(Ordering::Relaxed) && is_write_command(queued_cmd) {
                        results.push(RESPValue::Error("READONLY questa istanza è una Replica in sola lettura".to_string()));
                        continue;
                    }
                    let (resp, is_write) = execute_command(&store, &queued_args);
                    if is_write {
                        persist_and_replicate(&store, &persistence, &queued_args).await;
                    }
                    results.push(resp);
                }
                RESPValue::Array(results)
            };
            resp_buf.clear();
            response.encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // Se in transazione, accoda
        if in_transaction {
            queued_commands.push(args.clone());
            resp_buf.clear();
            RESPValue::SimpleString("QUEUED".to_string()).encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }



        // PUBLISH — non richiede modalità speciale, un comando normale come
        // gli altri, solo che non passa da execute_command/persistenza (i
        // messaggi pub/sub sono effimeri, non finiscono nel binlog).
        if cmd == "PUBLISH" {
            let channel = args.get(1).cloned().unwrap_or_default();
            let message = args.get(2).cloned().unwrap_or_default();
            let receiver_count = persistence.subscriptions.lock().unwrap()
                .get(&channel).map(|s| s.len()).unwrap_or(0);
            let _ = persistence.pubsub_tx.send((channel, message));
            resp_buf.clear();
            RESPValue::Integer(receiver_count as i64).encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // SUBSCRIBE — entra in "modalità pub/sub": da qui la connessione
        // smette di eseguire comandi normali e resta bloccata a inoltrare
        // messaggi dei canali sottoscritti, finché il client si disconnette.
        if cmd == "SUBSCRIBE" {
            let sub_id = persistence.next_subscriber_id.fetch_add(1, Ordering::SeqCst) + 1;
            let mut my_channels: std::collections::HashSet<String> = std::collections::HashSet::new();

            for channel in args[1..].to_vec() {
                my_channels.insert(channel.clone());
                persistence.subscriptions.lock().unwrap()
                    .entry(channel.clone()).or_insert_with(std::collections::HashSet::new)
                    .insert(sub_id);
                let count = my_channels.len();
                let confirm = RESPValue::Array(vec![
                    RESPValue::BulkString(Some("subscribe".to_string())),
                    RESPValue::BulkString(Some(channel)),
                    RESPValue::Integer(count as i64),
                ]);
                resp_buf.clear();
                confirm.encode_into(&mut resp_buf);
                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() {
                    return;
                }
            }
            let _ = buf_writer.flush().await;

            let mut pubsub_rx = persistence.pubsub_tx.subscribe();

            // Task dedicato a leggere ulteriori SUBSCRIBE/UNSUBSCRIBE dal
            // client mentre restiamo in ascolto dei messaggi pubblicati.
            // Stesso schema usato per gli ACK delle Repliche: un task
            // possiede la metà "lettura", il ciclo principale sceglie tra
            // canali cancel-safe (mpsc/broadcast), mai su read_line diretto.
            let (chan_tx, mut chan_rx) = tokio::sync::mpsc::unbounded_channel::<(bool, Vec<String>)>();
            let reader_task = tokio::spawn(async move {
                let mut sub_scratch = String::new();
                loop {
                    match read_command(&mut buf_reader, &mut sub_scratch).await {
                        Ok(Some(sub_args)) if !sub_args.is_empty() => {
                            let sub_cmd = sub_args[0].to_ascii_uppercase();
                            if sub_cmd == "SUBSCRIBE" {
                                let _ = chan_tx.send((true, sub_args[1..].to_vec()));
                            } else if sub_cmd == "UNSUBSCRIBE" {
                                let chans = if sub_args.len() > 1 { sub_args[1..].to_vec() } else { Vec::new() };
                                let _ = chan_tx.send((false, chans));
                            }
                            // Altri comandi vengono ignorati finché si è in
                            // modalità pub/sub (limitazione nota, come nella
                            // sottoscrizione RESP2 di base di Redis).
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
            });

            loop {
                tokio::select! {
                    update = chan_rx.recv() => {
                        let (is_subscribe, channels) = match update {
                            Some(u) => u,
                            None => break, // connessione chiusa dal client
                        };
                        if is_subscribe {
                            for channel in channels {
                                if my_channels.insert(channel.clone()) {
                                    persistence.subscriptions.lock().unwrap()
                                        .entry(channel.clone()).or_insert_with(std::collections::HashSet::new)
                                        .insert(sub_id);
                                }
                                let count = my_channels.len();
                                let confirm = RESPValue::Array(vec![
                                    RESPValue::BulkString(Some("subscribe".to_string())),
                                    RESPValue::BulkString(Some(channel)),
                                    RESPValue::Integer(count as i64),
                                ]);
                                resp_buf.clear();
                                confirm.encode_into(&mut resp_buf);
                                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() { break; }
                            }
                            let _ = buf_writer.flush().await;
                        } else {
                            let targets: Vec<String> = if channels.is_empty() {
                                my_channels.iter().cloned().collect()
                            } else {
                                channels
                            };
                            for channel in targets {
                                my_channels.remove(&channel);
                                if let Some(set) = persistence.subscriptions.lock().unwrap().get_mut(&channel) {
                                    set.remove(&sub_id);
                                }
                                let count = my_channels.len();
                                let confirm = RESPValue::Array(vec![
                                    RESPValue::BulkString(Some("unsubscribe".to_string())),
                                    RESPValue::BulkString(Some(channel)),
                                    RESPValue::Integer(count as i64),
                                ]);
                                resp_buf.clear();
                                confirm.encode_into(&mut resp_buf);
                                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() { break; }
                            }
                            let _ = buf_writer.flush().await;
                        }
                    }
                    msg = pubsub_rx.recv() => {
                        match msg {
                            Ok((channel, payload)) => {
                                if my_channels.contains(&channel) {
                                    let out = RESPValue::Array(vec![
                                        RESPValue::BulkString(Some("message".to_string())),
                                        RESPValue::BulkString(Some(channel)),
                                        RESPValue::BulkString(Some(payload)),
                                    ]);
                                    resp_buf.clear();
                                    out.encode_into(&mut resp_buf);
                                    if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() { break; }
                                    let _ = buf_writer.flush().await;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                warn!("Subscriber {} troppo lento, alcuni messaggi persi", sub_id);
                            }
                            Err(_) => break,
                        }
                    }
                }
            }

            reader_task.abort();
            {
                let mut subs = persistence.subscriptions.lock().unwrap();
                for channel in &my_channels {
                    if let Some(set) = subs.get_mut(channel) {
                        set.remove(&sub_id);
                    }
                }
            }
            return;
        }

        // SYNC
        if cmd == "SYNC" {
            // Nuovo formato: `SYNC <replid> <offset>`. `replid=0` significa
            // "non conosco ancora il replication ID del Master" (prima
            // connessione) e forza sempre un dump completo, così come un
            // replid che non combacia con quello attuale del Master (segno
            // che il Master è ripartito da zero da quando la Replica si è
            // vista l'ultima volta — il suo vecchio offset non ha più senso
            // qui, anche se il backlog risultasse "vuoto" per coincidenza).
            let requested_replid: u64 = args.get(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            let requested_offset: u64 = args.get(2).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            let replid_matches = replid_allows_partial(requested_replid, repl_id());
            let replica_id = persistence.next_replica_id.fetch_add(1, Ordering::SeqCst) + 1;

            if requested_replid != 0 && !replid_matches {
                info!(
                    "Replica {} presenta un replication ID diverso da quello attuale (probabile riavvio del Master): forzo un dump completo",
                    peer_addr
                );
            }

            // Sottoscrizione al canale broadcast prima di leggere backlog/snapshot,
            // così nessuna scrittura concorrente può sfuggire nella finestra tra
            // "decido cosa mandare" e "comincio a mandare in tempo reale".
            let mut replica_rx = persistence.replica_tx.subscribe();

            // Un resync parziale è possibile SOLO se il replication ID
            // combacia (stesso processo Master) E il backlog copre ancora
            // tutto da requested_offset+1 in poi (nessun buco).
            let backlog_snapshot: Option<Vec<(u64, String)>> = if replid_matches && requested_offset > 0 {
                let backlog = persistence.backlog.lock().unwrap();
                let backlog_oldest = backlog.front().map(|(off, _)| *off);
                let current_offset = persistence.repl_offset.load(Ordering::SeqCst);
                if partial_resync_possible(requested_offset, backlog_oldest, current_offset) {
                    Some(backlog.iter().filter(|(off, _)| *off > requested_offset).cloned().collect())
                } else {
                    None
                }
            } else {
                None
            };

            persistence.replica_status.lock().unwrap().insert(replica_id, ReplicaStatus {
                addr: peer_addr.clone(),
                last_ack_offset: requested_offset,
                last_ack_time: now(),
            });

            // Task dedicato a leggere gli ACK della Replica (REPLCONF ACK
            // <offset>): possiede esclusivamente la metà "lettura" della
            // connessione. Niente select! qui di proposito — read_line non è
            // cancel-safe, e in un loop { select! {...} } una riga letta a
            // metà e poi scartata corromperebbe silenziosamente il flusso.
            let persistence_ack = Arc::clone(&persistence);
            let peer_addr_ack = peer_addr.clone();
            let reader_task = tokio::spawn(async move {
                let mut ack_scratch = String::new();
                loop {
                    match read_command(&mut buf_reader, &mut ack_scratch).await {
                        Ok(Some(ack_args)) if !ack_args.is_empty() => {
                            if ack_args[0].eq_ignore_ascii_case("REPLCONF")
                                && ack_args.get(1).map(|s| s.eq_ignore_ascii_case("ACK")).unwrap_or(false)
                            {
                                if let Some(off) = ack_args.get(2).and_then(|s| s.parse::<u64>().ok()) {
                                    if let Some(status) = persistence_ack.replica_status.lock().unwrap().get_mut(&replica_id) {
                                        status.last_ack_offset = off;
                                        status.last_ack_time = now();
                                    }
                                }
                            }
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
                let _ = peer_addr_ack; // tenuto solo per eventuale futuro logging
            });

            let mut last_sent_offset;

            if let Some(missing) = backlog_snapshot {
                // RESYNC PARZIALE
                last_sent_offset = requested_offset;
                let marker = format!("+CONTINUE {} {}\r\n", repl_id(), requested_offset);
                if buf_writer.write_all(marker.as_bytes()).await.is_err() {
                    reader_task.abort();
                    persistence.replica_status.lock().unwrap().remove(&replica_id);
                    return;
                }
                for (off, cmd_line) in missing {
                    let line = format!("{}\r\n", cmd_line);
                    if buf_writer.write_all(line.as_bytes()).await.is_err() {
                        reader_task.abort();
                        persistence.replica_status.lock().unwrap().remove(&replica_id);
                        return;
                    }
                    last_sent_offset = off;
                }
                info!("Replica {} risincronizzata parzialmente da offset {}", peer_addr, requested_offset);
            } else {
                // FULL RESYNC (comportamento di prima: dump completo di tutte le chiavi)
                let full_sync_offset = persistence.repl_offset.load(Ordering::SeqCst);
                let marker = format!("+FULLRESYNC {} {}\r\n", repl_id(), full_sync_offset);
                if buf_writer.write_all(marker.as_bytes()).await.is_err() {
                    reader_task.abort();
                    persistence.replica_status.lock().unwrap().remove(&replica_id);
                    return;
                }

                for (k, entry) in store.snapshot_entries() {
                    let restore_cmd = match &entry.value {
                        OnyxValue::Blob(b) => format!("SET {} {}\r\n", k, String::from_utf8_lossy(b)),
                        OnyxValue::Int(n) => format!("SET {} {}\r\n", k, n),
                        OnyxValue::List(list) => {
                            let mut cmds = String::new();
                            for item in list.iter().rev() {
                                cmds.push_str(&format!("LPUSH {} {}\r\n", k, String::from_utf8_lossy(item)));
                            }
                            cmds
                        }
                        OnyxValue::Hash(map) => {
                            let mut cmds = String::new();
                            for (f, v) in map.iter() {
                                cmds.push_str(&format!("HSET {} {} {}\r\n", k, String::from_utf8_lossy(f), String::from_utf8_lossy(v)));
                            }
                            cmds
                        }
                        OnyxValue::Set(set) => {
                            let mut cmds = String::new();
                            for item in set.iter() {
                                cmds.push_str(&format!("SADD {} {}\r\n", k, String::from_utf8_lossy(item)));
                            }
                            cmds
                        }
                        _ => String::new(),
                    };
                    if buf_writer.write_all(restore_cmd.as_bytes()).await.is_err() {
                        reader_task.abort();
                        persistence.replica_status.lock().unwrap().remove(&replica_id);
                        return;
                    }
                }
                last_sent_offset = full_sync_offset;
                info!("Replica {} sincronizzata con dump completo (offset {})", peer_addr, full_sync_offset);
            }

            // Marcatore esplicito di "fine sincronizzazione iniziale": senza
            // questo la Replica non potrebbe distinguere in modo affidabile
            // tra righe del dump/backlog (che non corrispondono 1:1 a un
            // offset) e comandi live (che sì) — le direbbe come contare da qui.
            let syncdone_marker = format!("+SYNCDONE {}\r\n", last_sent_offset);
            if buf_writer.write_all(syncdone_marker.as_bytes()).await.is_err() {
                reader_task.abort();
                persistence.replica_status.lock().unwrap().remove(&replica_id);
                return;
            }
            let _ = buf_writer.flush().await;

            info!("Replica {} in streaming in tempo reale (offset corrente: {})", peer_addr, last_sent_offset);

            loop {
                match replica_rx.recv().await {
                    Ok((offset, cmd_line)) => {
                        if offset <= last_sent_offset {
                            // Già coperto dal backlog/dump iniziale: evita doppioni
                            // nella stretta finestra tra subscribe() e l'inizio dell'invio.
                            continue;
                        }
                        let line_with_newline = format!("{}\r\n", cmd_line);
                        if buf_writer.write_all(line_with_newline.as_bytes()).await.is_err() {
                            break;
                        }
                        let _ = buf_writer.flush().await;
                        last_sent_offset = offset;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // La Replica non riesce a stare al passo: il canale
                        // broadcast ha già scartato messaggi non ancora
                        // consegnati. Meglio chiudere e forzare un resync
                        // completo alla prossima riconnessione, piuttosto che
                        // lasciare un buco silenzioso nel flusso replicato.
                        warn!("Replica {} troppo lenta, disconnessione forzata (richiederà un nuovo SYNC)", peer_addr);
                        break;
                    }
                    Err(_) => break,
                }
            }

            reader_task.abort();
            persistence.replica_status.lock().unwrap().remove(&replica_id);
            info!("Replica {} disconnessa", peer_addr);
            return;
        }

        // Comando normale
        let response = if cmd == "SAVE" {
            persistence.compaction_pending.store(true, Ordering::SeqCst);
            let _ = persistence.log_tx.send(LogMessage::Compact).await;
            persistence.write_count.store(0, Ordering::SeqCst);
            RESPValue::SimpleString("OK".to_string())
        } else if IS_REPLICA.load(Ordering::Relaxed) && is_write_command(cmd) {
            // Una Replica non deve accettare scritture dirette dai client: i
            // suoi dati arrivano SOLO dal Master via replica_tx. Scritture
            // dirette qui la farebbero divergere silenziosamente, e verrebbero
            // pure sovrascritte al prossimo comando replicato — meglio
            // rifiutarle chiaramente, come fa Redis con READONLY.
            RESPValue::Error("READONLY questa istanza è una Replica in sola lettura".to_string())
        } else {
            if cmd.eq_ignore_ascii_case("REPLICAOF")
                && args.get(1).map(|s| s.eq_ignore_ascii_case("no")).unwrap_or(false)
                && args.get(2).map(|s| s.eq_ignore_ascii_case("one")).unwrap_or(false)
            {
                persistence.promote_to_master.store(true, Ordering::Relaxed);
                IS_REPLICA.store(false, Ordering::Relaxed);
                info!("Ricevuto REPLICAOF NO ONE: promozione a Master in corso");
            }

            TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
            let (mut resp, is_write) = execute_command(&store, &args);
            if cmd.eq_ignore_ascii_case("INFO") {
                if let RESPValue::BulkString(Some(ref mut text)) = resp {
                    let repl_offset = persistence.repl_offset.load(Ordering::SeqCst);
                    let statuses = persistence.replica_status.lock().unwrap();
                    let connected_replicas = statuses.len();
                    let max_lag = statuses.values()
                        .map(|s| repl_offset.saturating_sub(s.last_ack_offset))
                        .max()
                        .unwrap_or(0);
                    text.push_str(&format!(
                        "\nmaster_repl_offset:{}\nconnected_replicas:{}\nmax_replica_lag:{}",
                        repl_offset, connected_replicas, max_lag
                    ));
                    for (i, status) in statuses.values().enumerate() {
                        let lag = repl_offset.saturating_sub(status.last_ack_offset);
                        let last_ack_secs_ago = now().saturating_sub(status.last_ack_time);
                        text.push_str(&format!(
                            "\nslave{}:addr={},offset={},lag={},last_ack_secs_ago={}",
                            i, status.addr, status.last_ack_offset, lag, last_ack_secs_ago
                        ));
                    }
                    drop(statuses);
                }
            }
            if !is_write {
                match &resp {
                    RESPValue::BulkString(None) => { CACHE_MISSES.fetch_add(1, Ordering::Relaxed); }
                    RESPValue::BulkString(Some(_)) => { CACHE_HITS.fetch_add(1, Ordering::Relaxed); }
                    _ => {}
                }
            }
            if is_write {
                persist_and_replicate(&store, &persistence, &args).await;
            }
            resp
        };

        resp_buf.clear();
        response.encode_into(&mut resp_buf);
        let _ = buf_writer.write_all(resp_buf.as_bytes()).await;

        if buf_reader.buffer().is_empty() {
            let _ = buf_writer.flush().await;
        }
    }
    let _ = buf_writer.flush().await;
}

async fn active_expiration_task(store: Arc<ShardedStore>) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let count = store.gc_expired();
        if count > 0 { println!("🧹 GC: rimosse {} chiavi scadute.", count); }
    }
}
async fn time_updater_task() {
    loop {
        let now_sec = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        CURRENT_TIME.store(now_sec, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn do_compact(store: &Arc<ShardedStore>) {
    let store_c = Arc::clone(store);
    tokio::task::spawn_blocking(move || {
        let tmp_path = format!("{}.tmp", SNAPSHOT_PATH);
        match File::create(&tmp_path) {
            Ok(file) => {
                let mut writer = GzEncoder::new(BufWriter::new(file), Compression::default());
                for (key, entry) in store_c.snapshot_entries() {
                    let _ = writeln!(writer, "{}", value_to_line(&key, &entry));
                }
                if let Err(e) = writer.finish() {
                    error!("Errore durante la finalizzazione dello snapshot compresso: {}", e);
                }
                if let Err(e) = fs::rename(&tmp_path, SNAPSHOT_PATH) {
                    error!("Impossibile sostituire onyx.snapshot ({}). Riprovero alla prossima compattazione.", e);
                }
            }
            Err(e) => {
                error!("Impossibile creare lo snapshot temporaneo ({}). Compattazione saltata questa volta.", e);
            }
        }

        let _log_file = loop {
            match OpenOptions::new().create(true).write(true).truncate(true).open(LOG_PATH) {
                Ok(f) => break f,
                Err(e) => {
                    eprintln!("Impossibile riaprire onyx.log dopo la compattazione ({}). Ritento tra 3s...", e);
                    std::thread::sleep(Duration::from_secs(3));
                }
            }
        };
        info!("Compattazione eseguita: snapshot aggiornato, log svuotato");
    }).await.unwrap()
}
fn format_prometheus_metrics(store: &ShardedStore, persistence: &Persistence) -> String {
    let uptime = now().saturating_sub(START_TIME.load(Ordering::Relaxed));
    let num_keys = store.stats().total_keys;
    let active_conns = ACTIVE_CONNECTIONS.load(Ordering::Relaxed);
    let total_cmds = TOTAL_COMMANDS.load(Ordering::Relaxed);
    let hits = CACHE_HITS.load(Ordering::Relaxed);
    let misses = CACHE_MISSES.load(Ordering::Relaxed);
    let role_value = if IS_REPLICA.load(Ordering::Relaxed) { 0 } else { 1 };

    let repl_offset = persistence.repl_offset.load(Ordering::SeqCst);
    let statuses = persistence.replica_status.lock().unwrap();
    let connected_replicas = statuses.len();
    let max_lag = statuses.values()
        .map(|s| repl_offset.saturating_sub(s.last_ack_offset))
        .max()
        .unwrap_or(0);
    drop(statuses);

    format!(
        "# HELP onyxdb_uptime_seconds Tempo di attivita' del server in secondi\n\
         # TYPE onyxdb_uptime_seconds counter\n\
         onyxdb_uptime_seconds {}\n\
         # HELP onyxdb_keys_total Numero di chiavi attualmente presenti\n\
        TYPE onyxdb_keys_total gauge\n\
         onyxdb_keys_total {}\n\
         # HELP onyxdb_active_connections Numero di connessioni client attive\n\
         # TYPE onyxdb_active_connections gauge\n\
         onyxdb_active_connections {}\n\
         # HELP onyxdb_commands_total Numero totale di comandi eseguiti\n\
         # TYPE onyxdb_commands_total counter\n\
         onyxdb_commands_total {}\n\
         # HELP onyxdb_cache_hits_total Numero di letture andate a buon fine\n\
         # TYPE onyxdb_cache_hits_total counter\n\
         onyxdb_cache_hits_total {}\n\
         # HELP onyxdb_cache_misses_total Numero di letture su chiavi inesistenti\n\
         # TYPE onyxdb_cache_misses_total counter\n\
         onyxdb_cache_misses_total {}\n\
         # HELP onyxdb_is_master 1 se questa istanza e' un Master, 0 se e' una Replica\n\
         # TYPE onyxdb_is_master gauge\n\
         onyxdb_is_master {}\n\
         # HELP onyxdb_replication_offset Offset di replicazione corrente (numero di comandi replicati)\n\
         # TYPE onyxdb_replication_offset counter\n\
         onyxdb_replication_offset {}\n\
         # HELP onyxdb_connected_replicas Numero di Replica attualmente connesse\n\
         # TYPE onyxdb_connected_replicas gauge\n\
         onyxdb_connected_replicas {}\n\
         # HELP onyxdb_max_replica_lag Quanto e' indietro la Replica piu' lenta, in numero di comandi\n\
         # TYPE onyxdb_max_replica_lag gauge\n\
         onyxdb_max_replica_lag {}\n\
         # HELP onyxdb_memory_bytes Byte occupati (stima approssimativa)\n\
         # TYPE onyxdb_memory_bytes gauge\n\
         onyxdb_memory_bytes {}\n",
        uptime, num_keys, active_conns, total_cmds, hits, misses, role_value,
        repl_offset, connected_replicas, max_lag, store.used_memory_bytes()
    )
}

async fn run_metrics_server(store: Arc<ShardedStore>, persistence: Arc<Persistence>, port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Impossibile avviare il server metriche su {}: {}", addr, e);
            return;
        }
    };
    info!("Server metriche Prometheus in ascolto su http://{}/metrics", addr);

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        let store_clone = Arc::clone(&store);
        let persistence_clone = Arc::clone(&persistence);

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;

            let body = format_prometheus_metrics(&store_clone, &persistence_clone);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}  
// ============================================================
// HANDLER OBP (Onyx Binary Protocol) - Listener parallelo
// ============================================================

async fn handle_obp_client(stream: TcpStream, store: Arc<ShardedStore>, persistence: Arc<Persistence>) {
    let _ = stream.set_nodelay(true);
    let (reader, writer) = stream.into_split();
    let mut buf_reader = TokioBufReader::with_capacity(65536, reader);
    let mut buf_writer = TokioBufWriter::with_capacity(8192, writer);
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut authenticated = !auth_required();
    loop {
        match buf_reader.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }

        while let Some(frame) = OBPFrame::decode(&mut buf) {
            let response = execute_obp_command(&store, &persistence, frame, &mut authenticated).await;
            let mut out = bytes::BytesMut::new();
            response.encode(&mut out);
            if buf_writer.write_all(&out).await.is_err() {
                return;
            }
        }

        if buf.len() > 1024 * 1024 {
            break;
        }
    }

    let _ = buf_writer.flush().await;
}

/// Punto unico per ogni scrittura che deve arrivare al binlog E alle
/// Replica: usato sia dal percorso RESP (comando singolo ed EXEC, dentro
/// handle_client) sia dal percorso OBP. Assegna l'offset di replicazione,
/// lo mette nel backlog, lo trasmette in tempo reale, lo scrive sul binlog
/// e innesca la compattazione se serve.
async fn persist_and_replicate(store: &ShardedStore, persistence: &Persistence, cmd_args: &[String]) {
    let text_for_replica = normalize_for_log(store, cmd_args);
    // Usiamo la STESSA versione normalizzata sia per il binlog sia per lo
    // stream di replica: prima venivano ricalcolate separatamente (binlog
    // dai cmd_args originali, replica dal testo normalizzato), il che per
    // "SET ... EX ..." avrebbe fatto sì che il binlog perdesse la scadenza
    // mentre la Replica no (o viceversa) — due percorsi che potevano
    // silenziosamente divergere sullo stesso identico comando.
    let normalized_args: Vec<String> = text_for_replica.split_whitespace().map(|s| s.to_string()).collect();

    let new_offset = persistence.repl_offset.fetch_add(1, Ordering::SeqCst) + 1;
    {
        let mut backlog = persistence.backlog.lock().unwrap();
        backlog.push_back((new_offset, text_for_replica.clone()));
        while backlog.len() > BACKLOG_CAPACITY {
            backlog.pop_front();
        }
    }
    let _ = persistence.replica_tx.send((new_offset, text_for_replica));

    let cmd_name = normalized_args.get(0).map(|s| s.as_str()).unwrap_or("");
    if let Some(binary_record) = command_to_binary_record(cmd_name, &normalized_args, None) {
        let _ = persistence.log_tx.send(LogMessage::Append(binary_record)).await;
    }

    if persistence.write_count.fetch_add(1, Ordering::SeqCst) + 1 >= COMPACTION_THRESHOLD {
        if persistence.compaction_pending.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            let _ = persistence.log_tx.send(LogMessage::Compact).await;
        }
        persistence.write_count.store(0, Ordering::SeqCst);
    }
}

async fn execute_obp_command(
    store: &ShardedStore,
    persistence: &Persistence,
    frame: OBPFrame,
    authenticated: &mut bool,
) -> OBPFrame {
    let cmd = frame.cmd;
    let args = frame.args;

    // AUTH via OBP (codice 0x10): arg[0]=password, oppure arg[0]=utente e arg[1]=password.
    if cmd == 0x10 {
        let (user, pass) = if args.len() >= 2 {
            (String::from_utf8_lossy(&args[0]).to_string(),
             String::from_utf8_lossy(&args[1]).to_string())
        } else {
            ("default".to_string(),
             args.get(0).map(|a| String::from_utf8_lossy(a).to_string()).unwrap_or_default())
        };
        let ok = auth_required() && check_credentials(&user, &pass);
        if ok { *authenticated = true; }
        return OBPFrame {
            cmd: 0x00, flags: 0, correlation_id: frame.correlation_id,
            args: Vec::new(),
            payload: Some(Bytes::from(if ok { "OK" } else { "WRONGPASS" })),
        };
    }

    // Se serve login e non è stato fatto, rifiuta qualsiasi altro comando.
    if !*authenticated {
        return OBPFrame {
            cmd: 0x00, flags: 0, correlation_id: frame.correlation_id,
            args: Vec::new(),
            payload: Some(Bytes::from("NOAUTH auth richiesta")),
        };
    }


    let (value, _is_write) = match cmd {
        0x01 => {
            if let Some(key) = args.get(0) {
                (
                    store
                        .engine
                        .get(key)
                        .map(|e| e.value)
                        .unwrap_or(OnyxValue::Blob(Bytes::new())),
                    false,
                )
            } else {
                (OnyxValue::Blob(Bytes::new()), false)
            }
        }
        0x02 => {
            if args.len() >= 2 {
                let key = args[0].clone();
                let value = OnyxValue::Blob(args[1].clone());
                store.engine.set(key.clone(), value, None);

                let key_str = String::from_utf8_lossy(&key).to_string();
                let val_str = String::from_utf8_lossy(&args[1]).to_string();
                let cmd_args = vec!["SET".to_string(), key_str, val_str];
                persist_and_replicate(store, persistence, &cmd_args).await;

                (OnyxValue::Blob(Bytes::from("OK")), true)
            } else {
                (OnyxValue::Blob(Bytes::from("ERR")), false)
            }
        }
        0x03 => {
            if let Some(key) = args.get(0) {
                let deleted = store.engine.delete(key);
                if deleted {
                    let key_str = String::from_utf8_lossy(key).to_string();
                    let cmd_args = vec!["DEL".to_string(), key_str];
                    persist_and_replicate(store, persistence, &cmd_args).await;
                }
                (OnyxValue::Int(if deleted { 1 } else { 0 }), true)
            } else {
                (OnyxValue::Int(0), false)
            }
        }
        0xF0 => {
            (OnyxValue::Blob(Bytes::from("PONG")), false)
        }
        _ => {
            (OnyxValue::Blob(Bytes::from("ERR unknown command")), false)
        }
    };

    OBPFrame {
        cmd: 0x00,
        flags: 0,
        correlation_id: frame.correlation_id,
        args: Vec::new(),
        payload: Some(Bytes::from(format!("{:?}", value))),
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Starting OnyxDB");
    START_TIME.store(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), Ordering::Relaxed);
    let repl_id_val: u64 = {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let pid = std::process::id() as u64;
        // Non serve crittograficamente sicuro, solo "diverso ad ogni avvio
        // con probabilità di collisione trascurabile".
        nanos.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(pid)
    };
    REPL_ID.set(repl_id_val).ok();
    info!("Replication ID di questa istanza: {}", repl_id_val);
    let args: Vec<String> = env::args().collect();
    let mut master_addr: Option<String> = None;
    let mut password: Option<String> = None;
    let mut appendfsync: Option<String> = None;
    let mut maxmemory_arg: Option<String> = None;
    let mut maxmemory_policy_arg: Option<String> = None;
    let mut users_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut auto_failover = false;
    let mut failover_timeout_secs: u64 = 30;
    for i in 0..args.len() {
        if args[i] == "--replica-of" && i + 1 < args.len() {
            master_addr = Some(args[i + 1].clone());
        }
        if args[i] == "--requirepass" && i + 1 < args.len() {
            password = Some(args[i + 1].clone());
        }
        if args[i] == "--appendfsync" && i + 1 < args.len() {
            appendfsync = Some(args[i + 1].clone());
        }
        if args[i] == "--maxmemory" && i + 1 < args.len() {
            maxmemory_arg = Some(args[i + 1].clone());
        }
        if args[i] == "--maxmemory-policy" && i + 1 < args.len() {
            maxmemory_policy_arg = Some(args[i + 1].clone());
        }
        // --user nome:password — ripetibile, un utente per occorrenza.
        if args[i] == "--user" && i + 1 < args.len() {
            match args[i + 1].split_once(':') {
                Some((name, pw)) => { users_map.insert(name.to_string(), pw.to_string()); }
                None => warn!("Formato non valido per --user (atteso nome:password): '{}'", args[i + 1]),
            }
        }
        if args[i] == "--auto-failover" {
            auto_failover = true;
        }
        if args[i] == "--failover-timeout" && i + 1 < args.len() {
            failover_timeout_secs = args[i + 1].parse::<u64>().unwrap_or(30);
        }
    }
    if password.is_none() {
        password = env::var("ONYXDB_PASSWORD").ok();
    }
    // Compatibilità con la vecchia modalità a password unica: diventa
    // l'utente "default", utilizzabile anche con `AUTH password` (senza
    // nome utente esplicito).
    if let Some(pw) = password {
        users_map.insert("default".to_string(), pw);
    }
    let num_users = users_map.len();
    USERS.set(users_map).ok();
    if num_users > 0 {
        info!("Autenticazione richiesta: {} utente/i configurato/i", num_users);
    }

    let policy = match appendfsync.as_deref() {
        Some(s) => FsyncPolicy::parse(s).unwrap_or_else(|| {
            warn!("Valore non valido per --appendfsync ('{}'), uso 'everysec' di default", s);
            FsyncPolicy::EverySec
        }),
        None => FsyncPolicy::EverySec,
    };
    FSYNC_POLICY.set(policy).ok();
    info!("Politica fsync sul binlog: {:?}", policy);

    // maxmemory accetta suffissi come Redis: 100mb, 1gb, o un numero puro di byte.
    let maxmemory_val: usize = match maxmemory_arg.as_deref() {
        Some(s) => parse_memory_size(s).unwrap_or_else(|| {
            warn!("Valore non valido per --maxmemory ('{}'), nessun limite applicato", s);
            0
        }),
        None => 0,
    };
    MAXMEMORY_BYTES.set(maxmemory_val).ok();

    let mm_policy = match maxmemory_policy_arg.as_deref() {
        Some(s) => EvictionPolicy::parse(s).unwrap_or_else(|| {
            warn!("Valore non valido per --maxmemory-policy ('{}'), uso 'noeviction' di default", s);
            EvictionPolicy::NoEviction
        }),
        None => EvictionPolicy::NoEviction,
    };
    MAXMEMORY_POLICY.set(mm_policy).ok();
    if maxmemory_val > 0 {
        info!("Limite di memoria: {} byte, policy {:?}", maxmemory_val, mm_policy);
    }

    tokio::spawn(async { time_updater_task().await; });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let store = Arc::new(ShardedStore::new());
    if master_addr.is_none() {
        load_data(&store);
    }

    let store_gc = Arc::clone(&store);
    tokio::spawn(async move { active_expiration_task(store_gc).await; });

    let (tx, mut rx) = mpsc::channel::<LogMessage>(100_000);
    let (replica_tx, _) = tokio::sync::broadcast::channel::<(u64, String)>(4096);
    let (pubsub_tx, _) = tokio::sync::broadcast::channel::<(String, String)>(4096);
    let promote_flag = Arc::new(AtomicBool::new(false));
    let persistence = Arc::new(Persistence {
        log_tx: tx,
        write_count: AtomicUsize::new(0),
        compaction_pending: AtomicBool::new(false),
        replica_tx,
        promote_to_master: Arc::clone(&promote_flag),
        repl_offset: AtomicU64::new(0),
        backlog: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(BACKLOG_CAPACITY)),
        next_replica_id: AtomicU64::new(0),
        replica_status: std::sync::Mutex::new(std::collections::HashMap::new()),
        pubsub_tx,
        next_subscriber_id: AtomicU64::new(0),
        subscriptions: std::sync::Mutex::new(std::collections::HashMap::new()),
    });

    let store_worker = Arc::clone(&store);
    let persistence_worker = Arc::clone(&persistence);

    const BINLOG_PATH: &str = "onyx.binlog";
    let binlog_shared: Arc<std::sync::Mutex<File>> =
        Arc::new(std::sync::Mutex::new(open_binlog_file(BINLOG_PATH)));

    // Task periodico di fsync: solo se la policy e' "everysec" (il default,
    // come in Redis). Ogni secondo forza la scrittura fisica su disco del
    // binlog corrente, indipendentemente da quanto e' stato scritto nel
    // frattempo — se non c'e' nulla di nuovo l'fsync e' comunque economico.
    if fsync_policy() == FsyncPolicy::EverySec {
        let binlog_fsync = Arc::clone(&binlog_shared);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Ok(f) = binlog_fsync.lock() {
                    let _ = f.sync_data();
                }
            }
        });
    }

    let binlog_writer = Arc::clone(&binlog_shared);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                LogMessage::Append(first_record) => {
                    let mut batch = vec![first_record];
                    let mut compact_after = false;

                    while let Ok(next) = rx.try_recv() {
                        match next {
                            LogMessage::Append(r) => batch.push(r),
                            LogMessage::Compact => { compact_after = true; break; }
                        }
                    }

                    {
                        let mut f = binlog_writer.lock().unwrap();
                        for record in &batch {
                            let len = record.len() as u32;
                            let _ = f.write_all(&[
                                (len >> 24) as u8,
                                (len >> 16) as u8,
                                (len >> 8) as u8,
                                len as u8,
                            ]);
                            let _ = f.write_all(record);
                        }
                        let _ = f.flush();
                        // Con "always" ogni batch va fisicamente su disco prima
                        // di continuare: massima durabilita', al costo di più
                        // latenza per scrittura rispetto a "everysec"/"no".
                        if fsync_policy() == FsyncPolicy::Always {
                            let _ = f.sync_data();
                        }
                    }

                    if compact_after {
                        do_compact(&store_worker).await;
                        let _ = fs::remove_file(BINLOG_PATH);
                        let new_file = open_binlog_file(BINLOG_PATH);
                        *binlog_writer.lock().unwrap() = new_file;
                        persistence_worker.compaction_pending.store(false, Ordering::SeqCst);
                    }
                }
                LogMessage::Compact => {
                    do_compact(&store_worker).await;
                    let _ = fs::remove_file(BINLOG_PATH);
                    let new_file = open_binlog_file(BINLOG_PATH);
                    *binlog_writer.lock().unwrap() = new_file;
                    persistence_worker.compaction_pending.store(false, Ordering::SeqCst);
                }
            }
        }
    });

    if let Some(addr) = master_addr {
        IS_REPLICA.store(true, Ordering::Relaxed);
        if auto_failover {
            warn!(
                "--auto-failover attivo (timeout {}s): questa istanza si promuoverà da sola a Master \
                 se perde il contatto col Master per più del timeout. ATTENZIONE: sicuro solo con UNA \
                 sola Replica per Master — con più Repliche configurate tutte con --auto-failover, più \
                 di una potrebbe promuoversi in parallelo (split-brain), perché non c'è coordinamento \
                 tra Repliche in questa versione.",
                failover_timeout_secs
            );
        }
        let store_replica = Arc::clone(&store);
        let promote_flag_replica = Arc::clone(&promote_flag);
        tokio::spawn(async move {
            run_replica(addr, store_replica, promote_flag_replica, auto_failover, failover_timeout_secs).await;
        });
    }

    let mut port = "6380".to_string();
    for i in 0..args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            port = args[i + 1].clone();
        }
    }
    let bind_addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Server in ascolto su {}", bind_addr);
    let obp_port = port.parse::<u16>().unwrap_or(6380) + 1;
    let obp_addr = format!("127.0.0.1:{}", obp_port);
    let obp_listener = TcpListener::bind(&obp_addr).await?;
    info!("Server OBP (binario) in ascolto su {}", obp_addr);
    
    let store_obp = Arc::clone(&store);
    let persistence_obp = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            let (stream, _) = match obp_listener.accept().await {
                Ok(pair) => pair,
                Err(_) => continue,
            };
            let store_clone = Arc::clone(&store_obp);
            let persistence_clone = Arc::clone(&persistence_obp);
            tokio::spawn(async move { handle_obp_client(stream, store_clone, persistence_clone).await; });
        }
    });
    let metrics_port: u16 = port.parse::<u16>().unwrap_or(6380) + 1000;
    let store_metrics = Arc::clone(&store);
    let persistence_metrics = Arc::clone(&persistence);
    tokio::spawn(async move { run_metrics_server(store_metrics, persistence_metrics, metrics_port).await; });

    let store_shutdown = Arc::clone(&store);
    let persistence_shutdown = Arc::clone(&persistence);

    tokio::select! {
        result = async {
            loop {
                let (stream, _) = listener.accept().await?;
                let store_clone = Arc::clone(&store);
                let persistence_clone = Arc::clone(&persistence);
                tokio::spawn(async move { handle_client(stream, store_clone, persistence_clone).await; });
            }
            #[allow(unreachable_code)]
            Ok::<(), std::io::Error>(())
        } => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Segnale di chiusura ricevuto, salvataggio finale in corso...");
            do_compact(&store_shutdown).await;
            persistence_shutdown.write_count.store(0, Ordering::SeqCst);
            info!("Salvataggio completato, arrivederci!");
        }
    }

    Ok(())
}

async fn run_replica(
    master_addr: String,
    store: Arc<ShardedStore>,
    promote_flag: Arc<AtomicBool>,
    auto_failover: bool,
    failover_timeout_secs: u64,
) {
    const MIN_BACKOFF_SECS: u64 = 1;
    const MAX_BACKOFF_SECS: u64 = 30;
    let mut backoff_secs = MIN_BACKOFF_SECS;

    // Offset dell'ultimo comando live applicato con successo. Sopravvive
    // alle riconnessioni: è quello che permette il resync parziale invece
    // di un dump completo ogni volta che la connessione col Master cade.
    let local_offset = Arc::new(AtomicU64::new(0));
    // Replication ID dell'ultimo Master a cui ci siamo sincronizzati con
    // successo. 0 = sconosciuto (prima connessione, o dopo un dump completo
    // di cui non abbiamo ancora ricevuto il marker). Sopravvive alle
    // riconnessioni nello stesso avvio della Replica.
    let local_replid = Arc::new(AtomicU64::new(0));

    // Da quando il Master è irraggiungibile senza interruzioni. None finché
    // siamo connessi correttamente; si azzera appena la connessione torna a
    // funzionare. Usato solo se --auto-failover è attivo.
    let mut unreachable_since: Option<std::time::Instant> = None;

    // Se il Master resta irraggiungibile più del timeout configurato,
    // promuove questa istanza a Master da sola. Ritorna true se ha
    // promosso (il chiamante deve fermarsi subito dopo).
    let maybe_self_promote = |unreachable_since: &Option<std::time::Instant>| -> bool {
        if !auto_failover {
            return false;
        }
        match unreachable_since {
            Some(since) if since.elapsed().as_secs() >= failover_timeout_secs => {
                warn!(
                    "Master irraggiungibile da oltre {}s: auto-promozione a Master (--auto-failover)",
                    failover_timeout_secs
                );
                promote_flag.store(true, Ordering::Relaxed);
                IS_REPLICA.store(false, Ordering::Relaxed);
                true
            }
            _ => false,
        }
    };

    loop {
        if promote_flag.load(Ordering::Relaxed) {
            info!("Promozione a Master completata, interrotta la connessione col vecchio Master");
            return;
        }
        info!("Connessione al Master {}...", master_addr);

        match TcpStream::connect(&master_addr).await {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let (reader, mut writer) = stream.into_split();
                let mut buf_reader = TokioBufReader::with_capacity(65536, reader);

                let starting_offset = local_offset.load(Ordering::SeqCst);
                let known_replid = local_replid.load(Ordering::SeqCst);
                let sync_cmd = format!("SYNC {} {}\n", known_replid, starting_offset);
                if writer.write_all(sync_cmd.as_bytes()).await.is_err() {
                    warn!("Impossibile inviare SYNC al Master, riprovo tra {}s", backoff_secs);
                    unreachable_since.get_or_insert_with(std::time::Instant::now);
                    if maybe_self_promote(&unreachable_since) { return; }
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }

                let mut scratch = String::new();

                // Prima riga di risposta: +FULLRESYNC <offset> o +CONTINUE
                // <offset>. Se manca o è malformata, meglio riprovare da capo
                // che procedere alla cieca.
                let handshake_ok = match read_command(&mut buf_reader, &mut scratch).await {
                    Ok(Some(marker)) if !marker.is_empty()
                        && (marker[0] == "+FULLRESYNC" || marker[0] == "+CONTINUE") =>
                    {
                        let is_full = marker[0] == "+FULLRESYNC";
                        if let Some(replid) = marker.get(1).and_then(|s| s.parse::<u64>().ok()) {
                            local_replid.store(replid, Ordering::SeqCst);
                        }
                        if is_full {
                            info!("Master ha risposto con dump completo");
                        } else {
                            info!("Master ha risposto con resync parziale (offset richiesto: {})", starting_offset);
                        }
                        true
                    }
                    _ => false,
                };
                if !handshake_ok {
                    warn!("Risposta inattesa dal Master al SYNC, riprovo tra {}s", backoff_secs);
                    unreachable_since.get_or_insert_with(std::time::Instant::now);
                    if maybe_self_promote(&unreachable_since) { return; }
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }

                info!("Connesso al Master, ricezione dati in corso...");
                backoff_secs = MIN_BACKOFF_SECS;
                unreachable_since = None; // connessione riuscita: azzera il timer del failover

                // Task separato che manda periodicamente REPLCONF ACK
                // <offset>, così il Master può monitorare quanto siamo
                // indietro (lag). Possiede la metà "scrittura" della
                // connessione da qui in poi.
                let ack_offset = Arc::clone(&local_offset);
                let ack_task = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let off = ack_offset.load(Ordering::SeqCst);
                        let ack_cmd = format!("REPLCONF ACK {}\n", off);
                        if writer.write_all(ack_cmd.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
                let mut receiving_initial_sync = true;

                loop {
                    if promote_flag.load(Ordering::Relaxed) {
                        info!("Promozione a Master completata, interrotta la connessione col vecchio Master");
                        ack_task.abort();
                        return;
                    }
                    match read_command(&mut buf_reader, &mut scratch).await {
                        Ok(Some(args)) if !args.is_empty() => {
                            if receiving_initial_sync && args[0] == "+SYNCDONE" {
                                if let Some(off) = args.get(1).and_then(|s| s.parse::<u64>().ok()) {
                                    local_offset.store(off, Ordering::SeqCst);
                                }
                                receiving_initial_sync = false;
                                continue;
                            }
                            execute_command(&store, &args);
                            if !receiving_initial_sync {
                                local_offset.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            warn!("Master disconnesso, riprovo tra {}s", backoff_secs);
                            unreachable_since.get_or_insert_with(std::time::Instant::now);
                            break;
                        }
                        Err(_) => {
                            warn!("Errore di lettura dal Master, riprovo tra {}s", backoff_secs);
                            unreachable_since.get_or_insert_with(std::time::Instant::now);
                            break;
                        }
                    }
                }
                ack_task.abort();
                if maybe_self_promote(&unreachable_since) { return; }
            }
            Err(_) => {
                warn!("Master non raggiungibile, riprovo tra {}s", backoff_secs);
                unreachable_since.get_or_insert_with(std::time::Instant::now);
                if maybe_self_promote(&unreachable_since) { return; }
            }
        }

        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let store = ShardedStore::new();
        store.set("chiave1".to_string(), "valore1".to_string());
        assert_eq!(store.get("chiave1"), Some("valore1".to_string()));
    }

    #[test]
    fn test_get_chiave_inesistente() {
        let store = ShardedStore::new();
        assert_eq!(store.get("non_esiste"), None);
    }

    #[test]
    fn test_incr_da_zero() {
        let store = ShardedStore::new();
        assert_eq!(store.incr("contatore"), 1);
        assert_eq!(store.incr("contatore"), 2);
    }

    #[test]
    fn test_incrby() {
        let store = ShardedStore::new();
        assert_eq!(store.incrby("c", 5), 5);
        assert_eq!(store.incrby("c", -2), 3);
    }

    #[test]
    fn test_delete() {
        let store = ShardedStore::new();
        store.set("k".to_string(), "v".to_string());
        assert_eq!(store.delete("k"), true);
        assert_eq!(store.delete("k"), false);
        assert_eq!(store.get("k"), None);
    }

    #[test]
    fn test_lpush_e_lrange() {
        let store = ShardedStore::new();
        store.lpush("lista", "uno".to_string());
        store.lpush("lista", "due".to_string());
        assert_eq!(store.lrange("lista", 0, -1), Some(vec!["due".to_string(), "uno".to_string()]));
    }

    #[test]
    fn test_hash() {
        let store = ShardedStore::new();
        store.hset("h", "campo", "valore");
        assert_eq!(store.hget("h", "campo"), Some("valore".to_string()));
        assert_eq!(store.hget("h", "non_esiste"), None);
    }

    #[test]
    fn test_set_type() {
        let store = ShardedStore::new();
        assert_eq!(store.sadd("s", "a"), true);
        assert_eq!(store.sadd("s", "a"), false);
        assert_eq!(store.sismember("s", "a"), true);
        assert_eq!(store.sismember("s", "b"), false);
    }

    #[test]
    fn test_ttl_scadenza() {
        let store = ShardedStore::new();
        store.set("temp".to_string(), "val".to_string());
        assert_eq!(store.ttl("temp"), -1);
    }

    #[test]
    fn test_rename() {
        let store = ShardedStore::new();
        store.set("vecchio".to_string(), "dato".to_string());
        assert_eq!(store.rename("vecchio", "nuovo"), true);
        assert_eq!(store.get("vecchio"), None);
        assert_eq!(store.get("nuovo"), Some("dato".to_string()));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("utente:*", "utente:42"));
        assert!(glob_match("*", "qualsiasi"));
        assert!(!glob_match("utente:*", "prodotto:1"));
        assert!(glob_match("esatto", "esatto"));
        assert!(!glob_match("esatto", "diverso"));
    }

    #[test]
    fn test_append() {
        let store = ShardedStore::new();
        store.append("s", "ciao");
        store.append("s", "mondo");
        assert_eq!(store.get("s"), Some("ciaomondo".to_string()));
    }

    #[test]
    fn test_strlen() {
        let store = ShardedStore::new();
        store.set("s".to_string(), "ciao".to_string());
        assert_eq!(store.strlen("s"), 4);
        assert_eq!(store.strlen("non_esiste"), 0);
    } 
// ============================================================
    // Round-trip binlog binario: ogni comando che scrive deve sopravvivere
    // intatto a command_to_binary_record -> binary_record_to_args.
    // ============================================================

    #[test]
    fn test_binlog_roundtrip_set() {
        let args = vec!["SET".to_string(), "k".to_string(), "v".to_string()];
        let record = command_to_binary_record("SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["SET", "k", "v"]);
    }

    #[test]
    fn test_binlog_roundtrip_set_con_scadenza() {
        let args = vec!["SET".to_string(), "k".to_string(), "v".to_string(), "EXAT".to_string(), "9999999999".to_string()];
        let record = command_to_binary_record("SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["SET", "k", "v", "EXAT", "9999999999"]);
    }

    #[test]
    fn test_binlog_roundtrip_set_senza_scadenza_non_include_exat() {
        // Un SET senza scadenza non deve risorgere con un EXAT fantasma.
        let args = vec!["SET".to_string(), "k".to_string(), "v".to_string()];
        let record = command_to_binary_record("SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn test_binlog_roundtrip_mset() {
        let args = vec!["MSET".to_string(), "a".to_string(), "1".to_string(), "b".to_string(), "2".to_string()];
        let record = command_to_binary_record("MSET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["MSET", "a", "1", "b", "2"]);
    }

    #[test]
    fn test_binlog_roundtrip_del() {
        let args = vec!["DEL".to_string(), "chiave".to_string()];
        let record = command_to_binary_record("DEL", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["DEL", "chiave"]);
    }

    #[test]
    fn test_binlog_roundtrip_expire_diventa_expireat() {
        // EXPIRE (relativo) va persistito come EXPIREAT (assoluto): il
        // record binario stesso è già in forma assoluta.
        let args = vec!["EXPIRE".to_string(), "k".to_string(), "12345".to_string()];
        let record = command_to_binary_record("EXPIRE", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["EXPIREAT", "k", "12345"]);
    }

    #[test]
    fn test_binlog_roundtrip_lpush_rpush() {
        let lpush = vec!["LPUSH".to_string(), "lista".to_string(), "x".to_string()];
        let record = command_to_binary_record("LPUSH", &lpush, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["LPUSH", "lista", "x"]);

        let rpush = vec!["RPUSH".to_string(), "lista".to_string(), "y".to_string()];
        let record = command_to_binary_record("RPUSH", &rpush, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["RPUSH", "lista", "y"]);
    }

    #[test]
    fn test_binlog_roundtrip_lpop_rpop() {
        let args = vec!["LPOP".to_string(), "lista".to_string()];
        let record = command_to_binary_record("LPOP", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["LPOP", "lista"]);
    }

    #[test]
    fn test_binlog_roundtrip_hset() {
        let args = vec!["HSET".to_string(), "h".to_string(), "campo".to_string(), "valore".to_string()];
        let record = command_to_binary_record("HSET", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["HSET", "h", "campo", "valore"]);
    }

    #[test]
    fn test_binlog_roundtrip_sadd_srem() {
        let sadd = vec!["SADD".to_string(), "s".to_string(), "membro".to_string()];
        let record = command_to_binary_record("SADD", &sadd, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["SADD", "s", "membro"]);

        let srem = vec!["SREM".to_string(), "s".to_string(), "membro".to_string()];
        let record = command_to_binary_record("SREM", &srem, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["SREM", "s", "membro"]);
    }

    #[test]
    fn test_binlog_roundtrip_rename() {
        let args = vec!["RENAME".to_string(), "vecchia".to_string(), "nuova".to_string()];
        let record = command_to_binary_record("RENAME", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["RENAME", "vecchia", "nuova"]);
    }

    #[test]
    fn test_binlog_roundtrip_incrby_decrby() {
        let incr = vec!["INCRBY".to_string(), "c".to_string(), "7".to_string()];
        let record = command_to_binary_record("INCRBY", &incr, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["INCRBY", "c", "7"]);
        let decr = vec!["DECRBY".to_string(), "c".to_string(), "3".to_string()];
        let record = command_to_binary_record("DECRBY", &decr, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["DECRBY", "c", "3"]);
    }

    #[test]
    fn test_binlog_roundtrip_append() {
        let args = vec!["APPEND".to_string(), "s".to_string(), "suffisso".to_string()];
        let record = command_to_binary_record("APPEND", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["APPEND", "s", "suffisso"]);
    }

    #[test]
    fn test_binlog_roundtrip_hdel() {
        let args = vec!["HDEL".to_string(), "h".to_string(), "campo".to_string()];
        let record = command_to_binary_record("HDEL", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["HDEL", "h", "campo"]);
    }

    #[test]
    fn test_binlog_roundtrip_copy() {
        let args = vec!["COPY".to_string(), "src".to_string(), "dst".to_string()];
        let record = command_to_binary_record("COPY", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["COPY", "src", "dst"]);
    }

    #[test]
    fn test_binlog_comando_sconosciuto_ritorna_none() {
        let args = vec!["PING".to_string()];
        assert!(command_to_binary_record("PING", &args, None).is_none());
    }

    #[test]
    fn test_binlog_argomenti_insufficienti_ritorna_none() {
        // Niente panic su un array troppo corto: solo None.
        let args = vec!["SET".to_string()];
        assert!(command_to_binary_record("SET", &args, None).is_none());
    }

    // ============================================================
    // Binlog corrotto: mai panic, solo None.
    // ============================================================

    #[test]
    fn test_record_vuoto_non_va_in_panic() {
        assert!(binary_record_to_args(&[]).is_none());
    }

    #[test]
    fn test_record_troncato_a_meta_chiave_non_va_in_panic() {
        // OP_SET, poi dichiara una chiave di 100 byte ma ne fornisce solo 2.
        let record = vec![OP_SET, 0x00, 0x64, b'a', b'b'];
        assert!(binary_record_to_args(&record).is_none());
    }

    #[test]
    fn test_record_troncato_a_meta_valore_non_va_in_panic() {
        // Chiave valida "k", poi dichiara un valore di 1000 byte inesistente.
        let mut record = vec![OP_SET];
        record.extend_from_slice(&[0x00, 0x01]); // key_len = 1
        record.push(b'k');
        record.push(1); // tipo stringa
        record.extend_from_slice(&[0x00, 0x00, 0x03, 0xE8]); // val_len = 1000, ma non seguono byte
        assert!(binary_record_to_args(&record).is_none());
    }

    #[test]
    fn test_read_u16_be_su_buffer_troncato() {
        let buf = [0x00u8]; // solo 1 byte, ne servono 2
        let mut offset = 0;
        assert_eq!(read_u16_be(&buf, &mut offset), None);
    }

    #[test]
    fn test_read_u64_be_su_buffer_troncato() {
        let buf = [0x00u8, 0x01, 0x02]; // solo 3 byte, ne servono 8
        let mut offset = 0;
        assert_eq!(read_u64_be(&buf, &mut offset), None);
    }

    #[test]
    fn test_safe_slice_oltre_i_limiti() {
        let buf = [1u8, 2, 3];
        assert!(safe_slice(&buf, 0, 10).is_none());
        assert!(safe_slice(&buf, 5, 1).is_none());
        assert_eq!(safe_slice(&buf, 0, 3), Some(&buf[..]));
    }

    // ============================================================
    // Round-trip formato snapshot testuale (value_to_line / line_to_entry)
    // ============================================================

    #[test]
    fn test_snapshot_roundtrip_stringa() {
        let entry = DataEntry {
            value: OnyxValue::Blob(Bytes::from("ciao")),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (key, decoded) = line_to_entry(&line).unwrap();
        assert_eq!(key, "k");
        match decoded.value {
            OnyxValue::Blob(b) => assert_eq!(b, Bytes::from("ciao")),
            _ => panic!("tipo sbagliato dopo il round-trip"),
        }
    }

    #[test]
    fn test_snapshot_roundtrip_con_scadenza() {
        let entry = DataEntry {
            value: OnyxValue::Blob(Bytes::from("v")),
            expires_at: Some(123456),
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        assert_eq!(decoded.expires_at, Some(123456));
    }

    #[test]
    fn test_snapshot_roundtrip_lista_vuota() {
        let entry = DataEntry {
            value: OnyxValue::List(vec![]),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        match decoded.value {
            OnyxValue::List(l) => assert!(l.is_empty()),
            _ => panic!("tipo sbagliato"),
        }
    }

    #[test]
    fn test_snapshot_roundtrip_hash() {
        let mut h = std::collections::HashMap::new();
        h.insert(Bytes::from("f1"), Bytes::from("v1"));
        let entry = DataEntry { value: OnyxValue::Hash(h), expires_at: None, created_at: 0, last_accessed: 0 };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        match decoded.value {
            OnyxValue::Hash(m) => assert_eq!(m.get(&Bytes::from("f1")), Some(&Bytes::from("v1"))),
            _ => panic!("tipo sbagliato"),
        }
    }

    #[test]
    fn test_snapshot_riga_malformata_ritorna_none() {
        assert!(line_to_entry("questa non e' una riga valida").is_none());
        assert!(line_to_entry("").is_none());
    }
    // ============================================================
    // Logica di resync: qui vive il regression test del bug che ha
    // scoperto Yousef (backlog vuoto dopo un riavvio del Master scambiato
    // per "già allineati").
    // ============================================================

    #[test]
    fn test_replid_stesso_id_permette_parziale() {
        assert!(replid_allows_partial(42, 42));
    }

    #[test]
    fn test_replid_diverso_richiede_full() {
        assert!(!replid_allows_partial(42, 99));
    }

    #[test]
    fn test_replid_sconosciuto_richiede_full() {
        // requested_replid = 0: la Replica non ha ancora mai visto un
        // replid valido (prima connessione in assoluto).
        assert!(!replid_allows_partial(0, 99));
    }

    #[test]
    fn test_partial_resync_backlog_copre_offset() {
        // Backlog parte da 5, richiesto offset 4 (il prossimo da mandare
        // sarebbe proprio il 5): nessun buco, parziale ammesso.
        assert!(partial_resync_possible(4, Some(5), 100));
    }

    #[test]
    fn test_partial_resync_gap_nel_backlog() {
        // Backlog parte da 20, ma si chiede di ripartire da 4: mancano i
        // comandi 5..19, non più recuperabili. Serve full resync.
        assert!(!partial_resync_possible(4, Some(20), 100));
    }

    #[test]
    fn test_partial_resync_backlog_vuoto_ma_davvero_allineati() {
        // Nessuna scrittura in backlog, ma l'offset richiesto combacia
        // esattamente con quello corrente del Master: qui è legittimo
        // concludere "non c'è nulla da rimandare".
        assert!(partial_resync_possible(9, None, 9));
    }

    #[test]
    fn test_partial_resync_backlog_vuoto_dopo_riavvio_master_e_il_bug_originale() {
        // Scenario esatto del bug: il Master è ripartito da zero
        // (repl_offset azzerato a 0, backlog vuoto perché nessuna
        // scrittura è ancora avvenuta nel nuovo processo), ma la Replica
        // si presenta con un offset "9" ereditato dal vecchio Master.
        // PRIMA del fix, backlog vuoto da solo bastava per concludere
        // "già allineati" — qui verifichiamo che NON sia più così: serve
        // che l'offset richiesto combaci con quello corrente (0), non con
        // un numero qualsiasi lasciato da un processo precedente.
        assert!(!partial_resync_possible(9, None, 0));
    }
    // ============================================================
    // JSON path: parser e navigazione
    // ============================================================

    #[test]
    fn test_parse_path_radice() {
        assert_eq!(parse_json_path("$"), Some(vec![]));
    }

    #[test]
    fn test_parse_path_campo_singolo() {
        assert_eq!(parse_json_path("$.nome"), Some(vec![JsonPathSegment::Field("nome".to_string())]));
    }

    #[test]
    fn test_parse_path_annidato() {
        assert_eq!(
            parse_json_path("$.indirizzo.città"),
            Some(vec![
                JsonPathSegment::Field("indirizzo".to_string()),
                JsonPathSegment::Field("città".to_string()),
            ])
        );
    }

    #[test]
    fn test_parse_path_indice_array() {
        assert_eq!(
            parse_json_path("$.tag[0]"),
            Some(vec![JsonPathSegment::Field("tag".to_string()), JsonPathSegment::Index(0)])
        );
    }

    #[test]
    fn test_parse_path_misto_lungo() {
        assert_eq!(
            parse_json_path("$.a[1].b[2]"),
            Some(vec![
                JsonPathSegment::Field("a".to_string()),
                JsonPathSegment::Index(1),
                JsonPathSegment::Field("b".to_string()),
                JsonPathSegment::Index(2),
            ])
        );
    }

    #[test]
    fn test_parse_path_senza_dollaro_iniziale_none() {
        assert_eq!(parse_json_path("nome"), None);
    }

    #[test]
    fn test_parse_path_doppio_punto_none() {
        assert_eq!(parse_json_path("$..nome"), None);
    }

    #[test]
    fn test_parse_path_parentesi_non_chiusa_none() {
        assert_eq!(parse_json_path("$.tag[0"), None);
    }

    #[test]
    fn test_parse_path_indice_non_numerico_none() {
        assert_eq!(parse_json_path("$.tag[x]"), None);
    }

    #[test]
    fn test_get_json_path_campo_esistente() {
        let val: serde_json::Value = serde_json::json!({"nome": "Yousef", "età": 18});
        let path = parse_json_path("$.nome").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("Yousef")));
    }

    #[test]
    fn test_get_json_path_annidato() {
        let val: serde_json::Value = serde_json::json!({"indirizzo": {"città": "Roma"}});
        let path = parse_json_path("$.indirizzo.città").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("Roma")));
    }

    #[test]
    fn test_get_json_path_campo_assente_none() {
        let val: serde_json::Value = serde_json::json!({"nome": "Yousef"});
        let path = parse_json_path("$.cognome").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_get_json_path_indice_array() {
        let val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[1]").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("rust")));
    }

    #[test]
    fn test_get_json_path_indice_fuori_range_none() {
        let val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[5]").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_get_json_path_tipo_sbagliato_none() {
        // Indice su un oggetto (non un array): non ha senso, deve dare None.
        let val: serde_json::Value = serde_json::json!({"nome": "Yousef"});
        let path = parse_json_path("$.nome[0]").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_set_json_path_documento_intero() {
        let mut val: serde_json::Value = serde_json::json!({"vecchio": true});
        let path = parse_json_path("$").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!({"nuovo": true})));
        assert_eq!(val, serde_json::json!({"nuovo": true}));
    }

    #[test]
    fn test_set_json_path_campo_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"nome": "Yousef"});
        let path = parse_json_path("$.nome").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("Ahmed")));
        assert_eq!(val, serde_json::json!({"nome": "Ahmed"}));
    }

    #[test]
    fn test_set_json_path_campo_nuovo_su_oggetto_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"nome": "Yousef"});
        let path = parse_json_path("$.età").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!(18)));
        assert_eq!(val, serde_json::json!({"nome": "Yousef", "età": 18}));
    }

    #[test]
    fn test_set_json_path_genitore_assente_fallisce() {
        // $.a.b.c ma "a" non esiste: niente auto-creazione, deve fallire.
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.a.b.c").unwrap();
        assert!(!set_json_path(&mut val, &path, serde_json::json!(1)));
    }

    #[test]
    fn test_set_json_path_indice_array_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[0]").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("go")));
        assert_eq!(val, serde_json::json!({"tag": ["go", "rust"]}));
    }

    #[test]
    fn test_set_json_path_append_in_coda_array() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[1]").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("rust")));
        assert_eq!(val, serde_json::json!({"tag": ["dev", "rust"]}));
    }

    #[test]
    fn test_set_json_path_indice_troppo_avanti_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[5]").unwrap();
        assert!(!set_json_path(&mut val, &path, serde_json::json!("x")));
    }

    #[test]
    fn test_delete_json_path_campo() {
        let mut val: serde_json::Value = serde_json::json!({"nome": "Yousef", "età": 18});
        let path = parse_json_path("$.età").unwrap();
        assert!(delete_json_path(&mut val, &path));
        assert_eq!(val, serde_json::json!({"nome": "Yousef"}));
    }

    #[test]
    fn test_delete_json_path_indice_array() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[0]").unwrap();
        assert!(delete_json_path(&mut val, &path));
        assert_eq!(val, serde_json::json!({"tag": ["rust"]}));
    }

    #[test]
    fn test_delete_json_path_campo_assente_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({"nome": "Yousef"});
        let path = parse_json_path("$.cognome").unwrap();
        assert!(!delete_json_path(&mut val, &path));
    }

    #[test]
    fn test_delete_json_path_radice_fallisce() {
        // DEL su "$" (documento intero) non passa da qui, va gestito
        // separatamente con un DEL normale sulla chiave.
        let mut val: serde_json::Value = serde_json::json!({"nome": "Yousef"});
        let path = parse_json_path("$").unwrap();
        assert!(!delete_json_path(&mut val, &path));
    }
}