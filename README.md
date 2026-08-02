# OnyxDB

Database in-memory chiave-valore, scritto in Rust, ispirato a Redis.
Multi-threaded, con protocollo di rete RESP (compatibile Redis a livello di
formato) e un secondo protocollo binario proprietario (OBP) in parallelo.

## Stato del progetto

Prototipo funzionante, in sviluppo attivo. **Non pronto per produzione.**
Mancano ancora: cluster/sharding distribuito, scripting, TLS nativo, ACL
granulari per comando. Pub/Sub, replica con resync parziale, failover
automatico opt-in ed eviction della memoria sono già presenti.
Vedi "Limitazioni note" in fondo per l'elenco completo.

## Come avviare

Dalla cartella del progetto:

    cargo run --release

Il server RESP parte in ascolto su `127.0.0.1:6380`. Vengono avviati in
automatico anche un listener per il protocollo binario OBP (porta 6381,
cioè porta+1) e un endpoint metriche Prometheus (porta 7380, cioè porta+1000).

## Come usarlo

In un altro terminale, usa il client incluso:

    cargo run --release --bin onyx-cli

Oppure lancia il benchmark:

    cargo run --release --bin onyx-bench

## Comandi supportati

**Stringhe e contatori**
- `SET chiave valore [EX secondi | PX millisecondi] [NX | XX]` (`EX`/`PX`
  impostano una scadenza; `NX` scrive solo se la chiave non esiste, `XX`
  solo se esiste già — combinabili, es. `SET lock 1 NX EX 30` per un lock
  con scadenza in un solo comando atomico)
- `GET chiave`
- `DEL chiave`
- `INCR chiave`
- `INCRBY chiave numero`
- `DECRBY chiave numero`
- `APPEND chiave testo`
- `STRLEN chiave`
- `GETSET chiave nuovo_valore` (scrive e ritorna il vecchio valore)
- `MSET chiave1 valore1 chiave2 valore2 ...`
- `MGET chiave1 chiave2 ...`
- `SETNX chiave valore` (scrive solo se la chiave non esiste già — utile per lock)

**Liste**
- `LPUSH chiave elemento` (inserisce in testa)
- `RPUSH chiave elemento` (inserisce in coda)
- `LPOP chiave` (rimuove e ritorna il primo elemento, nil se la lista è vuota/assente)
- `RPOP chiave` (rimuove e ritorna l'ultimo elemento)
- `LRANGE chiave [start stop]` (indici 0-based inclusivi, negativi contano
  dalla fine come in Redis — `-1` è l'ultimo elemento; senza argomenti,
  equivale a `LRANGE chiave 0 -1`, cioè tutta la lista)
- `LLEN chiave`

**Hash (oggetti campo->valore)**
- `HSET chiave campo valore`
- `HGET chiave campo`
- `HGETALL chiave`
- `HDEL chiave campo`
- `HKEYS chiave` (solo i nomi dei campi)
- `HVALS chiave` (solo i valori)

**Set (elementi unici)**
- `SADD chiave elemento`
- `SMEMBERS chiave`
- `SREM chiave elemento`
- `SISMEMBER chiave elemento`

**Scadenza (TTL)**
- `EXPIRE chiave secondi [NX|XX]` (NX: solo se non ha già scadenza, XX: solo se ne ha già una)
- `EXPIREAT chiave timestamp_unix`
- `TTL chiave`

**Utility**
- `EXISTS chiave`
- `TYPE chiave`
- `RENAME vecchia_chiave nuova_chiave`
- `KEYS pattern` (supporta `*` come jolly, es. `KEYS utente:*`)
- `COPY chiave_sorgente chiave_destinazione` (duplica una chiave)
- `AUTH password` oppure `AUTH utente password` (autenticazione, se il server è protetto — vedi sezione "Sicurezza")
- `PING`
- `SAVE` (forza il salvataggio su disco)
- `INFO` (ruolo, uptime, numero di chiavi, offset di replicazione, memoria usata, ecc.)
- `SYNC` (comando interno usato dalle Replica per sincronizzarsi — non pensato per uso manuale)

**Pub/Sub**
- `PUBLISH canale messaggio` — pubblica un messaggio, ritorna il numero di
  subscriber attualmente iscritti a quel canale.
- `SUBSCRIBE canale [canale2 ...]` — mette la connessione in modalità
  pub/sub: da quel momento riceve solo i messaggi dei canali sottoscritti
  (più conferme di `SUBSCRIBE`/`UNSUBSCRIBE`), finché non si disconnette.
- `UNSUBSCRIBE [canale ...]` — annulla l'iscrizione (senza argomenti,
  annulla tutte). Va mandato mentre si è già in modalità pub/sub.
- I messaggi pub/sub sono effimeri: non vengono scritti sul binlog né
  replicati alle Replica. In modalità pub/sub, altri comandi oltre a
  `SUBSCRIBE`/`UNSUBSCRIBE` vengono ignorati (limitazione nota, come nella
  sottoscrizione RESP2 di base di Redis).

**Nota sui nomi dei comandi**: non sono case-sensitive — `set`, `SET` e
`SeT` funzionano tutti allo stesso modo (il nome del comando viene
normalizzato in maiuscolo appena letto).

**Transazioni**
- `MULTI` / `EXEC` / `DISCARD`: raggruppano più comandi in un'unica
  operazione atomica; nessun altro client può intromettersi tra i comandi
  di una transazione in corso. Su una Replica, i comandi di scrittura dentro
  un `EXEC` vengono rifiutati uno per uno con `READONLY` (vedi "Replica"),
  esattamente come fuori da una transazione.

## Architettura in breve

- **Storage**: motore proprietario `OnyxEngine`, sharded manualmente su
  `NUM_SHARDS = 64` shard indipendenti. Ogni shard è una `HashMap` protetta
  da un proprio `Mutex`: la contesa è limitata alle chiavi che finiscono
  sullo stesso shard (hashing FNV-1a + bitmask), non è un lock globale.
  Le operazioni cross-shard (oggi solo `RENAME`) bloccano i due shard
  coinvolti sempre nello stesso ordine (indice crescente) per evitare
  deadlock tra operazioni concorrenti che si incrociano.
- **Rete**: async con Tokio. Due protocolli in parallelo sullo stesso
  processo:
  - **RESP** (porta principale, es. 6380) — stesso formato binario di Redis,
    usato da client, replica e benchmark.
  - **OBP** (Onyx Binary Protocol, porta principale + 1, es. 6381) —
    protocollo binario proprietario più compatto, ancora minimale
    (GET/SET/DEL/PING). *Le scritture via OBP non sono ancora collegate a
    persistenza e replica* (vedi Limitazioni note).
- **Persistenza**: snapshot periodico compresso gzip su `onyx.snapshot` +
  log binario delle scritture su `onyx.binlog` (formato: lunghezza + record
  binario, non testo), con compattazione automatica ogni N scritture
  (soglia in `COMPACTION_THRESHOLD`, dentro `main.rs`). Politica di
  `fsync` configurabile con `--appendfsync always|everysec|no` (default
  `everysec`, come Redis) — vedi sezione "Durabilità" più sotto. Il file
  `onyx.log` presente nel progetto è un residuo della vecchia persistenza
  testuale e oggi non viene più scritto.
- **TTL**: scadenza pigra (controllata alla lettura) + pulizia attiva ogni
  10 secondi in background.
- **Allocatore**: `mimalloc` al posto di quello di default, per ridurre
  l'overhead delle allocazioni frequenti.

## File del progetto

- `src/main.rs` — il server (comandi RESP, persistenza, replica, metriche)
- `src/engine.rs` — il motore di storage sharded (`OnyxEngine`)
- `src/protocol.rs` — encoder/decoder del protocollo binario OBP
- `src/resp.rs` — encoder/decoder del protocollo RESP
- `src/onyx-cli.rs` — client a riga di comando
- `src/onyx-bench.rs` — strumento di benchmark (modalità sincrona e pipeline)
- `src/storage.rs` — attualmente vuoto, riservato per refactoring futuro

## Performance (ultimo self-benchmark)

- ~54.000 ops/sec in modalità sincrona (un comando alla volta)
- ~467.000-484.000 ops/sec in modalità pipeline (comandi in batch)
- Testato su hardware a 4 core, punto ottimale di concorrenza intorno a
  50 connessioni simultanee, con `src/onyx-bench.rs`

**Nota metodologica**: questi numeri vengono dal benchmark incluso nel
progetto stesso (stessa macchina, stesso processo, nessun confronto diretto
con altri database sullo stesso hardware/condizioni). Sono indicativi, non
un claim comparativo verificato — per un confronto onesto serve un
benchmark standardizzato contro Redis vero (in programma, vedi "Prossimi
passi").

## Replica (Master → Replica)

OnyxDB supporta la replica in tempo reale, con **resync parziale**: una
Replica che si riconnette dopo una disconnessione breve riceve solo i
comandi mancanti invece di un dump completo, se il Master li ha ancora nel
backlog.

**Avviare un Master** (comportamento di default, nessun flag necessario):

    cargo run --release

**Avviare una Replica** (su una porta diversa, per girare sulla stessa macchina):

    cargo run --release -- --replica-of 127.0.0.1:6380 --port 6390

**Attenzione alle porte**: ogni istanza apre anche OBP su porta+1 e le
metriche su porta+1000. Il Master su 6380 occupa quindi anche 6381 (OBP) e
7380 (metriche) — per questo l'esempio sopra usa 6390 per la Replica e non
6381, che altrimenti collide con l'OBP del Master sulla stessa macchina.

### Come funziona il sync

- Il Master tiene un **offset di replicazione**: un contatore che cresce di
  1 a ogni comando di scrittura (non è byte-accurato come in Redis, è un
  conteggio di comandi — scelta voluta per semplicità).
- Il Master tiene anche un **backlog**: gli ultimi `BACKLOG_CAPACITY`
  comandi (10.000 di default, in `main.rs`), con il loro offset.
- Quando una Replica si connette manda `SYNC <ultimo_offset_noto>`
  (`SYNC 0` per una prima connessione, senza dati pregressi).
- Il Master risponde con:
  - `+CONTINUE <offset>` + solo i comandi mancanti, se il backlog copre
    ancora tutto da quell'offset in poi (**resync parziale**);
  - `+FULLRESYNC <offset>` + dump completo di tutte le chiavi, se il
    backlog non arriva più così indietro (**resync completo**, il
    comportamento di prima).
- Un marcatore `+SYNCDONE <offset>` segna la fine della sincronizzazione
  iniziale; da lì in poi la Replica riceve lo streaming live.
- La Replica manda `REPLCONF ACK <offset>` al Master una volta al secondo,
  così il Master sa quanto è indietro ciascuna Replica (lag).

Se il Master si disconnette, la Replica riprova automaticamente con backoff
esponenziale, ricordandosi l'ultimo offset applicato — così alla
riconnessione tenta prima un resync parziale invece di ripartire da zero.

Una Replica non legge né scrive persistenza locale (`onyx.snapshot`/`onyx.binlog`):
i suoi dati vivono solo in RAM e vengono sempre ricostruiti tramite
sincronizzazione col Master a ogni avvio.

**Monitoraggio**: `INFO` sul Master mostra `master_repl_offset`,
`connected_replicas`, `max_replica_lag`, e una riga per Replica connessa
(`slave0:addr=...,offset=...,lag=...,last_ack_secs_ago=...`). Le stesse
informazioni (in forma aggregata) sono anche su Prometheus:
`onyxdb_replication_offset`, `onyxdb_connected_replicas`,
`onyxdb_max_replica_lag`.

**Promozione manuale a Master** (in caso di failover): connettendosi alla
Replica e mandando `REPLICAOF NO ONE`, la Replica mantiene tutti i dati
ricevuti fino a quel momento e inizia ad accettare nuove scritture come un
Master normale.

**Promozione automatica** (opt-in, `--auto-failover`):

    cargo run --release -- --replica-of 127.0.0.1:6380 --port 6390 --auto-failover --failover-timeout 30

Se il Master resta irraggiungibile per più di `--failover-timeout` secondi
(default 30), la Replica si promuove da sola a Master, con lo stesso
effetto di `REPLICAOF NO ONE`.

**Attenzione, rischio di split-brain**: questa promozione automatica è
sicura solo con **una sola Replica** per Master. Se hai più Replica dello
stesso Master tutte con `--auto-failover` attivo, e il Master cade, **più
di una potrebbe promuoversi in parallelo**, perché in questa versione non
c'è nessun coordinamento tra Replica (niente quorum/voto, a differenza di
Redis Sentinel o di un vero Raft) — risultato: due "Master" che accettano
scritture diverse contemporaneamente, dati che divergono. Con una sola
Replica configurata, questo rischio non esiste (non c'è nessun'altra
istanza con cui entrare in conflitto). Il server stampa un avviso esplicito
all'avvio quando `--auto-failover` è attivo, apposta per non farlo passare
inosservato.

**Le scritture dirette su una Replica vengono rifiutate** con `READONLY`
(anche dentro `MULTI`/`EXEC`): i suoi dati devono arrivare solo dal Master,
altrimenti divergerebbero silenziosamente al primo comando replicato
successivo che li sovrascrive.

**Limitazioni attuali**: la promozione automatica non ha coordinamento
multi-Replica (vedi sopra). Nessun clustering/sharding distribuito tra più
processi/macchine (lo sharding attuale è interno a un singolo processo).
L'offset è un conteggio di comandi, non di byte: due Master con lo stesso
numero di comandi ma "pesi" diversi non sono direttamente confrontabili
come lo sarebbero con un offset byte-accurato.

## Client con routing automatico

`onyx-cli` supporta routing automatico delle letture verso le Repliche,
lasciando le scritture sempre al Master:

    cargo run --release --bin onyx-cli -- --master 127.0.0.1:6380 --replicas 127.0.0.1:6390,127.0.0.1:6391

I comandi di sola lettura (GET, MGET, LRANGE, HGET, HGETALL, SMEMBERS,
SISMEMBER, EXISTS, TYPE, TTL, KEYS, STRLEN, LLEN) vengono distribuiti a
rotazione tra le Repliche indicate. Tutto il resto va sempre al Master.

## Durabilità

Il binlog viene scritto e "flushato" (buffer userspace svuotato verso il
sistema operativo) a ogni batch di comandi. Quanto spesso quei dati vengono
poi forzati fisicamente su disco (`fsync`) dipende dalla policy scelta con
`--appendfsync`:

- `always` — fsync dopo ogni batch di scritture. Massima durabilità (perdi
  al più l'ultimo batch in corso in caso di crash), ma più latenza per
  comando, perché ogni scrittura aspetta il disco.
- `everysec` (default) — un task in background forza l'fsync una volta al
  secondo, indipendentemente dal traffico. In caso di crash improvviso del
  sistema operativo o dell'hardware puoi perdere fino a ~1 secondo di
  scritture recenti; un crash del solo processo OnyxDB invece non perde
  nulla, perché i dati sono già nei buffer del sistema operativo dopo il
  flush. È lo stesso compromesso di default che usa Redis.
- `no` — nessun fsync esplicito, solo il flush dei buffer userspace. Il più
  veloce, ma la durabilità dipende interamente da quando il sistema
  operativo decide di scrivere i suoi buffer su disco per conto suo.

Esempio:

    cargo run --release -- --appendfsync always

## Robustezza

- Limite configurabile sul numero massimo di chiavi (`MAX_KEYS` in `main.rs`,
  default 1.000.000): oltre la soglia, le scritture che creerebbero nuove
  chiavi vengono rifiutate con un errore chiaro, mentre le operazioni su
  chiavi esistenti restano possibili.
- Spegnimento sicuro: `Ctrl+C` esegue un salvataggio finale su disco prima
  di terminare il processo.
- Gli errori di I/O su disco (file bloccato, permessi, disco pieno) non
  causano il crash del server: il sistema ritenta automaticamente ogni
  3 secondi, restando comunque operativo in memoria nel frattempo.
- Logging strutturato con `tracing` (livelli INFO/WARN/ERROR, timestamp).
- Riconnessione delle Repliche con backoff esponenziale (1s → 2s → 4s → ... fino a un tetto di 30s).
- Snapshot su disco compresso con gzip, per ridurre lo spazio occupato.
- `RENAME` cross-shard con ordine di lock deterministico, per evitare
  deadlock tra rename concorrenti sugli stessi due shard.
- Recovery a prova di crash: se il binlog è troncato (processo terminato a
  metà scrittura) o un record è corrotto, il caricamento all'avvio scarta
  solo quel record/quella coda e continua, invece di andare in panic —
  con un `WARN` nei log a dirti quanto è stato scartato.
- Limite di memoria opzionale con eviction stile Redis (`--maxmemory`),
  vedi sezione "Memoria (eviction)" più sotto.

## Sicurezza

Il server può richiedere autenticazione tramite password. Modo semplice
(un solo utente, chiamato `default`):

    $env:ONYXDB_PASSWORD = "una-password-lunga-e-casuale"
    cargo run --release

Oppure con un flag esplicito (meno sicuro, la password resta nella cronologia
del terminale):

    cargo run --release -- --requirepass la-tua-password

**Utenti multipli** (ripeti `--user` una volta per utente):

    cargo run --release -- --user alice:passwordA --user bob:passwordB

Ogni client si autentica con `AUTH password` (assume l'utente `default`,
solo se configurato con `--requirepass`/`ONYXDB_PASSWORD`) oppure
`AUTH utente password` (per un utente specifico). Senza nessuna delle due
configurazioni, il server resta aperto come prima (nessuna regressione per
chi non ne ha bisogno).

**Nota**: non ci sono permessi granulari per comando (niente `ACL SETUSER
... +get -set` come in Redis) — un utente autenticato può fare tutto. È un
controllo "chi entra", non "chi può fare cosa". L'autenticazione copre solo
il protocollo RESP: il listener OBP (porta+1) non richiede AUTH.

**TLS**: non incluso di serie. Il modo più solido per aggiungerlo è mettere
OnyxDB dietro un proxy TLS-terminating (es. `stunnel`, `nginx stream`, o un
sidecar) invece di implementare TLS a mano nel codice — è la stessa strada
che raccomanda anche Redis stesso per i casi semplici. Se invece vuoi TLS
nativo nel processo, la libreria giusta in Rust è `tokio-rustls`; è
un'aggiunta che tocca l'accept-loop di `main()` e richiede una dipendenza
nuova in `Cargo.toml` che va compilata e testata sulla tua macchina.

## Memoria (eviction)

Per default non c'è limite di memoria (solo il tetto rigido su `MAX_KEYS`,
vedi sopra). Puoi configurarne uno stile Redis:

    cargo run --release -- --maxmemory 512mb --maxmemory-policy allkeys-lru

Policy disponibili:
- `noeviction` (default se `--maxmemory` è impostato senza policy) — le
  scritture che creerebbero nuove chiavi vengono rifiutate quando si supera
  il limite, invece di liberare spazio.
- `allkeys-lru` — libera la chiave usata meno di recente tra tutte.
- `volatile-lru` — come sopra, ma solo tra le chiavi con un TTL impostato.
- `allkeys-random` / `volatile-random` — libera una chiave a caso (tra
  tutte, o solo tra quelle con TTL).

Come in Redis, l'eviction è **approssimata**, non un minimo globale esatto:
ogni shard propone il suo miglior candidato locale (più vecchio per LRU, o
casuale), e si sceglie il migliore tra i 64. È un compromesso deliberato
per non dover bloccare tutti gli shard insieme a ogni eviction. L'uso di
memoria stimato è visibile con `INFO` (`used_memory_bytes`) e su
Prometheus (`onyxdb_memory_bytes`) — è una stima approssimativa (somma
lunghezze di chiavi/valori + un overhead fisso per entry), non un
conteggio byte-per-byte esatto della memoria reale del processo.

## Test automatizzati

Il progetto include una suite di test unitari sulle operazioni fondamentali
dello store (SET/GET, contatori, liste, hash, set, TTL, rename, pattern
matching). Per eseguirli:

    cargo test --release

## Statistiche e diagnostica

Il comando `INFO` fornisce, oltre a ruolo e uptime: numero di connessioni
attive, comandi totali eseguiti, hit rate delle letture (percentuale di
`GET` andate a buon fine rispetto a quelle su chiavi inesistenti), e una
stima approssimativa della memoria usata.

## Metriche (Prometheus)

Il server espone un endpoint HTTP separato con metriche in formato
Prometheus, sulla porta principale + 1000 (es. porta 6380 → metriche su 7380):

    http://127.0.0.1:7380/metrics

Include: uptime, numero di chiavi, connessioni attive, comandi totali
eseguiti, hit/miss delle letture, ruolo (Master/Replica). Compatibile con
Prometheus/Grafana per dashboard e monitoraggio in tempo reale.

## Limitazioni note

- Nessun cluster/sharding distribuito tra più nodi (solo sharding interno
  a un singolo processo).
- Nessuno scripting (Lua), TLS nativo. L'autenticazione supporta
  utenti multipli (`--user`) ma senza permessi granulari per comando (niente
  ACL vere come in Redis — un utente autenticato può fare tutto).
- Solo RESP2 di base, niente RESP3.
- Promozione automatica disponibile (`--auto-failover`) ma senza
  coordinamento multi-Replica: sicura solo con una Replica per Master,
  rischio di split-brain con più di una (vedi sezione "Replica").
- Nessuna struttura dati avanzata (streams, HyperLogLog, geo, bitmap).
- Nessun client ufficiale in altri linguaggi oltre a quello incluso.

## Prossimi passi previsti

- Benchmark comparativo rigoroso con Redis vero, sullo stesso hardware e
  con lo stesso strumento (`redis-benchmark`), non solo con lo strumento
  interno.
- Compressione dei valori di grandi dimensioni (solo se necessario).
- Consenso distribuito e sharding automatico multi-nodo (obiettivo a lungo termine).
