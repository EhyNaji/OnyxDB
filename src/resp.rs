#[derive(Debug, Clone)]
pub enum RESPValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>), // None rappresenta il null (nil)
    Array(Vec<RESPValue>),
}

// Implementiamo il metodo per convertire la nostra Enum in byte RESP pronti per l'invio
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

// Legge un comando RESP dal socket riusando un buffer di scratch fornito dal
// chiamante, invece di allocare una nuova String per ogni riga: riduce
// drasticamente le allocazioni per comando (da ~7 a quasi zero extra).
pub async fn read_command(
    reader: &mut BufReader<OwnedReadHalf>,
    scratch: &mut String,
) -> std::io::Result<Option<Vec<String>>> {
    scratch.clear();
    let bytes_read = reader.read_line(scratch).await?;

    if bytes_read == 0 {
        return Ok(None); // connessione chiusa dal client
    }

    let first_byte = scratch.as_bytes().first().copied();

    if first_byte != Some(b'*') {
        // Comando in testo semplice (compatibilita' con vecchi tool)
        let parts: Vec<String> = scratch.trim_end().split_whitespace().map(|s| s.to_string()).collect();
        return Ok(Some(parts));
    }

    let num_elements: i64 = scratch[1..].trim_end().parse().unwrap_or(-1);
    if num_elements <= 0 {
        return Ok(Some(Vec::new()));
    }

    // Limite di sicurezza: nessun comando reale ha bisogno di piu' di 1024
    // argomenti. Evita che un array dichiarato enorme faccia pre-allocare
    // un Vec troppo grande prima ancora di leggere i dati veri.
    const MAX_ARRAY_LEN: i64 = 1024;
    if num_elements > MAX_ARRAY_LEN {
        return Ok(Some(Vec::new()));
    }

    let mut command = Vec::with_capacity(num_elements as usize);

    for _ in 0..num_elements {
        scratch.clear();
        reader.read_line(scratch).await?;

        if scratch.as_bytes().first().copied() != Some(b'$') {
            return Ok(Some(Vec::new())); // formato inatteso, comando scartato
        }

        let len: i64 = scratch[1..].trim_end().parse().unwrap_or(-1);
        if len < 0 {
            command.push(String::new());
            continue;
        }

        // Limite di sicurezza: un singolo elemento non puo' superare 512MB
        // Evita che un client possa far
        // allocare memoria arbitraria al server con una lunghezza fasulla.
        const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;
        if len > MAX_BULK_LEN {
            return Ok(Some(Vec::new())); // comando scartato, connessione puo' continuare
        }

        let mut buf = vec![0u8; len as usize + 2]; // +2 per il \r\n finale
        reader.read_exact(&mut buf).await?;
        buf.truncate(len as usize);

        command.push(String::from_utf8_lossy(&buf).to_string());
    }

    Ok(Some(command))
}
impl RESPValue {
    // Come encode(), ma scrive dentro un buffer riusato invece di allocare
    // una nuova String ogni volta: riduce le allocazioni sul lato risposta.
    pub fn encode_into(&self, buf: &mut String) {
        use std::fmt::Write;
        match self {
            RESPValue::SimpleString(s) => { let _ = write!(buf, "+{}\r\n", s); }
            RESPValue::Error(s) => { let _ = write!(buf, "-{}\r\n", s); }
            RESPValue::Integer(n) => { let _ = write!(buf, ":{}\r\n", n); }
            RESPValue::BulkString(None) => { buf.push_str("$-1\r\n"); }
            RESPValue::BulkString(Some(s)) => { let _ = write!(buf, "${}\r\n{}\r\n", s.len(), s); }
            RESPValue::Array(arr) => {
                let _ = write!(buf, "*{}\r\n", arr.len());
                for item in arr {
                    item.encode_into(buf);
                }
            }
        }
    }
}