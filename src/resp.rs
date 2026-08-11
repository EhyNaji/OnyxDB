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

const MAX_RESP_HEADER_LINE_SIZE: usize = 64;

async fn read_bounded_line(
    reader: &mut BufReader<OwnedReadHalf>,
    scratch: &mut String,
    maximum_size: usize,
) -> std::io::Result<usize> {
    scratch.clear();
    let mut limited = reader.take((maximum_size + 1) as u64);
    let bytes_read = limited.read_line(scratch).await?;
    if bytes_read > maximum_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "RESP line exceeds the protocol limit",
        ));
    }
    Ok(bytes_read)
}

// Reads a RESP command while reusing the caller's line buffer.
pub async fn read_command(
    reader: &mut BufReader<OwnedReadHalf>,
    scratch: &mut String,
) -> std::io::Result<Option<Vec<String>>> {
    read_command_with_limits(reader, scratch, 1024, 512 * 1024 * 1024, 512 * 1024 * 1024).await
}

pub async fn read_command_with_limits(
    reader: &mut BufReader<OwnedReadHalf>,
    scratch: &mut String,
    max_array_len: i64,
    max_bulk_len: i64,
    max_inline_len: usize,
) -> std::io::Result<Option<Vec<String>>> {
    let first_byte = reader.fill_buf().await?.first().copied();
    let maximum_first_line_size = if first_byte == Some(b'*') {
        MAX_RESP_HEADER_LINE_SIZE
    } else {
        max_inline_len
    };
    let bytes_read = read_bounded_line(reader, scratch, maximum_first_line_size).await?;

    if bytes_read == 0 {
        return Ok(None);
    }

    if first_byte != Some(b'*') {
        // Inline commands remain supported for legacy clients and internal
        // simple-string replication markers.
        let parts: Vec<String> = scratch.split_whitespace().map(|s| s.to_string()).collect();
        return Ok(Some(parts));
    }

    let num_elements: i64 = scratch[1..].trim_end().parse().unwrap_or(-1);
    if num_elements <= 0 {
        return Ok(Some(Vec::new()));
    }

    if num_elements > max_array_len {
        return Ok(Some(Vec::new()));
    }

    let mut command = Vec::with_capacity(num_elements as usize);

    for _ in 0..num_elements {
        read_bounded_line(reader, scratch, MAX_RESP_HEADER_LINE_SIZE).await?;

        if scratch.as_bytes().first().copied() != Some(b'$') {
            return Ok(Some(Vec::new()));
        }

        let len: i64 = scratch[1..].trim_end().parse().unwrap_or(-1);
        if len < 0 {
            command.push(String::new());
            continue;
        }

        if len > max_bulk_len {
            return Ok(Some(Vec::new()));
        }

        let buffer_length = (len as usize).checked_add(2).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "RESP bulk string length overflow",
            )
        })?;
        let mut buf = vec![0u8; buffer_length];
        reader.read_exact(&mut buf).await?;
        if !buf.ends_with(b"\r\n") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "RESP bulk string is missing its CRLF terminator",
            ));
        }
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
            RESPValue::SimpleString(s) => {
                let _ = write!(buf, "+{}\r\n", s);
            }
            RESPValue::Error(s) => {
                let _ = write!(buf, "-{}\r\n", s);
            }
            RESPValue::Integer(n) => {
                let _ = write!(buf, ":{}\r\n", n);
            }
            RESPValue::BulkString(None) => {
                buf.push_str("$-1\r\n");
            }
            RESPValue::BulkString(Some(s)) => {
                let _ = write!(buf, "${}\r\n{}\r\n", s.len(), s);
            }
            RESPValue::Array(arr) => {
                let _ = write!(buf, "*{}\r\n", arr.len());
                for item in arr {
                    item.encode_into(buf);
                }
            }
        }
    }
}
