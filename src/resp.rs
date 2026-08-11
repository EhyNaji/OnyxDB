use std::io;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

#[derive(Debug, Clone)]
pub enum RESPValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RESPValue>),
}

const MAX_RESP_HEADER_LINE_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RESPReadLimits {
    pub max_array_len: usize,
    pub max_bulk_len: usize,
    pub max_inline_len: usize,
    pub max_frame_len: usize,
}

pub const CLIENT_RESP_LIMITS: RESPReadLimits = RESPReadLimits {
    max_array_len: 1024,
    max_bulk_len: 8 * 1024 * 1024,
    max_inline_len: 64 * 1024,
    max_frame_len: 16 * 1024 * 1024,
};

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

async fn read_bounded_line<R>(
    reader: &mut R,
    scratch: &mut Vec<u8>,
    maximum_size: usize,
) -> io::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    scratch.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(scratch.len());
        }

        let available_length = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        let remaining = maximum_size
            .checked_add(1)
            .and_then(|limit| limit.checked_sub(scratch.len()))
            .ok_or_else(|| invalid_data("RESP line exceeds the protocol limit"))?;
        let copied = available_length.min(remaining);
        scratch.extend_from_slice(&available[..copied]);
        reader.consume(copied);

        if scratch.len() > maximum_size {
            return Err(invalid_data("RESP line exceeds the protocol limit"));
        }
        if copied == available_length && scratch.ends_with(b"\n") {
            return Ok(scratch.len());
        }
    }
}

fn line_content(line: &[u8]) -> io::Result<&[u8]> {
    line.strip_suffix(b"\r\n")
        .ok_or_else(|| invalid_data("RESP line is missing its CRLF terminator"))
}

fn parse_decimal(bytes: &[u8], field: &'static str) -> io::Result<usize> {
    if bytes.is_empty() || bytes.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(invalid_data(field));
    }
    bytes.iter().try_fold(0usize, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add((byte - b'0') as usize))
            .ok_or_else(|| invalid_data(field))
    })
}

pub async fn read_command_with_limits<R>(
    reader: &mut R,
    scratch: &mut Vec<u8>,
    limits: RESPReadLimits,
) -> io::Result<Option<Vec<String>>>
where
    R: AsyncBufRead + Unpin,
{
    let first_byte = reader.fill_buf().await?.first().copied();
    let Some(first_byte) = first_byte else {
        return Ok(None);
    };
    let maximum_first_line_size = if first_byte == b'*' {
        MAX_RESP_HEADER_LINE_SIZE.min(limits.max_frame_len)
    } else {
        limits.max_inline_len.min(limits.max_frame_len)
    };
    let bytes_read = read_bounded_line(reader, scratch, maximum_first_line_size).await?;
    if bytes_read == 0 {
        return Ok(None);
    }
    let first_line = line_content(scratch)?;

    if first_byte != b'*' {
        let inline = std::str::from_utf8(first_line)
            .map_err(|_| invalid_data("RESP inline command is not valid UTF-8"))?;
        let parts = inline
            .split_ascii_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if parts.is_empty() {
            return Err(invalid_data("RESP inline command is empty"));
        }
        if parts.len() > limits.max_array_len {
            return Err(invalid_data("RESP command has too many arguments"));
        }
        return Ok(Some(parts));
    }

    let argument_count = parse_decimal(
        first_line
            .strip_prefix(b"*")
            .ok_or_else(|| invalid_data("RESP array header is malformed"))?,
        "RESP array length is invalid",
    )?;
    if argument_count == 0 {
        return Err(invalid_data("RESP command array must not be empty"));
    }
    if argument_count > limits.max_array_len {
        return Err(invalid_data("RESP command has too many arguments"));
    }

    let mut frame_length = bytes_read;
    let mut command = Vec::with_capacity(argument_count);
    for _ in 0..argument_count {
        let header_length = read_bounded_line(reader, scratch, MAX_RESP_HEADER_LINE_SIZE).await?;
        if header_length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "RESP command ended before a bulk string header",
            ));
        }
        let bulk_header = line_content(scratch)?;
        let bulk_length = parse_decimal(
            bulk_header
                .strip_prefix(b"$")
                .ok_or_else(|| invalid_data("RESP command arguments must be bulk strings"))?,
            "RESP bulk string length is invalid",
        )?;
        if bulk_length > limits.max_bulk_len {
            return Err(invalid_data("RESP bulk string exceeds the protocol limit"));
        }
        let encoded_bulk_length = bulk_length
            .checked_add(2)
            .ok_or_else(|| invalid_data("RESP bulk string length overflow"))?;
        let projected_frame_length = frame_length
            .checked_add(header_length)
            .and_then(|length| length.checked_add(encoded_bulk_length))
            .ok_or_else(|| invalid_data("RESP frame length overflow"))?;
        if projected_frame_length > limits.max_frame_len {
            return Err(invalid_data(
                "RESP frame exceeds the aggregate protocol limit",
            ));
        }

        let mut payload = vec![0u8; bulk_length];
        reader.read_exact(&mut payload).await?;
        let mut terminator = [0u8; 2];
        reader.read_exact(&mut terminator).await?;
        if terminator != *b"\r\n" {
            return Err(invalid_data(
                "RESP bulk string is missing its CRLF terminator",
            ));
        }
        command.push(
            String::from_utf8(payload)
                .map_err(|_| invalid_data("RESP command argument is not valid UTF-8"))?,
        );
        frame_length = projected_frame_length;
    }

    Ok(Some(command))
}

pub async fn read_command_with_timeouts<R>(
    reader: &mut R,
    scratch: &mut Vec<u8>,
    limits: RESPReadLimits,
    idle_timeout: Option<Duration>,
    frame_timeout: Duration,
) -> io::Result<Option<Vec<String>>>
where
    R: AsyncBufRead + Unpin,
{
    let has_input = async {
        reader
            .fill_buf()
            .await
            .map(|available| !available.is_empty())
    };
    let has_input = match idle_timeout {
        Some(timeout) => tokio::time::timeout(timeout, has_input)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RESP client idle timeout"))??,
        None => has_input.await?,
    };
    if !has_input {
        return Ok(None);
    }

    tokio::time::timeout(
        frame_timeout,
        read_command_with_limits(reader, scratch, limits),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RESP frame assembly timeout"))?
}

impl RESPValue {
    /// Encodes into a reusable response buffer.
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};

    fn small_limits() -> RESPReadLimits {
        RESPReadLimits {
            max_array_len: 4,
            max_bulk_len: 16,
            max_inline_len: 32,
            max_frame_len: 32,
        }
    }

    #[tokio::test]
    async fn aggregate_limit_is_checked_before_bulk_allocation() {
        let input = b"*2\r\n$10\r\n0123456789\r\n$10\r\n";
        let mut reader = BufReader::new(&input[..]);
        let mut scratch = Vec::new();
        let error = read_command_with_limits(&mut reader, &mut scratch, small_limits())
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "RESP frame exceeds the aggregate protocol limit"
        );
    }

    #[tokio::test]
    async fn malformed_command_forms_are_rejected() {
        let malformed: &[&[u8]] = &[
            b"*0\r\n",
            b"*-1\r\n",
            b"*invalid\r\n",
            b"*999999999999999999999999999999\r\n",
            b"*1\n$4\r\nPING\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$+1\r\na\r\n",
            b"*1\r\n+PING\r\n",
            b"*1\r\n$1\r\naXX",
            b"*1\r\n$17\r\n",
        ];
        for input in malformed {
            let mut reader = BufReader::new(*input);
            let mut scratch = Vec::new();
            let error = read_command_with_limits(&mut reader, &mut scratch, small_limits())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{input:?}");
        }
    }

    #[tokio::test]
    async fn empty_bulk_and_legacy_inline_commands_remain_valid() {
        let mut array_reader = BufReader::new(&b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n"[..]);
        let mut scratch = Vec::new();
        let array = read_command_with_limits(&mut array_reader, &mut scratch, small_limits())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(array, ["GET", ""]);

        let mut inline_reader = BufReader::new(&b"PING\r\n"[..]);
        let inline = read_command_with_limits(&mut inline_reader, &mut scratch, small_limits())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inline, ["PING"]);
    }

    #[tokio::test]
    async fn partial_reads_and_pipelined_frames_preserve_boundaries() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let sender = tokio::spawn(async move {
            for byte in b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n" {
                writer.write_all(&[*byte]).await.unwrap();
            }
        });
        let mut reader = BufReader::new(reader);
        let mut scratch = Vec::new();

        let first = read_command_with_limits(&mut reader, &mut scratch, CLIENT_RESP_LIMITS)
            .await
            .unwrap()
            .unwrap();
        let second = read_command_with_limits(&mut reader, &mut scratch, CLIENT_RESP_LIMITS)
            .await
            .unwrap()
            .unwrap();
        sender.await.unwrap();

        assert_eq!(first, ["PING"]);
        assert_eq!(second, ["GET", "key"]);
    }

    #[tokio::test]
    async fn partial_frame_times_out_after_the_first_byte() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"*").await.unwrap();
        let mut reader = BufReader::new(reader);
        let mut scratch = Vec::new();

        let error = read_command_with_timeouts(
            &mut reader,
            &mut scratch,
            CLIENT_RESP_LIMITS,
            None,
            Duration::from_millis(25),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn silent_peer_hits_the_idle_timeout() {
        let (_writer, reader) = tokio::io::duplex(64);
        let mut reader = BufReader::new(reader);
        let mut scratch = Vec::new();

        let error = read_command_with_timeouts(
            &mut reader,
            &mut scratch,
            CLIENT_RESP_LIMITS,
            Some(Duration::from_millis(25)),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(error.to_string(), "RESP client idle timeout");
    }
}
