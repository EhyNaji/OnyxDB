//! Bounded RESP client primitives shared by OnyxDB tooling.

use crate::resp::{
    CLIENT_RESP_LIMITS, encode_command, invalid_data, line_content, read_bounded_line,
};
use bytes::Bytes;
use std::fmt;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_HEADER_SIZE: usize = 64 * 1024;
const MAX_PIPELINE_COMMANDS: usize = 4096;
const MAX_PIPELINE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RESPResponseLimits {
    pub max_depth: usize,
    pub max_array_len: usize,
    pub max_total_elements: usize,
    pub max_bulk_len: usize,
    pub max_frame_len: usize,
}

pub const DEFAULT_RESPONSE_LIMITS: RESPResponseLimits = RESPResponseLimits {
    max_depth: 64,
    max_array_len: 1_000_000,
    max_total_elements: 1_000_000,
    max_bulk_len: 16 * 1024 * 1024,
    max_frame_len: 64 * 1024 * 1024,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RESPResponse {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RESPResponse>>),
}

impl RESPResponse {
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

impl fmt::Display for RESPResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SimpleString(value) | Self::BulkString(Some(value)) => {
                write!(formatter, "{}", String::from_utf8_lossy(value))
            }
            Self::Error(value) => write!(formatter, "(error) {}", String::from_utf8_lossy(value)),
            Self::Integer(value) => write!(formatter, "{value}"),
            Self::BulkString(None) | Self::Array(None) => formatter.write_str("(nil)"),
            Self::Array(Some(values)) if values.is_empty() => formatter.write_str("(empty array)"),
            Self::Array(Some(values)) => {
                formatter.write_str("[")?;
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{value}")?;
                }
                formatter.write_str("]")
            }
        }
    }
}

enum ResponseToken {
    Value(RESPResponse),
    Array(usize),
}

struct ArrayBuilder {
    expected: usize,
    values: Vec<RESPResponse>,
}

fn parse_signed_decimal(bytes: &[u8], field: &'static str) -> io::Result<i64> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_data(field))?;
    if text.is_empty()
        || text == "-"
        || text == "+"
        || text
            .strip_prefix(['-', '+'])
            .unwrap_or(text)
            .bytes()
            .any(|byte| !byte.is_ascii_digit())
    {
        return Err(invalid_data(field));
    }
    text.parse::<i64>().map_err(|_| invalid_data(field))
}

fn account_frame_bytes(consumed: &mut usize, added: usize, limit: usize) -> io::Result<()> {
    *consumed = consumed
        .checked_add(added)
        .ok_or_else(|| invalid_data("RESP response frame length overflow"))?;
    if *consumed > limit {
        return Err(invalid_data("RESP response exceeds the protocol limit"));
    }
    Ok(())
}

async fn read_response_token<R>(
    reader: &mut R,
    scratch: &mut Vec<u8>,
    limits: RESPResponseLimits,
    consumed: &mut usize,
    root: bool,
) -> io::Result<Option<ResponseToken>>
where
    R: AsyncBufRead + Unpin,
{
    let bytes_read = read_bounded_line(
        reader,
        scratch,
        MAX_RESPONSE_HEADER_SIZE.min(limits.max_frame_len),
    )
    .await?;
    if bytes_read == 0 {
        if root {
            return Ok(None);
        }
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "RESP response ended before the aggregate was complete",
        ));
    }
    account_frame_bytes(consumed, bytes_read, limits.max_frame_len)?;
    let line = line_content(scratch)?;
    let (&prefix, content) = line
        .split_first()
        .ok_or_else(|| invalid_data("RESP response header is empty"))?;

    let token = match prefix {
        b'+' => ResponseToken::Value(RESPResponse::SimpleString(Bytes::copy_from_slice(content))),
        b'-' => ResponseToken::Value(RESPResponse::Error(Bytes::copy_from_slice(content))),
        b':' => ResponseToken::Value(RESPResponse::Integer(parse_signed_decimal(
            content,
            "RESP integer response is invalid",
        )?)),
        b'$' => {
            let length = parse_signed_decimal(content, "RESP bulk response length is invalid")?;
            if length == -1 {
                ResponseToken::Value(RESPResponse::BulkString(None))
            } else if length < 0 {
                return Err(invalid_data("RESP bulk response length is invalid"));
            } else {
                let length = usize::try_from(length)
                    .map_err(|_| invalid_data("RESP bulk response length is invalid"))?;
                if length > limits.max_bulk_len {
                    return Err(invalid_data(
                        "RESP bulk response exceeds the protocol limit",
                    ));
                }
                account_frame_bytes(consumed, length.saturating_add(2), limits.max_frame_len)?;
                let mut payload = vec![0; length];
                reader.read_exact(&mut payload).await?;
                let mut terminator = [0; 2];
                reader.read_exact(&mut terminator).await?;
                if terminator != *b"\r\n" {
                    return Err(invalid_data(
                        "RESP bulk response is missing its CRLF terminator",
                    ));
                }
                ResponseToken::Value(RESPResponse::BulkString(Some(Bytes::from(payload))))
            }
        }
        b'*' => {
            let length = parse_signed_decimal(content, "RESP array response length is invalid")?;
            if length == -1 {
                ResponseToken::Value(RESPResponse::Array(None))
            } else if length < 0 {
                return Err(invalid_data("RESP array response length is invalid"));
            } else {
                let length = usize::try_from(length)
                    .map_err(|_| invalid_data("RESP array response length is invalid"))?;
                if length > limits.max_array_len {
                    return Err(invalid_data(
                        "RESP array response exceeds the protocol limit",
                    ));
                }
                ResponseToken::Array(length)
            }
        }
        _ => return Err(invalid_data("RESP response type is unsupported")),
    };
    Ok(Some(token))
}

pub async fn read_response<R>(
    reader: &mut R,
    scratch: &mut Vec<u8>,
    limits: RESPResponseLimits,
) -> io::Result<Option<RESPResponse>>
where
    R: AsyncBufRead + Unpin,
{
    let mut consumed = 0usize;
    let mut total_elements = 0usize;
    let mut stack = Vec::<ArrayBuilder>::new();

    loop {
        let root = stack.is_empty() && consumed == 0;
        let Some(token) = read_response_token(reader, scratch, limits, &mut consumed, root).await?
        else {
            return Ok(None);
        };
        let mut value = match token {
            ResponseToken::Value(value) => value,
            ResponseToken::Array(0) => RESPResponse::Array(Some(Vec::new())),
            ResponseToken::Array(expected) => {
                if stack.len() >= limits.max_depth {
                    return Err(invalid_data(
                        "RESP response nesting exceeds the protocol limit",
                    ));
                }
                total_elements = total_elements
                    .checked_add(expected)
                    .ok_or_else(|| invalid_data("RESP response element count overflow"))?;
                if total_elements > limits.max_total_elements {
                    return Err(invalid_data(
                        "RESP response element count exceeds the protocol limit",
                    ));
                }
                stack.push(ArrayBuilder {
                    expected,
                    values: Vec::with_capacity(expected.min(1024)),
                });
                continue;
            }
        };

        loop {
            let Some(builder) = stack.last_mut() else {
                return Ok(Some(value));
            };
            builder.values.push(value);
            if builder.values.len() < builder.expected {
                break;
            }
            let completed = stack.pop().expect("array builder exists");
            value = RESPResponse::Array(Some(completed.values));
        }
    }
}

pub struct RespClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    read_scratch: Vec<u8>,
    write_buffer: Vec<u8>,
    response_limits: RESPResponseLimits,
    io_timeout: Duration,
}

impl RespClient {
    pub async fn connect(address: &str) -> io::Result<Self> {
        let stream = tokio::time::timeout(DEFAULT_CONNECT_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RESP connection timed out"))??;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, reader),
            writer,
            read_scratch: Vec::with_capacity(256),
            write_buffer: Vec::with_capacity(1024),
            response_limits: DEFAULT_RESPONSE_LIMITS,
            io_timeout: DEFAULT_IO_TIMEOUT,
        })
    }

    pub async fn send(&mut self, arguments: &[String]) -> io::Result<RESPResponse> {
        self.write_buffer.clear();
        encode_command(arguments, &mut self.write_buffer, CLIENT_RESP_LIMITS)?;
        let write = async {
            self.writer.write_all(&self.write_buffer).await?;
            self.writer.flush().await
        };
        tokio::time::timeout(self.io_timeout, write)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RESP write timed out"))??;
        self.read().await
    }

    pub async fn write_pipeline(&mut self, commands: &[Vec<String>]) -> io::Result<()> {
        if commands.is_empty() || commands.len() > MAX_PIPELINE_COMMANDS {
            return Err(invalid_data("RESP pipeline command count is invalid"));
        }
        self.write_buffer.clear();
        for command in commands {
            encode_command(command, &mut self.write_buffer, CLIENT_RESP_LIMITS)?;
            if self.write_buffer.len() > MAX_PIPELINE_BYTES {
                self.write_buffer.clear();
                return Err(invalid_data(
                    "RESP pipeline exceeds the client buffer limit",
                ));
            }
        }
        let write = async {
            self.writer.write_all(&self.write_buffer).await?;
            self.writer.flush().await
        };
        tokio::time::timeout(self.io_timeout, write)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RESP write timed out"))?
    }

    pub async fn read(&mut self) -> io::Result<RESPResponse> {
        tokio::time::timeout(
            self.io_timeout,
            read_response(
                &mut self.reader,
                &mut self.read_scratch,
                self.response_limits,
            ),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RESP read timed out"))??
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "RESP connection closed"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandLineError(&'static str);

impl fmt::Display for CommandLineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for CommandLineError {}

/// Splits an interactive command line while preserving quoted and empty values.
pub fn parse_command_line(line: &str) -> Result<Vec<String>, CommandLineError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = Quote::None;
    let mut escaped = false;
    let mut active = false;

    for character in line.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            active = true;
            continue;
        }
        match quote {
            Quote::Single => {
                if character == '\'' {
                    quote = Quote::None;
                } else {
                    current.push(character);
                }
                active = true;
            }
            Quote::Double => match character {
                '"' => quote = Quote::None,
                '\\' => escaped = true,
                _ => current.push(character),
            },
            Quote::None => match character {
                '\'' => {
                    quote = Quote::Single;
                    active = true;
                }
                '"' => {
                    quote = Quote::Double;
                    active = true;
                }
                '\\' => {
                    escaped = true;
                    active = true;
                }
                character if character.is_whitespace() => {
                    if active {
                        arguments.push(std::mem::take(&mut current));
                        active = false;
                    }
                }
                _ => {
                    current.push(character);
                    active = true;
                }
            },
        }
    }
    if escaped {
        return Err(CommandLineError(
            "command line ends with an incomplete escape",
        ));
    }
    if quote != Quote::None {
        return Err(CommandLineError("command line contains an unclosed quote"));
    }
    if active {
        arguments.push(current);
    }
    Ok(arguments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};

    fn small_limits() -> RESPResponseLimits {
        RESPResponseLimits {
            max_depth: 2,
            max_array_len: 4,
            max_total_elements: 5,
            max_bulk_len: 8,
            max_frame_len: 64,
        }
    }

    #[tokio::test]
    async fn response_parser_preserves_binary_payloads_and_pipeline_boundaries() {
        let input = b"$5\r\na\0b\xffc\r\n*3\r\n+OK\r\n:-2\r\n$-1\r\n";
        let mut reader = BufReader::new(&input[..]);
        let mut scratch = Vec::new();
        let first = read_response(&mut reader, &mut scratch, small_limits())
            .await
            .unwrap()
            .unwrap();
        let second = read_response(&mut reader, &mut scratch, small_limits())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            RESPResponse::BulkString(Some(Bytes::from_static(b"a\0b\xffc")))
        );
        assert_eq!(
            second,
            RESPResponse::Array(Some(vec![
                RESPResponse::SimpleString(Bytes::from_static(b"OK")),
                RESPResponse::Integer(-2),
                RESPResponse::BulkString(None),
            ]))
        );
    }

    #[tokio::test]
    async fn response_parser_rejects_malformed_and_unbounded_frames() {
        let malformed: &[&[u8]] = &[
            b"+OK\n",
            b"$-2\r\n",
            b"$9\r\n",
            b"$3\r\nabcXX",
            b"*5\r\n",
            b"*2\r\n+one\r\n",
            b"*1\r\n*1\r\n*1\r\n+deep\r\n",
            b"*3\r\n*3\r\n",
            b"?unknown\r\n",
        ];
        for input in malformed {
            let mut reader = BufReader::new(*input);
            let mut scratch = Vec::new();
            assert!(
                read_response(&mut reader, &mut scratch, small_limits())
                    .await
                    .is_err(),
                "{input:?}"
            );
        }
    }

    #[tokio::test]
    async fn response_parser_handles_byte_at_a_time_partial_reads() {
        let (mut writer, reader) = tokio::io::duplex(8);
        let sender = tokio::spawn(async move {
            for byte in b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n" {
                writer.write_all(&[*byte]).await.unwrap();
            }
        });
        let mut reader = BufReader::new(reader);
        let mut scratch = Vec::new();
        let response = read_response(&mut reader, &mut scratch, small_limits())
            .await
            .unwrap();
        sender.await.unwrap();
        assert!(matches!(response, Some(RESPResponse::Array(Some(values))) if values.len() == 2));
    }

    #[test]
    fn command_line_parser_preserves_boundaries_and_empty_values() {
        assert_eq!(
            parse_command_line("SET 'space key' \"value with spaces\" NX").unwrap(),
            ["SET", "space key", "value with spaces", "NX"]
        );
        assert_eq!(
            parse_command_line("SET key \"\"").unwrap(),
            ["SET", "key", ""]
        );
        assert_eq!(
            parse_command_line(r"SET escaped\ key value\\suffix").unwrap(),
            ["SET", "escaped key", r"value\suffix"]
        );
        assert!(parse_command_line("SET key 'unterminated").is_err());
        assert!(parse_command_line("SET key trailing\\").is_err());
    }
}
