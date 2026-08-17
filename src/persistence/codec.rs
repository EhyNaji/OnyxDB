use super::{CommittedBatch, CommittedEffect, PersistenceError};
use bytes::Bytes;
use onyxdb::clock::unix_seconds as now;
use onyxdb::engine::{DataEntry, OnyxValue};

pub(crate) const BINLOG_RECORD_MAGIC: &[u8; 4] = b"ONX4";
pub(crate) const CHECKSUMLESS_BINLOG_RECORD_MAGIC: &[u8; 4] = b"ONX3";
pub(crate) const PREVIOUS_BINLOG_RECORD_MAGIC: &[u8; 4] = b"ONX2";
pub(crate) const BINLOG_CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const BINLOG_RECORD_LENGTH_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const MAX_BINLOG_RECORD_SIZE: usize = 512 * 1024 * 1024 + 1024;
pub(crate) const MAX_SNAPSHOT_RECORD_SIZE: usize = 512 * 1024 * 1024 + 1024;

// ============================================================
// LEGACY COMMAND BINLOG CODEC
// ============================================================
#[cfg(test)]
pub(crate) const OP_SET: u8 = 1;
#[cfg(test)]
const OP_DEL: u8 = 2;
#[cfg(test)]
const OP_EXPIRE: u8 = 3;
#[cfg(test)]
const OP_L_PUSH: u8 = 4;
#[cfg(test)]
const OP_HSET: u8 = 5;
#[cfg(test)]
const OP_SADD: u8 = 6;
#[cfg(test)]
const OP_RENAME: u8 = 7;
#[cfg(test)]
const OP_INCR: u8 = 8;
#[cfg(test)]
const OP_DECR: u8 = 9;
#[cfg(test)]
const OP_APPEND: u8 = 10;
#[cfg(test)]
const OP_HDEL: u8 = 11;
#[cfg(test)]
const OP_SREM: u8 = 12;
#[cfg(test)]
const OP_COPY: u8 = 13;
#[cfg(test)]
const OP_MSET: u8 = 14;
#[cfg(test)]
const OP_R_PUSH: u8 = 15;
#[cfg(test)]
const OP_LPOP: u8 = 16;
#[cfg(test)]
const OP_RPOP: u8 = 17;
#[cfg(test)]
const OP_JSON_SET: u8 = 18;
#[cfg(test)]
const OP_JSON_DEL: u8 = 19;
#[cfg(test)]
const OP_JSON_NUMINCRBY: u8 = 20;
#[cfg(test)]
const OP_JSON_ARRAPPEND: u8 = 21;
#[cfg(test)]
pub(crate) fn write_u16_be(buf: &mut Vec<u8>, val: u16) {
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

pub(crate) fn write_u32_be(buf: &mut Vec<u8>, val: u32) {
    buf.push((val >> 24) as u8);
    buf.push((val >> 16) as u8);
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

pub(crate) fn write_u64_be(buf: &mut Vec<u8>, val: u64) {
    buf.push((val >> 56) as u8);
    buf.push((val >> 48) as u8);
    buf.push((val >> 40) as u8);
    buf.push((val >> 32) as u8);
    buf.push((val >> 24) as u8);
    buf.push((val >> 16) as u8);
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

// Checked decoding primitives return `None` instead of panicking on truncated
// or corrupt input. Recovery decides separately whether a recognizable final
// partial record is truncatable or corruption must fail startup closed.
#[cfg(test)]
pub(crate) fn read_u16_be(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    if offset.checked_add(2)? > bytes.len() {
        return None;
    }
    let val = ((bytes[*offset] as u16) << 8) | (bytes[*offset + 1] as u16);
    *offset += 2;
    Some(val)
}

pub(crate) fn read_u32_be(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    if offset.checked_add(4)? > bytes.len() {
        return None;
    }
    let val = ((bytes[*offset] as u32) << 24)
        | ((bytes[*offset + 1] as u32) << 16)
        | ((bytes[*offset + 2] as u32) << 8)
        | (bytes[*offset + 3] as u32);
    *offset += 4;
    Some(val)
}

pub(crate) fn read_u64_be(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    if offset.checked_add(8)? > bytes.len() {
        return None;
    }
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

/// Returns a bounded slice and rejects truncated or corrupt record offsets.
pub(crate) fn safe_slice(bytes: &[u8], offset: usize, len: usize) -> Option<&[u8]> {
    let end = offset.checked_add(len)?;
    bytes.get(offset..end)
}

pub(crate) fn encode_versioned_binlog_record(
    sequence: u64,
    effect_record: &[u8],
) -> Result<Vec<u8>, PersistenceError> {
    if sequence == 0 {
        return Err(PersistenceError::new(
            "Versioned binlog records require a non-zero sequence",
        ));
    }
    let record_length = BINLOG_RECORD_MAGIC
        .len()
        .checked_add(BINLOG_RECORD_LENGTH_SIZE)
        .and_then(|length| length.checked_add(8))
        .and_then(|length| length.checked_add(effect_record.len()))
        .and_then(|length| length.checked_add(BINLOG_CHECKSUM_SIZE))
        .ok_or_else(|| PersistenceError::new("Binlog record length overflow"))?;
    if record_length > MAX_BINLOG_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Binlog record exceeds the format limit",
        ));
    }
    let mut record = Vec::with_capacity(record_length);
    record.extend_from_slice(BINLOG_RECORD_MAGIC);
    write_u32_be(
        &mut record,
        u32::try_from(record_length)
            .map_err(|_| PersistenceError::new("Binlog record length exceeds u32"))?,
    );
    write_u64_be(&mut record, sequence);
    record.extend_from_slice(effect_record);
    let checksum = crc32fast::hash(&record);
    write_u32_be(&mut record, checksum);
    Ok(record)
}

pub(crate) fn framed_versioned_binlog_record_length(
    effect_record_length: usize,
) -> Result<usize, PersistenceError> {
    BINLOG_RECORD_MAGIC
        .len()
        .checked_add(BINLOG_RECORD_LENGTH_SIZE)
        .and_then(|length| length.checked_add(8))
        .and_then(|length| length.checked_add(effect_record_length))
        .and_then(|length| length.checked_add(BINLOG_CHECKSUM_SIZE))
        .and_then(|length| length.checked_add(BINLOG_RECORD_LENGTH_SIZE))
        .ok_or_else(|| PersistenceError::new("Framed binlog record length overflow"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BinlogRecordIntegrity {
    Checksummed,
    ChecksumlessLegacy,
}

#[derive(Debug)]
pub(crate) enum DecodedBinlogRecord<'a> {
    Versioned {
        sequence: u64,
        effects: &'a [u8],
        integrity: BinlogRecordIntegrity,
    },
}

pub(crate) fn decode_binlog_record(
    record: &[u8],
) -> Result<DecodedBinlogRecord<'_>, PersistenceError> {
    let (magic, effects_end, integrity) = if record.starts_with(BINLOG_RECORD_MAGIC) {
        let checksum_offset = record
            .len()
            .checked_sub(BINLOG_CHECKSUM_SIZE)
            .filter(|offset| *offset >= BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE + 8)
            .ok_or_else(|| PersistenceError::new("Truncated checksummed binlog record"))?;
        let expected_checksum = u32::from_be_bytes(
            record[checksum_offset..]
                .try_into()
                .map_err(|_| PersistenceError::new("Invalid binlog record checksum"))?,
        );
        let actual_checksum = crc32fast::hash(&record[..checksum_offset]);
        if actual_checksum != expected_checksum {
            return Err(PersistenceError::new("Binlog record checksum mismatch"));
        }
        (
            BINLOG_RECORD_MAGIC.as_slice(),
            checksum_offset,
            BinlogRecordIntegrity::Checksummed,
        )
    } else if record.starts_with(CHECKSUMLESS_BINLOG_RECORD_MAGIC) {
        (
            CHECKSUMLESS_BINLOG_RECORD_MAGIC.as_slice(),
            record.len(),
            BinlogRecordIntegrity::ChecksumlessLegacy,
        )
    } else {
        let format = if record.starts_with(PREVIOUS_BINLOG_RECORD_MAGIC) {
            "ONX2 command records"
        } else {
            "legacy command records"
        };
        return Err(PersistenceError::new(format!(
            "Unsupported unsafe binlog format: {}",
            format
        )));
    };

    let mut offset = magic.len();
    if integrity == BinlogRecordIntegrity::Checksummed {
        let embedded_length = read_u32_be(record, &mut offset)
            .ok_or_else(|| PersistenceError::new("Missing embedded binlog record length"))?
            as usize;
        if embedded_length != record.len() {
            return Err(PersistenceError::new(format!(
                "Binlog record length mismatch: outer length {}, embedded length {}",
                record.len(),
                embedded_length
            )));
        }
    }
    let sequence = read_u64_be(record, &mut offset)
        .ok_or_else(|| PersistenceError::new("Truncated versioned binlog record header"))?;
    if sequence == 0 {
        return Err(PersistenceError::new(
            "Versioned binlog records must have a non-zero sequence",
        ));
    }
    let effects = record
        .get(offset..effects_end)
        .ok_or_else(|| PersistenceError::new("Missing committed-effect payload"))?;
    if effects.is_empty() {
        return Err(PersistenceError::new(
            "Versioned binlog record contains an empty committed-effect payload",
        ));
    }
    Ok(DecodedBinlogRecord::Versioned {
        sequence,
        effects,
        integrity,
    })
}

/// Encodes a legacy command record used for backward-compatible decoding tests.
#[cfg(test)]
pub(crate) fn command_to_binary_record(
    cmd: &str,
    args: &[String],
    _entry: Option<&DataEntry>,
) -> Option<Vec<u8>> {
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
        "JSON.NUMINCRBY" => OP_JSON_NUMINCRBY,
        "JSON.ARRAPPEND" => OP_JSON_ARRAPPEND,
        "HSET" => OP_HSET,
        "SADD" => OP_SADD,
        "RENAME" => OP_RENAME,
        "INCR" | "INCRBY" => OP_INCR,
        "DECRBY" => OP_DECR,
        "APPEND" => OP_APPEND,
        "HDEL" => OP_HDEL,
        "SREM" => OP_SREM,
        "COPY" => OP_COPY,
        _ => return None, // Non-persistent command.
    };

    buf.push(op_code);

    match cmd {
        "SET" | "GETSET" | "SETNX" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let value = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            buf.push(1); // String value type.
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
            // Persist the normalized absolute expiration so replay does not
            // reinterpret a relative TTL at a later wall-clock time.
            let expiry: u64 = if args.len() >= 5 && args[3].eq_ignore_ascii_case("EXAT") {
                args[4].parse().unwrap_or(0)
            } else {
                0
            };
            write_u64_be(&mut buf, expiry);
        }
        "MSET" => {
            if args.len() < 3 {
                return None;
            }
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
            if args.len() < 2 {
                return None;
            }
            let key = &args[1];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
        }
        "EXPIRE" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let seconds = args[2].parse::<u64>().unwrap_or(0);
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, seconds);
        }
        "EXPIREAT" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let timestamp = args[2].parse::<u64>().unwrap_or(0);
            buf[0] = OP_EXPIRE; // Same opcode with an absolute timestamp.
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, timestamp);
        }
        "LPUSH" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let item = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, item.len() as u32);
            buf.extend_from_slice(item.as_bytes());
        }
        "RPUSH" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let item = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, item.len() as u32);
            buf.extend_from_slice(item.as_bytes());
        }
        "LPOP" | "RPOP" => {
            if args.len() < 2 {
                return None;
            }
            let key = &args[1];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
        }
        "HSET" => {
            if args.len() < 4 {
                return None;
            }
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
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let member = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, member.len() as u32);
            buf.extend_from_slice(member.as_bytes());
        }
        "RENAME" => {
            if args.len() < 3 {
                return None;
            }
            let old_key = &args[1];
            let new_key = &args[2];
            write_u16_be(&mut buf, old_key.len() as u16);
            buf.extend_from_slice(old_key.as_bytes());
            write_u16_be(&mut buf, new_key.len() as u16);
            buf.extend_from_slice(new_key.as_bytes());
        }
        "INCR" | "INCRBY" => {
            if args.len() < 2 {
                return None;
            }
            let key = &args[1];
            let delta = if cmd == "INCR" {
                1
            } else {
                args.get(2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(1)
            };
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, delta as u64);
        }
        "DECRBY" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let delta = args[2].parse::<i64>().unwrap_or(1);
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, delta.unsigned_abs());
        }
        "APPEND" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let suffix = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, suffix.len() as u32);
            buf.extend_from_slice(suffix.as_bytes());
        }
        "HDEL" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let field = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, field.len() as u16);
            buf.extend_from_slice(field.as_bytes());
        }
        "JSON.SET" => {
            // Arguments: command, key, path, compact JSON value.
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            let value = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
        "JSON.DEL" => {
            // Arguments: command, key, path.
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
        }
        "JSON.NUMINCRBY" => {
            // Arguments: command, key, path, numeric delta as text.
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            let delta = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
            write_u16_be(&mut buf, delta.len() as u16);
            buf.extend_from_slice(delta.as_bytes());
        }
        "JSON.ARRAPPEND" => {
            // Arguments: command, key, path, compact JSON value.
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            let value = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
        "COPY" => {
            if args.len() < 3 {
                return None;
            }
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

/// Decodes a legacy command record into command arguments.
#[cfg(test)]
pub(crate) fn binary_record_to_args(record: &[u8]) -> Option<Vec<String>> {
    if record.is_empty() {
        return None;
    }

    let op = record[0];
    let mut offset = 1;

    match op {
        OP_SET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let _val_type = *record.get(offset)?;
            offset += 1;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            offset += val_len;
            // Older records omit the final expiration field and therefore
            // represent a value without expiration.
            let expiry = read_u64_be(record, &mut offset).unwrap_or(0);
            if expiry > 0 {
                Some(vec![
                    "SET".to_string(),
                    key,
                    value,
                    "EXAT".to_string(),
                    expiry.to_string(),
                ])
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
                let value =
                    String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
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
            let member =
                String::from_utf8_lossy(safe_slice(record, offset, member_len)?).to_string();
            Some(vec!["SADD".to_string(), key, member])
        }
        OP_SREM => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let member_len = read_u32_be(record, &mut offset)? as usize;
            let member =
                String::from_utf8_lossy(safe_slice(record, offset, member_len)?).to_string();
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
            let suffix =
                String::from_utf8_lossy(safe_slice(record, offset, suffix_len)?).to_string();
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
        OP_JSON_NUMINCRBY => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            offset += path_len;
            let delta_len = read_u16_be(record, &mut offset)? as usize;
            let delta = String::from_utf8_lossy(safe_slice(record, offset, delta_len)?).to_string();
            Some(vec!["JSON.NUMINCRBY".to_string(), key, path, delta])
        }
        OP_JSON_ARRAPPEND => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            offset += path_len;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            Some(vec!["JSON.ARRAPPEND".to_string(), key, path, value])
        }
        _ => None,
    }
}

pub(crate) fn line_to_entry(line: &str) -> Option<(String, DataEntry)> {
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
                val_str
                    .split('|')
                    .map(|s| Bytes::from(s.to_string()))
                    .collect()
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
        "JSON" => serde_json::from_str::<serde_json::Value>(val_str)
            .ok()
            .map(OnyxValue::Json),
        "SET" => {
            let set: std::collections::HashSet<Bytes> = if val_str.is_empty() {
                std::collections::HashSet::new()
            } else {
                val_str
                    .split('|')
                    .map(|s| Bytes::from(s.to_string()))
                    .collect()
            };
            Some(OnyxValue::Set(set))
        }
        _ => None,
    }?;

    let ts = now();
    Some((
        key,
        DataEntry {
            value,
            expires_at,
            created_at: ts,
            last_accessed: ts,
        },
    ))
}

#[cfg(test)]
pub(crate) fn value_to_line(key: &str, entry: &DataEntry) -> String {
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
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        String::from_utf8_lossy(k),
                        String::from_utf8_lossy(v)
                    )
                })
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
        // The legacy text snapshot format does not support vectors.
        _ => ("STR", String::new()),
    };

    let exp_val = entry.expires_at.unwrap_or(0);
    format!("{}\t{}\t{}\t{}", key, val_type, exp_val, val_str)
}
pub(crate) fn checked_u32_length(
    length: usize,
    description: &str,
) -> Result<u32, PersistenceError> {
    u32::try_from(length)
        .map_err(|_| PersistenceError::new(format!("{} exceeds the format limit", description)))
}

pub(crate) fn append_snapshot_bytes(
    record: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), PersistenceError> {
    let length = checked_u32_length(bytes.len(), "Snapshot value")?;
    write_u32_be(record, length);
    record.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn encode_snapshot_entry(
    key: &[u8],
    entry: &DataEntry,
) -> Result<Vec<u8>, PersistenceError> {
    let mut record = Vec::new();
    append_snapshot_bytes(&mut record, key)?;
    write_u64_be(&mut record, entry.expires_at.unwrap_or(0));
    match &entry.value {
        OnyxValue::Blob(value) => {
            record.push(1);
            append_snapshot_bytes(&mut record, value)?;
        }
        OnyxValue::Int(value) => {
            record.push(2);
            record.extend_from_slice(&value.to_be_bytes());
        }
        OnyxValue::Float(value) => {
            record.push(3);
            write_u64_be(&mut record, value.to_bits());
        }
        OnyxValue::List(values) => {
            record.push(4);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot list is too large"))?,
            );
            for value in values {
                append_snapshot_bytes(&mut record, value)?;
            }
        }
        OnyxValue::Hash(values) => {
            record.push(5);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot hash is too large"))?,
            );
            for (field, value) in values {
                append_snapshot_bytes(&mut record, field)?;
                append_snapshot_bytes(&mut record, value)?;
            }
        }
        OnyxValue::Set(values) => {
            record.push(6);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot set is too large"))?,
            );
            for value in values {
                append_snapshot_bytes(&mut record, value)?;
            }
        }
        OnyxValue::Json(value) => {
            record.push(7);
            let encoded = serde_json::to_vec(value)
                .map_err(|error| PersistenceError::new(error.to_string()))?;
            append_snapshot_bytes(&mut record, &encoded)?;
        }
        OnyxValue::Vector(values) => {
            record.push(8);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot vector is too large"))?,
            );
            for value in values {
                record.extend_from_slice(&value.to_bits().to_be_bytes());
            }
        }
    }
    if record.len() > MAX_SNAPSHOT_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Snapshot entry exceeds the format limit",
        ));
    }
    Ok(record)
}

fn read_snapshot_bytes<'a>(record: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    let length = read_u32_be(record, offset)? as usize;
    let bytes = safe_slice(record, *offset, length)?;
    *offset = offset.checked_add(length)?;
    Some(bytes)
}

pub(crate) fn decode_snapshot_entry(record: &[u8]) -> Result<(Bytes, DataEntry), PersistenceError> {
    let mut offset = 0usize;
    let key = Bytes::copy_from_slice(
        read_snapshot_bytes(record, &mut offset)
            .ok_or_else(|| PersistenceError::new("Invalid snapshot key"))?,
    );
    let expiry = read_u64_be(record, &mut offset)
        .ok_or_else(|| PersistenceError::new("Invalid snapshot expiry"))?;
    let value_type = *record
        .get(offset)
        .ok_or_else(|| PersistenceError::new("Missing snapshot value type"))?;
    offset += 1;

    let read_values = |record: &[u8], offset: &mut usize| {
        let count = read_u32_be(record, offset)
            .ok_or_else(|| PersistenceError::new("Invalid snapshot collection count"))?;
        if count as usize > record.len().saturating_sub(*offset) / 4 {
            return Err(PersistenceError::new(
                "Snapshot collection count exceeds the record bounds",
            ));
        }
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let value = read_snapshot_bytes(record, offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot collection value"))?;
            values.push(Bytes::copy_from_slice(value));
        }
        Ok::<Vec<Bytes>, PersistenceError>(values)
    };

    let value = match value_type {
        1 => OnyxValue::Blob(Bytes::copy_from_slice(
            read_snapshot_bytes(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot blob"))?,
        )),
        2 => {
            let bytes: [u8; 8] = safe_slice(record, offset, 8)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| PersistenceError::new("Invalid snapshot integer"))?;
            offset += 8;
            OnyxValue::Int(i64::from_be_bytes(bytes))
        }
        3 => {
            let bits = read_u64_be(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot float"))?;
            OnyxValue::Float(f64::from_bits(bits))
        }
        4 => OnyxValue::List(read_values(record, &mut offset)?),
        5 => {
            let count = read_u32_be(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot hash count"))?;
            if count as usize > record.len().saturating_sub(offset) / 8 {
                return Err(PersistenceError::new(
                    "Snapshot hash count exceeds the record bounds",
                ));
            }
            let mut values = std::collections::HashMap::with_capacity(count as usize);
            for _ in 0..count {
                let field = Bytes::copy_from_slice(
                    read_snapshot_bytes(record, &mut offset)
                        .ok_or_else(|| PersistenceError::new("Invalid snapshot hash field"))?,
                );
                let value = Bytes::copy_from_slice(
                    read_snapshot_bytes(record, &mut offset)
                        .ok_or_else(|| PersistenceError::new("Invalid snapshot hash value"))?,
                );
                values.insert(field, value);
            }
            OnyxValue::Hash(values)
        }
        6 => OnyxValue::Set(read_values(record, &mut offset)?.into_iter().collect()),
        7 => {
            let bytes = read_snapshot_bytes(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot JSON value"))?;
            OnyxValue::Json(
                serde_json::from_slice(bytes)
                    .map_err(|error| PersistenceError::new(error.to_string()))?,
            )
        }
        8 => {
            let count = read_u32_be(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot vector count"))?
                as usize;
            let byte_length = count
                .checked_mul(4)
                .ok_or_else(|| PersistenceError::new("Snapshot vector length overflow"))?;
            let bytes = safe_slice(record, offset, byte_length)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot vector"))?;
            let values = bytes
                .chunks_exact(4)
                .map(|chunk| {
                    f32::from_bits(u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                })
                .collect();
            offset += byte_length;
            OnyxValue::Vector(values)
        }
        _ => return Err(PersistenceError::new("Unknown snapshot value type")),
    };
    if offset != record.len() {
        return Err(PersistenceError::new("Trailing bytes in snapshot entry"));
    }

    let timestamp = now();
    Ok((
        key,
        DataEntry {
            value,
            expires_at: (expiry != 0).then_some(expiry),
            created_at: timestamp,
            last_accessed: timestamp,
        },
    ))
}

const EFFECT_PUT: u8 = 1;
const EFFECT_DELETE: u8 = 2;

pub(crate) fn encode_committed_batch(batch: &CommittedBatch) -> Result<Vec<u8>, PersistenceError> {
    if batch.effects.is_empty() {
        return Err(PersistenceError::new(
            "Committed-effect batch cannot be empty",
        ));
    }
    let count = u32::try_from(batch.effects.len())
        .map_err(|_| PersistenceError::new("Committed-effect batch is too large"))?;
    let mut encoded = Vec::new();
    write_u32_be(&mut encoded, count);
    for effect in &batch.effects {
        match effect {
            CommittedEffect::Put { key, entry } => {
                encoded.push(EFFECT_PUT);
                let data_entry = DataEntry {
                    value: entry.value.clone(),
                    expires_at: entry.expires_at,
                    created_at: 0,
                    last_accessed: 0,
                };
                let record = encode_snapshot_entry(key, &data_entry)?;
                append_snapshot_bytes(&mut encoded, &record)?;
            }
            CommittedEffect::Delete { key } => {
                encoded.push(EFFECT_DELETE);
                append_snapshot_bytes(&mut encoded, key)?;
            }
        }
    }
    if encoded.len() > MAX_BINLOG_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Committed-effect batch exceeds the binlog record limit",
        ));
    }
    Ok(encoded)
}

pub(crate) fn decode_committed_batch(encoded: &[u8]) -> Result<CommittedBatch, PersistenceError> {
    let mut offset = 0usize;
    let count = read_u32_be(encoded, &mut offset)
        .ok_or_else(|| PersistenceError::new("Missing committed-effect count"))?;
    if count == 0 {
        return Err(PersistenceError::new(
            "Committed-effect batch cannot be empty",
        ));
    }
    if count as usize > encoded.len().saturating_sub(offset) / 5 {
        return Err(PersistenceError::new(
            "Committed-effect count exceeds the record bounds",
        ));
    }

    let mut effects = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let opcode = *encoded
            .get(offset)
            .ok_or_else(|| PersistenceError::new("Missing committed-effect opcode"))?;
        offset += 1;
        let payload = read_snapshot_bytes(encoded, &mut offset)
            .ok_or_else(|| PersistenceError::new("Invalid committed-effect payload"))?;
        match opcode {
            EFFECT_PUT => {
                let (key, entry) = decode_snapshot_entry(payload)?;
                effects.push(CommittedEffect::Put {
                    key,
                    entry: entry.into(),
                });
            }
            EFFECT_DELETE => effects.push(CommittedEffect::Delete {
                key: Bytes::copy_from_slice(payload),
            }),
            _ => {
                return Err(PersistenceError::new(format!(
                    "Unknown committed-effect opcode: {}",
                    opcode
                )));
            }
        }
    }
    if offset != encoded.len() {
        return Err(PersistenceError::new(
            "Trailing bytes in committed-effect batch",
        ));
    }
    CommittedBatch::new(effects)
}
