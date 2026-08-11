use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

pub const OBP_MAGIC: u8 = 0x4F;
pub const OBP_VERSION: u8 = 0x01;
pub const MAX_OBP_ARGUMENTS: usize = 1024;
pub const MAX_OBP_ARGUMENT_SIZE: usize = 512 * 1024;
pub const MAX_OBP_PAYLOAD_SIZE: usize = 512 * 1024;
pub const MAX_OBP_FRAME_SIZE: usize = 1024 * 1024;

const OBP_FIXED_HEADER_SIZE: usize = 11;
const OBP_PAYLOAD_LENGTH_SIZE: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OBPProtocolError {
    InvalidMagic,
    UnsupportedVersion(u8),
    TooManyArguments(usize),
    ArgumentTooLarge(usize),
    PayloadTooLarge(usize),
    FrameTooLarge,
    LengthOverflow,
}

impl fmt::Display for OBPProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "Invalid OBP magic byte"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "Unsupported OBP version: {version}")
            }
            Self::TooManyArguments(count) => {
                write!(
                    formatter,
                    "OBP argument count exceeds the protocol limit: {count}"
                )
            }
            Self::ArgumentTooLarge(length) => {
                write!(
                    formatter,
                    "OBP argument exceeds the protocol limit: {length}"
                )
            }
            Self::PayloadTooLarge(length) => {
                write!(
                    formatter,
                    "OBP payload exceeds the protocol limit: {length}"
                )
            }
            Self::FrameTooLarge => write!(formatter, "OBP frame exceeds the protocol limit"),
            Self::LengthOverflow => write!(formatter, "OBP frame length overflow"),
        }
    }
}

impl std::error::Error for OBPProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OBPFrame {
    pub cmd: u8,
    pub flags: u16,
    pub correlation_id: u32,
    pub args: Vec<Bytes>,
    pub payload: Option<Bytes>,
}

impl OBPFrame {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), OBPProtocolError> {
        if self.args.len() > MAX_OBP_ARGUMENTS {
            return Err(OBPProtocolError::TooManyArguments(self.args.len()));
        }

        let mut frame_size = OBP_FIXED_HEADER_SIZE
            .checked_add(OBP_PAYLOAD_LENGTH_SIZE)
            .ok_or(OBPProtocolError::LengthOverflow)?;
        for argument in &self.args {
            if argument.len() > MAX_OBP_ARGUMENT_SIZE {
                return Err(OBPProtocolError::ArgumentTooLarge(argument.len()));
            }
            frame_size = frame_size
                .checked_add(4)
                .and_then(|size| size.checked_add(argument.len()))
                .ok_or(OBPProtocolError::LengthOverflow)?;
        }
        let payload_length = self.payload.as_ref().map_or(0, Bytes::len);
        if payload_length > MAX_OBP_PAYLOAD_SIZE {
            return Err(OBPProtocolError::PayloadTooLarge(payload_length));
        }
        frame_size = frame_size
            .checked_add(payload_length)
            .ok_or(OBPProtocolError::LengthOverflow)?;
        if frame_size > MAX_OBP_FRAME_SIZE {
            return Err(OBPProtocolError::FrameTooLarge);
        }

        buf.reserve(frame_size);
        buf.put_u8(OBP_MAGIC);
        buf.put_u8(OBP_VERSION);
        buf.put_u8(self.cmd);
        buf.put_u16(self.flags);
        buf.put_u32(self.correlation_id);
        buf.put_u16(self.args.len() as u16);
        for argument in &self.args {
            buf.put_u32(argument.len() as u32);
            buf.put_slice(argument);
        }
        buf.put_u32(payload_length as u32);
        if let Some(payload) = &self.payload {
            buf.put_slice(payload);
        }
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, OBPProtocolError> {
        if let Some(magic) = buf.first()
            && *magic != OBP_MAGIC
        {
            return Err(OBPProtocolError::InvalidMagic);
        }
        if let Some(version) = buf.get(1)
            && *version != OBP_VERSION
        {
            return Err(OBPProtocolError::UnsupportedVersion(*version));
        }
        if buf.len() < OBP_FIXED_HEADER_SIZE {
            return Ok(None);
        }

        let cmd = buf[2];
        let flags = u16::from_be_bytes([buf[3], buf[4]]);
        let correlation_id = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
        let argument_count = u16::from_be_bytes([buf[9], buf[10]]) as usize;
        if argument_count > MAX_OBP_ARGUMENTS {
            return Err(OBPProtocolError::TooManyArguments(argument_count));
        }

        // First validate the complete frame without allocating argument or
        // payload storage. An incomplete peer-controlled frame must not cause
        // the same prefix to be repeatedly copied on every socket read.
        let mut offset = OBP_FIXED_HEADER_SIZE;
        for _ in 0..argument_count {
            let length_end = offset
                .checked_add(4)
                .ok_or(OBPProtocolError::LengthOverflow)?;
            if buf.len() < length_end {
                return Ok(None);
            }
            let argument_length = u32::from_be_bytes(
                buf[offset..length_end]
                    .try_into()
                    .map_err(|_| OBPProtocolError::LengthOverflow)?,
            ) as usize;
            if argument_length > MAX_OBP_ARGUMENT_SIZE {
                return Err(OBPProtocolError::ArgumentTooLarge(argument_length));
            }
            let argument_end = length_end
                .checked_add(argument_length)
                .ok_or(OBPProtocolError::LengthOverflow)?;
            if argument_end > MAX_OBP_FRAME_SIZE {
                return Err(OBPProtocolError::FrameTooLarge);
            }
            if buf.len() < argument_end {
                return Ok(None);
            }
            offset = argument_end;
        }

        let payload_length_end = offset
            .checked_add(OBP_PAYLOAD_LENGTH_SIZE)
            .ok_or(OBPProtocolError::LengthOverflow)?;
        if buf.len() < payload_length_end {
            return Ok(None);
        }
        let payload_length = u32::from_be_bytes(
            buf[offset..payload_length_end]
                .try_into()
                .map_err(|_| OBPProtocolError::LengthOverflow)?,
        ) as usize;
        if payload_length > MAX_OBP_PAYLOAD_SIZE {
            return Err(OBPProtocolError::PayloadTooLarge(payload_length));
        }
        let frame_end = payload_length_end
            .checked_add(payload_length)
            .ok_or(OBPProtocolError::LengthOverflow)?;
        if frame_end > MAX_OBP_FRAME_SIZE {
            return Err(OBPProtocolError::FrameTooLarge);
        }
        if buf.len() < frame_end {
            return Ok(None);
        }

        let mut args = Vec::with_capacity(argument_count);
        let mut argument_offset = OBP_FIXED_HEADER_SIZE;
        for _ in 0..argument_count {
            let length_end = argument_offset + 4;
            let argument_length = u32::from_be_bytes(
                buf[argument_offset..length_end]
                    .try_into()
                    .map_err(|_| OBPProtocolError::LengthOverflow)?,
            ) as usize;
            let argument_end = length_end + argument_length;
            args.push(Bytes::copy_from_slice(&buf[length_end..argument_end]));
            argument_offset = argument_end;
        }
        let payload = (payload_length > 0)
            .then(|| Bytes::copy_from_slice(&buf[payload_length_end..frame_end]));

        buf.advance(frame_end);
        Ok(Some(Self {
            cmd,
            flags,
            correlation_id,
            args,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(cmd: u8, payload: Option<&'static [u8]>) -> OBPFrame {
        OBPFrame {
            cmd,
            flags: 0,
            correlation_id: cmd as u32,
            args: vec![Bytes::from_static(b"argument")],
            payload: payload.map(Bytes::from_static),
        }
    }

    #[test]
    fn payload_bytes_are_consumed_before_the_next_frame() {
        let first = frame(1, Some(b"payload"));
        let second = frame(2, None);
        let mut encoded = BytesMut::new();
        first.encode(&mut encoded).unwrap();
        second.encode(&mut encoded).unwrap();

        let decoded_first = OBPFrame::decode(&mut encoded)
            .unwrap()
            .expect("first frame");
        let decoded_second = OBPFrame::decode(&mut encoded)
            .unwrap()
            .expect("second frame");

        assert_eq!(decoded_first.payload, Some(Bytes::from_static(b"payload")));
        assert_eq!(decoded_second.cmd, 2);
        assert!(encoded.is_empty());
    }

    #[test]
    fn unsupported_protocol_versions_are_rejected() {
        let mut encoded = BytesMut::new();
        frame(1, None).encode(&mut encoded).unwrap();
        encoded[1] = OBP_VERSION + 1;

        assert_eq!(
            OBPFrame::decode(&mut encoded),
            Err(OBPProtocolError::UnsupportedVersion(OBP_VERSION + 1))
        );
    }

    #[test]
    fn incomplete_frames_do_not_consume_input() {
        let mut encoded = BytesMut::new();
        frame(1, Some(b"payload")).encode(&mut encoded).unwrap();
        for length in 0..encoded.len() {
            let mut truncated = BytesMut::from(&encoded[..length]);
            assert_eq!(OBPFrame::decode(&mut truncated).unwrap(), None);
            assert_eq!(&truncated[..], &encoded[..length]);
        }
    }

    #[test]
    fn oversized_counts_and_lengths_are_rejected_from_headers() {
        let mut excessive_count = BytesMut::from(
            &[
                OBP_MAGIC,
                OBP_VERSION,
                1,
                0,
                0,
                0,
                0,
                0,
                1,
                ((MAX_OBP_ARGUMENTS + 1) >> 8) as u8,
                (MAX_OBP_ARGUMENTS + 1) as u8,
            ][..],
        );
        assert!(matches!(
            OBPFrame::decode(&mut excessive_count),
            Err(OBPProtocolError::TooManyArguments(_))
        ));

        let mut excessive_argument = BytesMut::from(
            &[
                OBP_MAGIC,
                OBP_VERSION,
                1,
                0,
                0,
                0,
                0,
                0,
                1,
                0,
                1,
                0,
                8,
                0,
                1,
            ][..],
        );
        assert!(matches!(
            OBPFrame::decode(&mut excessive_argument),
            Err(OBPProtocolError::ArgumentTooLarge(_))
        ));
    }

    #[test]
    fn encoder_rejects_oversized_frames() {
        let oversized = OBPFrame {
            cmd: 1,
            flags: 0,
            correlation_id: 1,
            args: vec![Bytes::from(vec![0; MAX_OBP_ARGUMENT_SIZE + 1])],
            payload: None,
        };
        assert!(matches!(
            oversized.encode(&mut BytesMut::new()),
            Err(OBPProtocolError::ArgumentTooLarge(_))
        ));
    }
}
