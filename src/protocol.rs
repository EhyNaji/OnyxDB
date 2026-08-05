use bytes::{Buf, BufMut, Bytes, BytesMut};
pub const OBP_MAGIC: u8 = 0x4F;
pub const OBP_VERSION: u8 = 0x01;
pub struct OBPFrame {
    pub cmd: u8,
    pub flags: u16,
    pub correlation_id: u32,
    pub args: Vec<Bytes>,
    pub payload: Option<Bytes>,
}
impl OBPFrame {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.reserve(16 + self.args.iter().map(|a| 4 + a.len()).sum::<usize>());
        buf.put_u8(OBP_MAGIC);
        buf.put_u8(OBP_VERSION);
        buf.put_u8(self.cmd);
        buf.put_u16(self.flags);
        buf.put_u32(self.correlation_id);
        buf.put_u16(self.args.len() as u16);
        for arg in &self.args {
            buf.put_u32(arg.len() as u32);
            buf.put_slice(arg);
        }
        if let Some(ref payload) = self.payload {
            buf.put_u32(payload.len() as u32);
            buf.put_slice(payload);
        } else {
            buf.put_u32(0);
        }
    }
    pub fn decode(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < 12 {
            return None;
        }
        let mut cursor = std::io::Cursor::new(&buf[..]);
        let magic = cursor.get_u8();
        if magic != OBP_MAGIC {
            return None;
        }
        let _version = cursor.get_u8();
        let cmd = cursor.get_u8();
        let flags = cursor.get_u16();
        let correlation_id = cursor.get_u32();
        let num_args = cursor.get_u16() as usize;
        let mut args = Vec::with_capacity(num_args);
        for _ in 0..num_args {
            if buf.len() < cursor.position() as usize + 4 {
                return None;
            }
            let len = cursor.get_u32() as usize;
            if buf.len() < cursor.position() as usize + len {
                return None;
            }
            let start = cursor.position() as usize;
            args.push(Bytes::copy_from_slice(&buf[start..start + len]));
            cursor.set_position((start + len) as u64);
        }
        if buf.len() < cursor.position() as usize + 4 {
            return None;
        }
        let payload_len = cursor.get_u32() as usize;
        let payload = if payload_len > 0 {
            if buf.len() < cursor.position() as usize + payload_len {
                return None;
            }
            let start = cursor.position() as usize;
            Some(Bytes::copy_from_slice(&buf[start..start + payload_len]))
        } else {
            None
        };
        let consumed = cursor.position() as usize;
        buf.advance(consumed);
        Some(Self {
            cmd,
            flags,
            correlation_id,
            args,
            payload,
        })
    }
}
