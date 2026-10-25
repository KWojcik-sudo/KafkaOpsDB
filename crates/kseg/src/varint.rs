//! LEB128 varints, zigzag and a bounds-checked read cursor.

use crate::error::{Result, corrupt};

pub fn put_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

pub fn put_zvarint(buf: &mut Vec<u8>, v: i64) {
    put_uvarint(buf, zigzag(v));
}

pub fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

pub fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_bytes(buf, s.as_bytes());
}

pub struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn rest(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return corrupt(format!(
                "unexpected end of data: need {n} bytes, have {}",
                self.remaining()
            ));
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn advance(&mut self, n: usize) -> Result<()> {
        self.take(n).map(|_| ())
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u16_le(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn u32_le(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64_le(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn uvarint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            let b = self.u8()?;
            if shift == 63 && b > 1 {
                return corrupt("varint overflow");
            }
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift > 63 {
                return corrupt("varint too long");
            }
        }
    }

    pub fn zvarint(&mut self) -> Result<i64> {
        Ok(unzigzag(self.uvarint()?))
    }

    /// Reads a varint that is used as a length or count, bounded by `max`.
    pub fn len(&mut self, max: usize) -> Result<usize> {
        let v = self.uvarint()?;
        if v > max as u64 {
            return corrupt(format!("length {v} exceeds limit {max}"));
        }
        Ok(v as usize)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.len(self.remaining())?;
        self.take(n)
    }

    pub fn string(&mut self) -> Result<String> {
        let b = self.bytes()?;
        match std::str::from_utf8(b) {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => corrupt("invalid utf-8 string"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let values = [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX];
        let mut buf = Vec::new();
        for v in values {
            put_uvarint(&mut buf, v);
        }
        let ivalues = [0i64, -1, 1, i64::MIN, i64::MAX, -123456789];
        for v in ivalues {
            put_zvarint(&mut buf, v);
        }
        let mut c = Cursor::new(&buf);
        for v in values {
            assert_eq!(c.uvarint().unwrap(), v);
        }
        for v in ivalues {
            assert_eq!(c.zvarint().unwrap(), v);
        }
        assert_eq!(c.remaining(), 0);
        assert!(c.u8().is_err());
    }
}
