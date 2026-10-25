//! Byte-string encodings: Const, Dict, Uuid16, DecimalText, BytesPlain.

use std::collections::HashMap;

use super::code;
use super::int::{read_ints, write_ints};
use crate::error::{Result, corrupt};
use crate::varint::{Cursor, put_bytes, put_uvarint};

/// Decoded nullable byte column. Null entries have an empty range.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BytesColumn {
    offsets: Vec<u32>,
    data: Vec<u8>,
    validity: Option<Vec<bool>>,
}

impl BytesColumn {
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, i: usize) -> Option<&[u8]> {
        if let Some(v) = &self.validity
            && !v[i]
        {
            return None;
        }
        Some(&self.data[self.offsets[i] as usize..self.offsets[i + 1] as usize])
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.len()).map(|i| self.get(i))
    }

    pub fn null_count(&self) -> usize {
        self.validity
            .as_ref()
            .map_or(0, |v| v.iter().filter(|p| !**p).count())
    }

    fn from_parts(present: Vec<Vec<u8>>, presence: Option<Vec<bool>>) -> Result<Self> {
        let count = presence.as_ref().map_or(present.len(), |p| p.len());
        let mut offsets = Vec::with_capacity(count + 1);
        let mut data = Vec::new();
        offsets.push(0u32);
        let mut it = present.into_iter();
        for i in 0..count {
            let is_present = presence.as_ref().is_none_or(|p| p[i]);
            if is_present {
                let Some(v) = it.next() else {
                    return corrupt("presence/value count mismatch");
                };
                data.extend_from_slice(&v);
            }
            offsets.push(data.len() as u32);
        }
        Ok(Self {
            offsets,
            data,
            validity: presence,
        })
    }

    pub fn from_values<'a>(values: impl IntoIterator<Item = Option<&'a [u8]>>) -> Self {
        let mut offsets = vec![0u32];
        let mut data = Vec::new();
        let mut validity = Vec::new();
        for v in values {
            if let Some(b) = v {
                data.extend_from_slice(b);
            }
            validity.push(v.is_some());
            offsets.push(data.len() as u32);
        }
        let validity = if validity.iter().all(|p| *p) {
            None
        } else {
            Some(validity)
        };
        Self {
            offsets,
            data,
            validity,
        }
    }
}

/// Encodes nullable byte values. Returns (encoding code, payload, null_count).
pub fn encode_bytes(values: &[Option<&[u8]>]) -> (u8, Vec<u8>, usize) {
    let null_count = values.iter().filter(|v| v.is_none()).count();
    let present: Vec<&[u8]> = values.iter().flatten().copied().collect();
    let mut out = Vec::new();
    if null_count > 0 {
        let presence: Vec<i64> = values.iter().map(|v| v.is_some() as i64).collect();
        write_ints(&presence, &mut out);
    }
    let (c, payload) = encode_present(&present);
    out.extend_from_slice(&payload);
    (c, out, null_count)
}

fn encode_present(values: &[&[u8]]) -> (u8, Vec<u8>) {
    if !values.is_empty() && values.iter().all(|v| *v == values[0]) {
        let mut out = Vec::new();
        put_bytes(&mut out, values[0]);
        return (code::CONST, out);
    }
    if let Some(case) = uuid_case(values) {
        let mut out = Vec::with_capacity(1 + values.len() * 16);
        out.push(case);
        for v in values {
            out.extend_from_slice(&parse_uuid(v));
        }
        return (code::UUID16, out);
    }
    if !values.is_empty() && values.iter().all(|v| parse_canonical_decimal(v).is_some()) {
        let ints: Vec<i64> = values
            .iter()
            .map(|v| parse_canonical_decimal(v).unwrap())
            .collect();
        let mut out = Vec::new();
        write_ints(&ints, &mut out);
        return (code::DECIMAL_TEXT, out);
    }
    let plain = encode_plain(values);
    if let Some(dict) = encode_dict(values)
        && dict.len() < plain.len()
    {
        return (code::DICT, dict);
    }
    (code::BYTES_PLAIN, plain)
}

fn encode_plain(values: &[&[u8]]) -> Vec<u8> {
    let lengths: Vec<i64> = values.iter().map(|v| v.len() as i64).collect();
    let mut out = Vec::new();
    write_ints(&lengths, &mut out);
    for v in values {
        out.extend_from_slice(v);
    }
    out
}

fn encode_dict(values: &[&[u8]]) -> Option<Vec<u8>> {
    let mut ids: HashMap<&[u8], i64> = HashMap::new();
    let mut entries: Vec<&[u8]> = Vec::new();
    let mut indexes = Vec::with_capacity(values.len());
    for v in values {
        let next = entries.len() as i64;
        let id = *ids.entry(v).or_insert_with(|| {
            entries.push(v);
            next
        });
        indexes.push(id);
        // dictionaries only pay off with repetition
        if entries.len() > values.len() / 2 + 1 {
            return None;
        }
    }
    let mut out = Vec::new();
    put_uvarint(&mut out, entries.len() as u64);
    for e in &entries {
        put_bytes(&mut out, e);
    }
    write_ints(&indexes, &mut out);
    Some(out)
}

pub fn decode_bytes(c: u8, payload: &[u8], count: usize, null_count: usize) -> Result<BytesColumn> {
    if null_count > count {
        return corrupt("null_count exceeds value_count");
    }
    let mut cur = Cursor::new(payload);
    let presence = if null_count > 0 {
        let p = read_ints(&mut cur, count)?;
        Some(p.into_iter().map(|v| v != 0).collect::<Vec<bool>>())
    } else {
        None
    };
    let n = count - null_count;
    let present: Vec<Vec<u8>> = match c {
        code::CONST => {
            if n == 0 {
                Vec::new()
            } else {
                let v = cur.bytes()?;
                vec![v.to_vec(); n]
            }
        }
        code::BYTES_PLAIN => {
            let lengths = read_ints(&mut cur, n)?;
            let mut out = Vec::with_capacity(n);
            for l in lengths {
                if l < 0 {
                    return corrupt("negative length");
                }
                out.push(cur.take(l as usize)?.to_vec());
            }
            out
        }
        code::DICT => {
            let entries_len = cur.len(cur.remaining())?;
            let mut entries = Vec::with_capacity(entries_len);
            for _ in 0..entries_len {
                entries.push(cur.bytes()?);
            }
            let indexes = read_ints(&mut cur, n)?;
            let mut out = Vec::with_capacity(n);
            for i in indexes {
                match entries.get(i as usize) {
                    Some(e) if i >= 0 => out.push(e.to_vec()),
                    _ => return corrupt("dictionary index out of range"),
                }
            }
            out
        }
        code::UUID16 => {
            let case = cur.u8()?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let raw: [u8; 16] = cur.take(16)?.try_into().unwrap();
                out.push(format_uuid(&raw, case == 1));
            }
            out
        }
        code::DECIMAL_TEXT => read_ints(&mut cur, n)?
            .into_iter()
            .map(|v| v.to_string().into_bytes())
            .collect(),
        other => return corrupt(format!("unknown bytes encoding {other}")),
    };
    if present.len() != n {
        return corrupt("bytes value count mismatch");
    }
    BytesColumn::from_parts(present, presence)
}

/// Returns the case flag (0 = lower, 1 = upper) if every value is a canonical UUID
/// whose letters all share one case, so the text roundtrips byte for byte.
fn uuid_case(values: &[&[u8]]) -> Option<u8> {
    if values.is_empty() {
        return None;
    }
    let mut case: Option<u8> = None;
    for v in values {
        if v.len() != 36 {
            return None;
        }
        for (i, b) in v.iter().enumerate() {
            match i {
                8 | 13 | 18 | 23 => {
                    if *b != b'-' {
                        return None;
                    }
                }
                _ => {
                    let c = match b {
                        b'0'..=b'9' => None,
                        b'a'..=b'f' => Some(0),
                        b'A'..=b'F' => Some(1),
                        _ => return None,
                    };
                    if let Some(c) = c {
                        match case {
                            None => case = Some(c),
                            Some(existing) if existing != c => return None,
                            _ => {}
                        }
                    }
                }
            }
        }
    }
    Some(case.unwrap_or(0))
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

fn parse_uuid(v: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    let mut nibbles = v.iter().filter(|b| **b != b'-');
    for byte in out.iter_mut() {
        let hi = hex_val(*nibbles.next().unwrap());
        let lo = hex_val(*nibbles.next().unwrap());
        *byte = (hi << 4) | lo;
    }
    out
}

fn format_uuid(raw: &[u8; 16], upper: bool) -> Vec<u8> {
    let digits: &[u8; 16] = if upper {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    let mut out = Vec::with_capacity(36);
    for (i, b) in raw.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push(b'-');
        }
        out.push(digits[(b >> 4) as usize]);
        out.push(digits[(b & 0xf) as usize]);
    }
    out
}

fn parse_canonical_decimal(v: &[u8]) -> Option<i64> {
    if v.is_empty() || v.len() > 20 {
        return None;
    }
    let s = std::str::from_utf8(v).ok()?;
    let n: i64 = s.parse().ok()?;
    // reject "+1", "007", "-0" and anything else that would not roundtrip
    if n.to_string() != s {
        return None;
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(values: &[Option<&[u8]>]) -> u8 {
        let (c, payload, nulls) = encode_bytes(values);
        let col = decode_bytes(c, &payload, values.len(), nulls).unwrap();
        let out: Vec<Option<&[u8]>> = col.iter().collect();
        assert_eq!(out, values, "encoding {c}");
        c
    }

    #[test]
    fn encodings() {
        let uuids: Vec<String> = (0..100)
            .map(|i| format!("0192f3a4-5b6c-7d8e-9f00-{i:012x}"))
            .collect();
        let v: Vec<Option<&[u8]>> = uuids.iter().map(|s| Some(s.as_bytes())).collect();
        assert_eq!(roundtrip(&v), code::UUID16);

        let upper: Vec<String> = uuids.iter().map(|s| s.to_uppercase()).collect();
        let v: Vec<Option<&[u8]>> = upper.iter().map(|s| Some(s.as_bytes())).collect();
        assert_eq!(roundtrip(&v), code::UUID16);

        // mixed case must not be normalised
        let mixed = [
            Some(&b"0192f3a4-5b6c-7d8e-9f00-00000000000a"[..]),
            Some(&b"0192F3A4-5B6C-7D8E-9F00-00000000000B"[..]),
        ];
        assert_ne!(roundtrip(&mixed), code::UUID16);

        let nums: Vec<String> = (0..100).map(|i| (i * 7 - 300).to_string()).collect();
        let v: Vec<Option<&[u8]>> = nums.iter().map(|s| Some(s.as_bytes())).collect();
        assert_eq!(roundtrip(&v), code::DECIMAL_TEXT);
        assert_ne!(roundtrip(&[Some(b"007"), Some(b"1")]), code::DECIMAL_TEXT);
        assert_ne!(roundtrip(&[Some(b"-0"), Some(b"1")]), code::DECIMAL_TEXT);

        let kinds = ["application/json", "text/plain", "application/avro"];
        let v: Vec<Option<&[u8]>> = (0..300).map(|i| Some(kinds[i % 3].as_bytes())).collect();
        assert_eq!(roundtrip(&v), code::DICT);

        assert_eq!(roundtrip(&[Some(b"x"), Some(b"x"), None]), code::CONST);
        roundtrip(&[None, None]);
        roundtrip(&[]);
        roundtrip(&[Some(b""), Some(&[0xff, 0x00, 0xfe]), None, Some(b"abc")]);
    }
}
