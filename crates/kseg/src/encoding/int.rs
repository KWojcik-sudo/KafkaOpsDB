//! Integer encodings: Const, BitPackedFor, DeltaOfDelta, OffsetGaps, Rle, Bitmap.

use super::code;
use crate::bitpack;
use crate::error::{Result, corrupt};
use crate::varint::{Cursor, put_uvarint, put_zvarint};

const PAGE: usize = 1024;

/// Picks the smallest applicable encoding for `values`.
pub fn encode_ints(values: &[i64]) -> (u8, Vec<u8>) {
    if values.is_empty() {
        return (code::BIT_PACKED_FOR, Vec::new());
    }
    if values.iter().all(|v| *v == values[0]) {
        let mut out = Vec::new();
        put_zvarint(&mut out, values[0]);
        return (code::CONST, out);
    }
    let mut best = (code::BIT_PACKED_FOR, encode_for(values));
    let mut consider = |c: u8, payload: Vec<u8>| {
        if payload.len() < best.1.len() {
            best = (c, payload);
        }
    };
    if values.len() >= 2 {
        consider(code::DELTA_OF_DELTA, encode_dod(values));
    }
    consider(code::RLE, encode_rle(values));
    if values.iter().all(|v| *v == 0 || *v == 1) {
        let mut out = Vec::new();
        bitpack::pack(values.iter().map(|v| *v as u64), 1, &mut out);
        consider(code::BITMAP, out);
    }
    best
}

/// Writes a self-describing nested int sequence: encoding code byte + payload.
pub fn write_ints(values: &[i64], out: &mut Vec<u8>) {
    let (c, payload) = encode_ints(values);
    out.push(c);
    out.extend_from_slice(&payload);
}

pub fn read_ints(cur: &mut Cursor<'_>, count: usize) -> Result<Vec<i64>> {
    let c = cur.u8()?;
    decode_ints(c, cur, count)
}

pub fn decode_ints(c: u8, cur: &mut Cursor<'_>, count: usize) -> Result<Vec<i64>> {
    match c {
        code::CONST => {
            let v = cur.zvarint()?;
            Ok(vec![v; count])
        }
        code::BIT_PACKED_FOR => decode_for(cur, count),
        code::DELTA_OF_DELTA => decode_dod(cur, count),
        code::RLE => decode_rle(cur, count),
        code::BITMAP => {
            let mut raw = Vec::with_capacity(count);
            let used = bitpack::unpack(cur.rest(), 1, count, &mut raw)?;
            cur.advance(used)?;
            Ok(raw.into_iter().map(|v| v as i64).collect())
        }
        other => corrupt(format!("unknown int encoding {other}")),
    }
}

fn encode_for(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for page in values.chunks(PAGE) {
        let min = *page.iter().min().unwrap();
        let max = *page.iter().max().unwrap();
        let range = (max as i128 - min as i128) as u64;
        let width = bitpack::bit_width(range);
        put_zvarint(&mut out, min);
        out.push(width);
        bitpack::pack(
            page.iter().map(|v| (*v as i128 - min as i128) as u64),
            width,
            &mut out,
        );
    }
    out
}

fn decode_for(cur: &mut Cursor<'_>, count: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(count);
    let mut raw = Vec::with_capacity(PAGE);
    let mut left = count;
    while left > 0 {
        let n = left.min(PAGE);
        let min = cur.zvarint()?;
        let width = cur.u8()?;
        raw.clear();
        let used = bitpack::unpack(cur.rest(), width, n, &mut raw)?;
        cur.advance(used)?;
        out.extend(raw.iter().map(|x| (min as i128 + *x as i128) as i64));
        left -= n;
    }
    Ok(out)
}

fn encode_dod(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    put_zvarint(&mut out, values[0]);
    let first_delta = values[1].wrapping_sub(values[0]);
    put_zvarint(&mut out, first_delta);
    let mut prev_delta = first_delta;
    let mut dods = Vec::with_capacity(values.len().saturating_sub(2));
    for w in values.windows(2).skip(1) {
        let d = w[1].wrapping_sub(w[0]);
        dods.push(d.wrapping_sub(prev_delta));
        prev_delta = d;
    }
    out.extend_from_slice(&encode_for(&dods));
    out
}

fn decode_dod(cur: &mut Cursor<'_>, count: usize) -> Result<Vec<i64>> {
    if count < 2 {
        return corrupt("delta-of-delta needs at least 2 values");
    }
    let first = cur.zvarint()?;
    let mut delta = cur.zvarint()?;
    let dods = decode_for(cur, count - 2)?;
    let mut out = Vec::with_capacity(count);
    out.push(first);
    let mut prev = first.wrapping_add(delta);
    out.push(prev);
    for dod in dods {
        delta = delta.wrapping_add(dod);
        prev = prev.wrapping_add(delta);
        out.push(prev);
    }
    Ok(out)
}

fn encode_rle(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < values.len() {
        let v = values[i];
        let mut j = i + 1;
        while j < values.len() && values[j] == v {
            j += 1;
        }
        put_uvarint(&mut out, (j - i) as u64);
        put_zvarint(&mut out, v);
        i = j;
    }
    out
}

fn decode_rle(cur: &mut Cursor<'_>, count: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let run = cur.len(count - out.len())?;
        if run == 0 {
            return corrupt("zero-length rle run");
        }
        let v = cur.zvarint()?;
        out.extend(std::iter::repeat_n(v, run));
    }
    Ok(out)
}

/// Offsets of one partition block: strictly increasing, starting at `first_offset`
/// (stored in the block metadata). Only gaps (deltas > 1) are written.
pub fn encode_offsets(offsets: &[i64]) -> Vec<u8> {
    let mut gaps: Vec<(u64, u64)> = Vec::new();
    let mut last_gap_row = 0usize;
    for (i, w) in offsets.windows(2).enumerate() {
        let extra = w[1] - w[0] - 1;
        if extra > 0 {
            let row = i + 1;
            gaps.push(((row - last_gap_row) as u64, extra as u64));
            last_gap_row = row;
        }
    }
    let mut out = Vec::new();
    put_uvarint(&mut out, gaps.len() as u64);
    for (row_delta, extra) in gaps {
        put_uvarint(&mut out, row_delta);
        put_uvarint(&mut out, extra);
    }
    out
}

pub fn decode_offsets(cur: &mut Cursor<'_>, first_offset: i64, count: usize) -> Result<Vec<i64>> {
    let gap_count = cur.len(count)?;
    let mut gaps = Vec::with_capacity(gap_count);
    let mut row = 0usize;
    for _ in 0..gap_count {
        row += cur.len(count)?;
        if row >= count {
            return corrupt("offset gap row out of range");
        }
        gaps.push((row, cur.uvarint()? as i64));
    }
    let mut out = Vec::with_capacity(count);
    let mut gi = 0;
    let mut cur_off = first_offset;
    for i in 0..count {
        if i > 0 {
            cur_off += 1;
        }
        if gi < gaps.len() && gaps[gi].0 == i {
            cur_off += gaps[gi].1;
            gi += 1;
        }
        out.push(cur_off);
    }
    Ok(out)
}

/// Nullable ints: presence bitmap (nested ints) when there are nulls, then the values.
pub fn encode_nullable_ints(values: &[Option<i64>]) -> (u8, Vec<u8>, usize) {
    let null_count = values.iter().filter(|v| v.is_none()).count();
    let present: Vec<i64> = values.iter().flatten().copied().collect();
    let mut out = Vec::new();
    if null_count > 0 {
        let presence: Vec<i64> = values.iter().map(|v| v.is_some() as i64).collect();
        write_ints(&presence, &mut out);
    }
    let (c, payload) = encode_ints(&present);
    out.extend_from_slice(&payload);
    (c, out, null_count)
}

pub fn decode_nullable_ints(
    c: u8,
    payload: &[u8],
    count: usize,
    null_count: usize,
) -> Result<Vec<Option<i64>>> {
    let mut cur = Cursor::new(payload);
    if null_count == 0 {
        return Ok(decode_ints(c, &mut cur, count)?
            .into_iter()
            .map(Some)
            .collect());
    }
    let presence = read_ints(&mut cur, count)?;
    let present = decode_ints(c, &mut cur, count - null_count)?;
    let mut it = present.into_iter();
    presence
        .into_iter()
        .map(|p| {
            if p != 0 {
                it.next()
                    .map(Some)
                    .ok_or_else(|| crate::Error::Corrupt("presence/value count mismatch".into()))
            } else {
                Ok(None)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(values: &[i64]) -> u8 {
        let (c, payload) = encode_ints(values);
        let mut cur = Cursor::new(&payload);
        let out = decode_ints(c, &mut cur, values.len()).unwrap();
        assert_eq!(out, values, "encoding {c}");
        assert_eq!(cur.remaining(), 0);
        // every applicable encoding must roundtrip as well
        for (code, payload) in [
            (code::BIT_PACKED_FOR, encode_for(values)),
            (code::RLE, encode_rle(values)),
        ] {
            let mut cur = Cursor::new(&payload);
            assert_eq!(decode_ints(code, &mut cur, values.len()).unwrap(), values);
        }
        if values.len() >= 2 {
            let payload = encode_dod(values);
            let mut cur = Cursor::new(&payload);
            assert_eq!(
                decode_ints(code::DELTA_OF_DELTA, &mut cur, values.len()).unwrap(),
                values
            );
        }
        c
    }

    #[test]
    fn picks_sensible_encodings() {
        assert_eq!(roundtrip(&[7; 100]), code::CONST);
        let ts: Vec<i64> = (0..5000).map(|i| 1_761_379_200_000 + i * 10).collect();
        assert_eq!(roundtrip(&ts), code::DELTA_OF_DELTA);
        let runs: Vec<i64> = (0..5000).map(|i| i / 1000).collect();
        assert_eq!(roundtrip(&runs), code::RLE);
        roundtrip(&[i64::MIN, i64::MAX, 0, -1, 1]);
        roundtrip(&[0, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0]);
        roundtrip(&[]);
        roundtrip(&[42]);
    }

    #[test]
    fn offsets_with_gaps() {
        let offsets = vec![100, 101, 102, 105, 106, 110, 111, 112];
        let payload = encode_offsets(&offsets);
        let mut cur = Cursor::new(&payload);
        assert_eq!(
            decode_offsets(&mut cur, 100, offsets.len()).unwrap(),
            offsets
        );
        let dense: Vec<i64> = (5..70_000).collect();
        let payload = encode_offsets(&dense);
        assert_eq!(payload.len(), 1);
        let mut cur = Cursor::new(&payload);
        assert_eq!(decode_offsets(&mut cur, 5, dense.len()).unwrap(), dense);
    }

    #[test]
    fn nullable() {
        let values = vec![Some(5), None, Some(7), None, None, Some(5)];
        let (c, payload, nulls) = encode_nullable_ints(&values);
        assert_eq!(nulls, 3);
        assert_eq!(
            decode_nullable_ints(c, &payload, values.len(), nulls).unwrap(),
            values
        );
        let all_null = vec![None; 4];
        let (c, payload, nulls) = encode_nullable_ints(&all_null);
        assert_eq!(
            decode_nullable_ints(c, &payload, 4, nulls).unwrap(),
            all_null
        );
    }
}
