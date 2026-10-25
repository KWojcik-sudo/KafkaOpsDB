//! LSB-first bitpacking of unsigned values with a fixed bit width.

use crate::error::{Result, corrupt};

pub fn bit_width(max: u64) -> u8 {
    (64 - max.leading_zeros()) as u8
}

pub fn packed_len(count: usize, width: u8) -> usize {
    (count * width as usize).div_ceil(8)
}

pub fn pack(values: impl IntoIterator<Item = u64>, width: u8, out: &mut Vec<u8>) {
    if width == 0 {
        return;
    }
    let mut acc: u128 = 0;
    let mut bits: u32 = 0;
    for v in values {
        acc |= (v as u128) << bits;
        bits += width as u32;
        while bits >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        out.push(acc as u8);
    }
}

/// Unpacks `count` values; returns them and the number of bytes consumed.
pub fn unpack(data: &[u8], width: u8, count: usize, out: &mut Vec<u64>) -> Result<usize> {
    if width > 64 {
        return corrupt(format!("invalid bit width {width}"));
    }
    if width == 0 {
        out.extend(std::iter::repeat_n(0, count));
        return Ok(0);
    }
    let len = packed_len(count, width);
    if data.len() < len {
        return corrupt("bitpacked data truncated");
    }
    let mask: u128 = if width == 64 {
        u64::MAX as u128
    } else {
        (1u128 << width) - 1
    };
    let mut acc: u128 = 0;
    let mut bits: u32 = 0;
    let mut idx = 0usize;
    out.reserve(count);
    for _ in 0..count {
        while bits < width as u32 {
            acc |= (data[idx] as u128) << bits;
            idx += 1;
            bits += 8;
        }
        out.push((acc & mask) as u64);
        acc >>= width;
        bits -= width as u32;
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_widths() {
        for width in 0..=64u8 {
            let max = if width == 64 {
                u64::MAX
            } else {
                (1u64 << width) - 1
            };
            let values: Vec<u64> = (0..1000u64)
                .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) & max)
                .collect();
            let mut buf = Vec::new();
            pack(values.iter().copied(), width, &mut buf);
            assert_eq!(buf.len(), packed_len(values.len(), width));
            let mut out = Vec::new();
            let used = unpack(&buf, width, values.len(), &mut out).unwrap();
            assert_eq!(used, buf.len());
            assert_eq!(out, values);
        }
    }
}
