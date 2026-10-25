//! Column chunk encodings and compression (see docs/format-kseg.md §6).

pub mod bytes;
pub mod int;

pub mod code {
    pub const CONST: u8 = 0;
    pub const BIT_PACKED_FOR: u8 = 1;
    pub const DELTA_OF_DELTA: u8 = 2;
    pub const OFFSET_GAPS: u8 = 3;
    pub const RLE: u8 = 4;
    pub const BITMAP: u8 = 5;
    pub const BYTES_PLAIN: u8 = 6;
    pub const DICT: u8 = 7;
    pub const UUID16: u8 = 8;
    pub const DECIMAL_TEXT: u8 = 9;
}

pub mod compression {
    pub const NONE: u8 = 0;
    pub const ZSTD: u8 = 1;
}

use crate::error::{Result, corrupt};

/// Compresses `payload` with zstd unless it does not pay off.
/// Returns (compression code, bytes on disk).
pub fn compress(payload: Vec<u8>, zstd_level: i32) -> Result<(u8, Vec<u8>)> {
    if payload.len() < 64 {
        return Ok((compression::NONE, payload));
    }
    let compressed = zstd::bulk::compress(&payload, zstd_level)?;
    if compressed.len() as f64 > payload.len() as f64 * 0.9 {
        Ok((compression::NONE, payload))
    } else {
        Ok((compression::ZSTD, compressed))
    }
}

pub fn decompress(compression: u8, data: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
    match compression {
        compression::NONE => Ok(data.to_vec()),
        compression::ZSTD => {
            let out = zstd::bulk::decompress(data, uncompressed_len)?;
            if out.len() != uncompressed_len {
                return corrupt("decompressed length mismatch");
            }
            Ok(out)
        }
        other => corrupt(format!("unknown compression {other}")),
    }
}
