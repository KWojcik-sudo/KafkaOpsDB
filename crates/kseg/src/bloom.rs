//! Split Block Bloom Filter (same algorithm as Parquet) and exact value sets.

use crate::error::{Result, corrupt};

const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

pub fn hash_value(value: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(value, 0)
}

fn mask(key: u32) -> [u32; 8] {
    let mut m = [0u32; 8];
    for i in 0..8 {
        let y = key.wrapping_mul(SALT[i]);
        m[i] = 1u32 << (y >> 27);
    }
    m
}

#[derive(Debug, Clone)]
pub struct Sbbf {
    blocks: Vec<[u32; 8]>,
}

impl Sbbf {
    pub fn with_ndv(ndv: usize, bits_per_value: u32) -> Self {
        // the block index uses multiply-shift, so any block count works
        let bytes = (ndv as u64 * bits_per_value as u64).div_ceil(8).max(32);
        Self {
            blocks: vec![[0u32; 8]; bytes.div_ceil(32) as usize],
        }
    }

    fn block_index(&self, hash: u64) -> usize {
        (((hash >> 32) * self.blocks.len() as u64) >> 32) as usize
    }

    pub fn insert(&mut self, hash: u64) {
        let idx = self.block_index(hash);
        let m = mask(hash as u32);
        for (w, bit) in self.blocks[idx].iter_mut().zip(m) {
            *w |= bit;
        }
    }

    pub fn contains(&self, hash: u64) -> bool {
        let block = &self.blocks[self.block_index(hash)];
        let m = mask(hash as u32);
        block.iter().zip(m).all(|(w, bit)| w & bit != 0)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.blocks.len() * 32);
        for b in &self.blocks {
            for w in b {
                out.extend_from_slice(&w.to_le_bytes());
            }
        }
        out
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() || !data.len().is_multiple_of(32) {
            return corrupt("invalid bloom filter size");
        }
        let blocks = data
            .chunks_exact(32)
            .map(|c| {
                let mut b = [0u32; 8];
                for (i, w) in c.chunks_exact(4).enumerate() {
                    b[i] = u32::from_le_bytes(w.try_into().unwrap());
                }
                b
            })
            .collect();
        Ok(Self { blocks })
    }
}

/// Checks membership against a serialized sorted value set (u64 LE hashes).
pub fn value_set_contains(data: &[u8], hash: u64) -> Result<bool> {
    if !data.len().is_multiple_of(8) {
        return corrupt("invalid value set size");
    }
    let n = data.len() / 8;
    let get = |i: usize| u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match get(mid).cmp(&hash) {
            std::cmp::Ordering::Equal => return Ok(true),
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    Ok(false)
}

pub fn value_set_bytes(sorted_hashes: &[u64]) -> Vec<u8> {
    sorted_hashes.iter().flat_map(|h| h.to_le_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_reasonable_fpp() {
        let n = 50_000;
        let mut bloom = Sbbf::with_ndv(n, 10);
        for i in 0..n {
            bloom.insert(hash_value(format!("value-{i}").as_bytes()));
        }
        let bloom = Sbbf::from_bytes(&bloom.to_bytes()).unwrap();
        for i in 0..n {
            assert!(bloom.contains(hash_value(format!("value-{i}").as_bytes())));
        }
        let fp = (0..n)
            .filter(|i| bloom.contains(hash_value(format!("other-{i}").as_bytes())))
            .count();
        assert!((fp as f64 / n as f64) < 0.03, "fpp too high: {fp}");
    }

    #[test]
    fn value_set() {
        let mut hashes: Vec<u64> = ["a", "b", "c"]
            .iter()
            .map(|s| hash_value(s.as_bytes()))
            .collect();
        hashes.sort_unstable();
        let data = value_set_bytes(&hashes);
        assert!(value_set_contains(&data, hash_value(b"b")).unwrap());
        assert!(!value_set_contains(&data, hash_value(b"d")).unwrap());
    }
}
