use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

pub use xxhash_rust::xxh3::Xxh3;

/// Return one deterministic `u64` xxh3 digest for the provided bytes.
#[must_use]
pub fn hash_u64(bytes: &[u8]) -> u64 {
    xxh3_64(bytes)
}

/// Return one deterministic `u128` xxh3 digest for the provided bytes.
#[must_use]
pub fn hash_u128(bytes: &[u8]) -> u128 {
    xxh3_128(bytes)
}
