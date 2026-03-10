//! Module: db::codec::hash_stream
//! Responsibility: module-local ownership and contracts for db::codec::hash_stream.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Shared hash-stream primitive writers for deterministic SHA256 contracts.

use sha2::{Digest, Sha256};

/// Build one empty SHA256 stream.
pub(in crate::db) fn new_hash_sha256() -> Sha256 {
    Sha256::new()
}

/// Build one SHA256 stream pre-seeded with a profile tag.
pub(in crate::db) fn new_hash_sha256_prefixed(prefix: &[u8]) -> Sha256 {
    let mut hasher = new_hash_sha256();
    hasher.update(prefix);
    hasher
}

/// Write one tag byte into a hash stream.
pub(in crate::db) fn write_hash_tag_u8(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

/// Write one exact `u32` value in network byte order into a hash stream.
pub(in crate::db) fn write_hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

/// Write one exact `u64` value in network byte order into a hash stream.
pub(in crate::db) fn write_hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

/// Write one saturating `usize` length as `u32` into a hash stream.
pub(in crate::db) fn write_hash_len_u32(hasher: &mut Sha256, len: usize) {
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    write_hash_u32(hasher, len);
}

/// Write one UTF-8 string with a `u32` length prefix into a hash stream.
pub(in crate::db) fn write_hash_str_u32(hasher: &mut Sha256, value: &str) {
    write_hash_len_u32(hasher, value.len());
    hasher.update(value.as_bytes());
}

/// Finalize one SHA256 stream into a fixed-width digest payload.
pub(in crate::db) fn finalize_hash_sha256(hasher: Sha256) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}
