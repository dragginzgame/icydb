//! Module: query::fingerprint
//! Responsibility: deterministic query-plan fingerprint/signature primitives.
//! Does not own: explain projection construction or query-plan validation.
//! Boundary: hash surface over planner-owned contracts for plan identity checks.

mod aggregate_hash;
pub(crate) mod fingerprint;
pub(crate) mod hash_parts;
mod projection_hash;
mod shape_signature;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use crate::db::codec::{finalize_hash_sha256, new_hash_sha256_prefixed};
use sha2::Sha256;

#[cfg(all(test, feature = "sql"))]
pub(in crate::db) use projection_hash::projection_hash_for_test;

const PLAN_FINGERPRINT_PROFILE_TAG_V2: &[u8] = b"planfp:v2";
const CONTINUATION_SIGNATURE_PROFILE_TAG_V1: &[u8] = b"contsig:v1";

// Build one SHA256 stream pre-seeded with the plan fingerprint profile tag.
pub(in crate::db::query::fingerprint) fn new_plan_fingerprint_hasher_v2() -> Sha256 {
    new_hash_sha256_prefixed(PLAN_FINGERPRINT_PROFILE_TAG_V2)
}

// Build one SHA256 stream pre-seeded with the continuation-signature profile tag.
pub(in crate::db::query::fingerprint) fn new_continuation_signature_hasher_v1() -> Sha256 {
    new_hash_sha256_prefixed(CONTINUATION_SIGNATURE_PROFILE_TAG_V1)
}

// Finalize one SHA256 stream into a fixed-width fingerprint/signature payload.
pub(in crate::db::query::fingerprint) fn finalize_sha256_digest(hasher: Sha256) -> [u8; 32] {
    finalize_hash_sha256(hasher)
}
