//! Module: query::fingerprint
//! Responsibility: deterministic query-plan fingerprint/signature primitives.
//! Does not own: explain projection construction or query-plan validation.
//! Boundary: hash surface over planner-owned contracts for plan identity checks.

mod aggregate_hash;
mod fingerprint;
mod hash_sections;
mod projection_hash;
mod shape_signature;
#[cfg(test)]
mod tests;

use crate::db::codec::{finalize_hash_sha256, new_hash_sha256_prefixed};
#[cfg(feature = "sql")]
use crate::db::query::{
    fingerprint::projection_hash::hash_scalar_filter_expr_structural_fingerprint, plan::expr::Expr,
};
use sha2::Sha256;

const PLAN_FINGERPRINT_PROFILE_TAG: &[u8] = b"planfp";
const CONTINUATION_SIGNATURE_PROFILE_TAG: &[u8] = b"contsig";
#[cfg(feature = "sql")]
const RESUMABLE_UPDATE_SCOPE_FINGERPRINT_PROFILE_TAG: &[u8] = b"resumable_update_scope_v1";

// Build one SHA256 stream pre-seeded with the plan fingerprint profile tag.
pub(in crate::db::query::fingerprint) fn new_plan_fingerprint_hasher() -> Sha256 {
    new_hash_sha256_prefixed(PLAN_FINGERPRINT_PROFILE_TAG)
}

// Build one SHA256 stream pre-seeded with the continuation-signature profile tag.
pub(in crate::db::query::fingerprint) fn new_continuation_signature_hasher() -> Sha256 {
    new_hash_sha256_prefixed(CONTINUATION_SIGNATURE_PROFILE_TAG)
}

// Finalize one SHA256 stream into a fixed-width fingerprint/signature payload.
pub(in crate::db::query::fingerprint) fn finalize_sha256_digest(hasher: Sha256) -> [u8; 32] {
    finalize_hash_sha256(hasher)
}

/// Fingerprint one canonical scalar scope expression for resumable updates.
///
/// This uses the same semantic expression encoder as plan and continuation
/// identity, but a distinct domain tag prevents scope identity from being
/// confused with a complete query-plan fingerprint.
#[must_use]
#[cfg(feature = "sql")]
pub(in crate::db) fn resumable_update_scope_fingerprint(expr: &Expr) -> [u8; 32] {
    let mut hasher = new_hash_sha256_prefixed(RESUMABLE_UPDATE_SCOPE_FINGERPRINT_PROFILE_TAG);
    hash_scalar_filter_expr_structural_fingerprint(&mut hasher, expr);

    finalize_hash_sha256(hasher)
}
