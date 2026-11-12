//! Test-only helpers for the parent module.

use super::*;

/// Return a canonical SHA-256 predicate digest for structural-signature tests.
pub(in crate::db) fn predicate_fingerprint(predicate: &Predicate) -> [u8; 32] {
    let mut hasher = crate::db::codec::new_hash_sha256();
    crate::db::query::preparation::with_preparation_work(|work| {
        // Raw fixture syntax enters the same normalization owner as planning.
        let normalized = crate::db::predicate::normalize_with_budget(
            work.copy_predicate(predicate).expect("fixture copy fits"),
            work,
        )
        .expect("fixture normalization fits");
        hash_canonical_predicate(&mut hasher, &normalized, work).expect("fixture hash fits");
    });

    crate::db::codec::finalize_hash_sha256(hasher)
}
