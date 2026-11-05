//! Test-only helpers for the parent module.

use super::*;

/// Return a canonical SHA-256 predicate digest for structural-signature tests.
pub(in crate::db) fn predicate_fingerprint(predicate: &Predicate) -> [u8; 32] {
    let mut hasher = crate::db::codec::new_hash_sha256();
    crate::db::query::preparation::with_preparation_work(|work| {
        hash_predicate(&mut hasher, predicate, work).expect("fixture hash fits");
    });

    crate::db::codec::finalize_hash_sha256(hasher)
}
