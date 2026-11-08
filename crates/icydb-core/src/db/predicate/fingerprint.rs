//! Module: predicate::fingerprint
//! Responsibility: deterministic predicate hashing for plan signatures.
//! Does not own: predicate normalization or runtime execution.
//! Boundary: separate in-memory cache identity from planner/continuation hashing.

#[cfg(test)]
mod admission_tests;

use crate::{
    db::{
        predicate::{
            Predicate,
            encoding::{
                normalized_predicate_key_capacity, raw_predicate_key_capacity,
                write_normalized_predicate_sort_key, write_predicate_sort_key,
            },
            normalize_with_budget,
        },
        resource::construction::ConstructionBudget,
    },
    error::InternalError,
};
use sha2::{Digest, Sha256};

/// Hash canonical predicate structure into the plan hash stream.
pub(in crate::db) fn hash_predicate(
    hasher: &mut Sha256,
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    // Copy, pair simplification and final encoding share the caller's authority.
    let normalized = normalize_with_budget(budget.copy_predicate(predicate)?, budget)?;
    hash_predicate_structural(hasher, &normalized, budget)
}

/// Return a BLAKE3 digest of canonical bytes for in-memory cache identity only.
pub(in crate::db) fn predicate_fingerprint_normalized(
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<[u8; 32], InternalError> {
    let capacity = normalized_predicate_key_capacity(predicate, budget)?;
    let mut encoded = budget.vec_with_capacity(capacity)?;
    write_normalized_predicate_sort_key(&mut encoded, predicate);
    debug_assert!(encoded.len() <= capacity);
    Ok(*blake3::hash(&encoded).as_bytes())
}

// Hash structural predicate bytes without running normalization.
//
// Predicate sort-key encoding already owns the canonical structural traversal
// for deterministic ordering. Reuse that same byte surface for hashing so the
// predicate subsystem does not carry a second recursive encoding tree.
fn hash_predicate_structural(
    hasher: &mut Sha256,
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    let capacity = raw_predicate_key_capacity(predicate, budget)?;
    let mut encoded = budget.vec_with_capacity(capacity)?;
    write_predicate_sort_key(&mut encoded, predicate);
    debug_assert!(encoded.len() <= capacity);
    hasher.update(encoded);
    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::predicate_fingerprint;
