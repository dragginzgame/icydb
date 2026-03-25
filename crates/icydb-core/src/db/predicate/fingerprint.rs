//! Module: predicate::fingerprint
//! Responsibility: deterministic predicate hashing for plan signatures.
//! Does not own: predicate normalization or runtime execution.
//! Boundary: used by planner/continuation fingerprinting.

use crate::db::predicate::{Predicate, encoding::encode_predicate_sort_key, normalize};
use sha2::{Digest, Sha256};

/// Hash canonical predicate structure into the plan hash stream.
pub(in crate::db) fn hash_predicate(hasher: &mut Sha256, predicate: &Predicate) {
    let normalized = normalize(predicate);
    hash_predicate_structural(hasher, &normalized);
}

// Hash structural predicate bytes without running normalization.
//
// Predicate sort-key encoding already owns the canonical structural traversal
// for deterministic ordering. Reuse that same byte surface for hashing so the
// predicate subsystem does not carry a second recursive encoding tree.
fn hash_predicate_structural(hasher: &mut Sha256, predicate: &Predicate) {
    hasher.update(encode_predicate_sort_key(predicate));
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{hash_predicate, hash_predicate_structural};
    use crate::{
        db::predicate::{CompareOp, ComparePredicate, Predicate, coercion::CoercionId, normalize},
        value::Value,
    };

    #[test]
    fn hash_predicate_preserves_raw_and_child_order_before_normalization() {
        let left = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        ]);
        let right = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
        ]);

        assert_ne!(digest_structural(&left), digest_structural(&right));
    }

    #[test]
    fn canonical_hash_is_order_insensitive_for_and() {
        let left = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        ]);
        let right = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
        ]);

        assert_eq!(normalize(&left), normalize(&right));
        assert_eq!(digest(&left), digest(&right));
    }

    #[test]
    fn canonical_hash_is_order_insensitive_for_or() {
        let left = Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        ]);
        let right = Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
        ]);

        assert_eq!(normalize(&left), normalize(&right));
        assert_eq!(digest(&left), digest(&right));
    }

    #[test]
    fn canonical_hash_treats_same_field_or_eq_and_in_as_equivalent() {
        let or_eq = Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::Uint(3),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::Uint(1),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::Uint(3),
                CoercionId::Strict,
            )),
        ]);
        let in_list = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::In,
            Value::List(vec![Value::Uint(1), Value::Uint(3)]),
            CoercionId::Strict,
        ));

        assert_eq!(normalize(&or_eq), normalize(&in_list));
        assert_eq!(digest(&or_eq), digest(&in_list));
    }

    #[test]
    fn canonical_hash_is_order_insensitive_for_in_list_literals() {
        let left = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![Value::Uint(3), Value::Uint(1), Value::Uint(2)],
        ));
        let right = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
        ));

        assert_ne!(normalize(&left), normalize(&right));
        assert_eq!(digest(&left), digest(&right));
    }

    #[test]
    fn canonical_hash_normalizes_in_list_duplicate_literals() {
        let left = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![
                Value::Uint(3),
                Value::Uint(1),
                Value::Uint(3),
                Value::Uint(2),
            ],
        ));
        let right = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
        ));

        assert_ne!(normalize(&left), normalize(&right));
        assert_eq!(digest(&left), digest(&right));
    }

    #[test]
    fn canonical_hash_treats_implicit_and_explicit_strict_coercion_as_equivalent() {
        let left = Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Int(7)));
        let right = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Int(7),
            CoercionId::Strict,
        ));

        assert_eq!(normalize(&left), normalize(&right));
        assert_eq!(digest(&left), digest(&right));
    }

    #[test]
    fn canonical_hash_distinguishes_different_coercion_ids() {
        let strict = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Int(7),
            CoercionId::Strict,
        ));
        let numeric_widen = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Int(7),
            CoercionId::NumericWiden,
        ));

        assert_ne!(normalize(&strict), normalize(&numeric_widen));
        assert_ne!(digest(&strict), digest(&numeric_widen));
    }

    fn digest(predicate: &Predicate) -> [u8; 32] {
        let mut hasher = crate::db::codec::new_hash_sha256();
        hash_predicate(&mut hasher, predicate);
        crate::db::codec::finalize_hash_sha256(hasher)
    }

    fn digest_structural(predicate: &Predicate) -> [u8; 32] {
        let mut hasher = crate::db::codec::new_hash_sha256();
        hash_predicate_structural(&mut hasher, predicate);
        crate::db::codec::finalize_hash_sha256(hasher)
    }
}
