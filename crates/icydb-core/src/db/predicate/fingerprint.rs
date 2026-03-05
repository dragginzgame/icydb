//! Module: predicate::fingerprint
//! Responsibility: deterministic predicate hashing for plan signatures.
//! Does not own: predicate normalization or runtime execution.
//! Boundary: used by planner/continuation fingerprinting.

use crate::db::predicate::{Predicate, encoding::hash_predicate_fingerprint};
use sha2::Sha256;

/// Hash predicate structure into the plan hash stream.
pub(in crate::db) fn hash_predicate(hasher: &mut Sha256, predicate: &Predicate) {
    hash_predicate_fingerprint(hasher, predicate);
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::hash_predicate;
    use crate::{
        db::predicate::{ComparePredicate, Predicate, normalize},
        value::Value,
    };
    use sha2::{Digest, Sha256};

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

        assert_ne!(digest(&left), digest(&right));
    }

    #[test]
    fn normalized_and_hash_is_order_insensitive() {
        let left = normalize(&Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        ]));
        let right = normalize(&Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
        ]));

        assert_eq!(left, right);
        assert_eq!(digest(&left), digest(&right));
    }

    #[test]
    fn normalized_or_hash_is_order_insensitive() {
        let left = normalize(&Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        ]));
        let right = normalize(&Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
        ]));

        assert_eq!(left, right);
        assert_eq!(digest(&left), digest(&right));
    }

    fn digest(predicate: &Predicate) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hash_predicate(&mut hasher, predicate);
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        out
    }
}
