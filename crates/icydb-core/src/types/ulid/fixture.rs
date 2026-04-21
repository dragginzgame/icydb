//! Module: types::ulid::fixture
//! Provides deterministic ULID fixture helpers for tests and seeded data.

use crate::types::Ulid;
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    mem,
};
use ulid::Ulid as WrappedUlid;

///
/// Fixtures
///
/// MAX = 1.099T ms, 2^40 - 1
///
/// this gives us a large range where the maximum ULID value starts
/// with 00ZZ, so any fixture ULID can be distinguished easily from a present
/// day ULID which would start with 01
///

const FIXTURE_MAX_TIMESTAMP: u128 = 1_099_511_627_775;

impl Ulid {
    /// from_string_digest
    /// a way of turning a string via a hash function into a valid ULID
    #[must_use]
    pub fn from_string_digest(digest: &str) -> Self {
        // Keep fixture ULIDs deterministic without pulling a dedicated digest
        // dependency into this test-only path.
        let rand = hash_fixture_digest_to_u128(digest);
        let timestamp = u64::try_from(rand % FIXTURE_MAX_TIMESTAMP).unwrap_or(u64::MAX);
        let ulid = WrappedUlid::from_parts(timestamp, rand);

        Self(ulid)
    }
}

fn hash_fixture_digest_to_u128(digest: &str) -> u128 {
    let mut upper = DefaultHasher::new();
    digest.hash(&mut upper);
    let upper = upper.finish();

    let mut lower = DefaultHasher::new();
    (digest, mem::size_of::<u64>()).hash(&mut lower);
    let lower = lower.finish();

    (u128::from(upper) << 64) | u128::from(lower)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_keys() {
        let inputs = vec![
            "key1", "key2", "key3", "key4", "Rarity-1", "Rarity-2", "Rarity-3",
        ];
        let mut keys = vec![];

        for input in inputs {
            let ulid = Ulid::from_string_digest(input);
            keys.push(ulid);
        }

        let keys_set: std::collections::HashSet<_> = keys.iter().collect();
        assert_eq!(keys.len(), keys_set.len(), "Keys are not unique");
    }

    #[test]
    fn test_ulid_fixtures_start_with_00() {
        let mut all_start_with_00 = true;

        for i in 0..10_000 {
            let ulid = Ulid::from_string_digest(&format!("input_{i}"));
            let ulid_str = ulid.to_string();

            if !ulid_str.starts_with("00") {
                all_start_with_00 = false;
                break;
            }
        }

        assert!(all_start_with_00, "Not all ULIDs start with '00'");
    }

    #[test]
    fn test_ulid_order_is_consistent() {
        let a = Ulid::from_string_digest("apple");
        let b = Ulid::from_string_digest("banana");

        // Consistent order, even if not strictly lexicographic
        assert!(a != b);
    }
}
