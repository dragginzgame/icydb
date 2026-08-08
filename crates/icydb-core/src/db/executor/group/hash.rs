//! Module: executor::group::hash
//! Responsibility: stable hash derivation for canonical grouped/distinct keys.
//! Does not own: key canonicalization policy or grouping equality checks.
//! Boundary: hash utilities consumed by grouped key materialization.

use crate::{
    error::InternalError,
    value::{Value, hash_value},
};
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hash, Hasher},
    mem::size_of,
};

///
/// StableHash
///
/// StableHash is the canonical fixed-width hash identifier used by grouping
/// and distinct key materialization paths.
///

pub(in crate::db::executor) type StableHash = u64;

///
/// StableHashMap
///
/// StableHashMap indexes already-derived stable value hashes without running
/// them through the standard library's general-purpose hash builder again.
///

pub(in crate::db::executor) type StableHashMap<V> = HashMap<StableHash, V, StableHashBuildHasher>;

// Reserve four physical slots per retained logical entry. Incrementally grown
// standard hash tables and vectors stay within this envelope under the pinned
// toolchain, including their lowest-cardinality allocation. Hash-table slots
// additionally include one machine word for control/alignment overhead.
const RETAINED_CAPACITY_SLOTS_PER_ENTRY: usize = 4;

/// Return the conservative backing reservation charged before one logical hash
/// table entry may allocate.
#[must_use]
pub(in crate::db::executor) fn retained_hash_entry_backing_bytes<K, V>() -> u64 {
    let physical_slot_bytes = size_of::<(K, V)>().saturating_add(size_of::<usize>());
    u64::try_from(physical_slot_bytes.saturating_mul(RETAINED_CAPACITY_SLOTS_PER_ENTRY))
        .unwrap_or(u64::MAX)
}

/// Return the conservative backing reservation charged before one retained
/// vector element may allocate.
#[must_use]
pub(in crate::db::executor) fn retained_vec_element_backing_bytes<T>() -> u64 {
    u64::try_from(size_of::<T>().saturating_mul(RETAINED_CAPACITY_SLOTS_PER_ENTRY))
        .unwrap_or(u64::MAX)
}

/// Fallibly reserve one additional hash-map entry after its conservative
/// backing reservation has been charged.
pub(in crate::db::executor) fn try_reserve_hash_entry<K, V, S>(
    map: &mut HashMap<K, V, S>,
) -> Result<(), InternalError>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    map.try_reserve(1)
        .map_err(|_| InternalError::executor_internal())
}

/// Fallibly reserve one additional hash-set entry after its conservative
/// backing reservation has been charged.
pub(in crate::db::executor) fn try_reserve_hash_set_entry<K, S>(
    set: &mut HashSet<K, S>,
) -> Result<(), InternalError>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    set.try_reserve(1)
        .map_err(|_| InternalError::executor_internal())
}

/// Fallibly reserve vector elements after their conservative backing
/// reservation has been charged.
pub(in crate::db::executor) fn try_reserve_vec_elements<T>(
    values: &mut Vec<T>,
    additional: usize,
) -> Result<(), InternalError> {
    values
        .try_reserve(additional)
        .map_err(|_| InternalError::executor_internal())
}

///
/// StableHashBuildHasher
///
/// StableHashBuildHasher constructs the identity-style hasher used only for
/// maps keyed by `StableHash`; callers must hash values canonically first.
///

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::db::executor) struct StableHashBuildHasher;

///
/// StableHashHasher
///
/// StableHashHasher accepts the `u64` emitted by `StableHash`'s `Hash`
/// implementation and returns it directly for bucket placement.
///

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::db::executor) struct StableHashHasher {
    hash: u64,
}

impl BuildHasher for StableHashBuildHasher {
    type Hasher = StableHashHasher;

    fn build_hasher(&self) -> Self::Hasher {
        StableHashHasher::default()
    }
}

impl Hasher for StableHashHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        // This fallback is only for accidental non-u64 keys. Keep it cheap but
        // deterministic so the hasher remains well-defined if a caller drifts.
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }
        self.hash = hash;
    }

    fn write_u64(&mut self, value: u64) {
        self.hash = value;
    }

    fn write_usize(&mut self, value: usize) {
        self.hash = value as u64;
    }
}

/// Derive one stable 64-bit hash from the canonical value hash digest.
#[must_use]
pub(in crate::db::executor) const fn stable_hash_from_digest(digest: [u8; 16]) -> StableHash {
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

/// Hash one value with the stable grouping/distinct hashing contract.
pub(in crate::db::executor) fn stable_hash_value(
    value: &Value,
) -> Result<StableHash, InternalError> {
    let digest = hash_value(value)?;
    Ok(stable_hash_from_digest(digest))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::Decimal, value::Value};

    #[test]
    fn incremental_hash_and_vector_capacity_stays_inside_retained_envelope() {
        let mut map = StableHashMap::<usize>::with_hasher(StableHashBuildHasher);
        let mut values = Vec::<usize>::new();

        for entry in 1..=1_024_usize {
            try_reserve_hash_entry(&mut map).expect("hash capacity reservation");
            map.insert(u64::try_from(entry).expect("test key"), entry);
            try_reserve_vec_elements(&mut values, 1).expect("vector capacity reservation");
            values.push(entry);

            let admitted_slots = entry.saturating_mul(RETAINED_CAPACITY_SLOTS_PER_ENTRY);
            assert!(map.capacity() <= admitted_slots);
            assert!(values.capacity() <= admitted_slots);
        }
    }

    #[test]
    fn stable_hash_uses_digest_prefix_contract() {
        let digest = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xF0,
            0x0A, 0x0B,
        ];
        assert_eq!(
            stable_hash_from_digest(digest),
            0x1122_3344_5566_7788,
            "stable hash must use the canonical leading 64 bits of the value digest",
        );
    }

    #[test]
    fn stable_hash_is_deterministic_for_same_value() {
        let value = Value::Decimal(Decimal::new(12300, 4));
        let left = stable_hash_value(&value).expect("stable hash");
        let right = stable_hash_value(&value).expect("stable hash");
        assert_eq!(left, right);
    }

    #[test]
    fn stable_hash_respects_canonical_map_order() {
        let left = Value::Map(vec![
            (Value::Text("z".to_string()), Value::Nat64(9)),
            (Value::Text("a".to_string()), Value::Nat64(1)),
        ]);
        let right = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Nat64(1)),
            (Value::Text("z".to_string()), Value::Nat64(9)),
        ]);
        assert_eq!(
            stable_hash_value(&left).expect("stable hash"),
            stable_hash_value(&right).expect("stable hash"),
            "stable hash must not depend on non-canonical map insertion order",
        );
    }

    #[test]
    fn stable_hash_contract_vectors_are_frozen_for_upgrade_stability() {
        let vectors = vec![
            ("null", Value::Null, 0x07d3_310a_0679_d482),
            ("nat_42", Value::Nat64(42), 0x8c99_03a0_7f2c_731c),
            ("int_neg7", Value::Int64(-7), 0x7470_6cc5_9093_df80),
            (
                "text_alpha",
                Value::Text("alpha".to_string()),
                0x6ec7_96a5_45c2_ad82,
            ),
            (
                "decimal_1",
                Value::Decimal(Decimal::new(10, 1)),
                0x7d42_1e3f_fffc_9100,
            ),
            (
                "map_a1_z9",
                Value::Map(vec![
                    (Value::Text("a".to_string()), Value::Nat64(1)),
                    (Value::Text("z".to_string()), Value::Nat64(9)),
                ]),
                0xea0e_28c9_f878_6d85,
            ),
        ];
        for (label, value, expected_hash) in vectors {
            let actual_hash = stable_hash_value(&value).expect("stable hash");
            assert_eq!(
                actual_hash, expected_hash,
                "stable hash vector drift for {label}; seed/version/encoding contract changed",
            );
        }
    }
}
