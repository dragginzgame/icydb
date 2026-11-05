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
mod tests;
