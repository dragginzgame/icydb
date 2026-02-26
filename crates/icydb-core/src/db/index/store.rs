use crate::db::index::{
    entry::{MAX_INDEX_ENTRY_BYTES, RawIndexEntry},
    key::RawIndexKey,
};
use crate::traits::Storable;

use canic_cdk::structures::{
    BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound as StorableBound,
};
use canic_utils::hash::Xxh3;
use std::borrow::Cow;

///
/// IndexStore
///
/// Architectural Notes:
///
/// - Thin persistence wrapper over a stable BTreeMap.
/// - RawIndexKey and RawIndexEntry are fully validated before insertion.
/// - Fingerprints are non-authoritative diagnostic witnesses.
/// - Fingerprints are always stored, but only verified in debug builds.
/// - This layer does NOT enforce commit/transaction discipline.
///   Higher layers are responsible for write coordination.
/// - IndexStore intentionally does NOT implement Deref to avoid leaking
///   internal storage representation (StoredIndexValue).
///

pub struct IndexStore {
    map: BTreeMap<RawIndexKey, StoredIndexValue, VirtualMemory<DefaultMemoryImpl>>,
    generation: u64,
}

impl IndexStore {
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            map: BTreeMap::init(memory),
            generation: 0,
        }
    }

    /// Snapshot all index entry pairs (diagnostics only).
    pub(crate) fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().entry))
            .collect()
    }

    pub(in crate::db) fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let value = self.map.get(key);

        #[cfg(debug_assertions)]
        if let Some(ref stored) = value {
            Self::verify_if_debug(key, stored);
        }

        value.map(|stored| stored.entry)
    }

    pub fn len(&self) -> u64 {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[must_use]
    pub(in crate::db) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn insert(
        &mut self,
        key: RawIndexKey,
        entry: RawIndexEntry,
    ) -> Option<RawIndexEntry> {
        let fingerprint = Self::entry_fingerprint(&key, &entry);

        let stored = StoredIndexValue { entry, fingerprint };
        let previous = self.map.insert(key, stored).map(|prev| prev.entry);
        self.bump_generation();
        previous
    }

    pub(crate) fn remove(&mut self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let previous = self.map.remove(key).map(|prev| prev.entry);
        self.bump_generation();
        previous
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.bump_generation();
    }

    /// Sum of bytes used by all stored index entries.
    pub fn memory_bytes(&self) -> u64 {
        self.map
            .iter()
            .map(|entry| {
                entry.key().as_bytes().len() as u64
                    + entry.value().entry.len() as u64
                    + u64::from(RawIndexFingerprint::STORED_SIZE)
            })
            .sum()
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    fn entry_fingerprint(key: &RawIndexKey, entry: &RawIndexEntry) -> RawIndexFingerprint {
        const VERSION: u8 = 1;

        let mut hasher = Xxh3::with_seed(0);
        hasher.update(&[VERSION]);
        hasher.update(key.as_bytes());
        hasher.update(entry.as_bytes());

        RawIndexFingerprint(hasher.digest128().to_be_bytes())
    }

    #[cfg(debug_assertions)]
    fn verify_if_debug(key: &RawIndexKey, stored: &StoredIndexValue) {
        let expected = Self::entry_fingerprint(key, &stored.entry);

        debug_assert!(
            stored.fingerprint == expected,
            "debug invariant violation: index fingerprint mismatch"
        );
    }
}

///
/// StoredIndexValue
///
/// Raw entry plus non-authoritative diagnostic fingerprint.
/// Encoded as: [RawIndexEntry bytes | 16-byte fingerprint]
///

#[derive(Clone, Debug)]
struct StoredIndexValue {
    entry: RawIndexEntry,
    fingerprint: RawIndexFingerprint,
}

impl StoredIndexValue {
    const STORED_SIZE: u32 = MAX_INDEX_ENTRY_BYTES + RawIndexFingerprint::STORED_SIZE;
}

impl Storable for StoredIndexValue {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.clone().into_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let bytes = bytes.as_ref();

        let (entry_bytes, fingerprint_bytes) =
            if bytes.len() < RawIndexFingerprint::STORED_SIZE as usize {
                (bytes, &[][..])
            } else {
                bytes.split_at(bytes.len() - RawIndexFingerprint::STORED_SIZE as usize)
            };

        let mut out = [0u8; 16];
        if fingerprint_bytes.len() == out.len() {
            out.copy_from_slice(fingerprint_bytes);
        }

        Self {
            entry: RawIndexEntry::from_bytes(Cow::Borrowed(entry_bytes)),
            fingerprint: RawIndexFingerprint(out),
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = self.entry.into_bytes();
        bytes.extend_from_slice(&self.fingerprint.0);
        bytes
    }

    const BOUND: StorableBound = StorableBound::Bounded {
        max_size: Self::STORED_SIZE,
        is_fixed_size: false,
    };
}

///
/// RawIndexFingerprint
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawIndexFingerprint([u8; 16]);

impl RawIndexFingerprint {
    pub(crate) const STORED_SIZE: u32 = 16;
}

impl Storable for RawIndexFingerprint {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let mut out = [0u8; 16];
        if bytes.len() == out.len() {
            out.copy_from_slice(bytes.as_ref());
        }
        Self(out)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    const BOUND: StorableBound = StorableBound::Bounded {
        max_size: Self::STORED_SIZE,
        is_fixed_size: true,
    };
}

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        index::{
            IndexKey, continuation_advanced, envelope_is_empty,
            predicate::{IndexPredicateExecution, eval_index_execution_on_decoded_key},
            range::anchor_within_envelope,
            resume_bounds_from_refs,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
};
use std::ops::Bound;

impl IndexStore {
    pub(in crate::db) fn resolve_data_values_in_raw_range_limited<E: EntityKind>(
        &self,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation_start_exclusive: Option<&RawIndexKey>,
        direction: Direction,
        limit: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        Self::ensure_anchor_within_envelope(direction, continuation_start_exclusive, bounds)?;

        let (start_raw, end_raw) = match continuation_start_exclusive {
            Some(anchor) => resume_bounds_from_refs(direction, bounds.0, bounds.1, anchor),
            None => (bounds.0.clone(), bounds.1.clone()),
        };

        if envelope_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        match direction {
            Direction::Asc => {
                for entry in self.map.range((start_raw, end_raw)) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    Self::ensure_continuation_advanced(
                        direction,
                        raw_key,
                        continuation_start_exclusive,
                    )?;

                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "range resolve",
                        index_predicate_execution,
                    )? {
                        return Ok(out);
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range((start_raw, end_raw)).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();

                    Self::ensure_continuation_advanced(
                        direction,
                        raw_key,
                        continuation_start_exclusive,
                    )?;

                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "range resolve",
                        index_predicate_execution,
                    )? {
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    // Validate strict continuation advancement when an anchor is present.
    //
    // IMPORTANT CROSS-LAYER CONTRACT:
    // - Planner/cursor-spine validation ensures envelope/signature compatibility.
    // - This store-layer guard independently enforces strict monotonic advancement.
    // - Keep both layers explicit; do not collapse this into planner-only checks.
    fn ensure_continuation_advanced(
        direction: Direction,
        candidate: &RawIndexKey,
        anchor: Option<&RawIndexKey>,
    ) -> Result<(), InternalError> {
        if let Some(anchor) = anchor
            && !continuation_advanced(direction, candidate, anchor)
        {
            return Err(InternalError::index_invariant(
                "index-range continuation scan did not advance beyond the anchor",
            ));
        }

        Ok(())
    }

    // Validate that continuation anchor is contained by the original range envelope.
    //
    // Keep this guard in the store layer even though planner/cursor validation already
    // checks containment: this is a defensive contract check against cross-layer misuse.
    fn ensure_anchor_within_envelope(
        direction: Direction,
        anchor: Option<&RawIndexKey>,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    ) -> Result<(), InternalError> {
        if let Some(anchor) = anchor
            && !anchor_within_envelope(direction, anchor, bounds.0, bounds.1)
        {
            return Err(InternalError::index_invariant(
                "index-range continuation anchor is outside the requested range envelope",
            ));
        }

        Ok(())
    }

    fn decode_index_entry_and_push<E: EntityKind>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &StoredIndexValue,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        #[cfg(debug_assertions)]
        Self::verify_if_debug(raw_key, value);

        let decoded_key = IndexKey::try_from_raw(raw_key).map_err(|err| {
            InternalError::index_corruption(format!("index key corrupted during {context}: {err}"))
        })?;

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        let storage_keys = value
            .entry
            .decode_keys()
            .map_err(|err| InternalError::index_corruption(err.to_string()))?;

        if index.unique && storage_keys.len() != 1 {
            return Err(InternalError::index_corruption(
                "unique index entry contains an unexpected number of keys",
            ));
        }

        for storage_key in storage_keys {
            out.push(DataKey::from_key::<E>(storage_key));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use crate::{
        db::{direction::Direction, index::store::RawIndexKey},
        error::{ErrorClass, ErrorOrigin},
        traits::Storable,
    };
    use std::{borrow::Cow, ops::Bound};

    use super::IndexStore;

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_asc() {
        let anchor = raw_key(0x10);
        let candidate = raw_key(0x10);

        let err =
            IndexStore::ensure_continuation_advanced(Direction::Asc, &candidate, Some(&anchor))
                .expect_err("ASC continuation candidate equal to anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn continuation_advancement_guard_rejects_non_advanced_candidate_desc() {
        let anchor = raw_key(0x10);
        let candidate = raw_key(0x11);

        let err =
            IndexStore::ensure_continuation_advanced(Direction::Desc, &candidate, Some(&anchor))
                .expect_err(
                    "DESC continuation candidate not strictly after anchor must be rejected",
                );

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    fn anchor_containment_guard_rejects_out_of_envelope_anchor() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x20));
        let anchor = raw_key(0x20);

        let err = IndexStore::ensure_anchor_within_envelope(
            Direction::Asc,
            Some(&anchor),
            (&lower, &upper),
        )
        .expect_err("out-of-envelope continuation anchor must be rejected");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }
}
