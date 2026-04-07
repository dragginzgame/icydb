//! Module: index::store
//! Responsibility: stable index-entry persistence primitives.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::{
    db::{
        data::StorageKey,
        index::{entry::RawIndexEntry, key::RawIndexKey},
    },
    error::InternalError,
};

use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};

///
/// IndexState
///
/// Explicit lifecycle validity state for one index store.
/// Validity matters because probe-free covering authority only makes sense once
/// the index contents are fully built and query-visible for reads.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexState {
    Building,
    Valid,
    Dropping,
}

///
/// IndexStore
///
/// Thin persistence wrapper over one stable BTreeMap.
///
/// Invariant: callers provide already-validated `RawIndexKey`/`RawIndexEntry`.
///

pub struct IndexStore {
    pub(super) map: BTreeMap<RawIndexKey, RawIndexEntry, VirtualMemory<DefaultMemoryImpl>>,
    generation: u64,
    state: IndexState,
    secondary_covering_authoritative: bool,
    secondary_existence_witness_authoritative: bool,
}

impl IndexStore {
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            map: BTreeMap::init(memory),
            generation: 0,
            // Existing stores default to Valid until one explicit build/drop
            // lifecycle is introduced. Probe-free routing still also requires
            // the narrower authority witnesses below.
            state: IndexState::Valid,
            secondary_covering_authoritative: false,
            secondary_existence_witness_authoritative: false,
        }
    }

    /// Snapshot all index entry pairs (diagnostics only).
    #[allow(clippy::redundant_closure_for_method_calls)]
    pub(crate) fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.map.iter().map(|entry| entry.into_pair()).collect()
    }

    pub(in crate::db) fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.map.get(key)
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

    /// Return the explicit lifecycle state for this index store.
    #[must_use]
    pub(in crate::db) const fn state(&self) -> IndexState {
        self.state
    }

    /// Return whether this index store is query-visible for probe-free reads.
    #[must_use]
    pub(in crate::db) const fn is_valid(&self) -> bool {
        matches!(self.state, IndexState::Valid)
    }

    /// Mark this index store as in-progress and therefore ineligible for
    /// probe-free covering authority until a full authoritative rebuild ends.
    pub(in crate::db) const fn mark_building(&mut self) {
        self.state = IndexState::Building;
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();
    }

    /// Mark this index store as fully built and eligible for later authority
    /// promotion once the narrower synchronized witness bits are also restored.
    pub(in crate::db) const fn mark_valid(&mut self) {
        self.state = IndexState::Valid;
    }

    /// Mark this index store as dropping and therefore fail closed for
    /// authority-sensitive covering execution.
    pub(in crate::db) const fn mark_dropping(&mut self) {
        self.state = IndexState::Dropping;
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();
    }

    pub(crate) fn insert(
        &mut self,
        key: RawIndexKey,
        entry: RawIndexEntry,
    ) -> Option<RawIndexEntry> {
        let previous = self.map.insert(key, entry);
        self.bump_generation();
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();
        previous
    }

    pub(crate) fn remove(&mut self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let previous = self.map.remove(key);
        self.bump_generation();
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();
        previous
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.bump_generation();
        self.invalidate_secondary_covering_authority();
        self.invalidate_secondary_existence_witness_authority();
    }

    /// Return whether this secondary-index store currently participates in a
    /// synchronized covering-authority witness with its paired row store.
    #[must_use]
    pub(in crate::db) const fn secondary_covering_authoritative(&self) -> bool {
        self.secondary_covering_authoritative
    }

    /// Mark this secondary-index store as synchronized with its paired row
    /// store after successful commit or recovery.
    pub(in crate::db) fn mark_secondary_covering_authoritative(&mut self) {
        debug_assert!(
            self.is_valid(),
            "secondary covering authority must not be restored while the index is Building or Dropping",
        );
        self.secondary_covering_authoritative = true;
    }

    /// Return whether this secondary-index store currently carries explicit
    /// per-entry row-existence witness state.
    #[must_use]
    pub(in crate::db) const fn secondary_existence_witness_authoritative(&self) -> bool {
        self.secondary_existence_witness_authoritative
    }

    /// Mark this secondary-index store as synchronized with one explicit
    /// storage-owned existence witness contract.
    pub(in crate::db) fn mark_secondary_existence_witness_authoritative(&mut self) {
        debug_assert!(
            self.is_valid(),
            "storage existence witness authority must not be restored while the index is Building or Dropping",
        );
        self.secondary_existence_witness_authoritative = true;
    }

    /// Mark one storage key as missing anywhere it still appears inside this
    /// secondary index store, while preserving the surrounding entry itself.
    pub(in crate::db) fn mark_memberships_missing_for_storage_key(
        &mut self,
        storage_key: StorageKey,
    ) -> Result<usize, InternalError> {
        let mut entries = Vec::new();

        for entry in self.map.iter() {
            entries.push(entry.into_pair());
        }

        let mut marked = 0usize;

        for (raw_key, mut raw_entry) in entries {
            if raw_entry
                .mark_key_missing(storage_key)
                .map_err(InternalError::index_entry_decode_failed)?
            {
                self.map.insert(raw_key, raw_entry);
                marked = marked.saturating_add(1);
            }
        }

        if marked > 0 {
            self.bump_generation();
            self.invalidate_secondary_covering_authority();
            self.invalidate_secondary_existence_witness_authority();
        }

        Ok(marked)
    }

    /// Sum of bytes used by all stored index entries.
    pub fn memory_bytes(&self) -> u64 {
        self.map
            .iter()
            .map(|entry| entry.key().as_bytes().len() as u64 + entry.value().len() as u64)
            .sum()
    }

    const fn bump_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }

    const fn invalidate_secondary_covering_authority(&mut self) {
        self.secondary_covering_authoritative = false;
    }

    const fn invalidate_secondary_existence_witness_authority(&mut self) {
        self.secondary_existence_witness_authoritative = false;
    }
}
