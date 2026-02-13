mod fingerprint_debug;
mod lookup;
mod registry;

pub use registry::IndexStoreRegistry;

use crate::{
    db::index::{
        entry::RawIndexEntry,
        key::{IndexKey, RawIndexKey},
    },
    traits::Storable,
};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound};
use canic_utils::hash::Xxh3;
use std::borrow::Cow;

/*
Index Fingerprints — Design Contract (0.7)

Fingerprints are *non-authoritative diagnostic witnesses* stored alongside
index entries. They exist solely to detect divergence during development.

Authoritative correctness comes from:
- Stored index entries
- Decoded row data
- Commit/recovery replay

Key properties:
- Fingerprints are written and removed in lockstep with index entries.
- Release builds do not read or validate fingerprints.
- Debug builds verify fingerprints opportunistically and panic on mismatch.
- These panics are intentional debug-time invariant sentinels only.
- Divergence is detectable, not repaired.
- Rebuild is the migration boundary for fingerprint format changes.

This file intentionally does *not* attempt healing, validation in release,
or correctness enforcement via fingerprints.
*/

///
/// RawIndexFingerprint
/// Raw, fixed-size fingerprint bytes stored alongside index entries.
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

    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORED_SIZE,
        is_fixed_size: true,
    };
}

///
/// IndexStore
///

pub struct IndexStore {
    entry: VirtualMemory<DefaultMemoryImpl>,
    fingerprint: VirtualMemory<DefaultMemoryImpl>,
}

impl IndexStore {
    #[must_use]
    pub const fn init(
        entry: VirtualMemory<DefaultMemoryImpl>,
        fingerprint: VirtualMemory<DefaultMemoryImpl>,
    ) -> Self {
        Self { entry, fingerprint }
    }

    /// Snapshot all index entry pairs (diagnostics only).
    pub fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.entry_map()
            .iter()
            .map(|entry| (*entry.key(), entry.value()))
            .collect()
    }

    pub fn len(&self) -> u64 {
        self.entry_map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entry_map().is_empty()
    }

    pub fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let entry = self.entry_map().get(key);

        // Debug-only verification: fingerprints are non-authoritative and
        // checked only to surface divergence during development.
        #[cfg(debug_assertions)]
        if let Some(ref value) = entry
            && let Err(err) = self.verify_entry_fingerprint(None, key, value)
        {
            panic!(
                "invariant violation (debug-only): index fingerprint verification failed: {err:?}"
            );
        }

        entry
    }

    pub fn insert(&mut self, key: RawIndexKey, value: RawIndexEntry) -> Option<RawIndexEntry> {
        let fingerprint = Self::entry_fingerprint(&key, &value);
        let prev = self.entry_map().insert(key, value);

        // NOTE: Mid-write traps may cause divergence. This is acceptable;
        // fingerprints are diagnostic only and verified in debug builds.
        let _ = self.fingerprint_map().insert(key, fingerprint);

        prev
    }

    pub fn remove(&mut self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let removed = self.entry_map().remove(key);

        // See insert(): divergence is acceptable and detectable in debug builds.
        let _ = self.fingerprint_map().remove(key);

        removed
    }

    pub fn clear(&mut self) {
        self.entry_map().clear();
        self.fingerprint_map().clear();
    }

    pub fn memory_bytes(&self) -> u64 {
        let entry_bytes = self
            .entry_map()
            .iter()
            .map(|entry| {
                let value: RawIndexEntry = entry.value();
                IndexKey::STORED_SIZE_BYTES + value.len() as u64
            })
            .sum::<u64>();

        let fingerprint_bytes = self
            .fingerprint_map()
            .iter()
            .map(|_| IndexKey::STORED_SIZE_BYTES + u64::from(RawIndexFingerprint::STORED_SIZE))
            .sum::<u64>();

        entry_bytes.saturating_add(fingerprint_bytes)
    }

    fn entry_map(&self) -> BTreeMap<RawIndexKey, RawIndexEntry, VirtualMemory<DefaultMemoryImpl>> {
        BTreeMap::init(self.entry.clone())
    }

    fn fingerprint_map(
        &self,
    ) -> BTreeMap<RawIndexKey, RawIndexFingerprint, VirtualMemory<DefaultMemoryImpl>> {
        BTreeMap::init(self.fingerprint.clone())
    }

    fn entry_fingerprint(key: &RawIndexKey, entry: &RawIndexEntry) -> RawIndexFingerprint {
        const VERSION: u8 = 1;

        let mut hasher = Xxh3::with_seed(0);
        hasher.update(&[VERSION]);
        hasher.update(key.as_bytes());
        hasher.update(entry.as_bytes());

        RawIndexFingerprint(hasher.digest128().to_be_bytes())
    }
}
