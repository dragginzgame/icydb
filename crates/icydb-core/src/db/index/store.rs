//! Module: index::store
//! Responsibility: stable index-entry persistence primitives.
//! Does not own: range-scan resolution, continuation semantics, or predicate execution.
//! Boundary: scan/executor layers depend on this storage boundary.

use crate::{
    db::index::{
        entry::{MAX_INDEX_ENTRY_BYTES, RawIndexEntry},
        key::RawIndexKey,
    },
    traits::Storable,
};

use canic_cdk::structures::{
    BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound as StorableBound,
};
use canic_utils::hash::Xxh3;
use std::borrow::Cow;

///
/// IndexStore
///
/// Thin persistence wrapper over one stable BTreeMap.
///
/// Invariant: callers provide already-validated `RawIndexKey`/`RawIndexEntry`.
/// Fingerprints are diagnostic witnesses and are debug-verified only.
///

pub struct IndexStore {
    pub(super) map: BTreeMap<RawIndexKey, StoredIndexValue, VirtualMemory<DefaultMemoryImpl>>,
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
    pub(super) fn verify_if_debug(key: &RawIndexKey, stored: &StoredIndexValue) {
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
pub(super) struct StoredIndexValue {
    pub(super) entry: RawIndexEntry,
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
