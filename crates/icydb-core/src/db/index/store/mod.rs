mod fingerprint_debug;
mod lookup;

use crate::{
    db::index::{
        entry::{MAX_INDEX_ENTRY_BYTES, RawIndexEntry},
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
/// InlineIndexValue
/// Raw entry plus a non-authoritative debug fingerprint in one stored value.
/// Encoded as: `[RawIndexEntry bytes | 16-byte fingerprint]`.
///

#[derive(Clone, Debug)]
struct InlineIndexValue {
    entry: RawIndexEntry,
    fingerprint: RawIndexFingerprint,
}

impl InlineIndexValue {
    const STORED_SIZE: u32 = MAX_INDEX_ENTRY_BYTES + RawIndexFingerprint::STORED_SIZE;
}

impl Storable for InlineIndexValue {
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

    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORED_SIZE,
        is_fixed_size: false,
    };
}

///
/// IndexStore
///

pub struct IndexStore {
    entry: VirtualMemory<DefaultMemoryImpl>,
}

impl IndexStore {
    #[must_use]
    pub const fn init(entry: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self { entry }
    }

    /// Snapshot all index entry pairs (diagnostics only).
    pub fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.entry_map()
            .iter()
            .map(|entry| (*entry.key(), entry.value().entry))
            .collect()
    }

    pub fn len(&self) -> u64 {
        self.entry_map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entry_map().is_empty()
    }

    pub fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        let value = self.entry_map().get(key);

        // Debug-only verification: fingerprints are non-authoritative and
        // checked only to surface divergence during development.
        #[cfg(debug_assertions)]
        if let Some(ref inline) = value
            && let Err(err) = Self::verify_entry_fingerprint(None, key, inline)
        {
            panic!(
                "invariant violation (debug-only): index fingerprint verification failed: {err:?}"
            );
        }

        value.map(|inline| inline.entry)
    }

    pub fn insert(&mut self, key: RawIndexKey, value: RawIndexEntry) -> Option<RawIndexEntry> {
        let fingerprint = Self::entry_fingerprint(&key, &value);
        let inline = InlineIndexValue {
            entry: value,
            fingerprint,
        };
        self.entry_map().insert(key, inline).map(|prev| prev.entry)
    }

    pub fn remove(&mut self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.entry_map().remove(key).map(|prev| prev.entry)
    }

    pub fn clear(&mut self) {
        self.entry_map().clear();
    }

    pub fn memory_bytes(&self) -> u64 {
        self.entry_map()
            .iter()
            .map(|entry| {
                let value: InlineIndexValue = entry.value();
                IndexKey::STORED_SIZE_BYTES
                    + value.entry.len() as u64
                    + u64::from(RawIndexFingerprint::STORED_SIZE)
            })
            .sum::<u64>()
    }

    fn entry_map(
        &self,
    ) -> BTreeMap<RawIndexKey, InlineIndexValue, VirtualMemory<DefaultMemoryImpl>> {
        BTreeMap::init(self.entry.clone())
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
