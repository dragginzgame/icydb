use crate::{
    db::{
        index::{
            entry::RawIndexEntry,
            fingerprint,
            key::{IndexId, IndexKey, RawIndexKey},
        },
        store::{DataKey, StoreRegistry},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    traits::{EntityKind, Storable},
    value::Value,
};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound};
use canic_utils::hash::Xxh3;
use derive_more::{Deref, DerefMut};
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
- Divergence is detectable, not repaired.
- Rebuild is the migration boundary for fingerprint format changes.

This file intentionally does *not* attempt healing, validation in release,
or correctness enforcement via fingerprints.
*/

/// Raw, fixed-size fingerprint bytes stored alongside index entries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RawIndexFingerprint([u8; 16]);

impl RawIndexFingerprint {
    const STORED_SIZE: u32 = 16;
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
/// IndexStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct IndexStoreRegistry(StoreRegistry<IndexStore>);

impl IndexStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
}

impl Default for IndexStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
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
            .map(|e| (*e.key(), e.value()))
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
            panic!("index fingerprint verification failed: {err:?} (debug-only)");
        }

        entry
    }

    pub fn insert(&mut self, key: RawIndexKey, value: RawIndexEntry) -> Option<RawIndexEntry> {
        let nt = Self::entry_fingerprint(&key, &value);
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

    pub(crate) fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, InternalError> {
        if prefix.len() > index.fields.len() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Index,
                format!(
                    "index prefix length {} exceeds field count {}",
                    prefix.len(),
                    index.fields.len()
                ),
            ));
        }

        let index_id = IndexId::new::<E>(index);

        let mut fps = Vec::with_capacity(prefix.len());
        for value in prefix {
            let Some(fp) = fingerprint::to_index_fingerprint(value)? else {
                return Err(InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    "index prefix value is not indexable",
                ));
            };
            fps.push(fp);
        }

        let (start, end) = IndexKey::bounds_for_prefix(index_id, index.fields.len(), &fps);
        let (start_raw, end_raw) = (start.to_raw(), end.to_raw());

        let mut out = Vec::new();

        for entry in self.entry_map().range(start_raw..=end_raw) {
            let raw_key = entry.key();
            let raw_entry = entry.value();

            #[cfg(debug_assertions)]
            if let Err(err) = self.verify_entry_fingerprint(Some(index), raw_key, &raw_entry) {
                panic!("index fingerprint verification failed: {err:?} (debug-only)");
            }

            // Validate index key structure
            IndexKey::try_from_raw(raw_key).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!("index key corrupted during resolve: {err}"),
                )
            })?;

            // Decode storage keys
            let storage_keys = raw_entry.decode_keys().map_err(|err| {
                InternalError::new(ErrorClass::Corruption, ErrorOrigin::Index, err.to_string())
            })?;

            if index.unique && storage_keys.len() != 1 {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    "unique index entry contains an unexpected number of keys",
                ));
            }

            // Convert to DataKeys (storage boundary — no typed IDs)
            out.extend(
                storage_keys
                    .into_iter()
                    .map(|sk| DataKey::from_key::<E>(sk)),
            );
        }

        #[cfg(debug_assertions)]
        self.debug_verify_no_orphaned_fingerprints(index, &start_raw, &end_raw);

        Ok(out)
    }

    pub fn memory_bytes(&self) -> u64 {
        let entry_bytes = self
            .entry_map()
            .iter()
            .map(|e| {
                let v: RawIndexEntry = e.value();
                IndexKey::STORED_SIZE_BYTES + v.len() as u64
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

        let mut h = Xxh3::with_seed(0);
        h.update(&[VERSION]);
        h.update(key.as_bytes());
        h.update(entry.as_bytes());

        RawIndexFingerprint(h.digest128().to_be_bytes())
    }

    #[cfg(debug_assertions)]
    fn verify_entry_fingerprint(
        &self,
        index: Option<&IndexModel>,
        key: &RawIndexKey,
        entry: &RawIndexEntry,
    ) -> Result<(), Box<FingerprintVerificationError>> {
        let expected = Self::entry_fingerprint(key, entry);
        let stored = self.fingerprint_map().get(key);

        let label = index
            .map(|idx| format!("index='{}'", idx.name))
            .or_else(|| {
                IndexKey::try_from_raw(key)
                    .ok()
                    .map(|decoded| format!("index_key={decoded:?}"))
            })
            .unwrap_or_else(|| "index=<unknown>".to_string());

        match stored {
            None => Err(Box::new(FingerprintVerificationError::Missing {
                label,
                key: *key,
            })),
            Some(actual) if actual != expected => {
                Err(Box::new(FingerprintVerificationError::Mismatch {
                    label,
                    key: *key,
                    expected,
                    actual,
                }))
            }
            Some(_) => Ok(()),
        }
    }

    #[cfg(test)]
    #[expect(dead_code)]
    pub(crate) fn debug_fingerprint_for(&self, key: &RawIndexKey) -> Option<[u8; 16]> {
        self.fingerprint_map().get(key).map(|fp| fp.0)
    }

    #[cfg(debug_assertions)]
    fn debug_verify_no_orphaned_fingerprints(
        &self,
        index: &IndexModel,
        start: &RawIndexKey,
        end: &RawIndexKey,
    ) {
        for fp in self.fingerprint_map().range(*start..=*end) {
            assert!(
                self.entry_map().get(fp.key()).is_some(),
                "index fingerprint orphaned: index='{}' key={:?} (debug-only)",
                index.name,
                fp.key()
            );
        }
    }
}

///
/// FingerprintVerificationError
///

#[cfg(debug_assertions)]
#[allow(dead_code)]
#[derive(Debug)]
enum FingerprintVerificationError {
    Missing {
        label: String,
        key: RawIndexKey,
    },
    Mismatch {
        label: String,
        key: RawIndexKey,
        expected: RawIndexFingerprint,
        actual: RawIndexFingerprint,
    },
}
