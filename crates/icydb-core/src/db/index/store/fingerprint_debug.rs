use crate::{
    db::index::{
        RawIndexKey,
        store::{IndexStore, RawIndexFingerprint},
    },
    model::index::IndexModel,
};

impl IndexStore {
    #[cfg(debug_assertions)]
    pub(super) fn verify_entry_fingerprint(
        &self,
        index: Option<&IndexModel>,
        key: &RawIndexKey,
        entry: &crate::db::index::RawIndexEntry,
    ) -> Result<(), Box<FingerprintVerificationError>> {
        let expected = Self::entry_fingerprint(key, entry);
        let stored = self.fingerprint_map().get(key);

        let label = index
            .map(|idx| format!("index='{}'", idx.name))
            .or_else(|| {
                crate::db::index::IndexKey::try_from_raw(key)
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
        self.fingerprint_map()
            .get(key)
            .map(|fingerprint| fingerprint.0)
    }

    #[cfg(debug_assertions)]
    pub(super) fn debug_verify_no_orphaned_fingerprints(
        &self,
        index: &IndexModel,
        start: &RawIndexKey,
        end: &RawIndexKey,
    ) {
        for fingerprint in self.fingerprint_map().range(*start..=*end) {
            assert!(
                self.entry_map().get(fingerprint.key()).is_some(),
                "invariant violation (debug-only): index fingerprint orphaned: index='{}' key={:?}",
                index.name,
                fingerprint.key()
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
pub(super) enum FingerprintVerificationError {
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
