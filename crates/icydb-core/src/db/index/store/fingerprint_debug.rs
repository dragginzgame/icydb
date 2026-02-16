use crate::{
    db::index::{
        RawIndexKey,
        store::{IndexStore, InlineIndexValue, RawIndexFingerprint},
    },
    model::index::IndexModel,
};

impl IndexStore {
    #[cfg(debug_assertions)]
    pub(super) fn verify_entry_fingerprint(
        index: Option<&IndexModel>,
        key: &RawIndexKey,
        value: &InlineIndexValue,
    ) -> Result<(), Box<FingerprintVerificationError>> {
        let expected = Self::entry_fingerprint(key, &value.entry);
        let actual = value.fingerprint;

        let label = index
            .map(|idx| format!("index='{}'", idx.name))
            .or_else(|| {
                crate::db::index::IndexKey::try_from_raw(key)
                    .ok()
                    .map(|decoded| format!("index_key={decoded:?}"))
            })
            .unwrap_or_else(|| "index=<unknown>".to_string());

        if actual == expected {
            Ok(())
        } else {
            Err(Box::new(FingerprintVerificationError::Mismatch {
                label,
                key: key.clone(),
                expected,
                actual,
            }))
        }
    }

    #[cfg(test)]
    #[expect(dead_code)]
    pub(crate) fn debug_fingerprint_for(&self, key: &RawIndexKey) -> Option<[u8; 16]> {
        self.entry_map().get(key).map(|value| value.fingerprint.0)
    }
}

///
/// FingerprintVerificationError
///

#[cfg(debug_assertions)]
#[expect(dead_code)]
#[derive(Debug)]
pub(super) enum FingerprintVerificationError {
    Mismatch {
        label: String,
        key: RawIndexKey,
        expected: RawIndexFingerprint,
        actual: RawIndexFingerprint,
    },
}
