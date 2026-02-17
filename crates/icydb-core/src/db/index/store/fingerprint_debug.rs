use crate::{
    db::index::{
        key::IndexKey,
        store::{IndexStore, InlineIndexValue, RawIndexFingerprint, RawIndexKey},
    },
    model::index::IndexModel,
};
use std::fmt::{Display, Formatter, Result as FmtResult};

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
                IndexKey::try_from_raw(key)
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
}

///
/// FingerprintVerificationError
///

#[cfg(debug_assertions)]
#[derive(Debug)]
pub(super) enum FingerprintVerificationError {
    Mismatch {
        label: String,
        key: RawIndexKey,
        expected: RawIndexFingerprint,
        actual: RawIndexFingerprint,
    },
}

#[cfg(debug_assertions)]
impl Display for FingerprintVerificationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Mismatch {
                label,
                key,
                expected,
                actual,
            } => write!(
                f,
                "fingerprint mismatch ({label}): key={key:?} expected={expected:?} actual={actual:?}"
            ),
        }
    }
}
