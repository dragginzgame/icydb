use crate::{
    db::store::{DataKey, StorageKey},
    value::Value,
};

///
/// EntityRef
///
/// Concrete reference extracted from an entity instance.
/// Carries the target entity path and the referenced key value.
/// Produced by [`EntityReferences`] during pre-commit planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityRef {
    pub target_path: &'static str,
    key: StorageKey,
}

impl EntityRef {
    pub(crate) fn from_storage_key(target_path: &'static str, key: StorageKey) -> Self {
        Self { target_path, key }
    }

    /// Returns true if this reference points at the given data key.
    ///
    /// Invariant:
    /// - `target_path` must already have been validated
    /// - comparison is storage-key–level only
    #[inline]
    pub fn matches_data_key(&self, data_key: &DataKey) -> bool {
        data_key.storage_key() == self.key
    }

    /// Expose the referenced storage key (for diagnostics / logging).
    #[inline]
    pub fn storage_key(&self) -> StorageKey {
        self.key
    }

    /// Return the referenced key as a semantic Value.
    ///
    /// This is the ONLY semantic accessor.
    #[inline]
    pub fn value(&self) -> Value {
        self.key.as_value()
    }

    /// Construct from a known key Value.
    pub(crate) fn new(target_path: &'static str, value: Value) -> Self {
        let key = value
            .as_storage_key()
            .expect("EntityRef constructed with non-key Value");

        Self { target_path, key }
    }
}
