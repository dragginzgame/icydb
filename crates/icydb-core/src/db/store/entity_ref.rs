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
    #[must_use]
    pub const fn from_storage_key(target_path: &'static str, key: StorageKey) -> Self {
        Self { target_path, key }
    }

    /// Returns true if this reference points at the given data key.
    ///
    /// Invariant:
    /// - `target_path` must already have been validated
    /// - comparison is storage-key–level only
    #[must_use]
    pub fn matches_data_key(&self, data_key: &DataKey) -> bool {
        data_key.storage_key() == self.key
    }

    /// Expose the referenced storage key (for diagnostics / logging).
    #[must_use]
    pub const fn storage_key(&self) -> StorageKey {
        self.key
    }

    /// Return the referenced key as a semantic Value.
    ///
    /// This is the ONLY semantic accessor.
    #[must_use]
    pub const fn value(&self) -> Value {
        self.key.as_value()
    }
}
