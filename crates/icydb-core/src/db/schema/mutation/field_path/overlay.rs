use super::*;
use std::collections::BTreeMap;

/// SchemaFieldPathIndexStagedStoreOverlay
///
/// Isolated in-memory store overlay for staged field-path index writes. The
/// overlay implements the staged read, write, and rollback contracts without
/// exposing or mutating a runtime-visible `IndexStore`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreOverlay {
    store: String,
    entries: BTreeMap<RawIndexStoreKey, IndexEntryValue>,
}

impl SchemaFieldPathIndexStagedStoreOverlay {
    #[must_use]
    pub(in crate::db::schema) fn new(store: &str) -> Self {
        Self {
            store: store.to_string(),
            entries: BTreeMap::new(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn from_entries(
        store: &str,
        entries: impl IntoIterator<Item = (RawIndexStoreKey, IndexEntryValue)>,
    ) -> Self {
        Self {
            store: store.to_string(),
            entries: entries.into_iter().collect(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub(in crate::db::schema) fn get(&self, key: &RawIndexStoreKey) -> Option<&IndexEntryValue> {
        self.entries.get(key)
    }

    pub(in crate::db::schema) fn validate_batch(
        &self,
        batch: &SchemaFieldPathIndexStagedStoreWriteBatch,
    ) -> Result<
        SchemaFieldPathIndexStagedStoreOverlayValidation,
        SchemaFieldPathIndexStagedStoreOverlayValidationError,
    > {
        if self.store != batch.store() {
            return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::StoreMismatch);
        }
        if self.entries.len() != batch.entries().len() {
            return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryCountMismatch);
        }

        for entry in batch.entries() {
            let Some(overlay_entry) = self.entries.get(entry.key()) else {
                return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::MissingEntry);
            };
            if overlay_entry != entry.entry() {
                return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryMismatch);
            }
        }

        Ok(SchemaFieldPathIndexStagedStoreOverlayValidation {
            store: self.store.clone(),
            entry_count: self.entries.len(),
        })
    }

    #[must_use]
    fn accepts_store(&self, store: &str) -> bool {
        self.store == store
    }
}

impl SchemaFieldPathIndexStagedStoreReadView for SchemaFieldPathIndexStagedStoreOverlay {
    fn read_staged_entry(&self, store: &str, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        self.accepts_store(store)
            .then(|| self.entries.get(key).cloned())
            .flatten()
    }
}

impl SchemaFieldPathIndexStagedStoreWriter for SchemaFieldPathIndexStagedStoreOverlay {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey, entry: &IndexEntryValue) {
        if self.accepts_store(store) {
            self.entries.insert(key.clone(), entry.clone());
        }
    }
}

impl SchemaFieldPathIndexStagedStoreRollbackWriter for SchemaFieldPathIndexStagedStoreOverlay {
    fn restore_staged_entry(
        &mut self,
        store: &str,
        key: &RawIndexStoreKey,
        entry: &IndexEntryValue,
    ) {
        if self.accepts_store(store) {
            self.entries.insert(key.clone(), entry.clone());
        }
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey) {
        if self.accepts_store(store) {
            self.entries.remove(key);
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreOverlayValidationError
///
/// Fail-closed validation reasons for isolated staged-store overlays. These
/// checks keep overlay state from being treated as physically ready unless it
/// exactly matches the validated staged write batch.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStoreOverlayValidationError {
    StoreMismatch,
    EntryCountMismatch,
    MissingEntry,
    EntryMismatch,
}

///
/// SchemaFieldPathIndexStagedStoreOverlayValidation
///
/// Positive validation report for an isolated staged-store overlay after it has
/// accepted one staged write batch. This report remains staged-only and does
/// not imply physical ready state.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreOverlayValidation {
    store: String,
    entry_count: usize,
}

impl SchemaFieldPathIndexStagedStoreOverlayValidation {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }
}
