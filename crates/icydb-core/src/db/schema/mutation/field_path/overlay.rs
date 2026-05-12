use super::*;

/// SchemaFieldPathIndexStagedStoreOverlay
///
/// Isolated in-memory store overlay for staged field-path index writes. The
/// overlay implements the staged read, write, and rollback contracts without
/// exposing or mutating a runtime-visible `IndexStore`.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated physical-store overlays before IndexStore mutation exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreOverlay {
    store: String,
    entries: BTreeMap<RawIndexKey, RawIndexEntry>,
    pub(in crate::db::schema) store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated physical-store overlays before IndexStore mutation exists"
)]
impl SchemaFieldPathIndexStagedStoreOverlay {
    #[must_use]
    pub(in crate::db::schema) fn new(store: &str) -> Self {
        Self {
            store: store.to_string(),
            entries: BTreeMap::new(),
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn from_entries(
        store: &str,
        entries: impl IntoIterator<Item = (RawIndexKey, RawIndexEntry)>,
    ) -> Self {
        Self {
            store: store.to_string(),
            entries: entries.into_iter().collect(),
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
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
    pub(in crate::db::schema) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub(in crate::db::schema) fn get(&self, key: &RawIndexKey) -> Option<&RawIndexEntry> {
        self.entries.get(key)
    }

    #[must_use]
    pub(in crate::db::schema) fn entries(&self) -> Vec<(RawIndexKey, RawIndexEntry)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
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
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaFieldPathIndexStagedStoreOverlayValidationError::PublishedVisibility);
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
            store_visibility: self.store_visibility,
            runner_report: batch.runner_report().clone(),
        })
    }

    #[must_use]
    fn accepts_store(&self, store: &str) -> bool {
        self.store == store
    }
}

impl SchemaFieldPathIndexStagedStoreReadView for SchemaFieldPathIndexStagedStoreOverlay {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.accepts_store(store)
            .then(|| self.entries.get(key).cloned())
            .flatten()
    }
}

impl SchemaFieldPathIndexStagedStoreWriter for SchemaFieldPathIndexStagedStoreOverlay {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.entries.insert(key.clone(), entry.clone());
        }
    }
}

impl SchemaFieldPathIndexStagedStoreRollbackWriter for SchemaFieldPathIndexStagedStoreOverlay {
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.entries.insert(key.clone(), entry.clone());
        }
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey) {
        if self.accepts_store(store) {
            self.entries.remove(key);
        }
    }
}

///
/// SchemaFieldPathIndexStagedStoreOverlayValidationError
///
/// Fail-closed validation reasons for isolated staged-store overlays. These
/// checks keep overlay state from being treated as publication-ready unless it
/// exactly matches the validated staged write batch.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated overlay validation before publication exists"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStoreOverlayValidationError {
    StoreMismatch,
    PublishedVisibility,
    EntryCountMismatch,
    MissingEntry,
    EntryMismatch,
}

///
/// SchemaFieldPathIndexStagedStoreOverlayValidation
///
/// Positive validation report for an isolated staged-store overlay after it has
/// accepted one staged write batch. This report remains staged-only and does
/// not imply publication readiness.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated overlay validation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStoreOverlayValidation {
    store: String,
    entry_count: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated overlay validation before publication exists"
)]
impl SchemaFieldPathIndexStagedStoreOverlayValidation {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_overlay_validation(self)
    }
}
