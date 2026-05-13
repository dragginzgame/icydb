use super::*;

///
/// SchemaFieldPathIndexIsolatedIndexStoreWriter
///
/// Adapter from staged field-path index write and rollback contracts into a
/// caller-provided isolated `IndexStore`. The adapter marks the supplied store
/// as building and keeps schema mutation visibility staged-only; callers must
/// not pass a runtime-visible store until publication and invalidation wiring
/// exists.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore mutation before publication exists"
)]
pub(in crate::db::schema) struct SchemaFieldPathIndexIsolatedIndexStoreWriter<'a> {
    store: String,
    pub(in crate::db::schema) index_store: &'a mut IndexStore,
    generation_before: u64,
    pub(in crate::db::schema) store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore mutation before publication exists"
)]
impl<'a> SchemaFieldPathIndexIsolatedIndexStoreWriter<'a> {
    pub(in crate::db::schema) fn new(store: &str, index_store: &'a mut IndexStore) -> Self {
        let generation_before = index_store.generation();
        index_store.mark_building();

        Self {
            store: store.to_string(),
            index_store,
            generation_before,
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation(&self) -> u64 {
        self.index_store.generation()
    }

    #[must_use]
    pub(in crate::db::schema) fn len(&self) -> u64 {
        self.index_store.len()
    }

    #[must_use]
    pub(in crate::db::schema) fn get(&self, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.index_store.get(key)
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_store.state()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    pub(in crate::db::schema) fn validate_batch(
        &self,
        batch: &SchemaFieldPathIndexStagedStoreWriteBatch,
    ) -> Result<
        SchemaFieldPathIndexIsolatedIndexStoreValidation,
        SchemaFieldPathIndexIsolatedIndexStoreValidationError,
    > {
        self.validate_batch_with_scope(batch, None)
    }

    pub(in crate::db::schema) fn validate_batch_for_target_index(
        &self,
        target_index_id: &IndexId,
        batch: &SchemaFieldPathIndexStagedStoreWriteBatch,
    ) -> Result<
        SchemaFieldPathIndexIsolatedIndexStoreValidation,
        SchemaFieldPathIndexIsolatedIndexStoreValidationError,
    > {
        self.validate_batch_with_scope(batch, Some(target_index_id))
    }

    fn validate_batch_with_scope(
        &self,
        batch: &SchemaFieldPathIndexStagedStoreWriteBatch,
        target_index_id: Option<&IndexId>,
    ) -> Result<
        SchemaFieldPathIndexIsolatedIndexStoreValidation,
        SchemaFieldPathIndexIsolatedIndexStoreValidationError,
    > {
        if self.store != batch.store() {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreMismatch);
        }
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::PublishedVisibility);
        }
        if self.index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreNotBuilding);
        }

        let expected_entry_count =
            u64::try_from(batch.entries().len()).expect("staged entry count should fit in u64");
        match target_index_id {
            Some(target_index_id) => {
                if self.target_index_entry_count(target_index_id)? != expected_entry_count {
                    return Err(
                        SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryCountMismatch,
                    );
                }
            }
            None => {
                if self.index_store.len() != expected_entry_count {
                    return Err(
                        SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryCountMismatch,
                    );
                }
            }
        }

        for entry in batch.entries() {
            if let Some(target_index_id) = target_index_id {
                let index_key = IndexKey::try_from_raw(entry.key()).map_err(|_| {
                    SchemaFieldPathIndexIsolatedIndexStoreValidationError::IndexKeyDecode
                })?;
                if index_key.index_id() != target_index_id {
                    return Err(
                        SchemaFieldPathIndexIsolatedIndexStoreValidationError::TargetMismatch,
                    );
                }
            }
            let Some(index_entry) = self.index_store.get(entry.key()) else {
                return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::MissingEntry);
            };
            if index_entry != *entry.entry() {
                return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryMismatch);
            }
        }

        Ok(SchemaFieldPathIndexIsolatedIndexStoreValidation {
            store: self.store.clone(),
            entry_count: batch.entries().len(),
            generation_before: self.generation_before,
            generation_after: self.index_store.generation(),
            index_state: self.index_store.state(),
            store_visibility: self.store_visibility,
            runner_report: batch.runner_report().clone(),
        })
    }

    fn target_index_entry_count(
        &self,
        target_index_id: &IndexId,
    ) -> Result<u64, SchemaFieldPathIndexIsolatedIndexStoreValidationError> {
        let mut entry_count = 0u64;
        for (raw_key, _) in self.index_store.entries() {
            let index_key = IndexKey::try_from_raw(&raw_key).map_err(|_| {
                SchemaFieldPathIndexIsolatedIndexStoreValidationError::IndexKeyDecode
            })?;
            if index_key.index_id() == target_index_id {
                entry_count += 1;
            }
        }

        Ok(entry_count)
    }

    #[must_use]
    fn accepts_store(&self, store: &str) -> bool {
        self.store == store
    }
}

impl SchemaFieldPathIndexStagedStoreReadView for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_> {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.accepts_store(store)
            .then(|| self.index_store.get(key))
            .flatten()
    }
}

impl SchemaFieldPathIndexStagedStoreWriter for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_> {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.index_store.insert(key.clone(), entry.clone());
        }
    }
}

impl SchemaFieldPathIndexStagedStoreRollbackWriter
    for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_>
{
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        if self.accepts_store(store) {
            self.index_store.insert(key.clone(), entry.clone());
        }
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey) {
        if self.accepts_store(store) {
            self.index_store.remove(key);
        }
    }
}

///
/// SchemaFieldPathIndexIsolatedIndexStoreValidationError
///
/// Fail-closed validation reasons for an isolated `IndexStore` after staged
/// writes have been applied. These checks keep a physical staged store from
/// becoming a publication candidate unless it exactly matches the staged batch.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore validation before publication exists"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexIsolatedIndexStoreValidationError {
    StoreMismatch,
    PublishedVisibility,
    StoreNotBuilding,
    EntryCountMismatch,
    IndexKeyDecode,
    TargetMismatch,
    MissingEntry,
    EntryMismatch,
}

///
/// SchemaFieldPathIndexIsolatedIndexStoreValidation
///
/// Positive validation report for an isolated `IndexStore` after staged
/// field-path index writes have landed. The report records generation movement
/// and building-state visibility, but does not mark the store publishable.
///

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore validation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexIsolatedIndexStoreValidation {
    store: String,
    entry_count: usize,
    generation_before: u64,
    generation_after: u64,
    index_state: IndexState,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages isolated IndexStore validation before publication exists"
)]
impl SchemaFieldPathIndexIsolatedIndexStoreValidation {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_after(&self) -> u64 {
        self.generation_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_state
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
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_isolated_index_store_validation(
            self,
        )
    }
}
