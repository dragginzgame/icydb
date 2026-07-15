use super::*;

///
/// SchemaFieldPathIndexIsolatedIndexStoreWriter
///
/// Adapter from staged field-path index write and rollback contracts into a
/// caller-provided isolated `IndexStore`. The adapter marks the supplied store
/// as building and keeps schema mutation visibility staged-only. The runner
/// must validate the batch or roll it back before returning the store to ready
/// state.
///

pub(in crate::db::schema) struct SchemaFieldPathIndexIsolatedIndexStoreWriter<'a> {
    store: String,
    pub(in crate::db::schema) index_store: &'a mut IndexStore,
    #[cfg(test)]
    generation_before: u64,
}

impl<'a> SchemaFieldPathIndexIsolatedIndexStoreWriter<'a> {
    pub(in crate::db::schema) fn new(store: &str, index_store: &'a mut IndexStore) -> Self {
        #[cfg(test)]
        let generation_before = index_store.generation();
        index_store.mark_building();

        Self {
            store: store.to_string(),
            index_store,
            #[cfg(test)]
            generation_before,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn generation(&self) -> u64 {
        self.index_store.generation()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn len(&self) -> u64 {
        self.index_store.len()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn get(&self, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        self.index_store.get(key)
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_store.state()
    }

    #[cfg(test)]
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
        if self.index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreNotBuilding);
        }

        let expected_entry_count = u64::try_from(batch.entries().len()).map_err(|_| {
            SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryCountMismatch
        })?;
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
            #[cfg(test)]
            generation_before: self.generation_before,
            #[cfg(test)]
            generation_after: self.index_store.generation(),
            index_state: self.index_store.state(),
            validation: batch.validation(),
        })
    }

    fn target_index_entry_count(
        &self,
        target_index_id: &IndexId,
    ) -> Result<u64, SchemaFieldPathIndexIsolatedIndexStoreValidationError> {
        let mut entry_count = 0u64;
        let result: Result<(), SchemaFieldPathIndexIsolatedIndexStoreValidationError> =
            self.index_store.visit_entries(|raw_key, _| {
                let index_key = IndexKey::try_from_raw(raw_key).map_err(|_| {
                    SchemaFieldPathIndexIsolatedIndexStoreValidationError::IndexKeyDecode
                })?;
                if index_key.index_id() == target_index_id {
                    entry_count += 1;
                }
                Ok(IndexStoreVisit::Continue)
            });
        result?;

        Ok(entry_count)
    }

    #[must_use]
    fn accepts_store(&self, store: &str) -> bool {
        self.store == store
    }
}

impl SchemaFieldPathIndexStagedStoreReadView for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_> {
    fn read_staged_entry(&self, store: &str, key: &RawIndexStoreKey) -> Option<IndexEntryValue> {
        self.accepts_store(store)
            .then(|| self.index_store.get(key))
            .flatten()
    }
}

impl SchemaFieldPathIndexStagedStoreWriter for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_> {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey, entry: &IndexEntryValue) {
        if self.accepts_store(store) {
            self.index_store.insert(key.clone(), entry.clone());
        }
    }
}

impl SchemaFieldPathIndexStagedStoreRollbackWriter
    for SchemaFieldPathIndexIsolatedIndexStoreWriter<'_>
{
    fn restore_staged_entry(
        &mut self,
        store: &str,
        key: &RawIndexStoreKey,
        entry: &IndexEntryValue,
    ) {
        if self.accepts_store(store) {
            self.index_store.insert(key.clone(), entry.clone());
        }
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexStoreKey) {
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
/// returning to ready state unless it exactly matches the staged batch.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexIsolatedIndexStoreValidationError {
    StoreMismatch,
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
/// and building-state visibility, but does not mark the store ready.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexIsolatedIndexStoreValidation {
    store: String,
    entry_count: usize,
    #[cfg(test)]
    generation_before: u64,
    #[cfg(test)]
    generation_after: u64,
    index_state: IndexState,
    validation: SchemaFieldPathIndexStagedValidation,
}

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
    #[cfg(test)]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn generation_after(&self) -> u64 {
        self.generation_after
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_state
    }

    #[must_use]
    pub(in crate::db::schema) const fn staged_validation(
        &self,
    ) -> SchemaFieldPathIndexStagedValidation {
        self.validation
    }
}
