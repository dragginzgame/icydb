use super::*;

/// Fail-closed reasons for returning validated field-path index work to a
/// ready `IndexStore` state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexReadyStoreError {
    StoreNotBuilding,
    IndexKeyDecode,
    EntryCountMismatch,
}

/// Physical ready-state plan derived exclusively from validated index-store
/// state. Accepted-schema publication remains owned by `SchemaPublicationGate`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexReadyStorePlan {
    store: String,
    entry_count: usize,
    staged_validation: SchemaFieldPathIndexStagedValidation,
}

impl SchemaFieldPathIndexReadyStorePlan {
    pub(in crate::db::schema) fn from_validation(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
    ) -> Self {
        Self {
            store: validation.store().to_string(),
            entry_count: validation.entry_count(),
            staged_validation: validation.staged_validation(),
        }
    }

    #[cfg(test)]
    pub(in crate::db::schema) fn mark_index_store_ready(
        &self,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexReadyStoreReport, SchemaFieldPathIndexReadyStoreError> {
        self.mark_index_store_ready_with_scope(index_store, None)
    }

    pub(in crate::db::schema) fn mark_index_store_ready_for_target_index(
        &self,
        target_index_id: &IndexId,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexReadyStoreReport, SchemaFieldPathIndexReadyStoreError> {
        self.mark_index_store_ready_with_scope(index_store, Some(target_index_id))
    }

    fn mark_index_store_ready_with_scope(
        &self,
        index_store: &mut IndexStore,
        target_index_id: Option<&IndexId>,
    ) -> Result<SchemaFieldPathIndexReadyStoreReport, SchemaFieldPathIndexReadyStoreError> {
        if index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexReadyStoreError::StoreNotBuilding);
        }

        let entry_count = match target_index_id {
            Some(target_index_id) => target_index_entry_count(index_store, target_index_id)?,
            None => usize::try_from(index_store.len())
                .map_err(|_| SchemaFieldPathIndexReadyStoreError::EntryCountMismatch)?,
        };
        if entry_count != self.entry_count {
            return Err(SchemaFieldPathIndexReadyStoreError::EntryCountMismatch);
        }

        index_store.mark_ready();

        Ok(SchemaFieldPathIndexReadyStoreReport {
            #[cfg(test)]
            store: self.store.clone(),
            #[cfg(test)]
            entry_count,
            #[cfg(test)]
            index_state: index_store.state(),
            staged_validation: self.staged_validation,
        })
    }
}

fn target_index_entry_count(
    index_store: &IndexStore,
    target_index_id: &IndexId,
) -> Result<usize, SchemaFieldPathIndexReadyStoreError> {
    let mut entry_count = 0usize;
    let result: Result<(), SchemaFieldPathIndexReadyStoreError> =
        index_store.visit_entries(|raw_key, _| {
            let index_key = IndexKey::try_from_raw(raw_key)
                .map_err(|_| SchemaFieldPathIndexReadyStoreError::IndexKeyDecode)?;
            if index_key.index_id() == target_index_id {
                entry_count = entry_count.saturating_add(1);
            }
            Ok(IndexStoreVisit::Continue)
        });
    result?;

    Ok(entry_count)
}

/// Positive report after validated physical work returns to ready state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexReadyStoreReport {
    #[cfg(test)]
    store: String,
    #[cfg(test)]
    entry_count: usize,
    #[cfg(test)]
    index_state: IndexState,
    staged_validation: SchemaFieldPathIndexStagedValidation,
}

impl SchemaFieldPathIndexReadyStoreReport {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
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
        self.staged_validation
    }
}
