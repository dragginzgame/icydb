//! Test-only constructors and adapters for the parent module.

use super::*;

impl SchemaStore {
    pub(in crate::db) fn take_canonical_bundle_load_count_for_tests() -> u64 {
        CANONICAL_SCHEMA_BUNDLE_LOADS.with(|loads| loads.replace(0))
    }

    pub(super) fn publish_positioned_journal_entry(
        &mut self,
        key: RawSchemaKey,
        snapshot: Option<RawSchemaValue>,
        position: JournalOverlayPosition,
    ) -> Result<Option<RawSchemaValue>, InternalError> {
        let SchemaStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        positions.preflight_publish(&key, position)?;
        self.invalidate_accepted_bundle_cache_for_key(key);
        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
        } = &mut self.backend
        else {
            return Err(InternalError::store_invariant());
        };
        let previous = if tombstones.contains(&key) {
            None
        } else {
            match live.get(&key) {
                Some(value) => Some(value.clone()),
                None => canonical.get(&key).map_err(InternalError::from)?,
            }
        };
        if let Some(snapshot) = snapshot {
            tombstones.remove(&key);
            live.insert(key, snapshot);
        } else {
            live.remove(&key);
            tombstones.insert(key);
        }
        positions.publish_preflighted(key, position);
        Ok(previous)
    }

    /// Resolve one accepted entity display name without materializing the full store catalog.
    pub(in crate::db) fn current_accepted_runtime_entity_for_name(
        &self,
        registered_store_path: &'static str,
        entity_name: &str,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        self.current_accepted_runtime_entity_matching(registered_store_path, |_, snapshot_name| {
            snapshot_name == entity_name
        })
    }

    /// Derive accepted catalog metadata from the selected accepted bundle.
    ///
    /// This function intentionally reads only the persisted schema store. It
    /// does not reconstruct metadata from generated models when the store has
    /// no accepted bundle.
    pub(in crate::db) fn catalog_metadata(
        &self,
    ) -> Result<Option<SchemaStoreCatalogMetadata>, InternalError> {
        Ok(self
            .allocation_metadata()?
            .map(SchemaStoreAllocationMetadata::schema))
    }

    #[must_use]
    pub(in crate::db) fn canonical_len_for_tests(&self) -> u64 {
        match &self.backend {
            SchemaStoreBackend::Journaled { canonical: map, .. } => map.len(),
            SchemaStoreBackend::Heap(_) => 0,
        }
    }
}

pub(super) fn reset_accepted_schema_bundle_cache_miss_count_for_tests() {
    ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES.with(|misses| misses.set(0));
}

pub(super) fn accepted_schema_bundle_cache_miss_count_for_tests() -> u64 {
    ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES.with(Cell::get)
}

thread_local! {
    pub(super) static ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES: Cell<u64> = const { Cell::new(0) };
    pub(super) static IDENTITY_STATE_INVENTORY_READS: Cell<u64> = const { Cell::new(0) };
    pub(super) static CANONICAL_SCHEMA_BUNDLE_LOADS: Cell<u64> = const { Cell::new(0) };
}
