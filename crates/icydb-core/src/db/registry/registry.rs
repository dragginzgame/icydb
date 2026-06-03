use crate::{
    db::{
        data::DataStore,
        index::IndexStore,
        journal::JournalTailStore,
        registry::{
            StoreAllocationIdentities, StoreHandle, StoreRegistryError,
            StoreRuntimeStorageCapabilities, StoreRuntimeStorageMode,
        },
        schema::SchemaStore,
    },
    error::InternalError,
};
use std::{cell::RefCell, thread::LocalKey};

///
/// StoreRegistry
///
/// StoreRegistry owns the generated mapping from schema `Store` paths to their
/// row, index, and schema store handles.
/// It validates registration invariants once at generated wiring time and then
/// serves cheap immutable lookups during runtime operations.
///

#[derive(Default)]
pub struct StoreRegistry {
    stores: Vec<(&'static str, StoreHandle)>,
}

impl StoreRegistry {
    /// Create an empty store registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Iterate registered stores.
    ///
    /// Iteration order follows registration order. Semantic result ordering
    /// must still not depend on this iteration order; callers that need
    /// deterministic ordering must sort by store path.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, StoreHandle)> {
        self.stores.iter().copied()
    }

    /// Register a `Store` path to its row/index/schema store triplet with an
    /// explicit allocation identity decision.
    ///
    /// Generated stable-store wiring supplies stable allocation identities.
    /// Tests and future non-stable stores must pass
    /// [`StoreAllocationIdentities::absent`] explicitly when allocation
    /// identities are intentionally unavailable.
    pub fn register_store(
        &mut self,
        name: &'static str,
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        allocations: StoreAllocationIdentities,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> Result<(), InternalError> {
        self.validate_register_store_shape(name, data, index, schema, allocations, capabilities)?;
        if capabilities.storage_mode() == StoreRuntimeStorageMode::Journaled {
            return Err(
                StoreRegistryError::StoreAllocationCapabilityMismatch(name.to_string()).into(),
            );
        }

        self.stores.push((
            name,
            StoreHandle::new(data, index, schema, allocations, capabilities),
        ));

        Ok(())
    }

    /// Register one journaled store with its journal-tail storage handle.
    #[expect(
        clippy::too_many_arguments,
        reason = "generated journaled registration adds one journal-tail handle to the existing store triplet"
    )]
    pub fn register_journaled_store(
        &mut self,
        name: &'static str,
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        journal: &'static LocalKey<RefCell<JournalTailStore>>,
        allocations: StoreAllocationIdentities,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> Result<(), InternalError> {
        self.validate_register_store_shape(name, data, index, schema, allocations, capabilities)?;
        if capabilities.storage_mode() != StoreRuntimeStorageMode::Journaled
            || allocations.journal().is_none()
        {
            return Err(
                StoreRegistryError::StoreAllocationCapabilityMismatch(name.to_string()).into(),
            );
        }

        self.stores.push((
            name,
            StoreHandle::new_journaled(data, index, schema, journal, allocations, capabilities),
        ));

        Ok(())
    }

    /// Look up a store handle by path.
    pub fn try_get_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.stores
            .iter()
            .find_map(|(existing_path, handle)| (*existing_path == path).then_some(*handle))
            .ok_or_else(|| StoreRegistryError::StoreNotFound(path.to_string()).into())
    }

    fn validate_register_store_shape(
        &self,
        name: &'static str,
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        allocations: StoreAllocationIdentities,
        capabilities: StoreRuntimeStorageCapabilities,
    ) -> Result<(), InternalError> {
        if self
            .stores
            .iter()
            .any(|(existing_name, _)| *existing_name == name)
        {
            return Err(StoreRegistryError::StoreAlreadyRegistered(name.to_string()).into());
        }

        // Keep one canonical logical store name per physical row/index/schema
        // store triplet.
        if let Some(existing_name) =
            self.stores
                .iter()
                .find_map(|(existing_name, existing_handle)| {
                    (std::ptr::eq(existing_handle.data_store(), data)
                        && std::ptr::eq(existing_handle.index_store(), index)
                        && std::ptr::eq(existing_handle.schema_store(), schema))
                    .then_some(*existing_name)
                })
        {
            return Err(StoreRegistryError::StoreHandleTripletAlreadyRegistered {
                name: name.to_string(),
                existing_name: existing_name.to_string(),
            }
            .into());
        }

        if allocations.allocation_identity_capability() != Some(capabilities.allocation_identity())
            || !allocations.matches_storage_capabilities(capabilities)
        {
            return Err(
                StoreRegistryError::StoreAllocationCapabilityMismatch(name.to_string()).into(),
            );
        }

        Ok(())
    }
}
