use crate::{
    db::{
        data::DataStore,
        index::IndexStore,
        registry::{StoreHandle, StoreRegistryError},
    },
    error::InternalError,
};
use std::{cell::RefCell, thread::LocalKey};

///
/// StoreRegistry
///
/// StoreRegistry owns the generated mapping from schema `Store` paths to their
/// row/index store handles.
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

    /// Register a `Store` path to its row/index store pair.
    pub fn register_store(
        &mut self,
        name: &'static str,
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
    ) -> Result<(), InternalError> {
        if self
            .stores
            .iter()
            .any(|(existing_name, _)| *existing_name == name)
        {
            return Err(StoreRegistryError::StoreAlreadyRegistered(name.to_string()).into());
        }

        // Keep one canonical logical store name per physical row/index store pair.
        if let Some(existing_name) =
            self.stores
                .iter()
                .find_map(|(existing_name, existing_handle)| {
                    (std::ptr::eq(existing_handle.data_store(), data)
                        && std::ptr::eq(existing_handle.index_store(), index))
                    .then_some(*existing_name)
                })
        {
            return Err(StoreRegistryError::StoreHandlePairAlreadyRegistered {
                name: name.to_string(),
                existing_name: existing_name.to_string(),
            }
            .into());
        }

        self.stores.push((name, StoreHandle::new(data, index)));

        Ok(())
    }

    /// Look up a store handle by path.
    pub fn try_get_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.stores
            .iter()
            .find_map(|(existing_path, handle)| (*existing_path == path).then_some(*handle))
            .ok_or_else(|| StoreRegistryError::StoreNotFound(path.to_string()).into())
    }
}
