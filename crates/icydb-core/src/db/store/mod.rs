mod data;
mod data_key;
mod row;
mod storage_key;

pub use data::*;
pub use data_key::*;
pub use row::*;
pub use storage_key::*;

use crate::{
    db::index::IndexStore,
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use std::{cell::RefCell, collections::HashMap, thread::LocalKey};
use thiserror::Error as ThisError;

///
/// StoreRegistryError
///

#[derive(Debug, ThisError)]
pub enum StoreRegistryError {
    #[error("store '{0}' not found")]
    StoreNotFound(String),
}

impl StoreRegistryError {
    pub(crate) const fn class() -> ErrorClass {
        ErrorClass::Internal
    }
}

impl From<StoreRegistryError> for InternalError {
    fn from(err: StoreRegistryError) -> Self {
        Self::new(
            StoreRegistryError::class(),
            ErrorOrigin::Store,
            err.to_string(),
        )
    }
}

///
/// StoreHandle
///
/// Bound pair of row and index stores for one schema `Store` path.
///

#[derive(Clone, Copy, Debug)]
pub struct StoreHandle {
    data: &'static LocalKey<RefCell<DataStore>>,
    index: &'static LocalKey<RefCell<IndexStore>>,
}

impl StoreHandle {
    /// Build a store handle from thread-local row/index stores.
    #[must_use]
    pub const fn new(
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
    ) -> Self {
        Self { data, index }
    }

    /// Borrow the row store immutably.
    pub fn with_data<R>(&self, f: impl FnOnce(&DataStore) -> R) -> R {
        self.data.with_borrow(f)
    }

    /// Borrow the row store mutably.
    pub fn with_data_mut<R>(&self, f: impl FnOnce(&mut DataStore) -> R) -> R {
        self.data.with_borrow_mut(f)
    }

    /// Borrow the index store immutably.
    pub fn with_index<R>(&self, f: impl FnOnce(&IndexStore) -> R) -> R {
        self.index.with_borrow(f)
    }

    /// Borrow the index store mutably.
    pub fn with_index_mut<R>(&self, f: impl FnOnce(&mut IndexStore) -> R) -> R {
        self.index.with_borrow_mut(f)
    }

    /// Return the raw row-store accessor.
    #[must_use]
    pub const fn data_store(&self) -> &'static LocalKey<RefCell<DataStore>> {
        self.data
    }

    /// Return the raw index-store accessor.
    #[must_use]
    pub const fn index_store(&self) -> &'static LocalKey<RefCell<IndexStore>> {
        self.index
    }
}

///
/// StoreRegistry
///
/// Thread-local registry for both row and index stores.
///

#[derive(Default)]
pub struct StoreRegistry {
    stores: HashMap<&'static str, StoreHandle>,
}

impl StoreRegistry {
    /// Create an empty store registry.
    #[must_use]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Iterate registered stores.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, StoreHandle)> {
        self.stores.iter().map(|(k, v)| (*k, *v))
    }

    /// Register a `Store` path to its row/index store pair.
    pub fn register_store(
        &mut self,
        name: &'static str,
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
    ) {
        self.stores.insert(name, StoreHandle::new(data, index));
    }

    /// Look up a store handle by path.
    pub fn try_get_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.stores
            .get(path)
            .copied()
            .ok_or_else(|| StoreRegistryError::StoreNotFound(path.to_string()).into())
    }

    /// Borrow a row store immutably by path.
    pub fn with_data_store<R>(
        &self,
        path: &str,
        f: impl FnOnce(&DataStore) -> R,
    ) -> Result<R, InternalError> {
        Ok(self.try_get_store(path)?.with_data(f))
    }

    /// Borrow a row store mutably by path.
    pub fn with_data_store_mut<R>(
        &self,
        path: &str,
        f: impl FnOnce(&mut DataStore) -> R,
    ) -> Result<R, InternalError> {
        Ok(self.try_get_store(path)?.with_data_mut(f))
    }

    /// Borrow an index store immutably by path.
    pub fn with_index_store<R>(
        &self,
        path: &str,
        f: impl FnOnce(&IndexStore) -> R,
    ) -> Result<R, InternalError> {
        Ok(self.try_get_store(path)?.with_index(f))
    }

    /// Borrow an index store mutably by path.
    pub fn with_index_store_mut<R>(
        &self,
        path: &str,
        f: impl FnOnce(&mut IndexStore) -> R,
    ) -> Result<R, InternalError> {
        Ok(self.try_get_store(path)?.with_index_mut(f))
    }

    /// Look up a row-store accessor by path.
    pub fn try_get_data_store(
        &self,
        path: &str,
    ) -> Result<&'static LocalKey<RefCell<DataStore>>, InternalError> {
        Ok(self.try_get_store(path)?.data_store())
    }

    /// Look up an index-store accessor by path.
    pub fn try_get_index_store(
        &self,
        path: &str,
    ) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError> {
        Ok(self.try_get_store(path)?.index_store())
    }
}
