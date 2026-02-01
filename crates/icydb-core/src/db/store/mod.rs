mod data;
mod data_key;
mod entity_ref;
mod row;
mod storage_key;

pub use data::*;
pub use data_key::*;
pub use entity_ref::*;
pub use row::*;
pub use storage_key::*;

use crate::error::{ErrorClass, ErrorOrigin, InternalError};
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
/// StoreRegistry
///

#[derive(Default)]
pub struct StoreRegistry<T: 'static>(HashMap<&'static str, &'static LocalKey<RefCell<T>>>);

impl<T: 'static> StoreRegistry<T> {
    /// Create an empty store registry.
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Iterate registered store names and thread-local keys.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &'static LocalKey<RefCell<T>>)> {
        self.0.iter().map(|(k, v)| (*k, *v))
    }

    /// Borrow each registered store immutably.
    pub fn for_each<R>(&self, mut f: impl FnMut(&'static str, &T) -> R) {
        for (path, accessor) in &self.0 {
            accessor.with(|cell| {
                let store = cell.borrow();
                f(path, &store);
            });
        }
    }

    /// Register a thread-local store accessor under a path.
    pub fn register(&mut self, name: &'static str, accessor: &'static LocalKey<RefCell<T>>) {
        self.0.insert(name, accessor);
    }

    /// Look up a store accessor by path.
    pub fn try_get_store(
        &self,
        path: &str,
    ) -> Result<&'static LocalKey<RefCell<T>>, InternalError> {
        self.0
            .get(path)
            .copied()
            .ok_or_else(|| StoreRegistryError::StoreNotFound(path.to_string()).into())
    }

    /// Borrow a store immutably by path.
    pub fn with_store<R>(&self, path: &str, f: impl FnOnce(&T) -> R) -> Result<R, InternalError> {
        let store = self.try_get_store(path)?;

        Ok(store.with_borrow(|s| f(s)))
    }

    /// Borrow a store mutably by path.
    pub fn with_store_mut<R>(
        &self,
        path: &str,
        f: impl FnOnce(&mut T) -> R,
    ) -> Result<R, InternalError> {
        let store = self.try_get_store(path)?;

        Ok(store.with_borrow_mut(|s| f(s)))
    }
}
