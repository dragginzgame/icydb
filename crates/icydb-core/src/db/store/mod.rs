mod data;
mod index;

pub use data::*;
pub use index::*;

use crate::runtime_error::{ErrorClass, ErrorOrigin, RuntimeError};
use std::{cell::RefCell, collections::HashMap, thread::LocalKey};
use thiserror::Error as ThisError;

///
/// StoreError
///

#[derive(Debug, ThisError)]
pub enum StoreError {
    #[error("store '{0}' not found")]
    StoreNotFound(String),
}

impl StoreError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::StoreNotFound(_) => ErrorClass::Internal,
        }
    }
}

impl From<StoreError> for RuntimeError {
    fn from(err: StoreError) -> Self {
        Self::new(err.class(), ErrorOrigin::Store, err.to_string())
    }
}

///
/// StoreRegistry
///

#[derive(Default)]
pub struct StoreRegistry<T: 'static>(HashMap<&'static str, &'static LocalKey<RefCell<T>>>);

impl<T: 'static> StoreRegistry<T> {
    // new
    #[must_use]
    /// Create an empty store registry.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    // iter
    /// Iterate registered store names and thread-local keys.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &'static LocalKey<RefCell<T>>)> {
        self.0.iter().map(|(k, v)| (*k, *v))
    }

    // for_each
    /// Borrow each registered store immutably.
    pub fn for_each<R>(&self, mut f: impl FnMut(&'static str, &T) -> R) {
        for (path, accessor) in &self.0 {
            accessor.with(|cell| {
                let store = cell.borrow();
                f(path, &store);
            });
        }
    }

    // register
    /// Register a thread-local store accessor under a path.
    pub fn register(&mut self, name: &'static str, accessor: &'static LocalKey<RefCell<T>>) {
        self.0.insert(name, accessor);
    }

    // try_get_store
    /// Look up a store accessor by path.
    pub fn try_get_store(&self, path: &str) -> Result<&'static LocalKey<RefCell<T>>, RuntimeError> {
        self.0
            .get(path)
            .copied()
            .ok_or_else(|| StoreError::StoreNotFound(path.to_string()).into())
    }

    // with_store
    /// Borrow a store immutably by path.
    pub fn with_store<R>(&self, path: &str, f: impl FnOnce(&T) -> R) -> Result<R, RuntimeError> {
        let store = self.try_get_store(path)?;

        Ok(store.with_borrow(|s| f(s)))
    }

    // with_store_mut
    /// Borrow a store mutably by path.
    pub fn with_store_mut<R>(
        &self,
        path: &str,
        f: impl FnOnce(&mut T) -> R,
    ) -> Result<R, RuntimeError> {
        let store = self.try_get_store(path)?;

        Ok(store.with_borrow_mut(|s| f(s)))
    }
}
