mod data;
mod index;

pub use data::*;
pub use index::*;

use crate::{Error, db::DbError};
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

impl From<StoreError> for Error {
    fn from(err: StoreError) -> Self {
        DbError::from(err).into()
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
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    // iter
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &'static LocalKey<RefCell<T>>)> {
        self.0.iter().map(|(k, v)| (*k, *v))
    }

    // for_each
    pub fn for_each<R>(&self, mut f: impl FnMut(&'static str, &T) -> R) {
        for (path, accessor) in &self.0 {
            accessor.with(|cell| {
                let store = cell.borrow();
                f(path, &store);
            });
        }
    }

    // register
    pub fn register(&mut self, name: &'static str, accessor: &'static LocalKey<RefCell<T>>) {
        self.0.insert(name, accessor);
    }

    // try_get_store
    pub fn try_get_store(&self, path: &str) -> Result<&'static LocalKey<RefCell<T>>, Error> {
        self.0
            .get(path)
            .copied()
            .ok_or_else(|| StoreError::StoreNotFound(path.to_string()).into())
    }

    // with_store
    pub fn with_store<R>(&self, path: &str, f: impl FnOnce(&T) -> R) -> Result<R, Error> {
        let store = self.try_get_store(path)?;

        Ok(store.with_borrow(|s| f(s)))
    }

    // with_store_mut
    pub fn with_store_mut<R>(&self, path: &str, f: impl FnOnce(&mut T) -> R) -> Result<R, Error> {
        let store = self.try_get_store(path)?;

        Ok(store.with_borrow_mut(|s| f(s)))
    }
}
