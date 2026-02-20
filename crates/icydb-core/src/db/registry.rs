use crate::{
    db::{data::DataStore, index::IndexStore},
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

    #[error("store '{0}' already registered")]
    StoreAlreadyRegistered(String),
}

impl StoreRegistryError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::StoreNotFound(_) => ErrorClass::Internal,
            Self::StoreAlreadyRegistered(_) => ErrorClass::InvariantViolation,
        }
    }
}

impl From<StoreRegistryError> for InternalError {
    fn from(err: StoreRegistryError) -> Self {
        Self::classified(err.class(), ErrorOrigin::Store, err.to_string())
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
    ) -> Result<(), InternalError> {
        if self.stores.contains_key(name) {
            return Err(StoreRegistryError::StoreAlreadyRegistered(name.to_string()).into());
        }

        self.stores.insert(name, StoreHandle::new(data, index));
        Ok(())
    }

    /// Look up a store handle by path.
    pub fn try_get_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.stores
            .get(path)
            .copied()
            .ok_or_else(|| StoreRegistryError::StoreNotFound(path.to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{data::DataStore, index::IndexStore, registry::StoreRegistry},
        error::{ErrorClass, ErrorOrigin},
        test_support::test_memory,
    };
    use std::{cell::RefCell, ptr};

    const STORE_PATH: &str = "store_registry_tests::Store";

    thread_local! {
        static TEST_DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(151)));
        static TEST_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(test_memory(152)));
    }

    fn test_registry() -> StoreRegistry {
        let mut registry = StoreRegistry::new();
        registry
            .register_store(STORE_PATH, &TEST_DATA_STORE, &TEST_INDEX_STORE)
            .expect("test store registration should succeed");
        registry
    }

    #[test]
    fn register_store_binds_data_and_index_handles() {
        let registry = test_registry();
        let handle = registry
            .try_get_store(STORE_PATH)
            .expect("registered store path should resolve");

        assert!(
            ptr::eq(handle.data_store(), &TEST_DATA_STORE),
            "store handle should expose the registered data store accessor"
        );
        assert!(
            ptr::eq(handle.index_store(), &TEST_INDEX_STORE),
            "store handle should expose the registered index store accessor"
        );

        let data_rows = handle.with_data(|store| store.len());
        let index_rows = handle.with_index(IndexStore::len);
        assert_eq!(data_rows, 0, "fresh test data store should be empty");
        assert_eq!(index_rows, 0, "fresh test index store should be empty");
    }

    #[test]
    fn missing_store_path_rejected_before_access() {
        let registry = StoreRegistry::new();
        let err = registry
            .try_get_store("store_registry_tests::Missing")
            .expect_err("missing path should fail lookup");

        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message
                .contains("store 'store_registry_tests::Missing' not found"),
            "missing store lookup should include the missing path"
        );
    }

    #[test]
    fn duplicate_store_registration_is_rejected() {
        let mut registry = StoreRegistry::new();
        registry
            .register_store(STORE_PATH, &TEST_DATA_STORE, &TEST_INDEX_STORE)
            .expect("initial store registration should succeed");

        let err = registry
            .register_store(STORE_PATH, &TEST_DATA_STORE, &TEST_INDEX_STORE)
            .expect_err("duplicate registration should fail");
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message
                .contains("store 'store_registry_tests::Store' already registered"),
            "duplicate registration should include the conflicting path"
        );
    }
}
