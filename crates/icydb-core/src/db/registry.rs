//! Module: db::registry
//! Responsibility: thread-local store registry lifecycle and lookup authority.
//! Does not own: store encode/decode semantics or query/executor planning behavior.
//! Boundary: manages registry state for named data/index stores and typed registry errors.

use crate::{
    db::{
        data::DataStore,
        index::{IndexState, IndexStore},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use std::{cell::RefCell, thread::LocalKey};
use thiserror::Error as ThisError;

///
/// StoreRegistryError
///

#[derive(Debug, ThisError)]
#[expect(clippy::enum_variant_names)]
pub enum StoreRegistryError {
    #[error("store '{0}' not found")]
    StoreNotFound(String),

    #[error("store '{0}' already registered")]
    StoreAlreadyRegistered(String),

    #[error(
        "store '{name}' reuses the same row/index store pair already registered as '{existing_name}'"
    )]
    StoreHandlePairAlreadyRegistered { name: String, existing_name: String },
}

impl StoreRegistryError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::StoreNotFound(_) => ErrorClass::Internal,
            Self::StoreAlreadyRegistered(_) | Self::StoreHandlePairAlreadyRegistered { .. } => {
                ErrorClass::InvariantViolation
            }
        }
    }
}

impl From<StoreRegistryError> for InternalError {
    fn from(err: StoreRegistryError) -> Self {
        Self::classified(err.class(), ErrorOrigin::Store, err.to_string())
    }
}

///
/// SecondaryReadAuthoritySnapshot
///
/// Immutable authority snapshot for one store-backed secondary read.
/// This keeps index lifecycle truth and synchronized witness bits together at
/// the registry boundary so executor authority resolution can consume one
/// stable input instead of reaching back into the live store handle.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SecondaryReadAuthoritySnapshot {
    index_state: IndexState,
    secondary_covering_authoritative: bool,
    secondary_existence_witness_authoritative: bool,
}

impl SecondaryReadAuthoritySnapshot {
    // Build one immutable authority snapshot from the current store state.
    const fn new(
        index_state: IndexState,
        secondary_covering_authoritative: bool,
        secondary_existence_witness_authoritative: bool,
    ) -> Self {
        Self {
            index_state,
            secondary_covering_authoritative,
            secondary_existence_witness_authoritative,
        }
    }

    // Return the explicit lifecycle state captured for this secondary read.
    pub(in crate::db) const fn index_state(self) -> IndexState {
        self.index_state
    }

    // Return whether this captured index state is probe-free eligible.
    pub(in crate::db) const fn index_is_valid(self) -> bool {
        matches!(self.index_state, IndexState::Valid)
    }

    // Return whether the stronger synchronized pair witness was captured.
    pub(in crate::db) const fn secondary_covering_authoritative(self) -> bool {
        self.secondary_covering_authoritative
    }

    // Return whether the narrower storage existence witness was captured.
    pub(in crate::db) const fn secondary_existence_witness_authoritative(self) -> bool {
        self.secondary_existence_witness_authoritative
    }
}

///
/// StoreHandle
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

    /// Return the explicit lifecycle state of the bound index store.
    #[must_use]
    pub(in crate::db) fn index_state(&self) -> IndexState {
        self.with_index(IndexStore::state)
    }

    /// Return whether the bound index store is currently valid for probe-free
    /// covering authority.
    #[must_use]
    pub(in crate::db) fn index_is_valid(&self) -> bool {
        self.with_index(IndexStore::is_valid)
    }

    /// Mark the bound index store as Building.
    pub(in crate::db) fn mark_index_building(&self) {
        self.with_index_mut(IndexStore::mark_building);
    }

    /// Mark the bound index store as Valid.
    pub(in crate::db) fn mark_index_valid(&self) {
        self.with_index_mut(IndexStore::mark_valid);
    }

    /// Mark the bound index store as Dropping.
    pub(in crate::db) fn mark_index_dropping(&self) {
        self.with_index_mut(IndexStore::mark_dropping);
    }

    /// Return whether this store pair currently carries a synchronized
    /// secondary covering-authority witness.
    #[must_use]
    pub(in crate::db) fn secondary_covering_authoritative(&self) -> bool {
        self.with_data(DataStore::secondary_covering_authoritative)
            && self.with_index(IndexStore::secondary_covering_authoritative)
    }

    /// Mark this row/index store pair as synchronized for witness-backed
    /// secondary covering after successful commit or recovery.
    pub(in crate::db) fn mark_secondary_covering_authoritative(&self) {
        self.with_data_mut(DataStore::mark_secondary_covering_authoritative);
        self.with_index_mut(IndexStore::mark_secondary_covering_authoritative);
    }

    /// Return whether this store pair currently carries one explicit
    /// storage-owned secondary existence witness contract.
    #[must_use]
    pub(in crate::db) fn secondary_existence_witness_authoritative(&self) -> bool {
        self.with_data(DataStore::secondary_existence_witness_authoritative)
            && self.with_index(IndexStore::secondary_existence_witness_authoritative)
    }

    /// Capture one immutable authority snapshot for a single secondary read
    /// resolution pass. This keeps lifecycle truth at the registry boundary
    /// instead of letting deeper executor code rediscover it from `StoreHandle`.
    #[must_use]
    pub(in crate::db) fn secondary_read_authority_snapshot(
        &self,
    ) -> SecondaryReadAuthoritySnapshot {
        SecondaryReadAuthoritySnapshot::new(
            self.index_state(),
            self.secondary_covering_authoritative(),
            self.secondary_existence_witness_authoritative(),
        )
    }

    /// Mark this row/index store pair as synchronized for one explicit
    /// storage-owned secondary existence witness contract.
    pub(in crate::db) fn mark_secondary_existence_witness_authoritative(&self) {
        self.with_data_mut(DataStore::mark_secondary_existence_witness_authoritative);
        self.with_index_mut(IndexStore::mark_secondary_existence_witness_authoritative);
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
/// Thread-local registry for both row and index stores.
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{data::DataStore, index::IndexStore, registry::StoreRegistry},
        error::{ErrorClass, ErrorOrigin},
        testing::test_memory,
    };
    use std::{cell::RefCell, ptr};

    const STORE_PATH: &str = "store_registry_tests::Store";
    const ALIAS_STORE_PATH: &str = "store_registry_tests::StoreAlias";

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

    #[test]
    fn alias_store_registration_reusing_same_store_pair_is_rejected() {
        let mut registry = StoreRegistry::new();
        registry
            .register_store(STORE_PATH, &TEST_DATA_STORE, &TEST_INDEX_STORE)
            .expect("initial store registration should succeed");

        let err = registry
            .register_store(ALIAS_STORE_PATH, &TEST_DATA_STORE, &TEST_INDEX_STORE)
            .expect_err("alias registration reusing the same store pair should fail");
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains(
                "store 'store_registry_tests::StoreAlias' reuses the same row/index store pair"
            ),
            "alias registration should include conflicting alias path"
        );
        assert!(
            err.message
                .contains("registered as 'store_registry_tests::Store'"),
            "alias registration should include original path"
        );
    }
}
