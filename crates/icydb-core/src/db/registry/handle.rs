use crate::db::{
    data::DataStore,
    index::{IndexState, IndexStore},
};
use std::{cell::RefCell, thread::LocalKey};

///
/// StoreHandle
///
/// StoreHandle binds the row and index stores for one generated schema `Store`
/// path.
/// It is the stable access token passed across commit, recovery, executor, and
/// diagnostics boundaries instead of exposing registry internals directly.
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
        #[cfg(feature = "diagnostics")]
        {
            crate::db::physical_access::measure_physical_access_operation(|| {
                self.data.with_borrow(f)
            })
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            self.data.with_borrow(f)
        }
    }

    /// Borrow the row store mutably.
    pub fn with_data_mut<R>(&self, f: impl FnOnce(&mut DataStore) -> R) -> R {
        self.data.with_borrow_mut(f)
    }

    /// Borrow the index store immutably.
    pub fn with_index<R>(&self, f: impl FnOnce(&IndexStore) -> R) -> R {
        #[cfg(feature = "diagnostics")]
        {
            crate::db::physical_access::measure_physical_access_operation(|| {
                self.index.with_borrow(f)
            })
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            self.index.with_borrow(f)
        }
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

    /// Mark the bound index store as Building.
    pub(in crate::db) fn mark_index_building(&self) {
        self.with_index_mut(IndexStore::mark_building);
    }

    /// Mark the bound index store as Ready.
    pub(in crate::db) fn mark_index_ready(&self) {
        self.with_index_mut(IndexStore::mark_ready);
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
