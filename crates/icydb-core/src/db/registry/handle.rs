use crate::db::{
    data::DataStore,
    index::{IndexState, IndexStore},
    schema::SchemaStore,
};
use std::{cell::RefCell, thread::LocalKey};

///
/// StoreHandle
///
/// StoreHandle binds the row, index, and schema stores for one generated schema
/// `Store` path.
/// It is the stable access token passed across commit, recovery, executor, and
/// diagnostics boundaries instead of exposing registry internals directly.
///

#[derive(Clone, Copy, Debug)]
pub struct StoreHandle {
    data: &'static LocalKey<RefCell<DataStore>>,
    index: &'static LocalKey<RefCell<IndexStore>>,
    schema: &'static LocalKey<RefCell<SchemaStore>>,
    allocations: StoreAllocationIdentities,
}

///
/// StoreAllocationIdentity
///
/// Durable allocation identity for one physical stable-memory role.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreAllocationIdentity {
    memory_id: u8,
    stable_key: &'static str,
}

impl StoreAllocationIdentity {
    /// Build one stable allocation identity descriptor.
    #[must_use]
    pub const fn new(memory_id: u8, stable_key: &'static str) -> Self {
        Self {
            memory_id,
            stable_key,
        }
    }

    /// Stable-memory manager ID.
    #[must_use]
    pub const fn memory_id(self) -> u8 {
        self.memory_id
    }

    /// Durable stable-memory key.
    #[must_use]
    pub const fn stable_key(self) -> &'static str {
        self.stable_key
    }
}

///
/// StoreAllocationIdentities
///
/// Durable allocation identities for one logical store's data, index, and
/// schema memories.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreAllocationIdentities {
    data: Option<StoreAllocationIdentity>,
    index: Option<StoreAllocationIdentity>,
    schema: Option<StoreAllocationIdentity>,
}

impl StoreAllocationIdentities {
    /// Build an absent allocation identity bundle.
    #[must_use]
    pub const fn absent() -> Self {
        Self {
            data: None,
            index: None,
            schema: None,
        }
    }

    /// Build one allocation identity bundle.
    #[must_use]
    pub const fn new(
        data: StoreAllocationIdentity,
        index: StoreAllocationIdentity,
        schema: StoreAllocationIdentity,
    ) -> Self {
        Self {
            data: Some(data),
            index: Some(index),
            schema: Some(schema),
        }
    }

    /// Return data-memory allocation identity.
    #[must_use]
    pub const fn data(self) -> Option<StoreAllocationIdentity> {
        self.data
    }

    /// Return index-memory allocation identity.
    #[must_use]
    pub const fn index(self) -> Option<StoreAllocationIdentity> {
        self.index
    }

    /// Return schema-memory allocation identity.
    #[must_use]
    pub const fn schema(self) -> Option<StoreAllocationIdentity> {
        self.schema
    }
}

impl StoreHandle {
    /// Build a store handle with an explicit allocation identity decision.
    #[must_use]
    pub const fn new(
        data: &'static LocalKey<RefCell<DataStore>>,
        index: &'static LocalKey<RefCell<IndexStore>>,
        schema: &'static LocalKey<RefCell<SchemaStore>>,
        allocations: StoreAllocationIdentities,
    ) -> Self {
        Self {
            data,
            index,
            schema,
            allocations,
        }
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

    /// Borrow the schema store immutably.
    pub fn with_schema<R>(&self, f: impl FnOnce(&SchemaStore) -> R) -> R {
        self.schema.with_borrow(f)
    }

    /// Borrow the schema store mutably.
    pub fn with_schema_mut<R>(&self, f: impl FnOnce(&mut SchemaStore) -> R) -> R {
        self.schema.with_borrow_mut(f)
    }

    /// Return the explicit lifecycle state of the bound index store.
    #[must_use]
    pub(in crate::db) fn index_state(&self) -> IndexState {
        self.with_index(IndexStore::state)
    }

    /// Return whether this handle's data store is heap-backed and volatile.
    #[must_use]
    pub(in crate::db) fn data_is_heap_storage(&self) -> bool {
        self.with_data(DataStore::is_heap_storage)
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

    /// Return the raw schema-store accessor.
    #[must_use]
    pub const fn schema_store(&self) -> &'static LocalKey<RefCell<SchemaStore>> {
        self.schema
    }

    /// Return the data-memory allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn data_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.data()
    }

    /// Return the index-memory allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn index_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.index()
    }

    /// Return the schema-memory allocation identity when generated wiring
    /// supplied it.
    #[must_use]
    pub const fn schema_allocation(&self) -> Option<StoreAllocationIdentity> {
        self.allocations.schema()
    }
}
