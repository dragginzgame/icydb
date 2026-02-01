use crate::db::store::{DataKey, RawDataKey, RawRow, StoreRegistry};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory};
use derive_more::{Deref, DerefMut};

///
/// DataRow
///

pub type DataRow = (DataKey, RawRow);

///
/// DataStoreRegistry
///

#[derive(Deref, DerefMut)]
pub struct DataStoreRegistry(StoreRegistry<DataStore>);

impl DataStoreRegistry {
    /// Create an empty data store registry.
    #[must_use]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(StoreRegistry::new())
    }
}

///
/// DataStore
///

#[derive(Deref, DerefMut)]
pub struct DataStore(BTreeMap<RawDataKey, RawRow, VirtualMemory<DefaultMemoryImpl>>);

impl DataStore {
    #[must_use]
    /// Initialize a data store with the provided backing memory.
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self(BTreeMap::init(memory))
    }

    /// Clear all stored rows from the data store.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Sum of bytes used by all stored rows.
    pub fn memory_bytes(&self) -> u64 {
        self.iter()
            .map(|entry| DataKey::STORED_SIZE_BYTES + entry.value().len() as u64)
            .sum()
    }
}
