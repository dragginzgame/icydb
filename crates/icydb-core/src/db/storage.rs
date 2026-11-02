//! IcyDB's collection-opening policy over Storage-owned maps.
//! Storage owns collection formats, validation and allocation leases.

use ic_storage::{
    DefaultMemoryImpl, RuntimeMemory,
    adapter::{CollectionError, StableBTreeMap, Storable},
};

/// Create only in virgin memory; retained allocations must load without repair.
/// Keep Storage's error intact so each store retains its own error classification.
pub(in crate::db) fn open_stable_map<K: Storable + Ord + Clone, V: Storable>(
    memory: RuntimeMemory<DefaultMemoryImpl>,
) -> Result<StableBTreeMap<K, V>, CollectionError> {
    if memory.size() == 0 {
        StableBTreeMap::create(memory)
    } else {
        StableBTreeMap::load(memory)
    }
}
