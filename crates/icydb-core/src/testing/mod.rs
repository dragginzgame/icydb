//! Module: testing
//! Responsibility: shared crate-local test helpers and stable fixture constants.
//! Does not own: production runtime behavior or public testing APIs.
//! Boundary: internal-only support surface for `icydb-core` tests.

mod entity_tags;

use ic_stable_structures::{
    DefaultMemoryImpl,
    memory_manager::{MemoryId, MemoryManager, VirtualMemory},
};

pub(crate) use entity_tags::*;

pub(crate) const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;

/// Return a validated test memory id.
///
/// Memory id `255` is reserved by stable-structures internals and must never
/// be used by application or test memory allocations.
#[must_use]
pub(crate) const fn test_memory_id(id: u8) -> u8 {
    assert!(
        id != RESERVED_INTERNAL_MEMORY_ID,
        "memory id 255 is reserved for stable-structures internals",
    );
    id
}

/// Shared test-only stable memory allocation for in-memory stores.
pub(crate) fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
    let manager = MemoryManager::init(DefaultMemoryImpl::default());

    manager.get(MemoryId::new(test_memory_id(id)))
}
