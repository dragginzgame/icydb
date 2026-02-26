mod fixtures;

pub(crate) use fixtures::*;

use canic_cdk::structures::{
    DefaultMemoryImpl,
    memory::{MemoryId, MemoryManager, VirtualMemory},
};

pub(crate) const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;
pub(crate) const TEST_MEMORY_RANGE_START: u8 = 1;
pub(crate) const TEST_COMMIT_MEMORY_ID: u8 = RESERVED_INTERNAL_MEMORY_ID - 1;
pub(crate) const TEST_MEMORY_RANGE_END: u8 = TEST_COMMIT_MEMORY_ID;

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

/// Return the canonical commit memory id used by tests.
#[must_use]
pub(crate) const fn test_commit_memory_id() -> u8 {
    TEST_COMMIT_MEMORY_ID
}

/// Shared test-only stable memory allocation for in-memory stores.
pub(crate) fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
    let manager = MemoryManager::init(DefaultMemoryImpl::default());

    manager.get(MemoryId::new(test_memory_id(id)))
}
