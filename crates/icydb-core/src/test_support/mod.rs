mod fixtures;

pub(crate) use fixtures::*;

use canic_cdk::structures::{
    DefaultMemoryImpl,
    memory::{MemoryId, MemoryManager, VirtualMemory},
};

/// Shared test-only stable memory allocation for in-memory stores.
pub(crate) fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
    let manager = MemoryManager::init(DefaultMemoryImpl::default());

    manager.get(MemoryId::new(id))
}
