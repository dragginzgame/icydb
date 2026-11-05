//! Test-only helpers for the parent module.

use super::*;

pub(super) fn allocation_pages(allocation: StoreAllocationIdentity) -> Result<u64, InternalError> {
    TEST_CONVERGENCE_MEMORIES.with_borrow(|memories| {
        Ok(memories
            .iter()
            .find(|(id, key, _)| *id == allocation.memory_id() && key == allocation.stable_key())
            .map_or(0, |(_, _, owner)| owner.size()))
    })
}

pub(super) fn store_memory_owned(
    memory_id: u64,
    stable_key: &str,
) -> Result<RuntimeMemory<DefaultMemoryImpl>, InternalError> {
    TEST_CONVERGENCE_MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, _, memory)) = memories
            .iter()
            .find(|(id, key, _)| *id == memory_id && key == stable_key)
        {
            return Ok(memory.open());
        }
        let owner = crate::testing::test_memory_owner(memory_id);
        let memory = owner.open();
        memories.push((memory_id, stable_key.to_string(), owner));
        Ok(memory)
    })
}

pub(in crate::db) const CANISTER_CONTROL_ALLOCATION_COUNT: usize = 3;

thread_local! {
    pub(super) static TEST_CONVERGENCE_MEMORIES: std::cell::RefCell<
        Vec<(u64, String, crate::testing::TestMemoryOwner)>
    > = const { std::cell::RefCell::new(Vec::new()) };
}
