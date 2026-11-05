//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db) fn startup_memory<C: CanisterKind>()
-> Result<RuntimeMemory<DefaultMemoryImpl>, InternalError> {
    let id = C::startup_memory_id().map_err(|_| InternalError::store_internal())?;
    TEST_STARTUP_MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, _, memory)) = memories.iter().find(|(memory_id, stable_key, _)| {
            *memory_id == id && *stable_key == C::STARTUP_STABLE_KEY
        }) {
            return Ok(memory.open());
        }
        let owner = crate::testing::test_memory_owner(id);
        let memory = owner.open();
        memories.push((id, C::STARTUP_STABLE_KEY, owner));
        Ok(memory)
    })
}

thread_local! {
    pub(super) static TEST_STARTUP_MEMORIES: RefCell<
        Vec<(u64, &'static str, crate::testing::TestMemoryOwner)>
    > = const { RefCell::new(Vec::new()) };
}
