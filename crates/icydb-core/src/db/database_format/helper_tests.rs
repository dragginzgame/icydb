//! Test-only helpers for the parent module.

use super::*;

/// Model the volatile format-admission reset performed by an upgrade.
pub(in crate::db) fn forget_database_format_admission_for_tests() {
    DATABASE_FORMAT_ADMISSIONS.with_borrow_mut(Vec::clear);
}

pub(in crate::db) fn initialize_current_database_control_for_tests(
    memory: &std::rc::Rc<RuntimeMemory<DefaultMemoryImpl>>,
) {
    write_current_boot_record(memory).expect("test database control memory should grow");
    crate::db::commit::initialize_current_commit_control_for_tests(memory.clone())
        .expect("test current commit control should initialize");
}

pub(super) fn write_current_boot_record<M: Memory>(
    control_memory: &RuntimeMemory<M>,
) -> Result<(), InternalError> {
    if control_memory.size() == 0 {
        control_memory
            .grow(1)
            .map_err(|_| InternalError::recovery_database_format_control_unavailable())?;
    }

    control_memory.write(0, &DatabaseBootRecord::current().encode());
    Ok(())
}

pub(in crate::db) fn allocation_pages(
    allocation: StoreAllocationIdentity,
) -> Result<u64, InternalError> {
    TEST_STORE_ROLE_MEMORIES.with_borrow_mut(|memories| {
        if let Some((_, owner)) = memories
            .iter()
            .find(|(existing, _)| *existing == allocation)
        {
            return Ok(owner.size());
        }
        memories.push((
            allocation,
            crate::testing::test_memory_owner(allocation.memory_id()),
        ));
        Ok(0)
    })
}

thread_local! {
    pub(super) static TEST_STORE_ROLE_MEMORIES: RefCell<
        Vec<(StoreAllocationIdentity, crate::testing::TestMemoryOwner)>
    > = const { RefCell::new(Vec::new()) };
}
