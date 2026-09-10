use ic_memory::{
    AllocationBinding, MemoryManagerRangeMode, RuntimeDiagnosticError, RuntimeOpenError,
    committed_allocations, default_memory_manager_memory_allocations,
    open_default_memory_manager_memory, register_static_memory_manager_declaration,
    register_static_memory_manager_range,
};
use icydb::db::ensure_default_memory_manager;

const AUTHORITY: &str = "icydb.ensure-default-memory-manager-test";
const MEMORY_ID: u8 = 100;
const STABLE_KEY: &str = "icydb.ensure_default_memory_manager_test.data.v1";

#[test]
fn ensure_bootstraps_once_then_reuses_committed_allocations() {
    assert!(matches!(
        default_memory_manager_memory_allocations(),
        Err(RuntimeDiagnosticError::NotBootstrapped)
    ));
    assert!(matches!(
        committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    register_static_memory_manager_range(
        MEMORY_ID,
        MEMORY_ID,
        AUTHORITY,
        MemoryManagerRangeMode::Reserved,
        None,
    )
    .expect("test authority range should register");
    register_static_memory_manager_declaration(MEMORY_ID, AUTHORITY, "Data", STABLE_KEY)
        .expect("test allocation should register");

    ensure_default_memory_manager(AUTHORITY).expect("cold ensure should bootstrap the runtime");
    let generation = committed_allocations()
        .expect("cold ensure should publish committed allocations")
        .generation();

    ensure_default_memory_manager(AUTHORITY).expect("repeated ensure should adopt the runtime");
    assert_eq!(
        committed_allocations()
            .expect("repeated ensure should preserve committed allocations")
            .generation(),
        generation,
    );
    open_default_memory_manager_memory(STABLE_KEY, MEMORY_ID)
        .expect("the ensured allocation should open");

    let allocations = default_memory_manager_memory_allocations()
        .expect("the ensured runtime should report allocations");
    assert_eq!(allocations.current_generation, Some(generation));
    assert_eq!(allocations.bucket_size_pages, 128);
    let allocation = allocations
        .memories
        .iter()
        .find(|allocation| allocation.memory_manager_id == MEMORY_ID)
        .expect("the ensured slot should be reported");
    assert!(matches!(
        &allocation.binding,
        AllocationBinding::Current { stable_key, owner }
            if stable_key == STABLE_KEY && owner == AUTHORITY
    ));
    assert_eq!(
        default_memory_manager_memory_allocations()
            .expect("repeated allocation inspection should succeed"),
        allocations,
    );
}
