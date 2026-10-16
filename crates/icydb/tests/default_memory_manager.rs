use ic_memory::{
    AllocationBinding, MemoryAllocationPool, MemoryAuthority, MemoryRequest, RuntimeOpenError,
    SchemaMetadata, SealedDeclarationSnapshot,
};
use icydb::__macro::open_memory;
use icydb::db::{ensure_default_memory_manager, memory_runtime};
use icydb::traits::{CanisterKind, Path};

const AUTHORITY: &str = "icydb.ensure_default_memory_manager_test";
const MEMORY_ID: u8 = 10;
const STABLE_KEY: &str = "icydb.ensure_default_memory_manager_test.commit.control.v1";

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "test::Canister";
}

impl CanisterKind for TestCanister {
    const COMMIT_STABLE_KEY: &'static str = STABLE_KEY;
    const STARTUP_STABLE_KEY: &'static str =
        "icydb.ensure_default_memory_manager_test.startup.control.v1";
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.ensure_default_memory_manager_test.integrity.progress.v1";
}

#[test]
fn ensure_bootstraps_once_then_reuses_committed_allocations() {
    assert!(matches!(
        TestCanister::commit_memory_id(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert!(memory_runtime().is_none());
    assert!(matches!(
        open_memory(STABLE_KEY),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    let requirements = SealedDeclarationSnapshot::new(
        ["commit.control", "startup.control", "integrity.progress"]
            .into_iter()
            .map(|role| {
                MemoryRequest::new(
                    AUTHORITY,
                    &format!("{AUTHORITY}.{role}.v1"),
                    SchemaMetadata::default(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();

    ensure_default_memory_manager(AUTHORITY, &requirements, 16, || {
        MemoryAllocationPool::new(
            vec![MemoryAuthority::new(AUTHORITY, format!("{AUTHORITY}."))?],
            vec![],
        )
    })
    .expect("cold ensure should bootstrap the runtime");
    let generation = memory_runtime()
        .unwrap()
        .committed_allocations()
        .expect("cold ensure should publish committed allocations")
        .generation();

    ensure_default_memory_manager(AUTHORITY, &requirements, 16, || {
        MemoryAllocationPool::new(
            vec![MemoryAuthority::new(AUTHORITY, format!("{AUTHORITY}."))?],
            vec![],
        )
    })
    .expect("repeated ensure should adopt the runtime");
    assert_eq!(
        memory_runtime()
            .unwrap()
            .committed_allocations()
            .expect("repeated ensure should preserve committed allocations")
            .generation(),
        generation,
    );
    open_memory(STABLE_KEY).expect("the ensured allocation should open");
    // Unknown keys cannot open or replace committed authority.
    assert!(matches!(
        open_memory("icydb.ensure_default_memory_manager_test.unknown.v1"),
        Err(RuntimeOpenError::StableKeyNotCommitted(_))
    ));
    open_memory(STABLE_KEY).expect("rejected opens must not change committed authority");

    // Control consumers use the same committed mapping, including host pools
    // below 100. No generated type supplies physical placement constants.
    assert_eq!(TestCanister::commit_memory_id().unwrap(), MEMORY_ID);
    assert_eq!(
        TestCanister::integrity_progress_memory_id().unwrap(),
        MEMORY_ID + 1
    );
    assert_eq!(TestCanister::startup_memory_id().unwrap(), MEMORY_ID + 2);

    let allocations = memory_runtime()
        .unwrap()
        .memory_allocations()
        .expect("the ensured runtime should report allocations");
    assert_eq!(allocations.summary.current_generation, Some(generation));
    assert_eq!(allocations.summary.bucket_size_pages, 16);
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
        memory_runtime()
            .unwrap()
            .memory_allocations()
            .expect("repeated allocation inspection should succeed"),
        allocations,
    );
}
