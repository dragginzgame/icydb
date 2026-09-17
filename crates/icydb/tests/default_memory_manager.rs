use ic_memory::{
    AllocationBinding, MemoryManagerRangeMode, MemoryRequest, RuntimeDiagnosticError,
    RuntimeOpenError, SchemaMetadata, committed_allocations,
    default_memory_manager_memory_allocations, open_default_memory_manager_memory,
    open_default_memory_manager_memory_by_key, register_memory_request,
    register_static_memory_manager_range,
};
use icydb::db::ensure_default_memory_manager;
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
        MEMORY_ID + 9,
        AUTHORITY,
        MemoryManagerRangeMode::Allowed,
        None,
    )
    .expect("test authority range should register");
    for role in ["commit.control", "startup.control", "integrity.progress"] {
        register_memory_request(
            MemoryRequest::new(
                AUTHORITY,
                &format!("{AUTHORITY}.{role}.v1"),
                SchemaMetadata::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }

    ensure_default_memory_manager(AUTHORITY, 16).expect("cold ensure should bootstrap the runtime");
    let generation = committed_allocations()
        .expect("cold ensure should publish committed allocations")
        .generation();

    ensure_default_memory_manager(AUTHORITY, 16).expect("repeated ensure should adopt the runtime");
    assert_eq!(
        committed_allocations()
            .expect("repeated ensure should preserve committed allocations")
            .generation(),
        generation,
    );
    open_default_memory_manager_memory_by_key(STABLE_KEY)
        .expect("the ensured allocation should open");
    // The committed runtime, not a consumer's parallel collision registry,
    // rejects mismatched and undeclared opens without replacing authority.
    assert!(matches!(
        open_default_memory_manager_memory(STABLE_KEY, MEMORY_ID + 1),
        Err(RuntimeOpenError::MemoryIdMismatch { committed_id, requested_id, .. })
            if committed_id == MEMORY_ID && requested_id == MEMORY_ID + 1
    ));
    assert!(matches!(
        open_default_memory_manager_memory(
            "icydb.ensure_default_memory_manager_test.unknown.v1",
            MEMORY_ID
        ),
        Err(RuntimeOpenError::StableKeyNotCommitted(_))
    ));
    open_default_memory_manager_memory(STABLE_KEY, MEMORY_ID)
        .expect("rejected opens must not change committed authority");

    // Control consumers use the same committed mapping, including host pools
    // below 100. No generated type supplies physical placement constants.
    assert_eq!(TestCanister::commit_memory_id().unwrap(), MEMORY_ID);
    assert_eq!(
        TestCanister::integrity_progress_memory_id().unwrap(),
        MEMORY_ID + 1
    );
    assert_eq!(TestCanister::startup_memory_id().unwrap(), MEMORY_ID + 2);

    let allocations = default_memory_manager_memory_allocations()
        .expect("the ensured runtime should report allocations");
    assert_eq!(allocations.current_generation, Some(generation));
    assert_eq!(allocations.bucket_size_pages, 16);
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
