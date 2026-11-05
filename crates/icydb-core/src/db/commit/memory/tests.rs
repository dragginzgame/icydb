use super::*;
use ic_storage::Memory as _;

#[test]
fn current_commit_selection_can_switch_databases_and_return() {
    let first = CommitMemoryAllocation {
        memory_id: 12,
        stable_key: "icydb.test.commit.control.v1",
    };
    let second = CommitMemoryAllocation {
        memory_id: 30,
        stable_key: "icydb.test.commit.peer-control.v1",
    };

    for allocation in [first, first, second, first] {
        select_commit_memory_allocation(allocation.memory_id, allocation.stable_key);
        assert_eq!(current_commit_memory_allocation().unwrap(), allocation);
    }
}

#[test]
fn readiness_owner_changes_do_not_duplicate_the_control_lease() {
    struct Generated<const SOURCE: u8>;
    impl<const SOURCE: u8> crate::traits::Path for Generated<SOURCE> {
        const PATH: &'static str = "test::Generated";
    }
    impl<const SOURCE: u8> CanisterKind for Generated<SOURCE> {
        const COMMIT_STABLE_KEY: &'static str = "icydb.readiness.commit.v1";
        const STARTUP_STABLE_KEY: &'static str = "icydb.readiness.startup.v1";
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.readiness.integrity.v1";

        fn generated_schema_application_record_key() -> Result<Option<[u8; 32]>, InternalError> {
            Ok(Some([SOURCE; 32]))
        }
    }

    let allocation = CommitMemoryAllocation {
        memory_id: 94,
        stable_key: "icydb.readiness.commit.v1",
    };
    select_commit_memory_allocation_for::<Generated<1>>(94, allocation.stable_key);
    let lease = commit_memory_handle(allocation).unwrap();
    assert_eq!(
        current_schema_application_readiness_key().unwrap(),
        Some([1; 32])
    );
    select_commit_memory_allocation_for::<Generated<2>>(94, allocation.stable_key);
    assert_eq!(
        current_schema_application_readiness_key().unwrap(),
        Some([2; 32])
    );
    assert!(Rc::ptr_eq(
        &lease,
        &commit_memory_handle(allocation).unwrap()
    ));

    select_commit_memory_allocation(95, "icydb.readiness.other-control.v1");
    assert_eq!(current_schema_application_readiness_key().unwrap(), None);
    select_commit_memory_allocation_for::<Generated<1>>(94, allocation.stable_key);
    assert_eq!(
        current_schema_application_readiness_key().unwrap(),
        Some([1; 32])
    );
    assert!(Rc::ptr_eq(
        &lease,
        &commit_memory_handle(allocation).unwrap()
    ));
}

#[test]
fn selected_control_borrow_keeps_the_live_lease_and_refuses_another_key() {
    const KEY: &str = "icydb.test.borrowed.commit.control.v1";
    select_commit_memory_allocation(93, KEY);
    let owner = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    owner.as_ref().grow(1).unwrap();
    owner.write(0, b"retained");
    let bytes = with_selected_commit_memory(KEY, |memory| {
        let mut bytes = [0; 8];
        memory.read(0, &mut bytes);
        bytes
    })
    .unwrap();
    assert_eq!(&bytes, b"retained");

    let called = Cell::new(false);
    assert!(
        with_selected_commit_memory("icydb.test.other.commit.control.v1", |_| {
            called.set(true);
        })
        .is_err()
    );
    assert!(!called.get());
    let mut retained = [0; 8];
    owner.read(0, &mut retained);
    assert_eq!(&retained, b"retained");
}
