mod schema_snapshot;

use super::*;

#[test]
fn recovery_readiness_follows_registry_storage_and_commit_allocation() {
    thread_local! {
        static STORES: StoreRegistry = StoreRegistry::new();
        static OTHER_STORES: StoreRegistry = StoreRegistry::new();
    }
    static DRIVER: LocalKey<StoreRegistry> = STORES;
    static READER: LocalKey<StoreRegistry> = STORES;
    let domain = |stores| RecoveryDomainKey {
        commit_allocation: CommitMemoryAllocation {
            memory_id: 0,
            stable_key: "recovery-registry-test",
        },
        runtime_stores: runtime_store_domain_key(stores),
    };
    let driver = domain(&DRIVER);
    let reader = domain(&READER);
    let other_registry = domain(&OTHER_STORES);
    let other_allocation = RecoveryDomainKey {
        commit_allocation: CommitMemoryAllocation {
            memory_id: 1,
            ..driver.commit_allocation
        },
        ..reader
    };

    mark_recovery_domain_recovered(driver).unwrap();
    mark_recovery_domain_in_progress(driver);
    assert!(recovery_domain_recovered(reader).unwrap());
    assert!(recovery_domain_in_progress(reader));
    for isolated in [other_registry, other_allocation] {
        assert!(!recovery_domain_recovered(isolated).unwrap());
        assert!(!recovery_domain_in_progress(isolated));
    }
    clear_recovery_domain_in_progress(reader);
    assert!(!recovery_domain_in_progress(driver));
}

#[test]
fn missing_commit_allocation_reports_recovery_origin_before_selecting_memory() {
    struct UnbootstrappedCanister;
    impl crate::traits::Path for UnbootstrappedCanister {
        const PATH: &'static str = "recovery::UnbootstrappedCanister";
    }
    impl CanisterKind for UnbootstrappedCanister {
        fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
            Err(ic_storage::RuntimeOpenError::NotBootstrapped)
        }
        const COMMIT_STABLE_KEY: &'static str = "icydb.missing.commit.control.v1";
        const STARTUP_STABLE_KEY: &'static str = "icydb.missing.startup.control.v1";
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.missing.integrity.progress.v1";
    }
    thread_local! {
        static STORES: crate::db::StoreRegistry = crate::db::StoreRegistry::new();
    }
    let db = Db::<UnbootstrappedCanister>::new(
        &STORES,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    let before = crate::db::commit::memory::current_commit_memory_allocation_if_configured();
    let error = ensure_recovery_admitted(&db).expect_err("missing allocation must reject");
    assert_eq!(error.class, crate::error::ErrorClass::Internal);
    assert_eq!(error.origin, ErrorOrigin::Recovery);
    assert_eq!(
        crate::db::commit::memory::current_commit_memory_allocation_if_configured(),
        before
    );
}

#[test]
fn recovery_stage_is_bound_to_the_current_marker_identity() {
    let key = RecoveryDomainKey {
        commit_allocation: CommitMemoryAllocation {
            memory_id: 0,
            stable_key: "recovery-stage-test",
        },
        runtime_stores: RuntimeStoreDomainKey { store_registry: 0 },
    };
    mark_recovery_domain_in_progress(key);
    assert_eq!(
        recovery_stage(key, Some([1; COMMIT_ID_BYTES])).unwrap(),
        RecoveryStage::Replay
    );
    advance_recovery_stage(key, RecoveryStage::Verify).unwrap();
    assert_eq!(
        recovery_stage(key, Some([1; COMMIT_ID_BYTES])).unwrap(),
        RecoveryStage::Verify
    );
    assert_eq!(
        recovery_stage(key, Some([2; COMMIT_ID_BYTES])).unwrap(),
        RecoveryStage::Replay
    );
    advance_recovery_stage(key, RecoveryStage::Fold).unwrap();
    assert_eq!(recovery_stage(key, None).unwrap(), RecoveryStage::Replay);
    clear_recovery_domain_in_progress(key);
    assert!(!recovery_domain_in_progress(key));
}

#[test]
fn online_selector_order_is_database_then_allocation_then_tail_sequence() {
    let key = |database, allocation, sequence| JournalHeadOrder {
        database_commit_sequence: DatabaseCommitSequence::new(database),
        journal_allocation: allocation,
        journal_sequence: JournalSequence::new(sequence),
    };

    assert!(key(4, 250, 90) < key(5, 1, 1));
    assert!(key(5, 1, 90) < key(5, 2, 1));
    assert!(key(5, 2, 1) < key(5, 2, 2));
}
