//! Store membership cannot re-key an unaffected database, lineage or application.

use super::*;
use crate::db::{
    RequestExecutionRoot, database_format::forget_database_format_admission_for_tests,
};

thread_local! {
    static EXTRA_DATA: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static EXTRA_INDEX: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static EXTRA_SCHEMA: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    static WITH_HEAP: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        ABORT_REGISTRY.with(|base| {
            for (path, store) in base.iter() {
                registry.register_journaled_store(
                    path, store.data_store(), store.index_store(), store.schema_store(),
                    store.journal_tail_store().unwrap(), store.allocation_identities(),
                    store.storage_capabilities(),
                ).unwrap();
            }
        });
        registry.register_store(
            "schema_application_tests::UnusedHeap", &EXTRA_DATA, &EXTRA_INDEX, &EXTRA_SCHEMA,
            StoreAllocationIdentities::absent(), StoreRuntimeStorageCapabilities::heap(),
        ).unwrap();
        registry
    };
}

pub(super) fn add_unused_heap(db: &Db<AbortCanister>) -> Db<AbortCanister> {
    forget_recovered_domain_for_tests(db).unwrap();
    forget_database_format_admission_for_tests();
    let expanded = Db::new(
        &WITH_HEAP,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&expanded);
    expanded
}

pub(super) fn remove_unused_heap(db: &Db<AbortCanister>) -> Db<AbortCanister> {
    forget_recovered_domain_for_tests(db).unwrap();
    forget_database_format_admission_for_tests();
    let restored = Db::new(
        &ABORT_REGISTRY,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&restored);
    restored
}

#[test]
fn database_and_unaffected_store_identity_survive_heap_membership_changes() {
    let db = Db::<AbortCanister>::new(
        &ABORT_REGISTRY,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let before = schema_application_target(&db).unwrap();
    let (proposal, _, _) = generated_check_proposal(
        before.accepted_head().clone(),
        "topology-initial",
        false,
        before.database_identity(),
        before.stores()[0].identity(),
    );
    apply_schema(&db, &proposal).unwrap();
    let before = schema_application_target(&db).unwrap();
    let expanded = add_unused_heap(&db);
    let after = schema_application_target(&expanded).unwrap();
    assert_eq!(after.database_identity(), before.database_identity());
    assert_eq!(
        after
            .stores()
            .iter()
            .find(|store| store.path() == ABORT_STORE_PATH)
            .unwrap()
            .identity(),
        before.stores()[0].identity()
    );
    assert_ne!(after.accepted_head(), before.accepted_head());
    let restored = remove_unused_heap(&expanded);
    assert_eq!(schema_application_target(&restored).unwrap(), before);
    assert_eq!(
        apply_schema(&restored, &proposal).unwrap().outcome(),
        apply_schema(&db, &proposal).unwrap().outcome()
    );
}

#[cfg(feature = "migration")]
#[test]
fn migrated_lineage_remains_usable_after_an_unrelated_heap_store_is_removed() {
    use crate::db::schema::{
        SchemaMigrationCommand, SchemaMigrationPhase, application::migrate_schema,
    };
    let db = Db::<AbortCanister>::new(
        &WITH_HEAP,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let target = schema_application_target(&db).unwrap();
    let store = target
        .stores()
        .iter()
        .find(|store| store.path() == ABORT_STORE_PATH)
        .unwrap()
        .identity();
    let (initial, _, _) = generated_check_proposal(
        target.accepted_head().clone(),
        "topology-migrate-initial",
        false,
        target.database_identity(),
        store,
    );
    apply_schema(&db, &initial).unwrap();
    let migrate = |db: &Db<AbortCanister>, from: &str, to: &str, version: u32| {
        let target = schema_application_target(db).unwrap();
        let original = &initial.fragments()[0].entities()[0];
        let next = EntityFragment::try_new(
            name(to),
            DeclaredEntityVersion::try_new(version).unwrap(),
            original.fields().to_vec(),
            original.primary_key().to_vec(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let transition = EntityMigration::try_new(
            next.source_key().clone(),
            DeclaredEntityVersion::try_new(version - 1).unwrap(),
            Some(EntitySourceKey::try_new(from).unwrap()),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::VERSIONED_MIGRATIONS],
            target.database_identity(),
            SchemaSubmissionKey::try_new(format!("topology-migrate-{version}")).unwrap(),
            target.accepted_head().clone(),
            vec![SchemaFragment::try_new(vec![next.clone()], Vec::new()).unwrap()],
            vec![EntityStoreAssignment::new(next.source_key().clone(), store)],
            Vec::new(),
            Some(SchemaMigrationPlan::try_new(vec![transition]).unwrap()),
        )
        .unwrap();
        for _ in 0..32 {
            let page = migrate_schema(
                db,
                &proposal,
                SchemaMigrationCommand::Advance {
                    expected_database: proposal.target_database(),
                    expected_head: proposal.expected_head().clone(),
                    expected_plan: proposal.migration().unwrap().digest(),
                },
            )
            .unwrap();
            if page.phase() == SchemaMigrationPhase::Applied {
                return;
            }
        }
        panic!("metadata migration must complete within bounded advancement");
    };
    migrate(&db, "Item", "VersionTwo", 2);
    let restored = remove_unused_heap(&db);
    migrate(&restored, "VersionTwo", "VersionThree", 3);
}
