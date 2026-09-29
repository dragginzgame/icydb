//! A changed executable store binding must leave rollback able to recover.

use super::*;
use crate::db::{
    commit::observe_commit_control,
    database_format::forget_database_format_admission_for_tests,
    startup::{
        DatabaseStartupState, GeneratedStartupDriverStep, drive_generated_startup_recovery_page,
        observe_generated_startup_state,
    },
};
use icydb_diagnostic_code::ErrorCode;

const MOVED_PATH: &str = "renamed_schema::stores::MovedStore";
const SUBMISSION: &str = "generated/store-path-rejection";

thread_local! {
    static MOVED_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        MIGRATION_EXECUTION_REGISTRY.with(|old| {
            let (_, handle) = old.iter().next().unwrap();
            registry.register_journaled_store(
                MOVED_PATH,
                handle.data_store(),
                handle.index_store(),
                handle.schema_store(),
                handle.journal_tail_store().unwrap(),
                handle.allocation_identities(),
                handle.storage_capabilities(),
            ).unwrap();
        });
        registry
    };
}

fn check_rejection_and_revert(retained: bool, marker: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let original = initialize(&root, false);
    drive_startup_recovery_to_completion(&original);
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    if retained {
        insert(
            &session,
            "Item",
            vec![
                ("id", InputValue::nat64(3)),
                ("key", InputValue::nat64(103)),
                ("label", InputValue::nat64(203)),
                ("parent_id", InputValue::null()),
            ],
        );
    }
    if marker {
        let candidate = proposal(&schema_application_target(&original).unwrap(), true, false);
        recovery::interrupt_publication(&original, &candidate, false);
    }
    let physical = physical_state(&original);
    let control = observe_commit_control().unwrap();
    let handle = original
        .store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .unwrap();
    let journal = handle.journal_tail_store().unwrap();
    let proof = journal
        .with_borrow(JournalTailStore::proof_identity)
        .unwrap();
    assert_eq!(
        !journal
            .with_borrow(JournalTailStore::validate_current_tail_authority)
            .unwrap()
            .is_empty(),
        retained,
    );
    let bundle = handle
        .with_schema(crate::db::schema::store::SchemaStore::current_accepted_schema_bundle)
        .unwrap();

    forget_recovered_domain_for_tests(&original).unwrap();
    forget_database_format_admission_for_tests();
    let moved = Db::<MigrationExecutionCanister>::new(&MOVED_REGISTRY, root.scope());
    let moved_session = DbSession::<MigrationExecutionCanister>::new(&MOVED_REGISTRY, &root);
    for _ in 0..2 {
        let error =
            drive_generated_startup_recovery_page(&moved_session, &MOVED_REGISTRY, SUBMISSION)
                .expect_err("a deployment mismatch must reject without a terminal receipt");
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_UNSUPPORTED
        );
        assert_eq!(
            observe_generated_startup_state::<MigrationExecutionCanister>(
                &MOVED_REGISTRY,
                SUBMISSION
            ),
            Ok(DatabaseStartupState::Recovering),
        );
        assert_eq!(physical_state(&original), physical);
        assert_eq!(observe_commit_control().unwrap(), control);
        assert_eq!(
            journal
                .with_borrow(JournalTailStore::proof_identity)
                .unwrap(),
            proof
        );
        assert_eq!(
            handle
                .with_schema(crate::db::schema::store::SchemaStore::current_accepted_schema_bundle)
                .unwrap(),
            bundle
        );
    }
    forget_recovered_domain_for_tests(&moved).unwrap();
    forget_database_format_admission_for_tests();
    drive_startup_recovery_to_completion(&original);
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &MIGRATION_EXECUTION_REGISTRY, SUBMISSION)
            .unwrap(),
        GeneratedStartupDriverStep::ApplyGeneratedSchema,
    );
    let entity = if marker { "CatalogItem" } else { "Item" };
    assert_reverted_rows(&session, entity, retained);
    drive_startup_recovery_to_completion(&original);
}

fn assert_reverted_rows(
    session: &DbSession<MigrationExecutionCanister>,
    entity: &str,
    retained: bool,
) {
    let query = DynamicQuery::new(entity).select(["id"]);
    let mut rows = Vec::new();
    let mut continuation = None;
    for _ in 0..4 {
        let page = session
            .execute_trusted_live_page(&query, continuation.as_deref())
            .unwrap();
        rows.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(continuation.is_none());
    assert_eq!(
        rows,
        (1..=if retained { 3 } else { 2 })
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>()
    );
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: entity.into(),
            key: InputValue::nat64(if retained { 3 } else { 2 }),
        })
        .unwrap();
}

#[test]
fn changed_store_path_with_folded_rows_allows_revert() {
    check_rejection_and_revert(false, false);
}

#[test]
fn changed_store_path_preserves_retained_writes_for_revert() {
    check_rejection_and_revert(true, false);
}

#[test]
fn changed_store_path_preserves_publication_marker_for_revert() {
    check_rejection_and_revert(false, true);
}
