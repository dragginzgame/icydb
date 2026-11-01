//! Unique abort retires the staged physical generation before publishing success.

use super::*;
use crate::{
    db::{
        commit::{
            SchemaPublicationInterruption, interrupt_next_schema_publication_for_tests,
            retained_commit_marker_measurement_for_tests,
        },
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexStore, IndexStoreVisit,
            decode_index_key_frame,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        resource::budget::MaintenanceConstructionBudget,
        schema::{SchemaVersion, StagedUserIndexDomainReplacement},
    },
    error::InternalError,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

fn candidate_key_count(
    db: &Db<AbortCanister>,
    candidate: &crate::db::schema::PersistedIndexSnapshot,
) -> usize {
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let id =
        IndexId::new_with_generation(tag, candidate.ordinal(), candidate.physical_generation());
    db.store_handle(ABORT_STORE_PATH)
        .unwrap()
        .with_index(|store| {
            let mut count = 0;
            store
                .visit_entries(|key, _| {
                    let key = decode_index_key_frame(key).unwrap();
                    if key.key_kind() == IndexKeyKind::User && key.index_id() == id {
                        count += 1;
                    }
                    Ok::<_, InternalError>(IndexStoreVisit::Continue)
                })
                .unwrap();
            count
        })
}

fn staged_unique() -> (Db<AbortCanister>, DbSession<AbortCanister>) {
    let (db, session) = initialize();
    execute_ddl(&db, &session, UNIQUE);
    validate_constraint(&session, "sql_unique", None);
    validate_constraint(&session, "sql_unique", None);
    let before = snapshot(&db);
    assert_eq!(before.candidate_indexes().len(), 1);
    assert_eq!(candidate_key_count(&db, &before.candidate_indexes()[0]), 2);
    (db, session)
}

#[test]
fn aborted_unique_generation_does_not_poison_later_index_ddl() {
    let (db, session) = staged_unique();
    let candidate = snapshot(&db).candidate_indexes()[0].clone();
    let before = snapshot(&db);
    execute_ddl(&db, &session, "DROP INDEX sql_unique ON Item");
    assert!(snapshot(&db).candidate_indexes().is_empty());
    assert!(job(&db, &before).is_none());
    assert_eq!(candidate_key_count(&db, &candidate), 0);
    execute_ddl(&db, &session, "CREATE INDEX by_score ON Item (score)");
    execute_ddl(&db, &session, "DROP INDEX by_score ON Item");
}

#[test]
fn aborted_unique_with_a_conflict_receipt_does_not_poison_index_ddl() {
    let (db, session) = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Update {
            entity: "Item".into(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                "score".into(),
                DynamicWriteCell::Value(InputValue::int64(2)),
            )]),
        }])
        .unwrap();
    execute_ddl(&db, &session, UNIQUE);
    validate_constraint(&session, "sql_unique", None);
    let page = validate_constraint(&session, "sql_unique", None);
    assert!(!page.constraint_validation().unwrap().findings().is_empty());
    let before = snapshot(&db);
    assert!(candidate_key_count(&db, &before.candidate_indexes()[0]) > 0);
    execute_ddl(&db, &session, "DROP INDEX sql_unique ON Item");
    assert!(job(&db, &before).is_none());
    assert_eq!(candidate_key_count(&db, &before.candidate_indexes()[0]), 0);
    execute_ddl(&db, &session, "CREATE INDEX by_score ON Item (score)");
}

#[test]
fn aborted_unique_allows_empty_generated_entity_removal() {
    let (db, session) = staged_unique();
    execute_ddl(&db, &session, "DROP INDEX sql_unique ON Item");
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=2)
                .map(|id| DynamicMutation::Delete {
                    entity: "Item".into(),
                    key: InputValue::nat64(id),
                })
                .collect(),
        )
        .unwrap();
    drive_startup_recovery_to_completion(&db);
    let target = schema_application_target(&db).unwrap();
    let removal = SchemaProposal::try_compose(
        Vec::new(),
        target.database_identity(),
        SchemaSubmissionKey::try_new("remove-after-unique-abort").unwrap(),
        target.accepted_head().clone(),
        Vec::new(),
        Vec::new(),
        vec![icydb_schema::SchemaRemoval::Entity(
            EntitySourceKey::try_new("Item").unwrap(),
        )],
        None,
    )
    .unwrap();
    crate::db::schema::application::apply_schema(&db, &removal).unwrap();
    let bundle = db
        .store_handle(ABORT_STORE_PATH)
        .unwrap()
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    assert_eq!(bundle.entity_snapshots().len(), 1);
    assert_eq!(
        bundle
            .entity_snapshots()
            .values()
            .next()
            .unwrap()
            .entity_path(),
        "Other"
    );
}

#[test]
fn new_write_gate_can_abort_before_any_validation_job_exists() {
    let (db, session) = initialize();
    execute_ddl(&db, &session, UNIQUE);
    let before = snapshot(&db);
    assert!(job(&db, &before).is_none());
    execute_ddl(&db, &session, "DROP INDEX sql_unique ON Item");
    assert!(snapshot(&db).candidate_indexes().is_empty());
    execute_ddl(&db, &session, "CREATE INDEX by_score ON Item (score)");
}

#[test]
fn abort_keeps_an_accepted_index_generation() {
    let (db, session) = initialize();
    execute_ddl(&db, &session, "CREATE INDEX by_note ON Item (note)");
    execute_ddl(&db, &session, UNIQUE);
    validate_constraint(&session, "sql_unique", None);
    validate_constraint(&session, "sql_unique", None);
    let before = snapshot(&db);
    let remaining = before
        .indexes()
        .iter()
        .find(|index| index.name() == "by_note")
        .unwrap();
    assert_eq!(candidate_key_count(&db, remaining), 2);
    execute_ddl(&db, &session, "DROP INDEX sql_unique ON Item");
    assert_eq!(snapshot(&db).indexes(), before.indexes());
    assert_eq!(candidate_key_count(&db, remaining), 2);
    execute_ddl(&db, &session, "DROP INDEX by_note ON Item");
}

fn stage_abort(
    db: &Db<AbortCanister>,
    store: &IndexStore,
    work: &MaintenanceConstructionBudget,
) -> Result<StagedUserIndexDomainReplacement, InternalError> {
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let selection = db
        .store_handle(ABORT_STORE_PATH)
        .unwrap()
        .with_schema(|schema| {
            schema
                .current_accepted_catalog_selection(tag, "Item", ABORT_STORE_PATH)
                .unwrap()
                .unwrap()
        });
    let before = selection.snapshot();
    let snapshot = before.persisted_snapshot();
    let id = snapshot.constraint_activations()[0].id();
    let after = snapshot
        .with_aborted_unique_activation(id, SchemaVersion::new(snapshot.version().get() + 1))
        .unwrap();
    StagedUserIndexDomainReplacement::stage_unique_activation_abort(
        selection.identity(),
        &before,
        &after,
        id,
        store,
        work,
    )
}

#[test]
fn abort_staging_exhaustion_is_zero_write_and_preserves_the_exact_resource() {
    let (db, session) = staged_unique();
    let before = snapshot(&db);
    let before_job = job(&db, &before);
    drive_startup_recovery_to_completion(&db);
    let marker = retained_commit_marker_measurement_for_tests().unwrap();
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    for (resource, limit) in [
        (Resource::PredicateExpressionSteps, 1),
        (Resource::TemporaryBytes, 0),
    ] {
        let error = store
            .with_index(|index| {
                stage_abort(
                    &db,
                    index,
                    &MaintenanceConstructionBudget::with_limit_for_tests(resource, limit),
                )
            })
            .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        assert_eq!(snapshot(&db), before);
        assert_eq!(job(&db, &before), before_job);
        assert_eq!(candidate_key_count(&db, &before.candidate_indexes()[0]), 2);
        assert_eq!(
            retained_commit_marker_measurement_for_tests().unwrap(),
            marker
        );
    }
    let staged = store
        .with_index(|index| {
            stage_abort(
                &db,
                index,
                &MaintenanceConstructionBudget::with_limit_for_tests(
                    Resource::PredicateExpressionSteps,
                    2,
                ),
            )
        })
        .unwrap();
    assert_eq!(staged.deletion_keys().len(), 2);
    execute_ddl(&db, &session, "DROP INDEX sql_unique ON Item");
}

#[test]
fn abort_staging_selects_only_its_exact_namespace_and_rejects_bad_arity() {
    let (db, _) = staged_unique();
    let before = snapshot(&db);
    let candidate = &before.candidate_indexes()[0];
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let id =
        IndexId::new_with_generation(tag, candidate.ordinal(), candidate.physical_generation());
    let raw = |id: &IndexId, kind, arity| {
        IndexKey::new_from_components_with_primary_key_value(
            id,
            kind,
            &vec![vec![42]; arity],
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(1)),
        )
        .unwrap()
        .to_raw()
        .unwrap()
    };
    let mut store = IndexStore::init_heap();
    let wanted = raw(&id, IndexKeyKind::User, 1);
    for key in [
        wanted.clone(),
        raw(&id, IndexKeyKind::System, 1),
        raw(
            &IndexId::new_with_generation(tag, candidate.ordinal(), 0),
            IndexKeyKind::User,
            1,
        ),
        raw(
            &IndexId::new_with_generation(tag, u16::MAX, u64::MAX),
            IndexKeyKind::User,
            1,
        ),
        raw(
            &IndexId::new_with_generation(crate::types::EntityTag::new(tag.value() ^ 1), 0, 0),
            IndexKeyKind::User,
            1,
        ),
    ] {
        store.insert(key, IndexEntryValue::presence());
    }
    let staged = stage_abort(&db, &store, &MaintenanceConstructionBudget::new()).unwrap();
    assert_eq!(staged.deletion_keys(), &[wanted]);
    store.insert(raw(&id, IndexKeyKind::User, 2), IndexEntryValue::presence());
    let error = stage_abort(&db, &store, &MaintenanceConstructionBudget::new()).unwrap_err();
    assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
    assert_eq!(store.len(), 6);
}

#[test]
fn interrupted_abort_replays_and_folds_without_orphaning_keys_or_jobs() {
    for cut in [
        SchemaPublicationInterruption::MarkerPersisted,
        SchemaPublicationInterruption::IndexDeleted,
    ] {
        std::thread::spawn(move || {
            let (db, session) = staged_unique();
            drive_startup_recovery_to_completion(&db);
            let before = snapshot(&db);
            let version = before.version().get();
            interrupt_next_schema_publication_for_tests(cut);
            session.execute_admin_sql_ddl(&format!("DROP INDEX sql_unique ON Item EXPECT SCHEMA VERSION {version} SET SCHEMA VERSION {}", version + 1)).unwrap_err();
            assert!(retained_commit_marker_measurement_for_tests().unwrap().is_some());
            // Lose heap recovery progress both after Replay and after durable Fold.
            forget_recovered_domain_for_tests(&db).unwrap();
            assert!(!db.drive_startup_recovery_page().unwrap());
            forget_recovered_domain_for_tests(&db).unwrap();
            assert!(!db.drive_startup_recovery_page().unwrap());
            assert!(!db.drive_startup_recovery_page().unwrap());
            forget_recovered_domain_for_tests(&db).unwrap();
            drive_startup_recovery_to_completion(&db);
            assert!(snapshot(&db).candidate_indexes().is_empty());
            assert!(job(&db, &before).is_none());
            assert_eq!(candidate_key_count(&db, &before.candidate_indexes()[0]), 0);
            assert!(retained_commit_marker_measurement_for_tests().unwrap().is_none());
            assert!(!ABORT_JOURNAL.with_borrow(JournalTailStore::has_stored_batch));
            execute_ddl(&db, &session, "CREATE INDEX by_score ON Item (score)");
            execute_ddl(&db, &session, "DROP INDEX by_score ON Item");
        }).join().unwrap();
    }
}
