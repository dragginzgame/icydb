//! Optional count evidence must not become schema-application authority.

use super::*;
use crate::{
    db::{
        DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell, RequestExecutionRoot,
        data::{DecodedDataStoreKey, RawRow},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        record_generated_schema_startup_failure,
        schema::{
            SchemaChangeReceipt, application::apply_generated_schema,
            cardinality_generation::CardinalityGenerationState,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::InputValue,
};
use icydb_schema::SchemaRemoval;

fn initialize() -> (Db<AbortCanister>, EntitySourceKey, TargetStoreIdentity) {
    let db = Db::<AbortCanister>::new(
        &ABORT_REGISTRY,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let target = schema_application_target(&db).unwrap();
    let store_identity = target.stores()[0].identity();
    let (initial, entity, _) = generated_check_proposal(
        target.accepted_head().clone(),
        "presence-initial",
        false,
        target.database_identity(),
        store_identity,
    );
    apply_schema(&db, &initial).unwrap();
    drive_startup_recovery_to_completion(&db);
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    assert_eq!(store.exact_entity_count(tag), None);
    (db, entity, store_identity)
}

fn check_proposal(db: &Db<AbortCanister>, store: TargetStoreIdentity) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    generated_check_proposal(
        target.accepted_head().clone(),
        "presence-add-check",
        true,
        target.database_identity(),
        store,
    )
    .0
}

fn seed(session: &DbSession<AbortCanister>, count: u64, last_score: i64) {
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=count)
                .map(|id| DynamicMutation::Insert {
                    entity: "Item".into(),
                    patch: DynamicStructuralPatch::new(vec![
                        ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                        (
                            "score".into(),
                            DynamicWriteCell::Value(InputValue::int64(if id == count {
                                last_score
                            } else {
                                1
                            })),
                        ),
                    ]),
                })
                .collect(),
        )
        .unwrap();
}

// Exercise the same application/failure handoff used by generated startup hooks.
fn startup_apply(
    db: &Db<AbortCanister>,
    session: &DbSession<AbortCanister>,
    proposal: &SchemaProposal,
) -> SchemaChangeReceipt {
    assert_eq!(
        drive_generated_startup_recovery_page(
            session,
            &ABORT_REGISTRY,
            proposal.submission_key().as_str(),
        )
        .unwrap(),
        GeneratedStartupDriverStep::ApplyGeneratedSchema
    );
    let result = apply_generated_schema(db, proposal);
    if let Err(error) = &result {
        record_generated_schema_startup_failure::<AbortCanister>(
            &ABORT_REGISTRY,
            proposal.submission_key().as_str(),
            error.diagnostic(),
            error.diagnostic_facts(),
        )
        .unwrap();
    }
    result.expect("unavailable optional counts must not fail generated application")
}

#[test]
fn generated_empty_check_reaches_ready_without_cardinality() {
    let (db, _, store) = initialize();
    let session = DbSession::new(&ABORT_REGISTRY, &RequestExecutionRoot::__new_runtime_root());
    let proposal = check_proposal(&db, store);
    let receipt = startup_apply(&db, &session, &proposal);
    assert!(matches!(
        receipt.outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
    assert_eq!(
        observe_generated_startup_state::<AbortCanister>(
            &ABORT_REGISTRY,
            proposal.submission_key().as_str(),
        ),
        Ok(DatabaseStartupState::Ready)
    );
    assert_eq!(apply_generated_schema(&db, &proposal).unwrap(), receipt);
}

#[test]
fn generated_populated_check_uses_bounded_validation_without_cardinality() {
    let (db, _, store) = initialize();
    let session = DbSession::new(&ABORT_REGISTRY, &RequestExecutionRoot::__new_runtime_root());
    seed(&session, 257, 1);
    drive_startup_recovery_to_completion(&db);
    let handle = db.store_handle(ABORT_STORE_PATH).unwrap();
    let journal = handle.journal_tail_store().unwrap();
    assert_eq!(
        handle
            .with_data(
                |data| handle.with_index(|index| handle.with_schema_mut(|schema| {
                    drive_cardinality_generation_page(data, index, schema, |schema| {
                        CardinalityBuildAuthority::derive(
                            schema,
                            database_incarnation_id()?,
                            handle.allocation_identities(),
                            journal.with_borrow(JournalTailStore::fold_watermark)?,
                        )
                    })
                }))
            )
            .unwrap(),
        CardinalityGenerationPageOutcome::WorkRemaining
    );
    assert_eq!(
        handle.with_schema(|schema| schema
            .cardinality_generation_header()
            .unwrap()
            .unwrap()
            .state()),
        CardinalityGenerationState::Building
    );
    let proposal = check_proposal(&db, store);
    let receipt = startup_apply(&db, &session, &proposal);
    assert!(matches!(
        receipt.outcome(),
        SchemaChangeOutcome::Pending { .. }
    ));
    assert!((0..16).any(|_| {
        drive_generated_startup_recovery_page(
            &session,
            &ABORT_REGISTRY,
            proposal.submission_key().as_str(),
        )
        .unwrap()
            == GeneratedStartupDriverStep::Terminal
    }));
    assert_eq!(
        observe_generated_startup_state::<AbortCanister>(
            &ABORT_REGISTRY,
            proposal.submission_key().as_str(),
        ),
        Ok(DatabaseStartupState::Ready)
    );
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 257);
}

#[test]
fn direct_check_without_cardinality_retains_typed_constraint_rejection() {
    let (db, _, store) = initialize();
    let session = DbSession::new(&ABORT_REGISTRY, &RequestExecutionRoot::__new_runtime_root());
    seed(&session, 1, -1);
    let target = schema_application_target(&db).unwrap();
    let error = apply_generated_schema(&db, &check_proposal(&db, store)).unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
    );
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 1);
}

fn removal(db: &Db<AbortCanister>, entity: EntitySourceKey) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    SchemaProposal::try_compose(
        Vec::new(),
        target.database_identity(),
        SchemaSubmissionKey::try_new("presence-remove").unwrap(),
        target.accepted_head().clone(),
        Vec::new(),
        Vec::new(),
        vec![SchemaRemoval::Entity(entity)],
        None,
    )
    .unwrap()
}

#[test]
fn explicit_empty_removal_does_not_require_cardinality() {
    let (db, entity, _) = initialize();
    let proposal = removal(&db, entity);
    let receipt = apply_schema(&db, &proposal).unwrap();
    assert!(matches!(
        receipt.outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
    assert_eq!(apply_schema(&db, &proposal).unwrap(), receipt);
    assert!(
        db.store_handle(ABORT_STORE_PATH)
            .unwrap()
            .with_schema(|schema| schema
                .current_accepted_schema_bundle()
                .unwrap()
                .unwrap()
                .entity_snapshots()
                .is_empty())
    );
}

#[test]
fn explicit_nonempty_removal_keeps_typed_rejection_and_rows() {
    let (db, entity, _) = initialize();
    let session = DbSession::new(&ABORT_REGISTRY, &RequestExecutionRoot::__new_runtime_root());
    seed(&session, 1, 1);
    let target = schema_application_target(&db).unwrap();
    let error = apply_schema(&db, &removal(&db, entity)).unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 1);
}

#[test]
fn generated_check_still_rejects_malformed_rows_as_corruption() {
    let (db, _, store_identity) = initialize();
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let key = DecodedDataStoreKey::new(tag, &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(1)))
        .to_raw()
        .unwrap();
    store
        .with_data_mut(|data| {
            data.apply_recovered_journal_put(key, RawRow::try_new(vec![0xff]).unwrap())
        })
        .unwrap();
    let target = schema_application_target(&db).unwrap();
    let error = apply_generated_schema(&db, &check_proposal(&db, store_identity)).unwrap_err();
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
}

#[test]
fn targeted_rule_edits_apply_with_absent_then_stale_cardinality() {
    let db = Db::<EvolutionCanister>::new(
        &EVOLUTION_REGISTRY,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let target = schema_application_target(&db).unwrap();
    let store_identity = target.stores()[0].identity();
    let proposal = |key, operation| {
        let target = schema_application_target(&db).unwrap();
        targeted_rule_proposal(
            target.accepted_head().clone(),
            key,
            operation,
            target.database_identity(),
            store_identity,
        )
    };
    let (initial, entity_source, constraint_source) = proposal(
        "presence-rule-initial",
        SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(10),
        },
    );
    apply_schema(&db, &initial).unwrap();
    drive_startup_recovery_to_completion(&db);
    let store = db.store_handle(EVOLUTION_STORE_PATH).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Measured")
        .unwrap()
        .entity_tag();
    assert_eq!(store.exact_entity_count(tag), None);
    let (first_edit, _, _) = proposal(
        "presence-rule-first-edit",
        SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(8),
        },
    );
    assert!(matches!(
        apply_generated_schema(&db, &first_edit).unwrap().outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
    drive_startup_recovery_to_completion(&db);
    drive_cardinality_to_ready(store);
    assert_eq!(store.exact_entity_count(tag), Some(0));
    let prior =
        store.with_schema(|schema| schema.current_accepted_schema_bundle().unwrap().unwrap());
    let next = CandidateSchemaRevision::new(
        AcceptedSchemaRevisionBundle::new_with_source_bindings(
            prior.revision().checked_next().unwrap(),
            prior.store_path(),
            prior.enum_catalog().clone(),
            prior.composite_catalog().clone(),
            prior.source_bindings_for_tests().clone(),
            prior.entity_snapshots().clone(),
        )
        .unwrap(),
    )
    .unwrap();
    crate::db::commit::publish_accepted_schema_candidate(
        EVOLUTION_STORE_PATH,
        store,
        prior.revision(),
        &next,
    )
    .unwrap();
    assert_eq!(store.exact_entity_count(tag), None);
    let (edit, _, _) = proposal(
        "presence-rule-stale-edit",
        SourceRuleOperation::MultipleOf {
            divisor: ScalarLiteral::Nat(2),
        },
    );
    assert!(matches!(
        apply_generated_schema(&db, &edit).unwrap().outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
    let current =
        store.with_schema(|schema| schema.current_accepted_schema_bundle().unwrap().unwrap());
    assert_eq!(
        current.source_bindings_for_tests().entity(&entity_source),
        Some(tag)
    );
    assert_eq!(
        current
            .source_bindings_for_tests()
            .constraint(tag, &constraint_source),
        prior
            .source_bindings_for_tests()
            .constraint(tag, &constraint_source)
    );
}

#[test]
fn row_presence_handles_heap_and_maximum_entity_prefix_without_payload_reads() {
    let mut data = DataStore::init_heap();
    let tag = EntityTag::new(u64::MAX);
    let previous = EntityTag::new(u64::MAX - 1);
    let key = |entity| {
        DecodedDataStoreKey::new(
            entity,
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(u64::MAX)),
        )
        .to_raw()
        .unwrap()
    };
    assert!(!super::super::entity_has_rows(&data, tag).unwrap());
    data.insert_raw_for_test(key(previous), RawRow::try_new(vec![0xff]).unwrap());
    assert!(!super::super::entity_has_rows(&data, tag).unwrap());
    let key = key(tag);
    data.insert_raw_for_test(key.clone(), RawRow::try_new(vec![0xff]).unwrap());
    assert!(super::super::entity_has_rows(&data, tag).unwrap());
    data.remove(&key);
    assert!(!super::super::entity_has_rows(&data, tag).unwrap());
}

#[test]
fn empty_removal_probe_qualifies_live_rows_tombstones_and_entity_bounds() {
    let (db, _, _) = initialize();
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let other = EntityTag::new(tag.value().checked_add(1).unwrap());
    let key = DecodedDataStoreKey::new(tag, &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(1)))
        .to_raw()
        .unwrap();
    let other_key =
        DecodedDataStoreKey::new(other, &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(1)))
            .to_raw()
            .unwrap();
    // A presence proof must neither inspect adjacent entities nor decode payloads.
    store.with_data_mut(|data| {
        data.fold_recovered_journal_put(other_key, RawRow::try_new(vec![0xff]).unwrap())
            .unwrap();
    });
    assert!(super::super::require_exact_empty_entity(store, tag).is_ok());
    store
        .with_data_mut(|data| {
            data.apply_recovered_journal_put(key.clone(), RawRow::try_new(vec![0xff]).unwrap())
        })
        .unwrap();
    let error: InternalError = super::super::require_exact_empty_entity(store, tag).unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    store.with_data_mut(|data| {
        data.remove(&key);
    });
    assert!(super::super::require_exact_empty_entity(store, tag).is_ok());
    store
        .with_data_mut(|data| {
            data.fold_recovered_journal_put(key.clone(), RawRow::try_new(vec![0xff]).unwrap())
        })
        .unwrap();
    assert!(
        super::super::require_exact_empty_entity(store, tag).is_ok(),
        "an older canonical put remains hidden by the live deletion"
    );
    store
        .with_data_mut(DataStore::reset_journaled_live_projection)
        .unwrap();
    let error = super::super::require_exact_empty_entity(store, tag).unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    store.with_data_mut(|data| {
        data.remove(&key);
    });
    assert!(super::super::require_exact_empty_entity(store, tag).is_ok());
}
