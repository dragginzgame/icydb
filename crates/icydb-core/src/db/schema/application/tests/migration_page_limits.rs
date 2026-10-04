//! Public migration controls must make progress across entity page boundaries.

use super::*;
use crate::{
    db::{
        DynamicQuery, RequestExecutionRoot,
        schema::{
            SchemaApplicationTarget, SchemaMigrationCommand, SchemaMigrationFindingKind,
            SchemaMigrationPhase, SchemaMigrationStatusPage, migrate_schema,
        },
    },
    value::OutputValue,
};

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).unwrap()
}

fn proposal(target: &SchemaApplicationTarget, current: bool) -> SchemaProposal {
    let entities = ["LargeA", "LargeB"];
    let fragments = entities
        .iter()
        .map(|entity| {
            let mut fields = vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("payload"),
                    FieldType::Scalar(ScalarType::Blob { max_len: None }),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ];
            if current {
                fields.push(FieldFragment::new(
                    name("copied"),
                    FieldType::Scalar(ScalarType::Blob { max_len: None }),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ));
            }
            EntityFragment::try_new(
                name(entity),
                DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).unwrap(),
                fields,
                vec![field("id")],
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .unwrap()
        })
        .collect();
    let migration = current.then(|| {
        SchemaMigrationPlan::try_new(
            entities
                .iter()
                .map(|entity| {
                    EntityMigration::try_new(
                        EntitySourceKey::try_new(*entity).unwrap(),
                        version_one(),
                        None,
                        Vec::new(),
                        vec![SchemaMigrationTransform::Copy {
                            from: field("payload"),
                            to: field("copied"),
                        }],
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap()
    });
    SchemaProposal::try_compose(
        if current {
            vec![SchemaCapability::VERSIONED_MIGRATIONS]
        } else {
            Vec::new()
        },
        target.database_identity(),
        SchemaSubmissionKey::try_new(if current { "large-v2" } else { "large-v1" }).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(fragments, Vec::new()).unwrap()],
        entities
            .iter()
            .map(|entity| {
                EntityStoreAssignment::new(
                    EntitySourceKey::try_new(*entity).unwrap(),
                    target.stores()[0].identity(),
                )
            })
            .collect(),
        Vec::new(),
        migration,
    )
    .unwrap()
}

fn advance(
    db: &Db<MigrationExecutionCanister>,
    next: &SchemaProposal,
) -> SchemaMigrationStatusPage {
    migrate_schema(
        db,
        next,
        SchemaMigrationCommand::Advance {
            expected_database: next.target_database(),
            expected_head: next.expected_head().clone(),
            expected_plan: next.migration().unwrap().digest(),
            acknowledged_finding_page: None,
        },
    )
    .unwrap()
}

fn insert(session: &DbSession<MigrationExecutionCanister>, entity: &str, id: u64, bytes: usize) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: entity.into(),
            patch: DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                (
                    "payload".into(),
                    DynamicWriteCell::Value(InputValue::blob(vec![7; bytes])),
                ),
            ]),
        })
        .unwrap();
}

fn ordered_entities(db: &Db<MigrationExecutionCanister>) -> Vec<&'static str> {
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let accepted = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    let mut entities = vec!["LargeA", "LargeB"];
    entities.sort_by_key(|entity| {
        accepted
            .source_bindings_for_tests()
            .entity(&EntitySourceKey::try_new(*entity).unwrap())
            .unwrap()
    });
    entities
}

fn assert_large_migration_completes(first_bytes: usize, second_bytes: usize) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(
        &db,
        &proposal(&schema_application_target(&db).unwrap(), false),
    )
    .unwrap();
    let entities = ordered_entities(&db);
    let session =
        DbSession::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    insert(&session, entities[0], 1, first_bytes);
    insert(&session, entities[0], 2, first_bytes);
    insert(&session, entities[1], 1, second_bytes);
    let next = proposal(&schema_application_target(&db).unwrap(), true);
    let mut phases = Vec::new();
    let mut applied = None;
    for _ in 0..24 {
        let page = advance(&db, &next);
        phases.push(page.phase());
        if page.phase() == SchemaMigrationPhase::Applied {
            applied = Some(page);
            break;
        }
    }
    let applied = applied.expect("bounded migration must complete");
    assert_eq!(applied.rows_validated(), 3);
    assert_eq!(applied.rows_rewritten(), 3);
    assert!(phases.contains(&SchemaMigrationPhase::RewritingRows));
    assert!(phases.contains(&SchemaMigrationPhase::FinalValidation));
    for (entity, bytes) in [(entities[0], first_bytes), (entities[1], second_bytes)] {
        let rows = session
            .execute_trusted_live_page(
                &DynamicQuery::new(entity).select(["payload", "copied"]),
                None,
            )
            .unwrap();
        assert!(rows.continuation.is_none());
        assert!(!rows.rows.is_empty());
        for row in rows.rows {
            assert_eq!(row, vec![OutputValue::blob(vec![7; bytes]); 2]);
        }
    }
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_eq!(advance(&db, &next), applied);
}

#[test]
fn migration_page_limits_rewrite_and_final_validation_carry_between_entities() {
    // Validation fits one page. Rewriting A consumes 600 KiB; B needs 450 KiB.
    assert_large_migration_completes(150 * 1024, 225 * 1024);
}

#[test]
fn migration_page_limits_validation_carries_between_entities() {
    // B fits a fresh validation page but not the 424 KiB left by A.
    assert_large_migration_completes(300 * 1024, 450 * 1024);
}

fn assert_capacity_rejection_is_abortable(bytes: usize) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(
        &db,
        &proposal(&schema_application_target(&db).unwrap(), false),
    )
    .unwrap();
    let session =
        DbSession::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    insert(&session, "LargeA", 1, bytes);
    let next = proposal(&schema_application_target(&db).unwrap(), true);
    assert_eq!(advance(&db, &next).phase(), SchemaMigrationPhase::Prepared);
    assert_eq!(
        advance(&db, &next).phase(),
        SchemaMigrationPhase::Validating
    );
    let rejected = advance(&db, &next);
    assert_eq!(rejected.phase(), SchemaMigrationPhase::Rejected);
    assert_eq!(rejected.rows_validated(), 1);
    assert_eq!(rejected.rows_rewritten(), 0);
    assert_eq!(rejected.findings().len(), 1);
    assert_eq!(
        rejected.findings()[0].kind(),
        SchemaMigrationFindingKind::ResourceLimit
    );
    assert!(!rejected.findings()[0].primary_key().is_empty());
    // Both the public wire and the durable record retain the new classification.
    let wire = candid::encode_one(&rejected).unwrap();
    assert_eq!(
        candid::decode_one::<SchemaMigrationStatusPage>(&wire).unwrap(),
        rejected
    );
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_eq!(advance(&db, &next), rejected);
    let aborted = migrate_schema(
        &db,
        &next,
        SchemaMigrationCommand::Abort {
            expected_database: next.target_database(),
            expected_head: next.expected_head().clone(),
            expected_plan: next.migration().unwrap().digest(),
        },
    )
    .unwrap();
    assert_eq!(aborted.phase(), SchemaMigrationPhase::Aborted);
    let rows = session
        .execute_trusted_live_page(&DynamicQuery::new("LargeA").select(["id", "payload"]), None)
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![
            OutputValue::nat64(1),
            OutputValue::blob(vec![7; bytes])
        ]]
    );
}

#[test]
fn migration_page_limits_reject_candidate_growth_before_rewrite() {
    assert_capacity_rejection_is_abortable(600 * 1024);
}

#[test]
fn migration_page_limits_reject_oversized_before_row_without_decoding() {
    assert_capacity_rejection_is_abortable(1100 * 1024);
}
