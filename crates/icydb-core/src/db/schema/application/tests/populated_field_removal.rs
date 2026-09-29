//! Physical removal preserves field lineage across dense IDs and declared renames.

#[cfg(feature = "sql")]
mod sql_lineage;

use super::*;
use crate::{
    db::{
        DynamicMutation, DynamicQuery, DynamicStructuralPatch, DynamicWriteCell, FieldRef,
        RequestExecutionRoot,
        schema::{
            MigrationRewriteInterruption, SchemaMigrationCommand, SchemaMigrationPhase,
            interrupt_next_migration_rewrite_at, migrate_schema,
        },
        session::DbSession,
    },
    value::{InputValue, OutputValue},
};
use icydb_schema::{
    EntityMigration, IndexFragment, IndexKeyFragment, SchemaMigrationPlan, SchemaMigrationRename,
    SchemaMigrationTransform, SchemaRemoval,
};

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).unwrap()
}

fn proposal(db: &Db<MigrationExecutionCanister>, current: bool, generated: bool) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    let entity = EntitySourceKey::try_new("Remapped").unwrap();
    let status = if current { "state" } else { "status" };
    let value = if current { "z_value" } else { "a_old" };
    let fields = [
        ("id", ScalarType::Nat64),
        (status, ScalarType::Nat64),
        (
            value,
            if current {
                ScalarType::Nat8
            } else {
                ScalarType::Int64
            },
        ),
    ]
    .into_iter()
    .map(|(name_text, kind)| {
        FieldFragment::new(
            name(name_text),
            FieldType::Scalar(kind),
            false,
            if generated && name_text == "id" {
                FieldInsertPolicy::Generated
            } else {
                FieldInsertPolicy::Required
            },
            None,
        )
    })
    .collect();
    let fragment = EntityFragment::try_new(
        name("Remapped"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).unwrap(),
        fields,
        vec![field("id")],
        vec![
            IndexFragment::try_new(
                name("status_lookup"),
                vec![IndexKeyFragment::Field(field(status))],
                false,
                None,
            )
            .unwrap(),
        ],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let migration = current.then(|| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity.clone(),
                version_one(),
                None,
                vec![SchemaMigrationRename::Field {
                    from: field("status"),
                    to: field("state"),
                }],
                vec![SchemaMigrationTransform::CheckedCast {
                    from: field("a_old"),
                    to: field("z_value"),
                    target: ScalarType::Nat8,
                }],
            )
            .unwrap(),
        ])
        .unwrap()
    });
    let mut capabilities = vec![SchemaCapability::SECONDARY_INDEXES];
    if current {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        target.database_identity(),
        SchemaSubmissionKey::try_new(if current { "remap-v2" } else { "remap-v1" }).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(vec![fragment], Vec::new()).unwrap()],
        vec![EntityStoreAssignment::new(
            entity.clone(),
            target.stores()[0].identity(),
        )],
        current
            .then_some(SchemaRemoval::Field {
                entity,
                field: field("a_old"),
            })
            .into_iter()
            .collect(),
        migration,
    )
    .unwrap()
}

fn advance_command(proposal: &SchemaProposal) -> SchemaMigrationCommand {
    SchemaMigrationCommand::Advance {
        expected_database: proposal.target_database(),
        expected_head: proposal.expected_head().clone(),
        expected_plan: proposal.migration().unwrap().digest(),
        acknowledged_finding_page: None,
    }
}

fn restart(db: &Db<MigrationExecutionCanister>) {
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let generation = store.with_data_mut(|data| {
        data.reset_journaled_live_projection().unwrap();
        data.generation()
    });
    let watermark = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::fold_watermark)
        .unwrap();
    store
        .with_index_mut(|index| index.reset_journaled_live_projection(generation, watermark))
        .unwrap();
    store
        .with_schema_mut(SchemaStore::reset_journaled_live_projection)
        .unwrap();
    forget_recovered_domain_for_tests(db).unwrap();
    drive_startup_recovery_to_completion(db);
}

fn assert_rows(session: &DbSession<MigrationExecutionCanister>, current: bool) {
    let status = if current { "state" } else { "status" };
    let value = if current { "z_value" } else { "a_old" };
    for id in 1..=3 {
        let page = session
            .execute_trusted_live_page(
                &DynamicQuery::new("Remapped")
                    .select(["id", status, value])
                    .filter(FieldRef::new(status).eq(InputValue::nat64(id * 10))),
                None,
            )
            .unwrap();
        let expected = vec![
            OutputValue::nat64(id),
            OutputValue::nat64(id * 10),
            if current {
                OutputValue::nat64(id + 20)
            } else {
                OutputValue::int64(i64::try_from(id + 20).unwrap())
            },
        ];
        assert_eq!(page.rows, vec![expected]);
    }
}

fn initialize(
    generated: bool,
) -> (
    Db<MigrationExecutionCanister>,
    DbSession<MigrationExecutionCanister>,
    SchemaProposal,
) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::new(&MIGRATION_EXECUTION_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    apply_schema(&db, &proposal(&db, false, generated)).unwrap();
    let session = DbSession::new(&MIGRATION_EXECUTION_REGISTRY, &root);
    for id in 1..=3 {
        let mut cells = vec![
            (
                "status".into(),
                DynamicWriteCell::Value(InputValue::nat64(id * 10)),
            ),
            (
                "a_old".into(),
                DynamicWriteCell::Value(InputValue::int64(i64::try_from(id + 20).unwrap())),
            ),
        ];
        if !generated {
            cells.push(("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))));
        }
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "Remapped".into(),
                patch: DynamicStructuralPatch::new(cells),
            })
            .unwrap();
    }
    drive_startup_recovery_to_completion(&db);
    drive_cardinality_to_ready(db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap());
    assert_rows(&session, false);
    let candidate = proposal(&db, true, generated);
    (db, session, candidate)
}

#[derive(Clone, Copy)]
enum RemovalCase {
    CallerKey,
    GeneratedInterrupted,
    Abort,
}

fn run_removal(case: RemovalCase) {
    let generated = matches!(case, RemovalCase::GeneratedInterrupted);
    let interrupted = generated;
    let abort = matches!(case, RemovalCase::Abort);
    let (db, session, candidate) = initialize(generated);
    let mut finished = false;
    let mut recovered = false;
    for _ in 0..32 {
        let phase = migrate_schema(&db, &candidate, advance_command(&candidate))
            .unwrap()
            .phase();
        if phase == SchemaMigrationPhase::Applied {
            finished = true;
            break;
        }
        if abort && phase == SchemaMigrationPhase::ReadyToRewrite {
            let result = migrate_schema(
                &db,
                &candidate,
                SchemaMigrationCommand::Abort {
                    expected_database: candidate.target_database(),
                    expected_head: candidate.expected_head().clone(),
                    expected_plan: candidate.migration().unwrap().digest(),
                },
            )
            .unwrap();
            assert_eq!(result.phase(), SchemaMigrationPhase::Aborted);
            finished = true;
            break;
        }
        if interrupted && !recovered && phase == SchemaMigrationPhase::RewritingRows {
            interrupt_next_migration_rewrite_at(MigrationRewriteInterruption::JournalPublished);
            assert!(migrate_schema(&db, &candidate, advance_command(&candidate)).is_err());
            restart(&db);
            recovered = true;
        }
    }
    assert!(finished);
    assert_eq!(recovered, interrupted);
    assert_rows(&session, !abort);
    restart(&db);
    assert_rows(&session, !abort);
    if generated && !abort {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "Remapped".into(),
                patch: DynamicStructuralPatch::new(vec![
                    (
                        "state".into(),
                        DynamicWriteCell::Value(InputValue::nat64(40)),
                    ),
                    (
                        "z_value".into(),
                        DynamicWriteCell::Value(InputValue::nat64(24)),
                    ),
                ]),
            })
            .unwrap();
        let page = session
            .execute_trusted_live_page(
                &DynamicQuery::new("Remapped")
                    .select(["id"])
                    .filter(FieldRef::new("state").eq(InputValue::nat64(40))),
                None,
            )
            .unwrap();
        assert_eq!(page.rows, vec![vec![OutputValue::nat64(4)]]);
    }
}

#[test]
fn populated_removal_preserves_primary_key_values_and_renamed_index() {
    run_removal(RemovalCase::CallerKey);
}

#[test]
fn populated_removal_preserves_generated_keys_through_interrupted_rewrite() {
    run_removal(RemovalCase::GeneratedInterrupted);
}

#[test]
fn populated_removal_validation_remains_abortable() {
    run_removal(RemovalCase::Abort);
}
