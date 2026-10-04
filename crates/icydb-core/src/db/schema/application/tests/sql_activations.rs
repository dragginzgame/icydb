//! Generated reconciliation preserves unchanged SQL-owned activation authority.

use super::*;
use crate::{
    db::{
        DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell, RequestExecutionRoot,
        schema::{
            ConstraintValidationJob, ConstraintValidationPhase, PersistedSchemaSnapshot,
            application::apply_generated_schema,
        },
    },
    value::InputValue,
};

fn proposal(db: &Db<AbortCanister>, key: &str, changed_entity: Option<&str>) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    let entities = ["Item", "Other"]
        .into_iter()
        .map(|entity| {
            let fields = vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("score"),
                    FieldType::Scalar(ScalarType::Int64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ];
            let constraints = (changed_entity == Some(entity))
                .then(|| {
                    ConstraintFragment::check(
                        name("generated_nonnegative"),
                        SourceCheckExpr::try_new(vec![
                            SourceCheckInstruction::Field(
                                FieldSourceKey::try_new("score").unwrap(),
                            ),
                            SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
                            SourceCheckInstruction::GreaterThanOrEqual,
                        ])
                        .unwrap(),
                    )
                })
                .into_iter()
                .collect();
            EntityFragment::try_new(
                name(entity),
                version_one(),
                fields,
                vec![FieldSourceKey::try_new("id").unwrap()],
                Vec::new(),
                Vec::new(),
                constraints,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    SchemaProposal::try_compose(
        vec![SchemaCapability::ACCEPTED_CHECKS],
        target.database_identity(),
        SchemaSubmissionKey::try_new(key).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, Vec::new()).unwrap()],
        assignments,
        Vec::new(),
        None,
    )
    .unwrap()
}

fn initialize() -> (Db<AbortCanister>, DbSession<AbortCanister>) {
    let db = Db::<AbortCanister>::new(
        &ABORT_REGISTRY,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    apply_generated_schema(&db, &proposal(&db, "sql-activation-initial", None)).unwrap();
    let session = DbSession::new(&ABORT_REGISTRY, &RequestExecutionRoot::__new_runtime_root());
    // Nullability DDL is maintained for SQL-owned fields on generated entities.
    execute_ddl(&db, &session, "ALTER TABLE Item ADD COLUMN note TEXT NULL");
    seed(&session, 1..=2);
    drive_startup_recovery_to_completion(&db);
    (db, session)
}

fn seed(session: &DbSession<AbortCanister>, ids: impl Iterator<Item = u64>) {
    session
        .execute_trusted_dynamic_mutation_batch(
            ids.map(|id| DynamicMutation::Insert {
                entity: "Item".into(),
                patch: DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "score".into(),
                        DynamicWriteCell::Value(InputValue::int64(i64::try_from(id).unwrap())),
                    ),
                    (
                        "note".into(),
                        DynamicWriteCell::Value(InputValue::text(format!("note-{id}"))),
                    ),
                ]),
            })
            .collect(),
        )
        .unwrap();
}

fn snapshot(db: &Db<AbortCanister>) -> PersistedSchemaSnapshot {
    db.store_handle(ABORT_STORE_PATH)
        .unwrap()
        .with_schema(|schema| {
            schema
                .current_accepted_schema_bundle()
                .unwrap()
                .unwrap()
                .entity_snapshots()
                .values()
                .find(|snapshot| snapshot.entity_path() == "Item")
                .unwrap()
                .clone()
        })
}

fn execute_ddl(db: &Db<AbortCanister>, session: &DbSession<AbortCanister>, sql: &str) {
    let version = snapshot(db).version().get();
    session
        .execute_admin_sql_ddl(&format!(
            "{sql} EXPECT SCHEMA VERSION {version} SET SCHEMA VERSION {}",
            version + 1
        ))
        .unwrap();
}

fn job(
    db: &Db<AbortCanister>,
    snapshot: &PersistedSchemaSnapshot,
) -> Option<ConstraintValidationJob> {
    let tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    db.store_handle(ABORT_STORE_PATH)
        .unwrap()
        .with_schema(|schema| {
            schema
                .constraint_validation_job(tag, snapshot.constraint_activations()[0].id())
                .unwrap()
        })
}

fn qualify(ddl: &str, constraint: &str, pages: usize, changed_entity: Option<&str>) {
    let (db, session) = initialize();
    execute_ddl(&db, &session, ddl);
    let activation_name = snapshot(&db).constraint_activations()[0].name().to_string();
    for _ in 0..pages {
        session
            .execute_admin_sql_ddl(&format!(
                "ALTER TABLE Item VALIDATE CONSTRAINT {activation_name}"
            ))
            .unwrap();
    }
    drive_startup_recovery_to_completion(&db);
    let before = snapshot(&db);
    assert_eq!(before.constraint_activations().len(), 1);
    assert_eq!(
        before.constraint_activations()[0].origin(),
        ConstraintOrigin::SqlDdl
    );
    let before_job = job(&db, &before);
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let before_indexes = store.with_index(IndexStore::len);
    if pages != 0 {
        assert_eq!(
            before_job.as_ref().unwrap().phase(),
            if pages == 1 {
                ConstraintValidationPhase::Forward
            } else {
                ConstraintValidationPhase::Verify
            }
        );
    }
    let generated = proposal(&db, "sql-activation-successor", changed_entity);
    assert_eq!(
        drive_generated_startup_recovery_page(
            &session,
            &ABORT_REGISTRY,
            generated.submission_key().as_str()
        )
        .unwrap(),
        GeneratedStartupDriverStep::ApplyGeneratedSchema
    );
    let receipt = apply_generated_schema(&db, &generated)
        .expect("unchanged SQL activation must permit reconciliation");
    assert!(matches!(
        receipt.outcome(),
        SchemaChangeOutcome::NoOp { .. } | SchemaChangeOutcome::Applied { .. }
    ));
    drive_startup_recovery_to_completion(&db);
    assert_eq!(
        observe_generated_startup_state::<AbortCanister>(
            &ABORT_REGISTRY,
            generated.submission_key().as_str()
        ),
        Ok(DatabaseStartupState::Ready)
    );
    assert_eq!(snapshot(&db), before);
    assert_eq!(job(&db, &before), before_job);
    assert_eq!(store.with_index(IndexStore::len), before_indexes);
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 2);
    assert_eq!(apply_generated_schema(&db, &generated).unwrap(), receipt);
    if constraint == "sql_check" && pages == 2 {
        for _ in 0..4 {
            session
                .execute_admin_sql_ddl("ALTER TABLE Item VALIDATE CONSTRAINT sql_check")
                .unwrap();
        }
        let after = snapshot(&db);
        assert!(after.constraint_activations().is_empty());
        assert!(
            after
                .constraints()
                .iter()
                .any(|constraint| constraint.name() == "sql_check")
        );
        return;
    }
    // Ready admission now leaves the maintained SQL owner able to abort safely.
    let drop = if constraint == "note_not_null" {
        "ALTER TABLE Item ALTER COLUMN note DROP NOT NULL"
    } else if constraint == "sql_unique" {
        "DROP INDEX sql_unique ON Item"
    } else {
        "ALTER TABLE Item DROP CONSTRAINT sql_check"
    };
    execute_ddl(&db, &session, drop);
    assert!(snapshot(&db).constraint_activations().is_empty());
}

const CHECK: &str = "ALTER TABLE Item ADD CONSTRAINT sql_check CHECK (score >= 0) NOT VALID";
const UNIQUE: &str = "CREATE UNIQUE INDEX sql_unique ON Item (score)";
const NOT_NULL: &str = "ALTER TABLE Item ALTER COLUMN note SET NOT NULL";

#[test]
fn sql_check_new_writes_reconciles_unchanged() {
    qualify(CHECK, "sql_check", 0, None);
}
#[test]
fn sql_check_forward_reconciles_unchanged() {
    qualify(CHECK, "sql_check", 1, None);
}
#[test]
fn sql_check_verify_reconciles_unchanged() {
    qualify(CHECK, "sql_check", 2, None);
}
#[test]
fn sql_unique_new_writes_reconciles_unchanged() {
    qualify(UNIQUE, "sql_unique", 0, None);
}
#[test]
fn sql_unique_verify_reconciles_unchanged() {
    qualify(UNIQUE, "sql_unique", 2, None);
}
#[test]
fn sql_not_null_new_writes_reconciles_unchanged() {
    qualify(NOT_NULL, "note_not_null", 0, None);
}
#[test]
fn sql_not_null_verify_reconciles_unchanged() {
    qualify(NOT_NULL, "note_not_null", 2, None);
}
#[test]
fn unrelated_generated_change_preserves_sql_unique_job() {
    qualify(UNIQUE, "sql_unique", 2, Some("Other"));
}

#[test]
fn conflicting_generated_change_keeps_sql_activation_and_head() {
    let (db, session) = initialize();
    execute_ddl(&db, &session, CHECK);
    drive_startup_recovery_to_completion(&db);
    let before = snapshot(&db);
    let target = schema_application_target(&db).unwrap();
    let error =
        apply_generated_schema(&db, &proposal(&db, "sql-activation-conflict", Some("Item")))
            .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(snapshot(&db), before);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
}

#[test]
fn generated_pending_activation_keeps_its_exact_submission_owner() {
    let (db, session) = initialize();
    seed(&session, 3..=257);
    drive_startup_recovery_to_completion(&db);
    let generated = proposal(&db, "generated-activation-pending", Some("Item"));
    let receipt = apply_generated_schema(&db, &generated).unwrap();
    assert!(matches!(
        receipt.outcome(),
        SchemaChangeOutcome::Pending { .. }
    ));
    let before = snapshot(&db);
    assert_eq!(
        before.constraint_activations()[0].origin(),
        ConstraintOrigin::Generated
    );
    let target = schema_application_target(&db).unwrap();
    assert_eq!(apply_generated_schema(&db, &generated).unwrap(), receipt);
    let different = proposal(&db, "generated-activation-different", Some("Item"));
    let error = apply_generated_schema(&db, &different).unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(snapshot(&db), before);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 257);
}
