//! Generated reconciliation preserves unchanged SQL-owned activation authority.

use super::*;
use crate::{
    db::{
        DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell, RequestExecutionRoot,
        SqlStatementResult,
        schema::{
            ConstraintValidationJob, ConstraintValidationPhase, PersistedSchemaSnapshot,
            application::apply_generated_schema,
        },
        sql::ddl::{SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlPreparationReport},
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

fn validate_constraint(
    session: &DbSession<AbortCanister>,
    constraint: &str,
    after: Option<u64>,
) -> SqlDdlPreparationReport {
    let sql = match after {
        Some(sequence) => {
            format!("ALTER TABLE Item VALIDATE CONSTRAINT {constraint} AFTER {sequence}")
        }
        None => format!("ALTER TABLE Item VALIDATE CONSTRAINT {constraint}"),
    };
    let SqlStatementResult::Ddl(report) = session
        .execute_admin_sql_ddl(&sql)
        .unwrap_or_else(|error| panic!("validation {sql} failed: {error:?}"))
    else {
        panic!("validation must return its DDL report");
    };
    report
}

// Exercise lost-reply retries through SQL and durable job state. An old ACK
// must resume an empty receipt slot, but cannot consume a newer finding page.
fn qualify_retried_validation(ddl: &str, bad_field: &str, bad_value: InputValue) {
    let (db, session) = initialize();
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Update {
            entity: "Item".into(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                bad_field.into(),
                DynamicWriteCell::Value(bad_value),
            )]),
        }])
        .unwrap();
    execute_ddl(&db, &session, ddl);
    let constraint = snapshot(&db).constraint_activations()[0].name().to_string();
    let validate = |after| validate_constraint(&session, &constraint, after);
    assert_eq!(
        validate(None).execution_status(),
        SqlDdlExecutionStatus::ValidationStarted,
    );
    let first_finding = validate(None);
    let first_sequence = first_finding
        .constraint_validation()
        .unwrap()
        .page_sequence()
        .unwrap();
    let finding = validate(Some(first_sequence));
    let page = finding.constraint_validation().unwrap();
    let sequence = page.page_sequence().unwrap();
    assert_eq!(sequence, first_sequence + 1);
    assert!(!page.findings().is_empty());
    for wrong_ack in [
        None,
        Some(first_sequence),
        Some(sequence + 1),
        Some(u64::MAX),
    ] {
        assert_eq!(validate(wrong_ack).constraint_validation(), Some(page));
    }

    // Deleting the offending row is safe for each activation family, including
    // unique validation whose in-progress generation can reject key edits.
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: "Item".into(),
            key: InputValue::nat64(1),
        }])
        .unwrap();
    let acknowledged = validate(Some(sequence));
    assert!(
        acknowledged
            .constraint_validation()
            .unwrap()
            .page_sequence()
            .is_none()
    );
    assert!(!acknowledged.constraint_validation().unwrap().complete());
    let retained = snapshot(&db);
    let before = job(&db, &retained).unwrap();
    assert!(before.last_receipt().is_none());
    let mut complete = false;
    for _ in 0..4 {
        let retried = validate(Some(sequence));
        let page = retried.constraint_validation().unwrap();
        assert!(page.page_sequence().is_none());
        if page.complete() {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "retrying the consumed receipt must finish validation"
    );
    let accepted = snapshot(&db);
    assert!(accepted.constraint_activations().is_empty());
    assert!(
        accepted
            .constraints()
            .iter()
            .any(|active| active.name() == constraint)
    );
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 1);
}

#[test]
fn repeated_check_receipt_acknowledgement_resumes_validation() {
    qualify_retried_validation(CHECK, "score", InputValue::int64(-1));
}

#[test]
fn repeated_unique_receipt_acknowledgement_resumes_validation() {
    qualify_retried_validation(UNIQUE, "score", InputValue::int64(2));
}

#[test]
fn repeated_not_null_receipt_acknowledgement_resumes_validation() {
    qualify_retried_validation(NOT_NULL, "note", InputValue::null());
}

#[test]
fn consumed_receipt_retries_advance_bounded_clean_forward_pages() {
    let (db, session) = initialize();
    for start in (3..=300).step_by(100) {
        seed(&session, start..=(start + 99).min(300));
    }
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Update {
            entity: "Item".into(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                "score".into(),
                DynamicWriteCell::Value(InputValue::int64(-1)),
            )]),
        }])
        .unwrap();
    execute_ddl(&db, &session, CHECK);
    validate_constraint(&session, "sql_check", None);
    let finding = validate_constraint(&session, "sql_check", None);
    let sequence = finding
        .constraint_validation()
        .unwrap()
        .page_sequence()
        .unwrap();
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: "Item".into(),
            key: InputValue::nat64(1),
        }])
        .unwrap();
    let acknowledged = validate_constraint(&session, "sql_check", Some(sequence));
    let page = acknowledged.constraint_validation().unwrap();
    assert_eq!(page.state(), SqlConstraintValidationState::Forward);
    assert!(page.page_sequence().is_none());
    let mut rows = page.rows_scanned();
    let mut complete = false;
    for _ in 0..5 {
        let retried = validate_constraint(&session, "sql_check", Some(sequence));
        let page = retried.constraint_validation().unwrap();
        assert!(page.page_sequence().is_none());
        assert!((1..=256).contains(&(page.rows_scanned() - rows)));
        rows = page.rows_scanned();
        if page.complete() {
            complete = true;
            break;
        }
    }
    assert!(complete, "clean page retries must reach promotion");
    assert!(snapshot(&db).constraint_activations().is_empty());
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 299);
}

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

#[test]
fn wide_check_findings_replay_and_validation_finishes_after_repair() {
    let (db, session) = initialize();
    seed(&session, 3..=65);
    let columns: Vec<_> = (0..33).map(|index| format!("wide_{index}")).collect();
    for column in &columns {
        execute_ddl(
            &db,
            &session,
            &format!("ALTER TABLE Item ADD COLUMN {column} INT64 NULL"),
        );
    }
    // Only the last dependency violates the check. Limiting diagnostic fields
    // must not limit evaluation to the retained prefix.
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=65)
                .map(|id| DynamicMutation::Update {
                    entity: "Item".into(),
                    key: InputValue::nat64(id),
                    patch: DynamicStructuralPatch::new(
                        columns[..32]
                            .iter()
                            .map(|column| {
                                (
                                    column.clone(),
                                    DynamicWriteCell::Value(InputValue::int64(1)),
                                )
                            })
                            .collect(),
                    ),
                })
                .collect(),
        )
        .unwrap();
    let predicate = columns
        .iter()
        .map(|column| format!("{column} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    execute_ddl(
        &db,
        &session,
        &format!("ALTER TABLE Item ADD CONSTRAINT wide_check CHECK ({predicate}) NOT VALID"),
    );
    assert_eq!(
        validate_constraint(&session, "wide_check", None).execution_status(),
        SqlDdlExecutionStatus::ValidationStarted
    );
    let first = validate_constraint(&session, "wide_check", None);
    let page = first.constraint_validation().unwrap();
    assert_eq!(page.findings().len(), 64);
    assert!(
        page.findings()
            .iter()
            .all(|finding| finding.field_ids().len() == 32)
    );
    let first_sequence = page.page_sequence().unwrap();
    assert_eq!(
        validate_constraint(&session, "wide_check", None).constraint_validation(),
        Some(page)
    );
    let second = validate_constraint(&session, "wide_check", Some(first_sequence));
    let second_page = second.constraint_validation().unwrap();
    assert_eq!(second_page.findings().len(), 1);
    assert_eq!(second_page.rows_scanned(), 65);
    let second_sequence = second_page.page_sequence().unwrap();
    assert!(second_sequence > first_sequence);
    assert_eq!(
        validate_constraint(&session, "wide_check", None).constraint_validation(),
        Some(second_page)
    );
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=65)
                .map(|id| DynamicMutation::Delete {
                    entity: "Item".into(),
                    key: InputValue::nat64(id),
                })
                .collect(),
        )
        .unwrap();
    let mut report = validate_constraint(&session, "wide_check", Some(second_sequence));
    for _ in 0..4 {
        if report.constraint_validation().unwrap().complete() {
            break;
        }
        report = validate_constraint(&session, "wide_check", None);
    }
    assert!(report.constraint_validation().unwrap().complete());
    let accepted = snapshot(&db);
    assert!(accepted.constraint_activations().is_empty());
    assert!(
        accepted
            .constraints()
            .iter()
            .any(|constraint| constraint.name() == "wide_check")
    );
}
