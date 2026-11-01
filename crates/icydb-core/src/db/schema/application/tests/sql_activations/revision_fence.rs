//! Constraint verification fences only the rows owned by its activation.

use super::*;
use crate::db::{
    commit::publish_constraint_validation_job,
    journal::JournalTailStore,
    schema::{
        ConstraintStoreRevision, decode_constraint_validation_job, encode_constraint_validation_job,
    },
};

fn begin_verify(ddl: &str) -> (Db<AbortCanister>, DbSession<AbortCanister>, String) {
    let (db, session) = initialize();
    for start in (3..=300).step_by(100) {
        seed(&session, start..=(start + 99).min(300));
    }
    // An existing store-wide fence can exceed this entity's own last write.
    write_other(&session, 0);
    drive_startup_recovery_to_completion(&db);
    execute_ddl(&db, &session, ddl);
    let name = snapshot(&db).constraint_activations()[0].name().to_string();
    for _ in 0..3 {
        validate_constraint(&session, &name, None);
    }
    let job = job(&db, &snapshot(&db)).unwrap();
    assert_eq!(job.phase(), ConstraintValidationPhase::Verify);
    assert_eq!(job.rows_scanned(), 300);
    (db, session, name)
}

fn write_other(session: &DbSession<AbortCanister>, id: u64) {
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Insert {
            entity: "Other".into(),
            patch: DynamicStructuralPatch::new(vec![
                ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                (
                    "score".into(),
                    DynamicWriteCell::Value(InputValue::int64(1)),
                ),
            ]),
        }])
        .unwrap();
}

fn qualify_unrelated_writes(ddl: &str) {
    let (db, session, name) = begin_verify(ddl);
    let before = job(&db, &snapshot(&db)).unwrap();
    let captured = before.captured_store_revisions().unwrap()[0].revision();
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let journal = store.journal_tail_store().unwrap();

    // Keep the existing store-wide baseline and its encoded job. The entity's
    // last mutation precedes that baseline even as unrelated writes advance it.
    let encoded = encode_constraint_validation_job(&before).unwrap();
    assert_eq!(decode_constraint_validation_job(&encoded).unwrap(), before);
    for step in 1..=2 {
        write_other(&session, step);
        drive_startup_recovery_to_completion(&db);
        assert!(
            journal
                .with_borrow(JournalTailStore::data_mutation_revision)
                .unwrap()
                > captured
        );
        assert!(
            journal
                .with_borrow(|tail| tail.entity_mutation_revision(before.entity_tag()))
                .unwrap()
                < captured
        );
        let report = validate_constraint(&session, &name, None);
        let page = report.constraint_validation().unwrap();
        if step == 1 {
            assert_eq!(page.state(), SqlConstraintValidationState::Verify);
            assert_eq!(page.rows_scanned(), 556);
            let retained = job(&db, &snapshot(&db)).unwrap();
            assert_eq!(
                retained.captured_store_revisions(),
                before.captured_store_revisions()
            );
        } else {
            assert!(page.complete());
            assert_eq!(page.rows_scanned(), 600);
        }
    }
    assert!(snapshot(&db).constraint_activations().is_empty());
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 303);
}

#[test]
fn check_verify_finishes_with_unrelated_writes_between_pages() {
    qualify_unrelated_writes(CHECK);
}

#[test]
fn unique_verify_finishes_with_unrelated_writes_between_pages() {
    qualify_unrelated_writes(UNIQUE);
}

#[test]
fn not_null_verify_finishes_with_unrelated_writes_between_pages() {
    qualify_unrelated_writes(NOT_NULL);
}

fn qualify_target_mutation(ddl: &str) {
    let (db, session, name) = begin_verify(ddl);
    let first = validate_constraint(&session, &name, None);
    assert_eq!(first.constraint_validation().unwrap().rows_scanned(), 556);
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: "Item".into(),
            key: InputValue::nat64(1),
        }])
        .unwrap();
    let restarted = validate_constraint(&session, &name, None);
    assert_eq!(
        restarted.execution_status(),
        SqlDdlExecutionStatus::ValidationRestarted
    );
    let retained = job(&db, &snapshot(&db)).unwrap();
    assert_eq!(retained.phase(), ConstraintValidationPhase::Forward);
    assert!(retained.checkpoint().is_none());
    assert!(retained.captured_store_revisions().is_none());
    assert_eq!(retained.rows_scanned(), 556);
    let mut complete = false;
    for _ in 0..4 {
        let report = validate_constraint(&session, &name, None);
        let page = report.constraint_validation().unwrap();
        assert!(page.findings().is_empty());
        complete = page.complete();
        if complete {
            break;
        }
    }
    assert!(complete, "the restarted proof must still reach promotion");
    assert!(snapshot(&db).constraint_activations().is_empty());
}

#[test]
fn check_target_mutation_after_a_verify_page_restarts() {
    qualify_target_mutation(CHECK);
}

#[test]
fn unique_target_mutation_after_a_verify_page_restarts() {
    qualify_target_mutation(UNIQUE);
}

#[test]
fn not_null_target_mutation_after_a_verify_page_restarts() {
    qualify_target_mutation(NOT_NULL);
}

#[test]
fn future_store_fence_cannot_promote() {
    let (db, session, name) = begin_verify(CHECK);
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let current = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::data_mutation_revision)
        .unwrap();
    let mut invalid = job(&db, &snapshot(&db)).unwrap();
    invalid.restart_forward(0, Vec::new()).unwrap();
    invalid
        .record_forward_page(
            None,
            0,
            Vec::new(),
            true,
            Some(vec![ConstraintStoreRevision::new(
                ABORT_STORE_PATH.into(),
                current + 100,
            )]),
        )
        .unwrap();
    publish_constraint_validation_job(ABORT_STORE_PATH, store, &invalid).unwrap();
    let report = validate_constraint(&session, &name, None);
    assert_eq!(
        report.execution_status(),
        SqlDdlExecutionStatus::ValidationRestarted
    );
    assert_eq!(snapshot(&db).constraint_activations().len(), 1);
}

#[test]
fn missing_entity_revision_refuses_verify_without_job_progress() {
    let (db, session, name) = begin_verify(CHECK);
    let before = job(&db, &snapshot(&db)).unwrap();
    write_other(&session, 1);
    drive_startup_recovery_to_completion(&db);
    db.store_handle(ABORT_STORE_PATH)
        .unwrap()
        .journal_tail_store()
        .unwrap()
        .with_borrow_mut(|tail| tail.remove_entity_mutation_revision_for_tests(before.entity_tag()))
        .unwrap();
    let error = session
        .execute_admin_sql_ddl(&format!("ALTER TABLE Item VALIDATE CONSTRAINT {name}"))
        .expect_err("missing retained revision authority must refuse verification");
    assert_eq!(
        error.diagnostic().class(),
        icydb_diagnostic_code::ErrorClass::Corruption
    );
    assert_eq!(job(&db, &snapshot(&db)).unwrap(), before);
}
