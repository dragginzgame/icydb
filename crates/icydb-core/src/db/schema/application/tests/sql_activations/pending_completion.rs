//! Pending generated applications finish against current accepted authority.

use super::*;
use crate::db::schema::{
    ConstraintValidationProgress, SchemaChangeProgress,
    advance_accepted_row_local_constraint_activation,
};

fn pending_application() -> (
    Db<AbortCanister>,
    DbSession<AbortCanister>,
    SchemaProposal,
    SchemaChangeJob,
) {
    let (db, session) = initialize();
    seed(&session, 3..=257);
    drive_startup_recovery_to_completion(&db);
    drive_cardinality_to_ready(db.store_handle(ABORT_STORE_PATH).unwrap());
    let proposal = proposal(&db, "interleaved-generated-check", Some("Item"));
    let receipt = apply_schema(&db, &proposal).unwrap();
    let SchemaChangeOutcome::Pending { job, .. } = receipt.outcome() else {
        panic!("the historical domain must require bounded continuation")
    };
    (db, session, proposal, *job)
}

fn unrelated_publication(session: &DbSession<AbortCanister>) {
    session.execute_admin_sql_ddl(
        "ALTER TABLE Other ADD COLUMN extra TEXT NULL EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
    ).unwrap();
}

fn finish(db: &Db<AbortCanister>, job: &SchemaChangeJob) -> SchemaChangeProgress {
    for _ in 0..12 {
        let progress = continue_schema_application(db, job.id(), None).unwrap();
        if progress.status() == &SchemaChangeProgressStatus::Applied {
            return progress;
        }
    }
    panic!("clean validation must finish within bounded continuation pages")
}

fn assert_terminal_replay(
    db: &Db<AbortCanister>,
    proposal: &SchemaProposal,
    job: &SchemaChangeJob,
) {
    let progress = finish(db, job);
    assert_eq!(
        progress.receipt().outcome(),
        &SchemaChangeOutcome::Applied {
            accepted_head: schema_application_target(db)
                .unwrap()
                .accepted_head()
                .clone(),
        }
    );
    let head = schema_application_target(db)
        .unwrap()
        .accepted_head()
        .clone();
    assert_eq!(
        continue_schema_application(db, job.id(), None).unwrap(),
        progress
    );
    assert_eq!(
        abort_schema_application(db, job.id(), None).unwrap(),
        progress
    );
    assert_eq!(apply_schema(db, proposal).unwrap(), *progress.receipt());
    forget_recovered_domain_for_tests(db).unwrap();
    drive_startup_recovery_to_completion(db);
    assert_eq!(
        continue_schema_application(db, job.id(), None).unwrap(),
        progress
    );
    assert_eq!(
        schema_application_target(db).unwrap().accepted_head(),
        &head
    );
    assert!(snapshot(db).constraint_activations().is_empty());
    assert!(snapshot(db).constraints().iter().any(|constraint| {
        constraint.origin() == ConstraintOrigin::Generated
            && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
    }));
    assert_eq!(ABORT_DATA.with(|data| data.borrow().len()), 257);
}

#[test]
fn pending_application_completes_after_unrelated_schema_publication() {
    let (db, session, proposal, job) = pending_application();
    unrelated_publication(&session);
    assert_terminal_replay(&db, &proposal, &job);
}

#[test]
fn validating_application_completes_after_unrelated_schema_publication() {
    let (db, session, proposal, job) = pending_application();
    assert_eq!(
        continue_schema_application(&db, job.id(), None)
            .unwrap()
            .status(),
        &SchemaChangeProgressStatus::Started
    );
    unrelated_publication(&session);
    assert_terminal_replay(&db, &proposal, &job);
}

#[test]
fn promoted_application_finishes_after_recovery_with_a_stale_candidate_head() {
    let (db, session, proposal, job) = pending_application();
    unrelated_publication(&session);
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let pending = snapshot(&db);
    let activation = &pending.constraint_activations()[0];
    let entity_tag = db
        .accepted_runtime_entity_for_path("Item")
        .unwrap()
        .entity_tag();
    let mut promoted = false;
    for _ in 0..12 {
        if matches!(
            advance_accepted_row_local_constraint_activation(
                store,
                ABORT_STORE_PATH,
                entity_tag,
                "Item",
                activation.id(),
                None,
            )
            .unwrap(),
            ConstraintValidationProgress::Promoted { .. }
        ) {
            promoted = true;
            break;
        }
    }
    assert!(promoted);
    // Model a cut after durable promotion and before the separate receipt marker.
    // Recovery must retain the promoted constraint and the original pending job.
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_terminal_replay(&db, &proposal, &job);
}

#[test]
fn interleaved_pending_application_can_still_abort() {
    let (db, session, _, job) = pending_application();
    unrelated_publication(&session);
    let progress = abort_schema_application(&db, job.id(), None).unwrap();
    assert_eq!(progress.status(), &SchemaChangeProgressStatus::Aborted);
    assert_eq!(
        continue_schema_application(&db, job.id(), None).unwrap(),
        progress
    );
    assert!(snapshot(&db).constraint_activations().is_empty());
    assert!(!snapshot(&db).constraints().iter().any(|constraint| {
        constraint.origin() == ConstraintOrigin::Generated
            && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
    }));
}
