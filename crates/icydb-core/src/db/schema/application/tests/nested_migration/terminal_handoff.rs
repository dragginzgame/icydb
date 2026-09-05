//! Successive migration admission and terminal-record replay authority.

use super::*;
use crate::db::{
    commit::{CommitMarker, DatabaseControlOp, begin_commit, finish_commit},
    schema::{
        SchemaMigrationRecord, SchemaMigrationRecordOp,
        application::{
            accepted_head_after_candidates, application_authorities, existing_proposal_stores,
            load_current_application_bundles, load_entity_source_lineage_catalog,
            prepared_physical_schema_migration, schema_migration_status_for_target,
        },
        apply_schema_migration_record_op, load_schema_migration_record,
        migration_planner::plan_schema_migration,
    },
};
use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};

fn stored_record() -> SchemaMigrationRecord {
    load_schema_migration_record()
        .expect("record should decode")
        .expect("record should exist")
}

fn drive_to(
    db: &Db<MigrationExecutionCanister>,
    proposal: &SchemaProposal,
    phase: SchemaMigrationPhase,
) {
    for _ in 0..64 {
        if advance(db, proposal)
            .expect("migration should advance")
            .phase()
            == phase
        {
            return;
        }
    }
    panic!("fixture migration should reach {phase:?} within its bounded requests");
}

fn successor(db: &Db<MigrationExecutionCanister>) -> SchemaProposal {
    let target = schema_application_target(db).expect("target should issue");
    proposal_for_version(
        3,
        target.accepted_head().clone(),
        target.database_identity(),
        target.stores()[0].identity(),
    )
}

fn set_backup(session: &DbSession<MigrationExecutionCanister>, target: u64) {
    for id in 10..=12 {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: "Source".to_string(),
                key: InputValue::nat64(id),
                patch: DynamicStructuralPatch::new(vec![(
                    "backup".to_string(),
                    DynamicWriteCell::Value(nested_value(target)),
                )]),
            })
            .expect("unconstrained backup should update");
    }
}

fn assert_plan_changed(error: InternalError) {
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration {
            reason: SchemaMigrationCode::PlanChanged
        })
    );
}

#[test]
fn terminal_handoff_drains_old_journals_before_starting_a_successor() {
    let (db, session, first) = initialize(2);
    drive_to(&db, &first, SchemaMigrationPhase::Applied);
    assert_eq!(
        advance(&db, &first)
            .expect("exact terminal retry should succeed")
            .phase(),
        SchemaMigrationPhase::Applied
    );
    set_backup(&session, 1);
    let old = stored_record();
    let next = successor(&db);
    let target = schema_application_target(&db).expect("target should issue");
    assert_eq!(
        schema_migration_status_for_target(&db, &next, &target)
            .expect("new plan status should issue")
            .phase(),
        SchemaMigrationPhase::Idle
    );
    let journal = db
        .store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("store should resolve")
        .journal_tail_store()
        .expect("journal should exist");
    let mut count = journal.with_borrow(JournalTailStore::len);
    assert!(
        count > 1,
        "fixture must retain old migration journal effects"
    );
    let mut idle_pages = 0;
    loop {
        let page = advance(&db, &next).expect("bounded handoff should advance");
        let remaining = journal.with_borrow(JournalTailStore::len);
        assert_eq!(
            remaining + 1,
            count,
            "each request folds one old journal batch"
        );
        count = remaining;
        if page.phase() == SchemaMigrationPhase::Prepared {
            assert_eq!(
                count, 0,
                "old plan authority cannot retire with outstanding journals"
            );
            break;
        }
        assert_eq!(page.phase(), SchemaMigrationPhase::Idle);
        assert_eq!(stored_record(), old);
        assert_eq!(page.accepted_head(), next.expected_head());
        assert_eq!(
            page.plan_digest(),
            next.migration().map(SchemaMigrationPlan::digest)
        );
        idle_pages += 1;
        assert!(idle_pages < 64);
    }
    assert!(idle_pages > 0);
    assert_plan_changed(
        advance(&db, &first).expect_err("active successor must reject a different plan"),
    );
    forget_recovered_domain_for_tests(&db).expect("recovery should reset");
    drive_startup_recovery_to_completion(&db);
    drive_to(&db, &next, SchemaMigrationPhase::Applied);
    assert_edges(&db, &accepted_relations(&db), 1);
    let rows = row_bytes(&db);
    forget_recovered_domain_for_tests(&db).expect("recovery should reset");
    drive_startup_recovery_to_completion(&db);
    assert_eq!(row_bytes(&db), rows);
    assert_edges(&db, &accepted_relations(&db), 1);
}

// Both rejected and fully staged validation are abortable. Reusing a candidate
// generation must not retain the first submission's staged target edges.
fn correct_aborted_submission(staged: bool) {
    let (db, session, first) = initialize(if staged { 2 } else { 99 });
    drive_to(
        &db,
        &first,
        if staged {
            SchemaMigrationPhase::ReadyToRewrite
        } else {
            SchemaMigrationPhase::Rejected
        },
    );
    let abort = SchemaMigrationCommand::Abort {
        expected_database: first.target_database(),
        expected_head: first.expected_head().clone(),
        expected_plan: first.migration().expect("plan should exist").digest(),
    };
    assert_eq!(
        migrate_schema(&db, &first, abort)
            .expect("pre-rewrite plan should abort")
            .phase(),
        SchemaMigrationPhase::Aborted
    );
    assert_eq!(
        advance(&db, &first)
            .expect("exact aborted retry should remain terminal")
            .phase(),
        SchemaMigrationPhase::Aborted
    );
    let target = if staged { 1 } else { 2 };
    set_backup(&session, target);
    let corrected = SchemaProposal::try_compose(
        first.capabilities().to_vec(),
        first.target_database(),
        SchemaSubmissionKey::try_new("corrected").expect("submission should admit"),
        first.expected_head().clone(),
        first.fragments().to_vec(),
        first.assignments().to_vec(),
        first.removals().to_vec(),
        first.migration().cloned(),
    )
    .expect("corrected submission should compose");
    drive_to(&db, &corrected, SchemaMigrationPhase::Applied);
    assert_edges(&db, &accepted_relations(&db), target);
    forget_recovered_domain_for_tests(&db).expect("recovery should reset");
    drive_startup_recovery_to_completion(&db);
    assert_edges(&db, &accepted_relations(&db), target);
}

#[test]
fn terminal_handoff_recovery_finishes_old_journals_before_the_successor() {
    let (db, _, first) = initialize(2);
    drive_to(&db, &first, SchemaMigrationPhase::Applied);
    let old = stored_record();
    let next = successor(&db);
    assert_eq!(
        advance(&db, &next)
            .expect("handoff should start draining")
            .phase(),
        SchemaMigrationPhase::Idle
    );
    assert_eq!(stored_record(), old);
    forget_recovered_domain_for_tests(&db).expect("recovery should reset");
    drive_startup_recovery_to_completion(&db);
    assert_eq!(
        stored_record(),
        old,
        "old private effects must retain their replay authority"
    );
    assert_eq!(
        advance(&db, &next)
            .expect("drained handoff should prepare")
            .phase(),
        SchemaMigrationPhase::Prepared
    );
    drive_to(&db, &next, SchemaMigrationPhase::Applied);
    assert_edges(&db, &accepted_relations(&db), 2);
}

#[test]
fn terminal_handoff_admits_a_corrected_submission_after_abort() {
    correct_aborted_submission(false);
}

#[test]
fn terminal_handoff_reuses_an_aborted_staged_generation_without_stale_edges() {
    correct_aborted_submission(true);
}

// Use the same planner and record constructor as Advance, then interrupt the
// exact database-control marker before or after its CAS applies.
fn recover_handoff(applied_before_recovery: bool) {
    let (db, _, first) = initialize(2);
    drive_to(&db, &first, SchemaMigrationPhase::Applied);
    drive_startup_recovery_to_completion(&db);
    let old = stored_record();
    let next = successor(&db);
    let authorities = application_authorities(&db);
    let bundles = load_current_application_bundles(&authorities).expect("bundles should load");
    let stores = existing_proposal_stores(next.target_database(), &authorities, &bundles);
    let lineage = load_entity_source_lineage_catalog()
        .expect("lineage should load")
        .expect("lineage should exist");
    let planned = plan_schema_migration(&next, &stores, &lineage).expect("successor should plan");
    let head = accepted_head_after_candidates(&authorities, planned.candidates())
        .expect("candidate head should derive");
    let prepared = prepared_physical_schema_migration(
        &next,
        &planned,
        planned.candidates(),
        next.target_database(),
        next.expected_head(),
        &head,
        next.digest().expect("digest should derive"),
        next.migration().expect("plan should exist").digest(),
        &stores,
    )
    .expect("successor record should prepare");
    let operation =
        SchemaMigrationRecordOp::replace(&old, &prepared).expect("terminal CAS should admit");
    let marker = CommitMarker::from_parts_with_database_control(
        [0x75; 16],
        Vec::new(),
        vec![DatabaseControlOp::SchemaMigration(operation.clone())],
    )
    .expect("handoff marker should encode");
    let guard = begin_commit(marker).expect("handoff marker should persist");
    finish_commit(guard, |_| {
        if applied_before_recovery {
            apply_schema_migration_record_op(&operation)?;
        }
        Err(InternalError::executor_invariant())
    })
    .expect_err("interruption must retain the handoff marker");
    forget_recovered_domain_for_tests(&db).expect("recovery should reset");
    drive_startup_recovery_to_completion(&db);
    assert_eq!(stored_record(), prepared);
    drive_to(&db, &next, SchemaMigrationPhase::Applied);
    assert_edges(&db, &accepted_relations(&db), 2);
}

#[test]
fn terminal_handoff_recovers_before_record_replacement() {
    recover_handoff(false);
}

#[test]
fn terminal_handoff_recovers_after_record_replacement() {
    recover_handoff(true);
}
