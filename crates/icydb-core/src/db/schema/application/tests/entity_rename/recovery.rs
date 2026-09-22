//! Populated rename recovery from the existing compound publication marker.

use super::*;
use crate::db::{
    commit::{
        CommitMarker, DatabaseControlOp, begin_commit, finish_commit, generate_commit_id,
        generate_marker_batch_id, next_database_commit_sequence,
    },
    journal::{DatabaseCommitSequence, JournalBatch, JournalRecord},
    registry::StoreRegistry,
    schema::{
        EntitySourceLineageCatalogOp,
        application::{
            existing_proposal_stores, lineage_after_planned, load_current_application_bundles,
            migration_submission_key,
        },
        apply_schema_application_record_op,
        migration_planner::plan_schema_migration,
    },
};
use std::thread::LocalKey;

// Separate handles deliberately reach the same TLS registry, as generated
// startup and request code can do across compilation units.
static RECOVERY_DRIVER_REGISTRY: LocalKey<StoreRegistry> = MIGRATION_EXECUTION_REGISTRY;
static RECOVERY_READER_REGISTRY: LocalKey<StoreRegistry> = MIGRATION_EXECUTION_REGISTRY;

// Construct the same candidate, receipt and lineage operations as Advance, then
// retain the compound marker at its durable boundary. No recovery-specific
// schema derivation or alternate encoding participates in this fixture.
fn interrupt_publication(
    db: &Db<MigrationExecutionCanister>,
    proposal: &SchemaProposal,
    receipt_first: bool,
) {
    let authorities = application_authorities(db);
    let bundles = load_current_application_bundles(&authorities).unwrap();
    let stores = existing_proposal_stores(proposal.target_database(), &authorities, &bundles);
    let before = load_entity_source_lineage_catalog().unwrap().unwrap();
    let planned = plan_schema_migration(proposal, &stores, &before).unwrap();
    assert!(!planned.requires_physical_validation());
    let head = accepted_head_after_candidates(&authorities, planned.candidates()).unwrap();
    let after = lineage_after_planned(&before, planned.lineage(), &head).unwrap();
    let receipt = SchemaChangeReceipt::new(
        proposal.target_database(),
        migration_submission_key(Some(proposal.migration().unwrap().digest())).unwrap(),
        proposal.digest().unwrap(),
        proposal.expected_head().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: head,
        },
    )
    .unwrap();
    let record = SchemaApplicationRecord::new(receipt, Vec::new()).unwrap();
    let application = SchemaApplicationRecordOp::insert(&record).unwrap();
    let lineage = EntitySourceLineageCatalogOp::replace(Some(&before), &after).unwrap();
    let candidate = &planned.candidates()[0];
    let store = db.store_handle(MIGRATION_EXECUTION_STORE_PATH).unwrap();
    let sequence = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::next_mutation_append_sequence)
        .unwrap();
    let marker_id = generate_commit_id().unwrap();
    let batch = JournalBatch::new_with_database_commit_sequence(
        generate_marker_batch_id(marker_id, 0).unwrap(),
        marker_id,
        sequence,
        DatabaseCommitSequence::new(next_database_commit_sequence().unwrap()),
        vec![
            JournalRecord::accepted_schema_publish(
                MIGRATION_EXECUTION_STORE_PATH,
                bundles[0].as_ref().unwrap().revision(),
                candidate.encoded_bundle().to_vec(),
                candidate.encoded_root().to_vec(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let marker = CommitMarker::from_parts_with_database_control(
        marker_id,
        vec![batch],
        vec![
            DatabaseControlOp::SchemaApplication(application.clone()),
            DatabaseControlOp::EntitySourceLineage(lineage),
        ],
    )
    .unwrap();
    let guard = begin_commit(&marker).unwrap();
    finish_commit(guard, |_| {
        if receipt_first {
            apply_schema_application_record_op(&application)?;
        }
        Err(InternalError::executor_invariant())
    })
    .expect_err("interruption must leave the compound marker for recovery");
}

fn assert_recovery(receipt_first: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    initialize(&root, true);
    let db = Db::<MigrationExecutionCanister>::new(&RECOVERY_DRIVER_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    let reader = Db::<MigrationExecutionCanister>::new(&RECOVERY_READER_REGISTRY, root.scope());
    let state = physical_state(&db);
    let item = snapshot(&db, "Item");
    let holder = snapshot(&db, "Holder");
    let candidate = proposal(&schema_application_target(&db).unwrap(), true, true);
    interrupt_publication(&db, &candidate, receipt_first);
    for _ in 0..2 {
        // Model fresh readiness for both handles before startup resumes.
        forget_recovered_domain_for_tests(&reader).unwrap();
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert_eq!(
            crate::db::commit::startup_recovery_witness(&RECOVERY_READER_REGISTRY).unwrap(),
            (true, false),
        );
        assert_eq!(
            advance(&db, &candidate).unwrap().phase(),
            SchemaMigrationPhase::Applied
        );
        assert_eq!(physical_state(&db), state);
        assert_lineage(&db, &candidate);
        assert_snapshot_identity(&item, &snapshot(&db, "CatalogItem"));
        assert_snapshot_identity(&holder, &snapshot(&db, "Holder"));
        let session = DbSession::new(&RECOVERY_READER_REGISTRY, &root);
        assert_rows(&session);
        assert_constraints(&session, true);
    }
}

#[test]
fn populated_rename_recovers_from_durable_marker() {
    assert_recovery(false);
}

#[test]
fn populated_rename_recovers_receipt_before_catalog_and_lineage() {
    assert_recovery(true);
}
