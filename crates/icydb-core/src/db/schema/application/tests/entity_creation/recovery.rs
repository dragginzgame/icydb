//! Interrupted creation resumes the existing compound schema-publication protocol.

use super::*;
use crate::db::{
    commit::{
        CommitMarker, DatabaseControlOp, begin_commit, finish_commit, generate_commit_id,
        generate_marker_batch_id, next_database_commit_sequence,
    },
    journal::{DatabaseCommitSequence, JournalBatch, JournalRecord},
    schema::application::{attach_ordinary_lineage_publication, lower_application_candidates},
};

pub(super) fn interrupt_publication(
    db: &Db<EvolutionCanister>,
    proposal: &SchemaProposal,
    receipt_first: bool,
) {
    let target = schema_application_target(db).unwrap();
    let authorities = application_authorities(db);
    let lowered = lower_application_candidates::<true>(&target, proposal, &authorities).unwrap();
    assert!(lowered.pending.is_none());
    let head = accepted_head_after_candidates(&authorities, &lowered.candidates).unwrap();
    let receipt = SchemaChangeReceipt::new(
        proposal.target_database(),
        proposal.submission_key().clone(),
        proposal.digest().unwrap(),
        proposal.expected_head().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: head.clone(),
        },
    )
    .unwrap();
    let record = SchemaApplicationRecord::new(receipt, Vec::new()).unwrap();
    let application = SchemaApplicationRecordOp::insert(&record).unwrap();
    let controls = attach_ordinary_lineage_publication(
        proposal,
        target.accepted_head(),
        &head,
        &authorities,
        &lowered.current_bundles,
        &lowered.candidates,
        application.clone(),
    )
    .unwrap();
    let candidate = &lowered.candidates[0];
    interrupt_candidate_publication(
        db,
        lowered.current_bundles[0].as_ref().unwrap(),
        candidate,
        controls,
        application,
        receipt_first,
    );
}

pub(super) fn interrupt_candidate_publication(
    db: &Db<EvolutionCanister>,
    before: &AcceptedSchemaRevisionBundle,
    candidate: &CandidateSchemaRevision,
    controls: Vec<DatabaseControlOp>,
    application: SchemaApplicationRecordOp,
    receipt_first: bool,
) {
    let store = db.store_handle(EVOLUTION_STORE_PATH).unwrap();
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
                EVOLUTION_STORE_PATH,
                before.revision(),
                candidate.encoded_bundle().to_vec(),
                candidate.encoded_root().to_vec(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let marker =
        CommitMarker::from_parts_with_database_control(marker_id, vec![batch], controls).unwrap();
    let guard = begin_commit(&marker).unwrap();
    finish_commit(guard, |_| {
        if receipt_first {
            crate::db::schema::apply_schema_application_record_op(&application)?;
        }
        Err(InternalError::executor_invariant())
    })
    .expect_err("interruption must retain the durable compound marker");
}

fn assert_recovery(receipt_first: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let physical = physical_state(&db);
    let item = snapshot(&db, "Item");
    let candidate = proposal(
        &schema_application_target(&db).unwrap(),
        &[("Quest", 1)],
        "recover-creation",
    );
    interrupt_publication(&db, &candidate, receipt_first);
    for _ in 0..2 {
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert!(matches!(
            apply_schema(&db, &candidate).unwrap().outcome(),
            SchemaChangeOutcome::Applied { .. }
        ));
        assert_eq!(physical_state(&db), physical);
        assert_eq!(snapshot(&db, "Item"), item);
        let tag = db
            .accepted_runtime_entity_for_path("Quest")
            .unwrap()
            .entity_tag();
        let target = schema_application_target(&db).unwrap();
        let lineage = crate::db::schema::application::load_entity_source_lineage_catalog()
            .unwrap()
            .unwrap();
        assert_eq!(
            lineage
                .get(target.stores()[0].identity(), tag)
                .unwrap()
                .publication_head(),
            target.accepted_head()
        );
        let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, &root);
        assert!(
            session
                .execute_trusted_live_page(&DynamicQuery::new("Quest").select(["id"]), None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
    assert_rows_and_constraints(&root);
}

#[test]
fn creation_recovers_catalog_lineage_and_receipt_together() {
    assert_recovery(false);
}

#[test]
fn creation_recovers_receipt_before_catalog_and_lineage() {
    assert_recovery(true);
}
