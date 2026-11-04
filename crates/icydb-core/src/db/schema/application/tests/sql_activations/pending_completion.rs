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

fn promote_constraint(db: &Db<AbortCanister>) {
    let store = db.store_handle(ABORT_STORE_PATH).unwrap();
    let pending = snapshot(db);
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
}

#[test]
fn promoted_application_finishes_after_recovery_with_a_stale_candidate_head() {
    let (db, session, proposal, job) = pending_application();
    unrelated_publication(&session);
    promote_constraint(&db);
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

#[test]
fn pending_application_completes_after_heap_topology_change_and_restore() {
    let (db, _, proposal, job) = pending_application();
    let expanded = crate::db::schema::application::tests::topology::add_unused_heap(&db);
    assert_terminal_replay(&expanded, &proposal, &job);
    let restored = crate::db::schema::application::tests::topology::remove_unused_heap(&expanded);
    assert_eq!(
        continue_schema_application(&restored, job.id(), None)
            .unwrap()
            .status(),
        &SchemaChangeProgressStatus::Applied
    );
}

#[test]
fn pending_application_aborts_after_heap_topology_change_and_restore() {
    let (db, _, _, job) = pending_application();
    let expanded = crate::db::schema::application::tests::topology::add_unused_heap(&db);
    let aborted = abort_schema_application(&expanded, job.id(), None).unwrap();
    assert_eq!(aborted.status(), &SchemaChangeProgressStatus::Aborted);
    let restored = crate::db::schema::application::tests::topology::remove_unused_heap(&expanded);
    assert_eq!(
        continue_schema_application(&restored, job.id(), None).unwrap(),
        aborted
    );
}

#[cfg(feature = "migration")]
mod lineage {
    use super::*;
    use crate::db::schema::{
        live_schema_checkpoint::load_entity_source_lineage_catalog,
        migration_lineage::{AcceptedEntitySourceLineage, AcceptedEntitySourceLineageState},
        with_schema_application_store,
    };
    use crate::error::InternalError;

    fn item_lineage(db: &Db<AbortCanister>) -> AcceptedEntitySourceLineage {
        entity_lineage(db, "Item")
    }

    fn entity_lineage(db: &Db<AbortCanister>, path: &str) -> AcceptedEntitySourceLineage {
        let target = schema_application_target(db).unwrap();
        let tag = db
            .accepted_runtime_entity_for_path(path)
            .unwrap()
            .entity_tag();
        load_entity_source_lineage_catalog()
            .unwrap()
            .unwrap()
            .entries()
            .get(&(target.stores()[0].identity(), tag))
            .unwrap()
            .clone()
    }

    #[test]
    fn pending_completion_publishes_admitted_source_after_restart() {
        let (db, _, proposal, job) = pending_application();
        let prior = item_lineage(&db);
        let expected = proposal
            .entity_source_digest(&icydb_schema::EntitySourceKey::try_new("Item").unwrap())
            .unwrap();
        let AcceptedEntitySourceLineageState::Adopted { source_digest, .. } = prior.state() else {
            panic!("initial source is adopted")
        };
        assert_ne!(*source_digest, expected);
        let record = with_schema_application_store(|store| store.load_job(job.id()))
            .unwrap()
            .unwrap();
        assert_eq!(
            record.pending_source_lineage().unwrap().proposed_digest(),
            expected
        );
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert_eq!(item_lineage(&db), prior);
        assert_terminal_replay(&db, &proposal, &job);
        let accepted = item_lineage(&db);
        assert_eq!(
            accepted.publication_head(),
            schema_application_target(&db).unwrap().accepted_head()
        );
        let AcceptedEntitySourceLineageState::Adopted { source_digest, .. } = accepted.state()
        else {
            panic!("completion remains adopted")
        };
        assert_eq!(*source_digest, expected);
        let terminal = with_schema_application_store(|store| store.load_job(job.id()))
            .unwrap()
            .unwrap();
        assert!(terminal.pending_source_lineage().is_none());
    }

    #[test]
    fn completion_preserves_an_unrelated_completed_source_edit() {
        let (db, _, admitted, job) = pending_application();
        let before = item_lineage(&db);
        let target = schema_application_target(&db).unwrap();
        let current = proposal(&db, "other-default", None);
        let base = current.fragments()[0]
            .entities()
            .iter()
            .find(|entity| entity.name().as_str() == "Other")
            .unwrap();
        let fields = base
            .fields()
            .iter()
            .map(|field| {
                if field.name().as_str() == "score" {
                    FieldFragment::new(
                        field.name().clone(),
                        field.field_type().clone(),
                        false,
                        FieldInsertPolicy::Default(ScalarLiteral::Int(7)),
                        None,
                    )
                } else {
                    field.clone()
                }
            })
            .collect();
        let other = EntityFragment::try_new(
            base.name().clone(),
            base.version(),
            fields,
            base.primary_key().to_vec(),
            base.indexes().to_vec(),
            base.relations().to_vec(),
            base.constraints().to_vec(),
        )
        .unwrap();
        let source = other.source_key().clone();
        let changed = SchemaProposal::try_compose(
            vec![SchemaCapability::INSERT_DEFAULTS],
            target.database_identity(),
            SchemaSubmissionKey::try_new("independent-source").unwrap(),
            target.accepted_head().clone(),
            vec![SchemaFragment::try_new(vec![other], Vec::new()).unwrap()],
            vec![EntityStoreAssignment::new(
                source.clone(),
                target.stores()[0].identity(),
            )],
            Vec::new(),
            None,
        )
        .unwrap();
        assert!(matches!(
            apply_schema(&db, &changed).unwrap().outcome(),
            SchemaChangeOutcome::Applied { .. }
        ));
        let other = entity_lineage(&db, "Other");
        let AcceptedEntitySourceLineageState::Adopted { source_digest, .. } = other.state() else {
            panic!("source remains adopted")
        };
        assert_eq!(
            *source_digest,
            changed.entity_source_digest(&source).unwrap()
        );
        // Republishing the catalog may advance provenance; the pending entity's
        // admitted source meaning must remain unchanged until promotion.
        assert_eq!(item_lineage(&db).state(), before.state());
        assert_terminal_replay(&db, &admitted, &job);
        assert_eq!(entity_lineage(&db, "Other"), other);
    }

    #[test]
    fn interrupted_terminal_publication_recovers_receipt_and_captured_lineage_together() {
        use crate::db::{
            commit::{
                CommitMarker, DatabaseControlOp, begin_commit, finish_commit, generate_commit_id,
            },
            schema::{
                EntitySourceLineageCatalogOp, SchemaApplicationRecord, SchemaApplicationRecordOp,
                SchemaChangeReceipt, apply_entity_source_lineage_catalog_op,
                apply_schema_application_record_op,
                migration_lineage::complete_pending_source_lineage,
            },
        };
        for receipt_first in [false, true] {
            crate::testing::run_isolated(move || {
                let (db, _, proposal, job) = pending_application();
                promote_constraint(&db);
                let before = load_entity_source_lineage_catalog().unwrap().unwrap();
                let record = with_schema_application_store(|store| store.load_job(job.id()))
                    .unwrap()
                    .unwrap();
                let head = schema_application_target(&db)
                    .unwrap()
                    .accepted_head()
                    .clone();
                let after = complete_pending_source_lineage(
                    &before,
                    &record.activations()[0],
                    record.pending_source_lineage().unwrap(),
                    &head,
                )
                .unwrap();
                let prior = record.receipt();
                let terminal = SchemaApplicationRecord::new(
                    SchemaChangeReceipt::new(
                        prior.database_identity(),
                        prior.submission_key().clone(),
                        prior.proposal_digest(),
                        prior.prior_head().clone(),
                        SchemaChangeOutcome::Applied {
                            accepted_head: head,
                        },
                    )
                    .unwrap(),
                    Vec::new(),
                )
                .unwrap();
                let application = SchemaApplicationRecordOp::replace(&record, &terminal).unwrap();
                let lineage = EntitySourceLineageCatalogOp::replace(Some(&before), &after).unwrap();
                let marker = CommitMarker::from_parts_with_database_control(
                    generate_commit_id(0).unwrap(),
                    Vec::new(),
                    vec![
                        DatabaseControlOp::SchemaApplication(application.clone()),
                        DatabaseControlOp::EntitySourceLineage(lineage.clone()),
                    ],
                )
                .unwrap();
                let guard = begin_commit(&marker).unwrap();
                finish_commit(guard, |_| {
                    if receipt_first {
                        apply_schema_application_record_op(&application)?;
                    } else {
                        apply_entity_source_lineage_catalog_op(&lineage)?;
                    }
                    Err(InternalError::executor_invariant())
                })
                .expect_err("cut retains the compound marker");
                assert!(continue_schema_application(&db, job.id(), None).is_err());
                for _ in 0..2 {
                    forget_recovered_domain_for_tests(&db).unwrap();
                    drive_startup_recovery_to_completion(&db);
                    assert_eq!(
                        load_entity_source_lineage_catalog().unwrap().unwrap(),
                        after
                    );
                    assert_eq!(
                        continue_schema_application(&db, job.id(), None)
                            .unwrap()
                            .receipt(),
                        terminal.receipt()
                    );
                    assert_eq!(apply_schema(&db, &proposal).unwrap(), *terminal.receipt());
                }
            });
        }
    }

    #[test]
    fn abort_keeps_prior_source_and_new_source_cannot_supersede_pending_entity() {
        let (db, _, admitted, job) = pending_application();
        let before = item_lineage(&db);
        let contender = proposal(&db, "superseding-source", Some("Other"));
        assert!(apply_schema(&db, &contender).is_err());
        assert_eq!(item_lineage(&db), before);
        assert_eq!(
            apply_schema(&db, &admitted).unwrap().outcome(),
            &SchemaChangeOutcome::Pending {
                job,
                candidate_head: match apply_schema(&db, &admitted).unwrap().outcome() {
                    SchemaChangeOutcome::Pending { candidate_head, .. } => candidate_head.clone(),
                    _ => panic!("exact pending replay"),
                },
            }
        );
        let aborted = abort_schema_application(&db, job.id(), None).unwrap();
        assert_eq!(aborted.status(), &SchemaChangeProgressStatus::Aborted);
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert_eq!(item_lineage(&db), before);
    }
}
