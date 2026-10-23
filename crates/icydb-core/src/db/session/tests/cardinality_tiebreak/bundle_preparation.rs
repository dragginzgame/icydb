//! A runtime-root cache hit cannot strand admitted cardinality evidence after folding.

use super::*;
use crate::db::{
    DatabaseStartupState, GeneratedStartupDriverStep, drive_generated_startup_recovery_page,
    observe_generated_startup_state,
    schema::{
        SchemaApplicationRecord, SchemaApplicationRecordOp, SchemaChangeOutcome,
        SchemaChangeReceipt, apply_schema_application_record_op,
        cardinality_generation::CardinalityAcceptedRootIdentity, generated_schema_authority,
    },
};
use icydb_schema::{SchemaProposalDigest, SchemaSubmissionKey};
use std::rc::Rc;

#[test]
fn ready_driver_refills_bundle_cache_after_a_prepared_schema_root_is_folded() {
    const SUBMISSION: &str = "generated/bundle-preparation";
    let session = initialize_journaled();
    seed_rows(&session);
    assert!((0..16).any(|_| session.db.drive_startup_recovery_page().unwrap()));
    drive_journaled_cardinality_to_ready(&session);
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();

    // This fixture publishes schema directly. Supply its generated terminal
    // receipt through the same control-store owner used by generated startup.
    let (database, head) = generated_schema_authority(
        &JOURNALED_STORE_REGISTRY,
        database_incarnation_id().unwrap(),
    )
    .unwrap();
    let receipt = SchemaChangeReceipt::new(
        database,
        SchemaSubmissionKey::try_new(SUBMISSION).unwrap(),
        SchemaProposalDigest::from_bytes([0x82; 32]),
        head.clone(),
        SchemaChangeOutcome::NoOp {
            accepted_head: head,
        },
    )
    .unwrap();
    let record = SchemaApplicationRecord::new(receipt, Vec::new()).unwrap();
    apply_schema_application_record_op(&SchemaApplicationRecordOp::insert(&record).unwrap())
        .unwrap();
    assert_eq!(
        observe_generated_startup_state::<JournaledTestCanister>(
            &JOURNALED_STORE_REGISTRY,
            SUBMISSION
        ),
        Ok(DatabaseStartupState::Ready)
    );

    session.execute_admin_sql_ddl("CREATE INDEX unused_id ON PlannerRow (id) EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2").unwrap();
    // An intervening replicated read prepares the live successor before its
    // schema publication folds. Keep this exact root alive across retirement.
    let prepared = session
        .accepted_schema_runtime_root()
        .unwrap_or_else(|_| panic!("runtime root should compile"));
    let accepted = store.with_schema(|schema| {
        let selected = schema.current_accepted_schema_root().unwrap().unwrap();
        CardinalityAcceptedRootIdentity::new(
            selected.root().revision(),
            selected.root().fingerprint(),
        )
        .unwrap()
    });
    assert!(store.with_schema(|schema| {
        schema
            .cached_cardinality_domain_for_root(accepted)
            .unwrap()
            .is_some()
    }));
    assert!((0..16).any(|_| session.db.drive_startup_recovery_page().unwrap()));
    assert!(
        store.with_schema(|schema| schema
            .cached_cardinality_domain_for_root(accepted)
            .unwrap()
            .is_none()),
        "fold retirement must exercise the missing-bundle-cache precondition"
    );
    let retained = session
        .accepted_schema_runtime_root()
        .unwrap_or_else(|_| panic!("runtime root should remain current"));
    assert!(Rc::ptr_eq(&prepared, &retained));

    // Quiescence must prepare store domains even when the runtime root hits.
    assert!((0..16).any(|_| {
        drive_generated_startup_recovery_page(&session, &JOURNALED_STORE_REGISTRY, SUBMISSION)
            .unwrap()
            == GeneratedStartupDriverStep::Terminal
    }));
    let domain = store
        .with_schema(|schema| schema.cached_cardinality_domain_for_root(accepted).unwrap())
        .expect("the Ready driver must refill admitted-root cardinality authority");
    let current = session
        .accepted_schema_runtime_root()
        .unwrap_or_else(|_| panic!("prepared runtime should stay current"));
    assert!(Rc::ptr_eq(&prepared, &current));
    assert_eq!(store.exact_entity_count(ENTITY_TAG), Some(12));
    let plan = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(plan.contains("IndexPrefix(z_rare_idx)"), "{plan}");
    assert!(
        plan.contains("cardinality_evidence: exact_at_selection"),
        "{plan}"
    );
    let rows = projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    assert_eq!(
        rows,
        (0..6)
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>()
    );
    let retained = store
        .with_schema(|schema| schema.cached_cardinality_domain_for_root(accepted).unwrap())
        .unwrap();
    assert!(Rc::ptr_eq(&domain, &retained));
}
