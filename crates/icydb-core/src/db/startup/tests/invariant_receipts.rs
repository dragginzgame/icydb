//! Runtime defects cannot acquire durable authority over a corrected build.

use super::*;
use crate::db::{
    commit::{CommitControlObservation, observe_commit_control},
    startup::receipt::{
        AcceptedHeadBinding, DatabaseControlBinding, StartupFailureBinding, StartupFailureReceipt,
        StoreAllocationIdentityOwned,
    },
};

const SUBMISSION: &str = "generated/invariant-receipts";

fn ready_session() -> crate::db::DbSession<CardinalityDriverCanister> {
    select_commit_memory_allocation(
        CardinalityDriverCanister::commit_memory_id().unwrap(),
        CardinalityDriverCanister::COMMIT_STABLE_KEY,
    );
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    let root = RequestExecutionRoot::__new_runtime_root();
    let db =
        crate::db::Db::<CardinalityDriverCanister>::new(&CARDINALITY_DRIVER_STORES, root.scope());
    ensure_database_format_admitted(&db).unwrap();
    mark_startup_recovery_complete_for_tests(&CARDINALITY_DRIVER_STORES).unwrap();
    let (database, head) = generated_schema_authority(
        &CARDINALITY_DRIVER_STORES,
        database_incarnation_id().unwrap(),
    )
    .unwrap();
    let schema_receipt = SchemaChangeReceipt::new(
        database,
        SchemaSubmissionKey::try_new(SUBMISSION).unwrap(),
        SchemaProposalDigest::from_bytes([3; 32]),
        head.clone(),
        SchemaChangeOutcome::NoOp {
            accepted_head: head,
        },
    )
    .unwrap();
    let record = SchemaApplicationRecord::new(schema_receipt, Vec::new()).unwrap();
    apply_schema_application_record_op(&SchemaApplicationRecordOp::insert(&record).unwrap())
        .unwrap();
    let session =
        crate::db::DbSession::<CardinalityDriverCanister>::new(&CARDINALITY_DRIVER_STORES, &root);
    for expected in [
        GeneratedStartupDriverStep::Recovering,
        GeneratedStartupDriverStep::Terminal,
    ] {
        assert_eq!(drive_generated_startup_recovery_page(
            &session, &CARDINALITY_DRIVER_STORES, SUBMISSION,
        ).unwrap(), expected);
    }
    session
}

fn matching_receipts(error: &InternalError) -> Vec<StartupFailureReceipt> {
    let CommitControlObservation::Present {
        incarnation,
        empty_control_proof: Some(proof),
        ..
    } = observe_commit_control().unwrap()
    else {
        panic!("fixture must have a verified empty commit slot");
    };
    let store = CARDINALITY_DRIVER_STORES.with(|registry| registry.iter().next().unwrap().1);
    let journal_proof = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::proof_identity)
        .unwrap();
    let (_, head) = generated_schema_authority(&CARDINALITY_DRIVER_STORES, incarnation).unwrap();
    [
        (
            StartupFailureKind::DatabaseControl,
            StartupFailureBinding::DatabaseControl {
                commit_memory_id: CardinalityDriverCanister::commit_memory_id().unwrap(),
                commit_stable_key: CardinalityDriverCanister::COMMIT_STABLE_KEY.to_string(),
                control: Some(DatabaseControlBinding::new(incarnation, proof)),
            },
        ),
        (
            StartupFailureKind::JournalRecovery,
            StartupFailureBinding::JournalRecovery {
                incarnation,
                allocation: StoreAllocationIdentityOwned::from_identity(
                    store.journal_allocation().unwrap(),
                ),
                proof: journal_proof,
            },
        ),
        (
            StartupFailureKind::SchemaReconciliation,
            StartupFailureBinding::SchemaReconciliation {
                incarnation,
                submission_key: SUBMISSION.to_string(),
                accepted_head: AcceptedHeadBinding::from_expected_head(&head),
            },
        ),
    ]
    .into_iter()
    .map(|(kind, binding)| {
        // A retained receipt carries code/origin/facts, not transient details
        // from the in-memory InternalError diagnostic.
        let diagnostic = error.diagnostic();
        let failure = StartupFailure::new(
            kind,
            diagnostic.error_code().diagnostic(diagnostic.origin()),
            error.diagnostic_facts(),
        );
        StartupFailureReceipt::new(failure, binding).unwrap()
    })
    .collect()
}

fn receipt_bytes() -> [u8; receipt::MAX_STARTUP_FAILURE_RECEIPT_BYTES] {
    let memory = receipt::startup_memory::<CardinalityDriverCanister>().unwrap();
    let mut bytes = [0; receipt::MAX_STARTUP_FAILURE_RECEIPT_BYTES];
    memory.read(0, &mut bytes);
    bytes
}

#[test]
fn invariant_diagnostics_do_not_publish_terminal_receipts() {
    for error in [
        InternalError::store_invariant(),
        InternalError::executor_invariant(),
    ] {
        for kind in [
            StartupFailureKind::DatabaseControl,
            StartupFailureKind::JournalRecovery,
            StartupFailureKind::SchemaReconciliation,
        ] {
            assert!(classify_terminal_failure(kind, &error).is_none());
            assert!(
                classify_terminal_failure_parts(kind, error.diagnostic(), error.diagnostic_facts())
                    .is_none()
            );
        }
        assert!(
            !record_generated_schema_startup_failure::<DriverCanister>(
                &DRIVER_STORES,
                SUBMISSION,
                error.diagnostic(),
                error.diagnostic_facts(),
            )
            .unwrap()
        );
    }
    assert_eq!(
        receipt::startup_memory::<DriverCanister>().unwrap().size(),
        0
    );
}

#[test]
fn retained_invariant_receipts_allow_corrected_recovery_without_query_writes() {
    let session = ready_session();
    for error in [
        InternalError::store_invariant(),
        InternalError::executor_invariant(),
    ] {
        for retained in matching_receipts(&error) {
            receipt::publish::<CardinalityDriverCanister>(&retained).unwrap();
            let bytes = receipt_bytes();
            let control = observe_commit_control().unwrap();
            for _ in 0..2 {
                assert_eq!(
                    receipt::load::<CardinalityDriverCanister>().unwrap(),
                    Some(retained.clone())
                );
                assert_eq!(
                    observe_generated_startup_state::<CardinalityDriverCanister>(
                        &CARDINALITY_DRIVER_STORES,
                        SUBMISSION,
                    ),
                    Ok(DatabaseStartupState::Ready)
                );
                assert_eq!(receipt_bytes(), bytes);
                assert_eq!(observe_commit_control().unwrap(), control);
            }
            assert_eq!(
                drive_generated_startup_recovery_page(
                    &session,
                    &CARDINALITY_DRIVER_STORES,
                    SUBMISSION,
                )
                .unwrap(),
                GeneratedStartupDriverStep::Terminal
            );
            assert!(
                receipt::load::<CardinalityDriverCanister>()
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn persisted_corruption_receipts_keep_their_exact_terminal_authority() {
    let session = ready_session();
    for retained in matching_receipts(&InternalError::store_corruption()) {
        receipt::publish::<CardinalityDriverCanister>(&retained).unwrap();
        let bytes = receipt_bytes();
        assert_eq!(
            observe_generated_startup_state::<CardinalityDriverCanister>(
                &CARDINALITY_DRIVER_STORES,
                SUBMISSION,
            ),
            Err(retained.failure().clone())
        );
        assert_eq!(drive_generated_startup_recovery_page(
            &session, &CARDINALITY_DRIVER_STORES, SUBMISSION,
        ).unwrap(), GeneratedStartupDriverStep::Terminal);
        assert_eq!(receipt_bytes(), bytes);
        assert_eq!(
            receipt::load::<CardinalityDriverCanister>().unwrap(),
            Some(retained)
        );
    }
}

#[test]
fn malformed_invariant_receipt_still_fails_closed_before_policy_filtering() {
    let session = ready_session();
    let retained = matching_receipts(&InternalError::store_invariant()).remove(0);
    receipt::publish::<CardinalityDriverCanister>(&retained).unwrap();
    let mut bytes = receipt_bytes();
    // Corrupt a produced payload byte without changing its checksum.
    let index = bytes.iter().rposition(|byte| *byte != 0).unwrap();
    bytes[index] ^= 1;
    receipt::startup_memory::<CardinalityDriverCanister>()
        .unwrap()
        .write(0, &bytes);
    let expected = StartupFailure::from_internal(
        StartupFailureKind::DatabaseControl,
        &InternalError::startup_control_corruption()
            .with_origin(crate::error::ErrorOrigin::Recovery),
    );
    assert_eq!(
        observe_generated_startup_state::<CardinalityDriverCanister>(
            &CARDINALITY_DRIVER_STORES,
            SUBMISSION,
        ),
        Err(expected)
    );
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &CARDINALITY_DRIVER_STORES, SUBMISSION,)
            .unwrap(),
        GeneratedStartupDriverStep::Terminal
    );
    assert_eq!(receipt_bytes(), bytes);
}

#[test]
fn allocation_only_invariant_receipt_cannot_mask_a_retained_marker() {
    let session = ready_session();
    let marker = CommitMarker::from_parts([0x59; 16], Vec::new()).unwrap();
    let guard = begin_commit(&marker).unwrap();
    let retained = StartupFailureReceipt::new(
        StartupFailure::from_internal(
            StartupFailureKind::DatabaseControl,
            &InternalError::store_invariant(),
        ),
        StartupFailureBinding::DatabaseControl {
            commit_memory_id: CardinalityDriverCanister::commit_memory_id().unwrap(),
            commit_stable_key: CardinalityDriverCanister::COMMIT_STABLE_KEY.to_string(),
            control: None,
        },
    )
    .unwrap();
    receipt::publish::<CardinalityDriverCanister>(&retained).unwrap();
    let bytes = receipt_bytes();
    assert_eq!(
        observe_generated_startup_state::<CardinalityDriverCanister>(
            &CARDINALITY_DRIVER_STORES,
            SUBMISSION,
        ),
        Ok(DatabaseStartupState::Recovering)
    );
    assert_eq!(receipt_bytes(), bytes);
    finish_commit(guard, |_| Ok(())).unwrap();
    assert_eq!(
        drive_generated_startup_recovery_page(&session, &CARDINALITY_DRIVER_STORES, SUBMISSION,)
            .unwrap(),
        GeneratedStartupDriverStep::Terminal
    );
    assert!(
        receipt::load::<CardinalityDriverCanister>()
            .unwrap()
            .is_none()
    );
}
