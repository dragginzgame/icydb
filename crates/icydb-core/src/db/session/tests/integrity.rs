use super::*;
use crate::db::{
    QuickIntegrityStatus,
    data::{RawRow, StructuralSlotReader},
    index::IndexEntryValue,
    integrity::{
        DeepIntegrityPageStatus, DerivedInspectionLimits, IntegrityAbortStatus,
        IntegrityFindingKind, IntegrityJobError, IntegrityJobOwner, IntegrityJobReceipt,
        IntegrityPendingTerminal, IntegritySubmissionKey, IntegrityTerminalOutcome,
        PhysicalUnitCheckpoint, RowInspectionLimits, clear_progress_store_for_tests,
        corrupt_progress_job_for_tests, run_integrity_retention_page_for_tests,
    },
};

#[test]
fn quick_integrity_uses_accepted_plan_and_durable_database_incarnation() {
    let session = sql_session();

    let first = session
        .__icydb_execute_quick_integrity_for_entity(SessionSqlEntity::PATH)
        .expect("bounded Quick inspection should succeed");
    let second = session
        .__icydb_execute_quick_integrity_for_entity(SessionSqlEntity::PATH)
        .expect("ordinary reopen should preserve Quick control identity");

    assert_eq!(first.status(), &QuickIntegrityStatus::CompleteClean);
    assert_eq!(first.entity().entity_path(), SessionSqlEntity::PATH);
    assert_eq!(first.entity().store_path(), SessionSqlStore::PATH);
    assert_eq!(
        first.database_incarnation_id(),
        second.database_incarnation_id(),
    );
    assert_eq!(
        first.accepted_schema_fingerprint(),
        second.accepted_schema_fingerprint(),
    );
    assert_eq!(first.total_findings(), 0);
    assert_eq!(first.omitted_findings(), 0);
}

#[test]
fn deep_proof_capture_is_stable_and_invalidates_after_a_durable_write() {
    reset_session_sql_store();
    let session = sql_session();
    let before = session
        .capture_integrity_proof_for_entity(SessionSqlEntity::PATH)
        .expect("journaled Deep proof should capture");
    let repeated = session
        .capture_integrity_proof_for_entity(SessionSqlEntity::PATH)
        .expect("unchanged Deep proof should recapture");
    assert_eq!(repeated, before);

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "proof mutation".to_string(),
            age: 31,
        })
        .expect("proof fixture should insert");
    let after = session
        .capture_integrity_proof_for_entity(SessionSqlEntity::PATH)
        .expect("post-mutation Deep proof should capture");

    assert_ne!(after, before);
    assert_eq!(
        after.database_incarnation_id(),
        before.database_incarnation_id(),
        "ordinary mutation must invalidate proof without replacing lifecycle identity",
    );
}

#[test]
fn deep_proof_capture_rejects_heap_storage_without_a_runtime_epoch() {
    let error = sql_session()
        .capture_integrity_proof_for_entity(HeapSessionSqlEntity::PATH)
        .expect_err("heap Deep must reject until a trustworthy runtime epoch exists");

    assert_eq!(error.class(), ErrorClass::Unsupported);
}

#[test]
fn deep_job_replays_start_and_continue_then_acknowledges_clean_terminal() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-replay").expect("owner should admit");
    let submission = IntegritySubmissionKey::new("deep-replay-1").expect("submission should admit");

    let start = session
        .start_deep_integrity_for_entity(SessionSqlEntity::PATH, owner.clone(), submission.clone())
        .expect("Deep start should persist");
    let replayed_start = session
        .start_deep_integrity_for_entity(SessionSqlEntity::PATH, owner.clone(), submission)
        .expect("lost Deep start response should replay");
    assert_eq!(replayed_start, start);
    let (job_id, mut sequence) = match start {
        IntegrityJobReceipt::Page(page) => {
            assert_eq!(page.page_sequence(), 0);
            assert_eq!(page.status(), &DeepIntegrityPageStatus::InProgress);
            (page.job_id(), page.page_sequence())
        }
        IntegrityJobReceipt::Abort(_) => panic!("start must return a page"),
    };

    let first = session
        .continue_deep_integrity(job_id, &owner, sequence)
        .expect("first Deep page should advance");
    let replayed_first = session
        .continue_deep_integrity(job_id, &owner, sequence)
        .expect("lost first page should replay");
    assert_eq!(replayed_first, first);
    sequence = first.page_sequence();

    loop {
        let receipt = session
            .continue_deep_integrity(job_id, &owner, sequence)
            .expect("Deep should advance to completion");
        sequence = receipt.page_sequence();
        if matches!(
            receipt,
            IntegrityJobReceipt::Page(ref page)
                if page.status()
                    == &DeepIntegrityPageStatus::Terminal(
                        IntegrityTerminalOutcome::DeepCompleteClean
                    )
        ) {
            let acknowledged = session
                .continue_deep_integrity(job_id, &owner, sequence)
                .expect("terminal receipt acknowledgement should persist");
            assert_eq!(acknowledged, receipt);
            let replayed_ack = session
                .continue_deep_integrity(job_id, &owner, sequence)
                .expect("acknowledged terminal receipt should replay");
            assert_eq!(replayed_ack, receipt);
            break;
        }
        assert!(
            sequence < 8,
            "empty fixture should complete in bounded phases"
        );
    }
}

#[test]
fn deep_job_discards_candidate_progress_when_the_proof_changes() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-invalidation").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-invalidation-1").expect("submission should admit"),
        )
        .expect("Deep start should persist");
    let job_id = start.job_id();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "invalidate".to_string(),
            age: 44,
        })
        .expect("fixture mutation should commit");
    let receipt = session
        .continue_deep_integrity(job_id, &owner, 0)
        .expect("proof drift should be a stable terminal receipt");

    assert!(matches!(
        receipt,
        IntegrityJobReceipt::Page(ref page)
            if page.status()
                == &DeepIntegrityPageStatus::Terminal(
                    IntegrityTerminalOutcome::Invalidated
                )
            && page.findings().is_empty()
    ));
}

#[test]
fn deep_job_reports_progressable_journal_corruption_and_completes_with_findings() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    SESSION_SQL_JOURNAL_STORE.with_borrow_mut(|journal| {
        journal
            .append_batch(
                &JournalBatch::new([0x41; 16], [0x42; 16], JournalSequence::new(1), Vec::new())
                    .expect("empty inspection fixture batch should build"),
            )
            .expect("inspection fixture batch should append");
    });
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-journal-finding").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-journal-finding-1").expect("submission should admit"),
        )
        .expect("Deep start should capture the clean tail proof");
    SESSION_SQL_JOURNAL_STORE.with_borrow_mut(|journal| {
        journal
            .corrupt_batch_envelope_for_tests(JournalSequence::new(1))
            .expect("fixture should corrupt the batch without changing proof counters");
    });

    let mut sequence = start.page_sequence();
    let mut saw_journal_finding = false;
    for _ in 0..8 {
        let receipt = session
            .continue_deep_integrity(start.job_id(), &owner, sequence)
            .expect("progressable journal corruption should not terminate traversal");
        sequence = receipt.page_sequence();
        let IntegrityJobReceipt::Page(page) = receipt else {
            panic!("Deep advancement must return a page");
        };
        saw_journal_finding |= page
            .findings()
            .iter()
            .any(|finding| finding.kind() == IntegrityFindingKind::MalformedJournalBatch);
        if let DeepIntegrityPageStatus::Terminal(outcome) = page.status() {
            assert_eq!(outcome, &IntegrityTerminalOutcome::DeepCompleteWithFindings,);
            assert!(saw_journal_finding);
            assert!(
                page.blocked_verifier_families()
                    .contains(&crate::db::integrity::IntegrityVerifierFamily::JournalBatchIdentity),
            );
            return;
        }
    }

    panic!("Deep journal finding fixture should complete within eight pages");
}

#[test]
fn deep_abort_preserves_the_outstanding_page_until_exact_acknowledgement() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-abort").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-abort-1").expect("submission should admit"),
        )
        .expect("Deep start should persist");
    let job_id = start.job_id();

    let pending = DbSession::<SessionSqlCanister>::abort_deep_integrity(job_id, &owner)
        .expect("idle job should become abort-pending");
    assert!(matches!(
        pending,
        IntegrityJobReceipt::Abort(ref receipt)
            if receipt.page_sequence() == 0
                && receipt.status()
                    == &IntegrityAbortStatus::TerminationPending(
                        IntegrityPendingTerminal::Aborted
                    )
    ));
    let replayed_pending = DbSession::<SessionSqlCanister>::abort_deep_integrity(job_id, &owner)
        .expect("pending abort should replay");
    assert_eq!(replayed_pending, pending);

    let terminal = session
        .continue_deep_integrity(job_id, &owner, 0)
        .expect("acknowledging the outstanding page should persist abort");
    assert!(matches!(
        terminal,
        IntegrityJobReceipt::Abort(ref receipt)
            if receipt.page_sequence() == 1
                && receipt.status()
                    == &IntegrityAbortStatus::Terminal(
                        IntegrityTerminalOutcome::Aborted
                    )
    ));
}

#[test]
fn deep_job_rejects_wrong_owner_without_replaying_progress() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-owner").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner,
            IntegritySubmissionKey::new("deep-owner-1").expect("submission should admit"),
        )
        .expect("Deep start should persist");
    let wrong_owner =
        IntegrityJobOwner::new("tests::wrong-owner").expect("wrong owner should still admit");
    let error = session
        .continue_deep_integrity(start.job_id(), &wrong_owner, 0)
        .expect_err("wrong owner must fail before receipt replay");

    assert!(matches!(
        error,
        crate::db::integrity::IntegrityDeepError::Job(IntegrityJobError::JobOwnerMismatch)
    ));
}

#[test]
fn deep_expiry_preserves_then_terminally_acknowledges_the_outstanding_receipt() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-expiry").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-expiry-1").expect("submission should admit"),
        )
        .expect("Deep start should persist");
    let job_id = start.job_id();

    let expiry = run_integrity_retention_page_for_tests::<SessionSqlCanister>(None, u64::MAX)
        .expect("bounded retention should freeze the expired job");
    assert!(expiry.exhausted());
    assert_eq!(expiry.jobs_scanned(), 1);
    assert_eq!(expiry.jobs_expired(), 1);
    assert_eq!(expiry.jobs_deleted(), 0);
    assert!(expiry.corrupt_jobs().is_empty());
    assert_eq!(expiry.next_checkpoint(), Some(job_id));

    let replayed_start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-expiry-1").expect("submission should admit"),
        )
        .expect("pending expiry must preserve the lost start response");
    assert_eq!(replayed_start, start);

    let terminal = session
        .continue_deep_integrity(job_id, &owner, 0)
        .expect("exact page acknowledgement should persist expiry");
    assert!(matches!(
        terminal,
        IntegrityJobReceipt::Abort(ref receipt)
            if receipt.page_sequence() == 1
                && receipt.status()
                    == &IntegrityAbortStatus::Terminal(
                        IntegrityTerminalOutcome::Expired
                    )
    ));

    let unacknowledged =
        run_integrity_retention_page_for_tests::<SessionSqlCanister>(None, u64::MAX)
            .expect("unacknowledged terminal receipt must remain retained");
    assert_eq!(unacknowledged.jobs_deleted(), 0);

    let acknowledged = session
        .continue_deep_integrity(job_id, &owner, 1)
        .expect("terminal acknowledgement should persist");
    assert_eq!(acknowledged, terminal);
    let deleted = run_integrity_retention_page_for_tests::<SessionSqlCanister>(None, u64::MAX)
        .expect("acknowledged terminal receipt should become retention-eligible");
    assert_eq!(deleted.jobs_deleted(), 1);

    let error = session
        .continue_deep_integrity(job_id, &owner, 1)
        .expect_err("deleted progress must not resurrect");
    assert!(matches!(
        error,
        crate::db::integrity::IntegrityDeepError::Job(IntegrityJobError::JobNotFound)
    ));
}

#[test]
fn deep_start_enforces_the_frozen_per_owner_capacity() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-capacity").expect("owner should admit");
    for ordinal in 0..8 {
        session
            .start_deep_integrity_for_entity(
                SessionSqlEntity::PATH,
                owner.clone(),
                IntegritySubmissionKey::new(format!("deep-capacity-{ordinal}"))
                    .expect("submission should admit"),
            )
            .expect("the frozen per-owner capacity should admit eight jobs");
    }

    let error = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner,
            IntegritySubmissionKey::new("deep-capacity-overflow").expect("submission should admit"),
        )
        .expect_err("the ninth retained owner job must reject without eviction");
    assert!(matches!(
        error,
        crate::db::integrity::IntegrityDeepError::Job(IntegrityJobError::CapacityExceeded)
    ));
}

#[test]
fn deep_job_rejects_a_corrupt_current_progress_record_before_replay() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-corrupt-job").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-corrupt-job-1").expect("submission should admit"),
        )
        .expect("Deep start should persist");
    corrupt_progress_job_for_tests::<SessionSqlCanister>(start.job_id())
        .expect("test should corrupt the retained current-form job");

    let error = session
        .continue_deep_integrity(start.job_id(), &owner, 0)
        .expect_err("corrupt progress must fail before cached receipt replay");
    assert!(matches!(
        error,
        crate::db::integrity::IntegrityDeepError::Job(IntegrityJobError::CorruptProgressRecord)
    ));
}

#[test]
fn row_integrity_pages_resume_within_rows_until_the_entity_interval_is_exhausted() {
    reset_session_sql_store();
    let session = sql_session();
    for (name, age) in [("Ada", 31), ("Grace", 42), ("Linus", 53)] {
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: name.to_string(),
                age,
            })
            .expect("row fixture should insert");
    }
    session
        .insert(SessionAggregateEntity {
            id: Ulid::generate(),
            group: 1,
            rank: 1,
            label: "outside selected interval".to_string(),
        })
        .expect("neighbouring entity fixture should insert");

    let limits = RowInspectionLimits::for_tests(1, 2, 8, usize::MAX);
    let mut checkpoint = PhysicalUnitCheckpoint::BeforeFirst;
    let mut completed = 0_u32;
    let mut calls = 0_u32;
    loop {
        let page = session
            .execute_integrity_row_page_for_entity(SessionSqlEntity::PATH, checkpoint, limits)
            .expect("bounded row page should execute");
        assert!(page.findings().is_empty());
        assert!(page.atoms_classified() <= 2);
        assert_eq!(page.rows_started(), 1);
        assert!(page.decoded_bytes() > 0);
        completed = completed
            .checked_add(page.rows_completed())
            .expect("fixture row total should stay bounded");
        calls = calls
            .checked_add(1)
            .expect("fixture calls should stay bounded");
        if page.exhausted() {
            break;
        }
        assert!(calls < 32, "exact row continuation must make progress");
        checkpoint = page.checkpoint().clone();
    }

    assert_eq!(completed, 3);
    assert!(calls > 3, "the atom bound should force within-row resumes");
}

#[test]
fn row_integrity_classifies_a_malformed_row_without_silent_success() {
    reset_session_sql_store();
    let id = Ulid::generate();
    insert_fixed_session_sql_entity_for_test(id, "Ada", 31);
    let raw_key = DecodedDataStoreKey::try_new::<SessionSqlEntity>(id)
        .expect("fixture key should build")
        .to_raw()
        .expect("fixture key should encode");
    SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        store.insert_raw_for_test(
            raw_key,
            RawRow::try_new(vec![0xff]).expect("malformed fixture stays size-bounded"),
        );
    });

    let page = sql_session()
        .execute_integrity_row_page_for_entity(
            SessionSqlEntity::PATH,
            PhysicalUnitCheckpoint::BeforeFirst,
            RowInspectionLimits::standard(),
        )
        .expect("malformed physical state should produce a typed finding");

    assert!(page.exhausted());
    assert_eq!(page.rows_completed(), 1);
    assert_eq!(page.findings().len(), 1);
    assert_eq!(
        page.findings()[0].kind(),
        IntegrityFindingKind::MalformedRow,
    );
    assert!(!page.blocked_verifier_families().is_empty());
}

#[test]
fn row_integrity_point_checks_expected_forward_index_entries() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    session
        .insert(IndexedSessionSqlEntity {
            id: Ulid::generate(),
            name: "Ada".to_string(),
            age: 31,
        })
        .expect("indexed fixture should insert");
    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });

    let page = session
        .execute_integrity_row_page_for_entity(
            IndexedSessionSqlEntity::PATH,
            PhysicalUnitCheckpoint::BeforeFirst,
            RowInspectionLimits::standard(),
        )
        .expect("missing forward entry should be a typed finding");

    assert!(page.exhausted());
    assert!(page.findings().iter().any(|finding| {
        finding.kind() == IntegrityFindingKind::MissingIndexEntry
            && finding.schema_index_id().is_some()
    }));

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    session
        .insert(ExpressionIndexedSessionSqlEntity {
            id: Ulid::generate(),
            name: "Ada".to_string(),
            age: 31,
        })
        .expect("expression-indexed fixture should insert");
    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });
    let page = session
        .execute_integrity_row_page_for_entity(
            ExpressionIndexedSessionSqlEntity::PATH,
            PhysicalUnitCheckpoint::BeforeFirst,
            RowInspectionLimits::standard(),
        )
        .expect("missing expression-index entry should be a typed finding");
    assert!(page.findings().iter().any(|finding| {
        finding.kind() == IntegrityFindingKind::MissingIndexEntry
            && finding.schema_index_id().is_some()
    }));
}

#[test]
fn row_integrity_point_checks_expected_relation_targets_and_reverse_entries() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 1,
            parent: None,
        })
        .expect("relation target should insert");
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 2,
            parent: Some(1),
        })
        .expect("relation source should insert");
    SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });

    let page = session
        .execute_integrity_row_page_for_entity(
            SessionSqlSelfRelationEntity::PATH,
            PhysicalUnitCheckpoint::BeforeFirst,
            RowInspectionLimits::standard(),
        )
        .expect("missing reverse entry should be a typed finding");

    assert!(page.exhausted());
    assert!(page.findings().iter().any(|finding| {
        finding.kind() == IntegrityFindingKind::MissingReverseRelationEntry
            && finding.relation_id().is_some()
    }));
}

#[test]
fn row_integrity_fails_closed_when_a_within_row_checkpoint_disappears() {
    reset_session_sql_store();
    insert_fixed_session_sql_entity_for_test(Ulid::generate(), "Ada", 31);
    let session = sql_session();
    let first = session
        .execute_integrity_row_page_for_entity(
            SessionSqlEntity::PATH,
            PhysicalUnitCheckpoint::BeforeFirst,
            RowInspectionLimits::for_tests(1, 1, 8, usize::MAX),
        )
        .expect("first physical atom should classify");
    assert!(!first.exhausted());
    assert!(matches!(
        first.checkpoint(),
        PhysicalUnitCheckpoint::Within { .. },
    ));

    SESSION_SQL_DATA_STORE.with_borrow_mut(DataStore::clear);
    let error = session
        .execute_integrity_row_page_for_entity(
            SessionSqlEntity::PATH,
            first.checkpoint().clone(),
            RowInspectionLimits::standard(),
        )
        .expect_err("a missing exact physical resume row must fail closed");

    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn index_integrity_pages_resume_exactly_and_classify_orphan_entries() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let ids = [Ulid::generate(), Ulid::generate(), Ulid::generate()];
    for (id, name) in ids.into_iter().zip(["Ada", "Grace", "Linus"]) {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age: 31,
            })
            .expect("indexed fixture should insert");
    }
    let orphan_key = DecodedDataStoreKey::try_new::<IndexedSessionSqlEntity>(ids[1])
        .expect("orphan fixture key should build")
        .to_raw()
        .expect("orphan fixture key should encode");
    INDEXED_SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        assert!(
            store.remove(&orphan_key).is_some(),
            "fixture source row should exist",
        );
    });

    let limits = DerivedInspectionLimits::for_tests(1, 1, 8, usize::MAX);
    let mut checkpoint = PhysicalUnitCheckpoint::BeforeFirst;
    let mut findings = Vec::new();
    let mut completed = 0_u32;
    let mut calls = 0_u32;
    loop {
        let page = session
            .execute_integrity_index_page_for_entity(
                IndexedSessionSqlEntity::PATH,
                0,
                checkpoint,
                limits,
            )
            .expect("bounded index page should execute");
        findings.extend_from_slice(page.findings());
        completed = completed
            .checked_add(page.entries_completed())
            .expect("fixture count should stay bounded");
        calls = calls
            .checked_add(1)
            .expect("call count should stay bounded");
        if page.exhausted() {
            break;
        }
        assert!(calls < 8, "exact index continuation must make progress");
        checkpoint = page.checkpoint().clone();
    }

    assert_eq!(completed, 3);
    assert!(calls >= 3);
    assert_eq!(
        findings
            .iter()
            .filter(|finding| finding.kind() == IntegrityFindingKind::OrphanIndexEntry)
            .count(),
        1,
    );
    assert!(findings.iter().all(|finding| {
        finding.schema_index_id().is_some()
            && finding.phase() == crate::db::IntegrityPhase::IndexEntries
    }));
}

#[test]
fn index_integrity_classifies_malformed_values_and_row_projection_drift() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = Ulid::generate();
    session
        .insert(IndexedSessionSqlEntity {
            id,
            name: "Ada".to_string(),
            age: 31,
        })
        .expect("indexed fixture should insert");
    let mut active_key = None;
    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow(|store| {
        store
            .visit_entries(|key, _| {
                active_key = Some(key.clone());
                Ok::<_, std::convert::Infallible>(IndexStoreVisit::Stop)
            })
            .expect("infallible fixture traversal");
    });
    let active_key = active_key.expect("fixture index entry should exist");
    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.insert(
            active_key.clone(),
            IndexEntryValue::from_persisted_bytes(vec![0xff]),
        );
    });
    let malformed = session
        .execute_integrity_index_page_for_entity(
            IndexedSessionSqlEntity::PATH,
            0,
            PhysicalUnitCheckpoint::BeforeFirst,
            DerivedInspectionLimits::standard(),
        )
        .expect("malformed value should produce a finding");
    assert_eq!(malformed.findings().len(), 1);
    assert_eq!(
        malformed.findings()[0].kind(),
        IntegrityFindingKind::MalformedIndexEntry,
    );

    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.insert(active_key, IndexEntryValue::presence());
    });
    let changed = IndexedSessionSqlEntity {
        id,
        name: "Grace".to_string(),
        age: 31,
    };
    let changed_key = DecodedDataStoreKey::try_new::<IndexedSessionSqlEntity>(id)
        .expect("changed fixture key should build")
        .to_raw()
        .expect("changed fixture key should encode");
    let changed_row = canonical_row_from_entity_for_model_proposal_for_test(&changed)
        .expect("changed fixture row should encode")
        .into_raw_row();
    INDEXED_SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        assert!(
            store
                .insert_raw_for_test(changed_key, changed_row)
                .is_some(),
            "fixture source row should be replaced",
        );
    });
    let divergent = session
        .execute_integrity_index_page_for_entity(
            IndexedSessionSqlEntity::PATH,
            0,
            PhysicalUnitCheckpoint::BeforeFirst,
            DerivedInspectionLimits::standard(),
        )
        .expect("stale physical key should produce a finding");
    assert_eq!(divergent.findings().len(), 1);
    assert_eq!(
        divergent.findings()[0].kind(),
        IntegrityFindingKind::DivergentIndexEntry,
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the fixture must build two accepted rows, inject one impossible duplicate witness, and page across the physical sub-unit boundary"
)]
fn index_integrity_detects_duplicate_unique_keys_across_page_boundaries() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first = SessionUniquePrefixOffsetEntity {
        id: Ulid::from_u128(1),
        tier: "gold".to_string(),
        handle: "ada".to_string(),
        note: "first".to_string(),
    };
    let second = SessionUniquePrefixOffsetEntity {
        id: Ulid::from_u128(2),
        tier: "gold".to_string(),
        handle: "grace".to_string(),
        note: "second".to_string(),
    };
    session
        .insert(first.clone())
        .expect("first unique fixture should insert");
    session
        .insert(second.clone())
        .expect("second unique fixture should insert");

    let duplicate_second = SessionUniquePrefixOffsetEntity {
        tier: first.tier,
        handle: first.handle,
        ..second
    };
    let duplicate_key =
        DecodedDataStoreKey::try_new::<SessionUniquePrefixOffsetEntity>(duplicate_second.id)
            .expect("duplicate fixture key should build")
            .to_raw()
            .expect("duplicate fixture key should encode");
    let duplicate_row = canonical_row_from_entity_for_model_proposal_for_test(&duplicate_second)
        .expect("duplicate fixture row should encode")
        .into_raw_row();
    INDEXED_SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        assert!(
            store
                .insert_raw_for_test(duplicate_key.clone(), duplicate_row.clone())
                .is_some(),
            "second source row should be replaced",
        );
    });

    let hooks = session
        .db
        .runtime_hook_for_entity_path(SessionUniquePrefixOffsetEntity::PATH)
        .expect("unique fixture hook should resolve");
    let store = session
        .db
        .recovered_store(hooks.store_path)
        .expect("unique fixture store should recover");
    let plan = session
        .accepted_inspection_plan_for_runtime_hook(hooks, store)
        .map_err(
            crate::db::session::accepted_schema::AcceptedInspectionPlanLoadError::into_internal,
        )
        .expect("unique fixture accepted plan should load");
    let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(
        &duplicate_row,
        plan.row_contract(),
    )
    .expect("duplicate fixture row should decode");
    let duplicate_witness = plan
        .index_inspection()
        .project(
            0,
            SessionUniquePrefixOffsetEntity::ENTITY_TAG,
            &DecodedDataStoreKey::try_from_raw(&duplicate_key)
                .expect("duplicate fixture key should decode")
                .primary_key_value(),
            &reader,
        )
        .expect("duplicate witness should project")
        .expect("unique fixture belongs to its index");
    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.insert(
            duplicate_witness.raw_key().clone(),
            IndexEntryValue::presence(),
        );
    });

    let mut checkpoint = PhysicalUnitCheckpoint::BeforeFirst;
    let mut duplicate_findings = 0;
    loop {
        let page = session
            .execute_integrity_index_page_for_entity(
                SessionUniquePrefixOffsetEntity::PATH,
                0,
                checkpoint,
                DerivedInspectionLimits::for_tests(1, 1, 8, usize::MAX),
            )
            .expect("unique physical page should execute");
        duplicate_findings += page
            .findings()
            .iter()
            .filter(|finding| finding.kind() == IntegrityFindingKind::DuplicateUniqueIndexKey)
            .count();
        if page.exhausted() {
            break;
        }
        checkpoint = page.checkpoint().clone();
    }

    assert_eq!(
        duplicate_findings, 2,
        "both physical witnesses should report the duplicate logical key",
    );
}

#[test]
fn reverse_integrity_classifies_source_owned_orphan_entries() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 1,
            parent: None,
        })
        .expect("target fixture should insert");
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 2,
            parent: Some(1),
        })
        .expect("source fixture should insert");
    let source_key = DecodedDataStoreKey::try_new::<SessionSqlSelfRelationEntity>(2)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        assert!(
            store.remove(&source_key).is_some(),
            "source fixture should exist",
        );
    });

    let page = session
        .execute_integrity_reverse_page_for_entity(
            SessionSqlSelfRelationEntity::PATH,
            0,
            PhysicalUnitCheckpoint::BeforeFirst,
            DerivedInspectionLimits::standard(),
        )
        .expect("active reverse domain should scan");

    assert!(page.exhausted());
    assert_eq!(page.entries_completed(), 1);
    assert_eq!(page.findings().len(), 1);
    assert_eq!(
        page.findings()[0].kind(),
        IntegrityFindingKind::OrphanReverseRelationEntry,
    );
    assert!(page.findings()[0].relation_id().is_some());
    assert_eq!(
        page.findings()[0].phase(),
        crate::db::IntegrityPhase::ReverseRelations,
    );
}

#[test]
fn reverse_integrity_classifies_source_projection_drift() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 1,
            parent: None,
        })
        .expect("target fixture should insert");
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 2,
            parent: Some(1),
        })
        .expect("source fixture should insert");
    let source = SessionSqlSelfRelationEntity {
        id: 2,
        parent: None,
    };
    let source_key = DecodedDataStoreKey::try_new::<SessionSqlSelfRelationEntity>(source.id)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    let source_row = canonical_row_from_entity_for_model_proposal_for_test(&source)
        .expect("changed source row should encode")
        .into_raw_row();
    SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        assert!(
            store.insert_raw_for_test(source_key, source_row).is_some(),
            "source fixture should be replaced",
        );
    });

    let page = session
        .execute_integrity_reverse_page_for_entity(
            SessionSqlSelfRelationEntity::PATH,
            0,
            PhysicalUnitCheckpoint::BeforeFirst,
            DerivedInspectionLimits::standard(),
        )
        .expect("stale reverse edge should produce a finding");

    assert!(page.exhausted());
    assert_eq!(page.findings().len(), 1);
    assert_eq!(
        page.findings()[0].kind(),
        IntegrityFindingKind::DivergentReverseRelationEntry,
    );
    assert_eq!(page.findings()[0].field_paths(), &["parent".to_string()]);
}
