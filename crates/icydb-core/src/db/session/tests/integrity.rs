use super::*;
use crate::db::{
    IntegrityAuthorityClass, QuickIntegrityStatus, SqlIntegrityError,
    data::{RawRow, StructuralSlotReader},
    index::IndexEntryValue,
    integrity::{
        DeepIntegrityPageStatus, DerivedInspectionLimits, IntegrityAbortStatus,
        IntegrityCheckRequest, IntegrityCheckResult, IntegrityDeepError, IntegrityFindingKind,
        IntegrityJobError, IntegrityJobOwner, IntegrityJobReceipt, IntegrityPendingTerminal,
        IntegritySubmissionKey, IntegrityTerminalOutcome, PhysicalUnitCheckpoint,
        RowInspectionLimits, clear_progress_store_for_tests, corrupt_progress_job_for_tests,
        progress_job_encoded_len_for_tests, reset_integrity_retention_cursor_for_tests,
        run_integrity_retention_page_for_tests, run_next_integrity_retention_page_for_tests,
        set_progress_job_lease_deadline_for_tests,
    },
};

#[test]
fn quick_integrity_uses_accepted_plan_and_durable_database_incarnation() {
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::quick").expect("owner should admit");

    let IntegrityCheckResult::Quick(first) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlEntity>(),
            owner.clone(),
        )
        .expect("bounded Quick inspection should succeed")
    else {
        panic!("Quick request should return a Quick result");
    };
    let IntegrityCheckResult::Quick(second) = session
        .execute_admin_integrity(IntegrityCheckRequest::quick::<SessionSqlEntity>(), owner)
        .expect("ordinary reopen should preserve Quick control identity")
    else {
        panic!("Quick request should return a Quick result");
    };

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
fn quick_integrity_accepts_canonical_heap_and_relation_control_shapes() {
    reset_heap_session_sql_store();
    let owner = IntegrityJobOwner::new("tests::quick-control-shapes").expect("owner should admit");

    let IntegrityCheckResult::Quick(heap) = heap_sql_session()
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<HeapSessionSqlEntity>(),
            owner.clone(),
        )
        .expect("Quick should inspect canonical volatile storage without calling it corruption")
    else {
        panic!("heap Quick request should return a Quick result");
    };
    assert_eq!(heap.status(), &QuickIntegrityStatus::CompleteClean);

    reset_session_sql_store();
    let IntegrityCheckResult::Quick(relation) = sql_session()
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlSelfRelationEntity>(),
            owner,
        )
        .expect("Quick should resolve the accepted relation and target-store control closure")
    else {
        panic!("relation Quick request should return a Quick result");
    };
    assert_eq!(relation.status(), &QuickIntegrityStatus::CompleteClean);
}

#[test]
fn quick_integrity_reports_corrupt_journal_control_as_uninspectable() {
    reset_session_sql_store();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::quick-corrupt-control").expect("owner should admit");
    session
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlEntity>(),
            owner.clone(),
        )
        .expect("clean Quick should establish recovered runtime state");
    SESSION_SQL_JOURNAL_STORE.with_borrow_mut(|store| {
        store
            .corrupt_fold_watermark_for_tests()
            .expect("test should corrupt the bounded journal control envelope");
    });

    let IntegrityCheckResult::Quick(result) = session
        .execute_admin_integrity(IntegrityCheckRequest::quick::<SessionSqlEntity>(), owner)
        .expect("selected control corruption should remain a typed Quick result")
    else {
        panic!("corrupt-control Quick request should return a Quick result");
    };
    assert!(matches!(
        result.status(),
        QuickIntegrityStatus::Uninspectable(diagnostic)
            if diagnostic.class() == IntegrityAuthorityClass::Corruption
    ));

    reset_session_sql_store();
}

#[test]
fn quick_integrity_reports_readable_journal_control_drift_as_a_finding() {
    reset_session_sql_store();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::quick-control-finding").expect("owner should admit");
    session
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlEntity>(),
            owner.clone(),
        )
        .expect("clean Quick should establish recovered runtime state");
    SESSION_SQL_JOURNAL_STORE.with_borrow_mut(|store| {
        store
            .diverge_data_mutation_revision_for_tests(JournalSequence::new(7))
            .expect("test should persist a readable but inconsistent control revision");
    });

    let IntegrityCheckResult::Quick(result) = session
        .execute_admin_integrity(IntegrityCheckRequest::quick::<SessionSqlEntity>(), owner)
        .expect("readable control drift should complete with one typed finding")
    else {
        panic!("control-drift Quick request should return a Quick result");
    };
    assert_eq!(result.status(), &QuickIntegrityStatus::CompleteWithFindings,);
    assert_eq!(result.total_findings(), 1);
    assert_eq!(result.omitted_findings(), 0);
    assert_eq!(result.findings().len(), 1);
    assert_eq!(
        result.findings()[0].kind(),
        IntegrityFindingKind::JournalControlMismatch,
    );

    reset_session_sql_store();
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
fn deep_start_plan_load_failure_is_typed_and_publishes_no_job() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner =
        IntegrityJobOwner::new("tests::deep-start-uninspectable").expect("owner should admit");
    let submission =
        IntegritySubmissionKey::new("deep-start-uninspectable-1").expect("submission should admit");

    let error = session
        .start_deep_integrity_with_plan_load_failure_for_tests(
            SessionSqlEntity::PATH,
            owner.clone(),
            submission.clone(),
        )
        .expect_err("load-bearing plan failure must reject before job publication");
    assert!(matches!(
        error,
        IntegrityDeepError::Uninspectable(ref diagnostic)
            if diagnostic.class() == IntegrityAuthorityClass::Corruption
    ));

    let start = session
        .start_deep_integrity_for_entity(SessionSqlEntity::PATH, owner, submission)
        .expect("the rejected start must not occupy its submission identity");
    assert_eq!(start.page_sequence(), 0);
}

#[test]
fn deep_start_proof_drift_is_typed_and_publishes_no_job() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let proof_a = session
        .capture_integrity_proof_for_entity(SessionSqlEntity::PATH)
        .expect("opening proof should capture");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "start proof drift".to_string(),
            age: 41,
        })
        .expect("fixture mutation should commit");
    let proof_b = session
        .capture_integrity_proof_for_entity(SessionSqlEntity::PATH)
        .expect("changed proof should capture");
    let owner =
        IntegrityJobOwner::new("tests::deep-start-invalidated").expect("owner should admit");
    let submission =
        IntegritySubmissionKey::new("deep-start-invalidated-1").expect("submission should admit");

    let error = session
        .start_deep_integrity_with_proofs_for_tests(
            SessionSqlEntity::PATH,
            owner.clone(),
            submission.clone(),
            proof_a,
            proof_b,
        )
        .expect_err("A/B proof drift must reject before job publication");
    assert!(matches!(
        error,
        IntegrityDeepError::Job(IntegrityJobError::StartInvalidated)
    ));

    let start = session
        .start_deep_integrity_for_entity(SessionSqlEntity::PATH, owner, submission)
        .expect("the invalidated start must not occupy its submission identity");
    assert_eq!(start.page_sequence(), 0);
}

#[test]
fn deep_job_replays_start_and_continue_then_acknowledges_clean_terminal() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-replay").expect("owner should admit");
    let submission = IntegritySubmissionKey::new("deep-replay-1").expect("submission should admit");

    let IntegrityCheckResult::Deep(start) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_start::<SessionSqlEntity>(submission.clone()),
            owner.clone(),
        )
        .expect("Deep start should persist")
    else {
        panic!("Deep start should return a Deep receipt");
    };
    let IntegrityCheckResult::Deep(replayed_start) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_start::<SessionSqlEntity>(submission),
            owner.clone(),
        )
        .expect("lost Deep start response should replay")
    else {
        panic!("Deep start replay should return a Deep receipt");
    };
    assert_eq!(replayed_start, start);
    let (job_id, mut sequence) = match start {
        IntegrityJobReceipt::Page(page) => {
            assert_eq!(page.page_sequence(), 0);
            assert_eq!(page.status(), &DeepIntegrityPageStatus::InProgress);
            (page.job_id(), page.page_sequence())
        }
        IntegrityJobReceipt::Abort(_) => panic!("start must return a page"),
    };

    let IntegrityCheckResult::Deep(first) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_continue(job_id, sequence),
            owner.clone(),
        )
        .expect("first Deep page should advance")
    else {
        panic!("Deep continue should return a Deep receipt");
    };
    let IntegrityCheckResult::Deep(replayed_first) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_continue(job_id, sequence),
            owner.clone(),
        )
        .expect("lost first page should replay")
    else {
        panic!("Deep continue replay should return a Deep receipt");
    };
    assert_eq!(replayed_first, first);
    sequence = first.page_sequence();

    loop {
        let receipt = session
            .continue_deep_integrity_for_tests(job_id, &owner, sequence)
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
                .continue_deep_integrity_for_tests(job_id, &owner, sequence)
                .expect("terminal receipt acknowledgement should persist");
            assert_eq!(acknowledged, receipt);
            let replayed_ack = session
                .continue_deep_integrity_for_tests(job_id, &owner, sequence)
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
fn deep_start_replay_reports_advanced_and_conflicting_submission_identity() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::deep-start-replay").expect("owner should admit");
    let submission =
        IntegritySubmissionKey::new("deep-start-replay-1").expect("submission should admit");
    let start = session
        .start_deep_integrity_for_entity(SessionSqlEntity::PATH, owner.clone(), submission.clone())
        .expect("Deep start should persist");
    session
        .continue_deep_integrity_for_tests(start.job_id(), &owner, start.page_sequence())
        .expect("fixture job should advance once");

    let advanced = session
        .start_deep_integrity_for_entity(SessionSqlEntity::PATH, owner.clone(), submission.clone())
        .expect_err("start replay must not hide an already-delivered page");
    assert!(matches!(
        advanced,
        IntegrityDeepError::Job(IntegrityJobError::SubmissionAlreadyAdvanced)
    ));

    let conflict = session
        .start_deep_integrity_for_entity(SessionAggregateEntity::PATH, owner, submission)
        .expect_err("one owner/submission identity must not select another entity");
    assert!(matches!(
        conflict,
        IntegrityDeepError::Job(IntegrityJobError::SubmissionConflict)
    ));
}

#[test]
fn deep_continuation_plan_load_failure_persists_uninspectable_terminal() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner =
        IntegrityJobOwner::new("tests::deep-continue-uninspectable").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            owner.clone(),
            IntegritySubmissionKey::new("deep-continue-uninspectable-1")
                .expect("submission should admit"),
        )
        .expect("Deep start should persist");

    let terminal = session
        .continue_deep_integrity_with_plan_load_failure_for_tests(
            start.job_id(),
            &owner,
            start.page_sequence(),
        )
        .expect("load-bearing failure should persist one terminal receipt");
    assert!(matches!(
        terminal,
        IntegrityJobReceipt::Page(ref page)
            if matches!(
                page.status(),
                DeepIntegrityPageStatus::Terminal(
                    IntegrityTerminalOutcome::Uninspectable(diagnostic)
                ) if diagnostic.class() == IntegrityAuthorityClass::Corruption
            )
            && page.findings().is_empty()
    ));

    let replayed = session
        .continue_deep_integrity_for_tests(start.job_id(), &owner, start.page_sequence())
        .expect("the same acknowledgement must replay the terminal receipt");
    assert_eq!(replayed, terminal);
}

#[test]
fn integrity_response_and_progress_record_bytes_stay_bounded() {
    const MAX_MEASURED_BYTES: usize = 512 * 1024;

    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::integrity-bytes").expect("owner should admit");

    let quick = session
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlEntity>(),
            owner.clone(),
        )
        .expect("Quick measurement should execute");
    let quick_response_bytes = candid::encode_one(&quick)
        .expect("Quick response should encode")
        .len();

    let IntegrityCheckResult::Deep(start) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_start::<SessionSqlEntity>(
                IntegritySubmissionKey::new("integrity-bytes").expect("submission should admit"),
            ),
            owner.clone(),
        )
        .expect("Deep measurement start should execute")
    else {
        panic!("Deep start should return one receipt");
    };
    let job_id = start.job_id();
    let mut sequence = start.page_sequence();
    let mut max_response_bytes = candid::encode_one(&start)
        .expect("Deep response should encode")
        .len();
    let mut max_job_record_bytes = progress_job_encoded_len_for_tests::<SessionSqlCanister>(job_id)
        .expect("persisted Deep record should encode");

    loop {
        let receipt = session
            .continue_deep_integrity_for_tests(job_id, &owner, sequence)
            .expect("Deep measurement should advance");
        sequence = receipt.page_sequence();
        max_response_bytes = max_response_bytes.max(
            candid::encode_one(&receipt)
                .expect("Deep response should encode")
                .len(),
        );
        max_job_record_bytes = max_job_record_bytes.max(
            progress_job_encoded_len_for_tests::<SessionSqlCanister>(job_id)
                .expect("advanced Deep record should encode"),
        );

        if matches!(
            receipt,
            IntegrityJobReceipt::Page(ref page)
                if matches!(page.status(), DeepIntegrityPageStatus::Terminal(_))
        ) {
            break;
        }
        assert!(
            sequence < 8,
            "small measurement fixture should complete in bounded phases"
        );
    }

    eprintln!(
        "integrity closeout bytes: quick_response={quick_response_bytes}, \
         max_deep_response={max_response_bytes}, max_job_record={max_job_record_bytes}",
    );
    assert!(quick_response_bytes < MAX_MEASURED_BYTES);
    assert!(max_response_bytes < MAX_MEASURED_BYTES);
    assert!(max_job_record_bytes < MAX_MEASURED_BYTES);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the test builds one exact 64-finding physical page and proves it through the durable typed controller boundary"
)]
fn finding_saturated_deep_page_and_progress_record_stay_bounded() {
    const FINDING_SATURATION: usize = 64;
    const UNIQUE_WITNESSES: u128 = 32;
    const MAX_MEASURED_BYTES: usize = 512 * 1024;

    reset_indexed_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = indexed_sql_session();
    let rows = (1..=UNIQUE_WITNESSES)
        .map(|id| SessionUniquePrefixOffsetEntity {
            id: Ulid::from_u128(id),
            tier: "gold".to_string(),
            handle: format!("initial-{id:02}"),
            note: "source".to_string(),
        })
        .collect::<Vec<_>>();
    session
        .insert_many_atomic(rows)
        .expect("unique source fixture should insert atomically");

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
    let mut duplicate_witnesses = Vec::new();
    for id in 1..=UNIQUE_WITNESSES {
        let entity = SessionUniquePrefixOffsetEntity {
            id: Ulid::from_u128(id),
            tier: "gold".to_string(),
            handle: "shared".to_string(),
            note: "source".to_string(),
        };
        let key = DecodedDataStoreKey::try_new::<SessionUniquePrefixOffsetEntity>(entity.id)
            .expect("duplicate fixture key should build");
        let row = canonical_row_from_entity_for_model_proposal_for_test(&entity)
            .expect("duplicate fixture row should encode")
            .into_raw_row();
        let reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, plan.row_contract())
                .expect("duplicate fixture row should decode");
        duplicate_witnesses.push(
            plan.index_inspection()
                .project(
                    0,
                    SessionUniquePrefixOffsetEntity::ENTITY_TAG,
                    &key.primary_key_value(),
                    &reader,
                )
                .expect("duplicate fixture witness should project")
                .expect("unique fixture belongs to its index")
                .raw_key()
                .clone(),
        );
    }
    INDEXED_SESSION_SQL_DATA_STORE.with_borrow_mut(DataStore::clear);
    INDEXED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        for witness in duplicate_witnesses {
            store.insert(witness, IndexEntryValue::presence());
        }
    });

    let owner = IntegrityJobOwner::new("tests::finding-saturated").expect("owner should admit");
    let IntegrityCheckResult::Deep(start) = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_start::<SessionUniquePrefixOffsetEntity>(
                IntegritySubmissionKey::new("finding-saturated").expect("submission should admit"),
            ),
            owner.clone(),
        )
        .expect("Deep start should persist")
    else {
        panic!("Deep start should return one receipt");
    };
    let job_id = start.job_id();
    let mut acknowledged_sequence = start.page_sequence();
    let saturated = loop {
        let IntegrityCheckResult::Deep(receipt) = session
            .execute_admin_integrity(
                IntegrityCheckRequest::deep_continue(job_id, acknowledged_sequence),
                owner.clone(),
            )
            .expect("Deep finding page should advance")
        else {
            panic!("Deep continuation should return one receipt");
        };
        let IntegrityJobReceipt::Page(page) = &receipt else {
            panic!("Deep finding page should not abort");
        };
        if page.findings().len() == FINDING_SATURATION {
            let IntegrityCheckResult::Deep(replayed) = session
                .execute_admin_integrity(
                    IntegrityCheckRequest::deep_continue(job_id, acknowledged_sequence),
                    owner,
                )
                .expect("lost saturated page should replay")
            else {
                panic!("Deep replay should return one receipt");
            };
            assert_eq!(replayed, receipt);
            break receipt;
        }
        acknowledged_sequence = receipt.page_sequence();
        assert!(
            acknowledged_sequence < 8,
            "dense finding fixture should reach its saturated page promptly",
        );
    };
    let IntegrityJobReceipt::Page(page) = &saturated else {
        panic!("saturated receipt should be a page");
    };
    assert_eq!(
        page.findings_seen(),
        u64::try_from(FINDING_SATURATION).expect("finding bound should fit u64"),
    );
    assert_eq!(
        page.findings()
            .iter()
            .filter(|finding| finding.kind() == IntegrityFindingKind::OrphanIndexEntry)
            .count(),
        usize::try_from(UNIQUE_WITNESSES).expect("fixture count should fit usize"),
    );
    assert_eq!(
        page.findings()
            .iter()
            .filter(|finding| finding.kind() == IntegrityFindingKind::DuplicateUniqueIndexKey)
            .count(),
        usize::try_from(UNIQUE_WITNESSES).expect("fixture count should fit usize"),
    );
    let response_bytes = candid::encode_one(&saturated)
        .expect("saturated Deep receipt should encode")
        .len();
    let job_record_bytes = progress_job_encoded_len_for_tests::<SessionSqlCanister>(job_id)
        .expect("saturated persisted Deep record should encode");
    eprintln!(
        "finding-saturated integrity bytes: response={response_bytes}, \
         job_record={job_record_bytes}, findings={FINDING_SATURATION}",
    );
    assert!(response_bytes < MAX_MEASURED_BYTES);
    assert!(job_record_bytes < MAX_MEASURED_BYTES);

    clear_progress_store_for_tests::<SessionSqlCanister>();
    reset_indexed_session_sql_store();
}

#[test]
fn integrity_sql_lowers_to_identical_typed_requests_and_receipts() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::sql-parity").expect("owner should admit");

    let typed_quick = session
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlEntity>(),
            owner.clone(),
        )
        .expect("typed Quick should succeed");
    let sql_quick = session
        .execute_admin_integrity_sql("CHECK INTEGRITY SessionSqlEntity QUICK", owner.clone())
        .expect("SQL Quick should succeed");
    assert_eq!(sql_quick, typed_quick);

    let submission = IntegritySubmissionKey::new("sql-parity-1").expect("submission should admit");
    let typed_start = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_start::<SessionSqlEntity>(submission),
            owner.clone(),
        )
        .expect("typed Deep start should succeed");
    let sql_start = session
        .execute_admin_integrity_sql(
            "CHECK INTEGRITY SessionSqlEntity DEEP START 'sql-parity-1'",
            owner.clone(),
        )
        .expect("SQL Deep start should replay the typed receipt");
    assert_eq!(sql_start, typed_start);

    let IntegrityCheckResult::Deep(start) = typed_start else {
        panic!("Deep start should return a Deep receipt");
    };
    let job_id = start.job_id();
    let continue_sql = format!(
        "CHECK INTEGRITY DEEP CONTINUE '{}' AFTER {}",
        job_id.to_hex(),
        start.page_sequence(),
    );
    let sql_continue = session
        .execute_admin_integrity_sql(continue_sql.as_str(), owner.clone())
        .expect("SQL Deep continuation should advance");
    let typed_continue = session
        .execute_admin_integrity(
            IntegrityCheckRequest::deep_continue(job_id, start.page_sequence()),
            owner.clone(),
        )
        .expect("typed continuation should replay the SQL receipt");
    assert_eq!(typed_continue, sql_continue);

    let abort_sql = format!("CHECK INTEGRITY DEEP ABORT '{}'", job_id.to_hex());
    let sql_abort = session
        .execute_admin_integrity_sql(abort_sql.as_str(), owner.clone())
        .expect("SQL Deep abort should freeze the job");
    let typed_abort = session
        .execute_admin_integrity(IntegrityCheckRequest::deep_abort(job_id), owner)
        .expect("typed Deep abort should replay the SQL receipt");
    assert_eq!(typed_abort, sql_abort);
}

#[test]
fn integrity_sql_fails_closed_before_controller_execution() {
    let session = sql_session();
    let owner = IntegrityJobOwner::new("tests::sql-rejection").expect("owner should admit");

    for sql in [
        "SELECT * FROM SessionSqlEntity",
        "CHECK INTEGRITY MissingEntity QUICK",
    ] {
        assert!(
            matches!(
                session.execute_admin_integrity_sql(sql, owner.clone()),
                Err(SqlIntegrityError::Sql(_)),
            ),
            "grammar and entity-resolution failures should stay SQL-owned: {sql}",
        );
    }

    let malformed_job = "CHECK INTEGRITY DEEP CONTINUE 'not-a-current-job-id' AFTER 0";
    assert!(matches!(
        session.execute_admin_integrity_sql(malformed_job, owner),
        Err(SqlIntegrityError::Integrity(
            crate::db::integrity::IntegrityDeepError::Job(IntegrityJobError::InvalidJobId)
        )),
    ));
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
        .continue_deep_integrity_for_tests(job_id, &owner, 0)
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
            .continue_deep_integrity_for_tests(start.job_id(), &owner, sequence)
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

    let IntegrityCheckResult::Deep(pending) = session
        .execute_admin_integrity(IntegrityCheckRequest::deep_abort(job_id), owner.clone())
        .expect("idle job should become abort-pending")
    else {
        panic!("Deep abort should return a Deep receipt");
    };
    assert!(matches!(
        pending,
        IntegrityJobReceipt::Abort(ref receipt)
            if receipt.page_sequence() == 0
                && receipt.status()
                    == &IntegrityAbortStatus::TerminationPending(
                        IntegrityPendingTerminal::Aborted
                    )
    ));
    let IntegrityCheckResult::Deep(replayed_pending) = session
        .execute_admin_integrity(IntegrityCheckRequest::deep_abort(job_id), owner.clone())
        .expect("pending abort should replay")
    else {
        panic!("Deep abort replay should return a Deep receipt");
    };
    assert_eq!(replayed_pending, pending);

    let terminal = session
        .continue_deep_integrity_for_tests(job_id, &owner, 0)
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
        .continue_deep_integrity_for_tests(start.job_id(), &wrong_owner, 0)
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
        .continue_deep_integrity_for_tests(job_id, &owner, 0)
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
        .continue_deep_integrity_for_tests(job_id, &owner, 1)
        .expect("terminal acknowledgement should persist");
    assert_eq!(acknowledged, terminal);
    let deleted = run_integrity_retention_page_for_tests::<SessionSqlCanister>(None, u64::MAX)
        .expect("acknowledged terminal receipt should become retention-eligible");
    assert_eq!(deleted.jobs_deleted(), 1);

    let error = session
        .continue_deep_integrity_for_tests(job_id, &owner, 1)
        .expect_err("deleted progress must not resurrect");
    assert!(matches!(
        error,
        crate::db::integrity::IntegrityDeepError::Job(IntegrityJobError::JobNotFound)
    ));
}

#[test]
fn deep_retention_maintenance_rotates_across_the_bounded_progress_keyspace() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    reset_integrity_retention_cursor_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    for ordinal in 0..17 {
        let owner = IntegrityJobOwner::new(format!("tests::retention-owner-{}", ordinal / 8))
            .expect("owner should admit");
        session
            .start_deep_integrity_for_entity(
                SessionSqlEntity::PATH,
                owner,
                IntegritySubmissionKey::new(format!("retention-job-{ordinal}"))
                    .expect("submission should admit"),
            )
            .expect("the bounded global capacity should admit the fixture");
    }

    let first = run_next_integrity_retention_page_for_tests::<SessionSqlCanister>(u64::MAX)
        .expect("first maintained page should succeed");
    assert_eq!(first.jobs_scanned(), 16);
    assert_eq!(first.jobs_expired(), 16);
    assert!(!first.exhausted());

    let second = run_next_integrity_retention_page_for_tests::<SessionSqlCanister>(u64::MAX)
        .expect("second maintained page should reach the suffix");
    assert_eq!(second.jobs_scanned(), 1);
    assert_eq!(second.jobs_expired(), 1);
    assert!(second.exhausted());

    let wrapped = run_next_integrity_retention_page_for_tests::<SessionSqlCanister>(u64::MAX)
        .expect("exhaustion should wrap the advisory cursor");
    assert_eq!(wrapped.jobs_scanned(), 16);
    assert_eq!(wrapped.jobs_expired(), 0);
    assert!(!wrapped.exhausted());
}

#[test]
fn typed_integrity_requests_drive_retention_after_the_requested_operation() {
    reset_session_sql_store();
    clear_progress_store_for_tests::<SessionSqlCanister>();
    reset_integrity_retention_cursor_for_tests::<SessionSqlCanister>();
    let session = sql_session();
    let expired_owner =
        IntegrityJobOwner::new("tests::retention-expired").expect("owner should admit");
    let start = session
        .start_deep_integrity_for_entity(
            SessionSqlEntity::PATH,
            expired_owner.clone(),
            IntegritySubmissionKey::new("retention-expired-job").expect("submission should admit"),
        )
        .expect("Deep fixture should persist");
    set_progress_job_lease_deadline_for_tests::<SessionSqlCanister>(start.job_id(), 1)
        .expect("test should make the retained job maintenance-eligible");

    let quick_owner =
        IntegrityJobOwner::new("tests::retention-trigger").expect("owner should admit");
    let quick = session
        .execute_admin_integrity(
            IntegrityCheckRequest::quick::<SessionSqlEntity>(),
            quick_owner,
        )
        .expect("Quick should execute before bounded maintenance");
    assert!(matches!(quick, IntegrityCheckResult::Quick(_)));

    let abort = session
        .execute_admin_integrity(
            IntegrityCheckRequest::DeepAbort {
                job_id: start.job_id(),
            },
            expired_owner,
        )
        .expect("abort should observe the expiry frozen by prior maintenance");
    assert!(matches!(
        abort,
        IntegrityCheckResult::Deep(IntegrityJobReceipt::Abort(ref receipt))
            if receipt.status()
                == &IntegrityAbortStatus::TerminationPending(
                    IntegrityPendingTerminal::Expired
                )
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
        .continue_deep_integrity_for_tests(start.job_id(), &owner, 0)
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
