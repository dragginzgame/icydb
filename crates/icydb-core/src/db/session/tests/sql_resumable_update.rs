//! Trusted resumable SQL update preparation boundary tests.

use super::*;
use crate::db::commit::{
    CommitFailpoint, CommitFailpointMode, arm_commit_failpoint_for_tests,
    clear_commit_failpoint_for_tests,
};

fn prepare_journaled(
    session: &DbSession<SessionSqlCanister>,
    operation_id: u128,
    sql: &str,
) -> Result<crate::db::TrustedResumableUpdateContinuation, QueryError> {
    session.prepare_trusted_sql_resumable_update::<JournaledSessionSqlEntity>(
        Ulid::from_u128(operation_id),
        sql,
    )
}

fn resume_journaled(
    session: &DbSession<SessionSqlCanister>,
    operation_id: u128,
    sql: &str,
    continuation: &crate::db::TrustedResumableUpdateContinuation,
) -> Result<crate::db::TrustedResumableUpdateReceipt, QueryError> {
    session.resume_trusted_sql_resumable_update::<JournaledSessionSqlEntity>(
        Ulid::from_u128(operation_id),
        sql,
        continuation,
    )
}

fn journaled_accepted_schema_snapshot() -> PersistedSchemaSnapshot {
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow(|store| {
        store
            .current_accepted_persisted_snapshot(JournaledSessionSqlEntity::ENTITY_TAG)
            .expect("journaled resumable schema should remain readable")
            .expect("journaled resumable schema should be published")
    })
}

fn mutate_continuation_payload(
    continuation: &crate::db::TrustedResumableUpdateContinuation,
    mutate: impl FnOnce(&mut [u8]),
) -> Vec<u8> {
    let mut bytes = continuation.as_bytes().to_vec();
    let payload_len = bytes.len() - size_of::<u32>();
    mutate(&mut bytes[..payload_len]);
    let checksum = crate::db::database_format::crc32c(&bytes[..payload_len]);
    bytes[payload_len..].copy_from_slice(&checksum.to_be_bytes());

    bytes
}

fn insert_raw_journaled_row_without_revision(entity: &JournaledSessionSqlEntity) {
    let key = DecodedDataStoreKey::try_new::<JournaledSessionSqlEntity>(entity.id)
        .expect("raw resumable fixture key should build")
        .to_raw()
        .expect("raw resumable fixture key should encode");
    let row = canonical_row_from_entity_for_model_proposal_for_test(entity)
        .expect("raw resumable fixture row should encode")
        .into_raw_row();

    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow_mut(|store| {
        let _ = store.insert_raw_for_test(key, row);
    });
}

fn journaled_raw_row_for_test(id: u64) -> crate::db::data::RawRow {
    let key = DecodedDataStoreKey::try_new::<JournaledSessionSqlEntity>(id)
        .expect("resumable fixture key should build")
        .to_raw()
        .expect("resumable fixture key should encode");
    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(|store| {
        store
            .get(&key)
            .expect("resumable fixture row should remain present")
    })
}

fn journaled_index_entries_for_test() -> Vec<(
    crate::db::index::RawIndexStoreKey,
    crate::db::index::IndexEntryValue,
)> {
    JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(|store| {
        let mut entries = Vec::new();
        store
            .visit_entries(|key, value| {
                entries.push((key.clone(), value.clone()));
                Ok::<_, ()>(IndexStoreVisit::Continue)
            })
            .expect("infallible resumable fixture index visitor should complete");
        entries
    })
}

#[test]
fn trusted_resumable_update_prepare_is_deterministic_and_read_only() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("journaled resumable prepare fixture insert should succeed");
    let before_rows = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT id, name, age FROM JournaledSessionSqlEntity ORDER BY id ASC",
    )
    .expect("baseline resumable prepare rows should load");
    let before_data_len = JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(DataStore::len);
    let before_index_len = JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(IndexStore::len);
    let before_journal_len = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len);
    let before_schema = journaled_accepted_schema_snapshot();
    assert!(
        !crate::db::commit::commit_marker_present()
            .expect("resumable prepare baseline marker state should be readable"),
    );

    let (prepared, rows_scanned) =
        capture_rows_scanned_for_entity(JournaledSessionSqlEntity::PATH, || {
            prepare_journaled(
                &session,
                0x210_0001,
                "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21",
            )
        });
    let prepared = prepared.expect("eligible resumable update should prepare");
    let repeated = prepare_journaled(
        &session,
        0x210_0001,
        "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21",
    )
    .expect("identical resumable update should prepare deterministically");

    assert_eq!(prepared, repeated);
    assert_eq!(prepared.as_bytes().len(), 156);
    assert_eq!(&prepared.as_bytes()[..4], b"ICYU");
    assert_eq!(prepared.as_bytes()[4], 1);
    let (payload, checksum) = prepared.as_bytes().split_at(prepared.as_bytes().len() - 4);
    assert_eq!(
        u32::from_be_bytes(
            checksum
                .try_into()
                .expect("resumable continuation checksum is exactly four bytes"),
        ),
        crate::db::database_format::crc32c(payload),
    );
    assert_eq!(rows_scanned, 0, "prepare must not scan target rows");
    assert_eq!(
        statement_projection_rows::<JournaledSessionSqlEntity>(
            &session,
            "SELECT id, name, age FROM JournaledSessionSqlEntity ORDER BY id ASC",
        )
        .expect("rows should remain readable after prepare"),
        before_rows,
    );
    assert_eq!(
        JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(DataStore::len),
        before_data_len,
    );
    assert_eq!(
        JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow(IndexStore::len),
        before_index_len,
    );
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        before_journal_len,
    );
    assert_eq!(journaled_accepted_schema_snapshot(), before_schema);
    assert!(
        !crate::db::commit::commit_marker_present()
            .expect("resumable prepare final marker state should be readable"),
    );
}

#[test]
fn trusted_resumable_update_token_binds_operation_scope_and_fixed_patch() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();

    let baseline = prepare_journaled(
        &session,
        0x210_0010,
        "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21",
    )
    .expect("baseline resumable update should prepare");
    let other_operation = prepare_journaled(
        &session,
        0x210_0011,
        "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21",
    )
    .expect("alternate operation identity should prepare");
    let other_scope = prepare_journaled(
        &session,
        0x210_0010,
        "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 20",
    )
    .expect("alternate scope should prepare");
    let other_patch = prepare_journaled(
        &session,
        0x210_0010,
        "UPDATE JournaledSessionSqlEntity SET name = 'Changed' WHERE age = 21",
    )
    .expect("alternate patch should prepare");

    assert_ne!(baseline, other_operation);
    assert_ne!(baseline, other_scope);
    assert_ne!(baseline, other_patch);
}

#[test]
fn trusted_resumable_update_prepare_freezes_explicit_default() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity ADD COLUMN score nat64 NOT NULL DEFAULT 7",
                1,
            ),
        )
        .expect("resumable default fixture DDL should publish");

    let explicit_default = prepare_journaled(
        &session,
        0x210_0020,
        "UPDATE JournaledSessionSqlEntity SET score = DEFAULT WHERE age = 21",
    )
    .expect("fixed explicit DEFAULT should prepare");
    let explicit_literal = prepare_journaled(
        &session,
        0x210_0020,
        "UPDATE JournaledSessionSqlEntity SET score = 7 WHERE age = 21",
    )
    .expect("equivalent accepted literal should prepare");

    assert_eq!(
        explicit_default, explicit_literal,
        "continuation identity should bind the resolved accepted payload, not SQL spelling",
    );
}

#[test]
fn trusted_resumable_update_prepare_rejects_heap_window_and_returning() {
    reset_heap_session_sql_store();
    let heap = heap_sql_session();
    let err = heap
        .prepare_trusted_sql_resumable_update::<HeapSessionSqlEntity>(
            Ulid::from_u128(0x210_0030),
            "UPDATE HeapSessionSqlEntity SET name = 'Updated' WHERE age = 21",
        )
        .expect_err("heap resumable update must reject");
    assert_sql_write_boundary_detail(
        err,
        SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore,
    );

    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    for (sql, boundary) in [
        (
            "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21 LIMIT 1",
            SqlWriteBoundaryCode::ResumableUpdateWindowUnsupported,
        ),
        (
            "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21 RETURNING id",
            SqlWriteBoundaryCode::ResumableUpdateReturningUnsupported,
        ),
    ] {
        let err = prepare_journaled(&session, 0x210_0031, sql)
            .expect_err("unsupported resumable shape must reject");
        assert_sql_write_boundary_detail(err, boundary);
    }
}

#[test]
fn trusted_resumable_update_prepare_rejects_entity_mismatch() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();

    let err = prepare_journaled(
        &session,
        0x210_0032,
        "UPDATE SessionSqlWriteEntity SET name = 'Updated' WHERE age = 21",
    )
    .expect_err("resumable SQL target must match its typed entity");

    assert_sql_lowering_detail(err, SqlLoweringCode::EntityMismatch);
}

#[test]
fn trusted_resumable_update_prepare_rejects_database_owned_assignments() {
    reset_journaled_session_sql_store();
    let journaled = journaled_sql_session();
    let primary_key_err = prepare_journaled(
        &journaled,
        0x210_0033,
        "UPDATE JournaledSessionSqlEntity SET id = 2 WHERE age = 21",
    )
    .expect_err("resumable primary-key assignment must reject");
    assert_sql_write_boundary_detail(
        primary_key_err,
        SqlWriteBoundaryCode::UpdatePrimaryKeyMutation,
    );

    reset_session_sql_store();
    let session = sql_session();
    let generated_err = session
        .prepare_trusted_sql_resumable_update::<SessionSqlGeneratedTimestampEntity>(
            Ulid::from_u128(0x210_0034),
            "UPDATE SessionSqlGeneratedTimestampEntity \
             SET created_on_insert = 7 WHERE id = 1",
        )
        .expect_err("resumable generated-field assignment must reject");
    assert_sql_write_boundary_detail(generated_err, SqlWriteBoundaryCode::ExplicitGeneratedField);

    let managed_err = session
        .prepare_trusted_sql_resumable_update::<SessionSqlManagedWriteEntity>(
            Ulid::from_u128(0x210_0035),
            "UPDATE SessionSqlManagedWriteEntity SET updated_at = 0 WHERE id = 1",
        )
        .expect_err("resumable managed-field assignment must reject");
    assert_sql_write_boundary_detail(managed_err, SqlWriteBoundaryCode::ExplicitManagedField);
}

#[test]
fn trusted_resumable_update_prepare_rejects_scope_dependent_patch() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();

    let err = prepare_journaled(
        &session,
        0x210_0040,
        "UPDATE JournaledSessionSqlEntity SET age = 22 WHERE age = 21",
    )
    .expect_err("scope-dependent resumable patch must reject");

    assert_sql_write_boundary_detail(
        err,
        SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField,
    );
}

#[test]
fn trusted_resumable_update_prepare_rejects_unique_and_relation_targets() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "CREATE UNIQUE INDEX journaled_age_unique_idx ON JournaledSessionSqlEntity (age)",
                1,
            ),
        )
        .expect("resumable unique-index fixture DDL should publish an activation");
    for expected_status in [
        SqlDdlExecutionStatus::ValidationStarted,
        SqlDdlExecutionStatus::ValidationAdvanced,
        SqlDdlExecutionStatus::Validated,
    ] {
        let SqlStatementResult::Ddl(report) = session
            .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
                "ALTER TABLE JournaledSessionSqlEntity \
                 VALIDATE CONSTRAINT journaled_age_unique_idx",
            )
            .expect("resumable unique-index fixture validation should advance")
        else {
            panic!("VALIDATE CONSTRAINT should return a DDL report");
        };
        assert_eq!(report.execution_status(), expected_status);
    }
    let unique_err = prepare_journaled(
        &session,
        0x210_0050,
        "UPDATE JournaledSessionSqlEntity SET age = 22 WHERE name = 'Ada'",
    )
    .expect_err("unique-index resumable target must reject");
    assert_sql_write_boundary_detail(
        unique_err,
        SqlWriteBoundaryCode::ResumableUpdateAssignedFieldHasGlobalConstraint,
    );

    reset_mixed_journaled_relation_stores();
    let relation_session = mixed_journaled_relation_sql_session();
    let relation_err = relation_session
        .prepare_trusted_sql_resumable_update::<DurableSessionSqlSourceToJournaledTargetEntity>(
            Ulid::from_u128(0x210_0051),
            "UPDATE DurableSessionSqlSourceToJournaledTargetEntity \
             SET target_id = 2 WHERE id = 1",
        )
        .expect_err("relation resumable target must reject");
    assert_sql_write_boundary_detail(
        relation_err,
        SqlWriteBoundaryCode::ResumableUpdateAssignedFieldHasGlobalConstraint,
    );
}

#[test]
fn trusted_resumable_update_forward_commits_one_batch_and_replays_old_token() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=70).map(|id| JournaledSessionSqlEntity {
            id,
            name: format!("row-{id}"),
            age: 21,
        }))
        .expect("resumable multi-batch fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1000, sql)
        .expect("resumable multi-batch update should prepare");
    let journal_len_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len);

    let first = resume_journaled(&session, 0x210_1000, sql, &initial)
        .expect("first resumable Forward batch should commit");
    assert_eq!(
        first.phase(),
        crate::db::TrustedResumableUpdatePhase::Forward
    );
    assert_eq!(first.keys_scanned(), 64);
    assert_eq!(first.rows_updated(), 64);
    assert!(!first.complete());
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        journal_len_before + 1,
        "one Forward call must append exactly one atomic journal batch",
    );

    let second = resume_journaled(
        &session,
        0x210_1000,
        sql,
        first
            .continuation()
            .expect("first 64-row batch should carry its exact checkpoint"),
    )
    .expect("the row deferred after the 64-row bound should remain reachable");
    assert_eq!(
        second.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(second.keys_scanned(), 6);
    assert_eq!(second.rows_updated(), 6);
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        journal_len_before + 2,
        "the residual rows must commit as one second atomic batch",
    );

    let replay = resume_journaled(&session, 0x210_1000, sql, &initial)
        .expect("response-loss replay of the original continuation should converge as no-work");
    assert_eq!(
        replay.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(replay.keys_scanned(), 70);
    assert_eq!(replay.rows_updated(), 0);
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        journal_len_before + 2,
        "replay must not reapply a managed or authored mutation to converged rows",
    );
    let rows = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity ORDER BY id ASC",
    )
    .expect("converged resumable rows should load");
    assert_eq!(rows.len(), 70);
    assert!(
        rows.iter()
            .all(|row| row == &[Value::Text("Updated".to_string())])
    );
}

#[test]
fn trusted_resumable_update_forward_advances_clean_pages_without_a_marker() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=300).map(|id| JournaledSessionSqlEntity {
            id,
            name: "Already".to_string(),
            age: 21,
        }))
        .expect("resumable clean-page fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Already' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1001, sql)
        .expect("resumable clean-page update should prepare");
    let journal_len_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len);

    let first = resume_journaled(&session, 0x210_1001, sql, &initial)
        .expect("first clean Forward page should advance");
    assert_eq!(
        first.phase(),
        crate::db::TrustedResumableUpdatePhase::Forward
    );
    assert_eq!(first.keys_scanned(), 256);
    assert_eq!(first.rows_updated(), 0);
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        journal_len_before,
        "clean Forward progress must not open a commit marker",
    );

    let second = resume_journaled(
        &session,
        0x210_1001,
        sql,
        first
            .continuation()
            .expect("in-progress Forward receipt should carry continuation"),
    )
    .expect("final clean Forward page should reach verification");
    assert_eq!(
        second.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(second.keys_scanned(), 44);
    assert_eq!(second.rows_updated(), 0);
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        journal_len_before,
    );
}

#[test]
fn trusted_resumable_update_resume_rebinds_scope_patch_and_batch_policy() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("resumable binding fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1002, sql)
        .expect("resumable binding update should prepare");

    let (operation_err, operation_rows_scanned) =
        capture_rows_scanned_for_entity(JournaledSessionSqlEntity::PATH, || {
            resume_journaled(&session, 0x210_1003, sql, &initial)
        });
    let operation_err = operation_err
        .expect_err("another application operation identity must reject before execution");
    assert_eq!(operation_rows_scanned, 0);
    assert_sql_write_boundary_detail(
        operation_err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationOperationMismatch,
    );

    let scope_err = resume_journaled(
        &session,
        0x210_1002,
        "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 20",
        &initial,
    )
    .expect_err("changed resumable scope must reject before execution");
    assert_sql_write_boundary_detail(
        scope_err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationScopeMismatch,
    );
    let patch_err = resume_journaled(
        &session,
        0x210_1002,
        "UPDATE JournaledSessionSqlEntity SET name = 'Changed' WHERE age = 21",
        &initial,
    )
    .expect_err("changed resumable patch must reject before execution");
    assert_sql_write_boundary_detail(
        patch_err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationPatchMismatch,
    );

    let mut wrong_policy = initial.as_bytes().to_vec();
    let payload_len = wrong_policy.len() - size_of::<u32>();
    wrong_policy[payload_len - 1] ^= 1;
    let checksum = crate::db::database_format::crc32c(&wrong_policy[..payload_len]);
    wrong_policy[payload_len..].copy_from_slice(&checksum.to_be_bytes());
    let wrong_policy = crate::db::TrustedResumableUpdateContinuation::try_from_bytes(wrong_policy)
        .expect("structurally valid alternate policy token should decode");
    let policy_err = resume_journaled(&session, 0x210_1002, sql, &wrong_policy)
        .expect_err("another engine batch policy must reject before execution");
    assert_sql_write_boundary_detail(
        policy_err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationBatchPolicyMismatch,
    );

    let rows = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity ORDER BY id ASC",
    )
    .expect("binding rejection fixture should remain readable");
    assert_eq!(rows, vec![vec![Value::Text("Ada".to_string())]]);
}

#[test]
fn trusted_resumable_update_revision_exhaustion_rejects_before_marker_without_state_change() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("revision-exhaustion fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1008, sql)
        .expect("revision-exhaustion fixture should prepare");

    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow_mut(|journal| {
        journal
            .persist_fold_watermark(FoldWatermark::new(JournalSequence::new(u64::MAX - 1), 1))
            .expect("near-exhausted revision fixture should persist");
    });
    let raw_row_before = journaled_raw_row_for_test(1);
    let indexes_before = journaled_index_entries_for_test();
    let data_len_before = JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(DataStore::len);
    let journal_len_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len);
    let watermark_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
        journal
            .fold_watermark()
            .expect("revision-exhaustion watermark should remain readable")
    });
    assert!(
        !crate::db::commit::commit_marker_present()
            .expect("revision-exhaustion baseline marker state should be readable"),
    );

    let error = resume_journaled(&session, 0x210_1008, sql, &initial)
        .expect_err("a mutation must reject before consuming the final journal sequence");

    assert_eq!(
        error.diagnostic().code(),
        DiagnosticCode::RuntimeUnsupported
    );
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::JournalMutationRevisionExhausted,
        }),
    );
    assert_eq!(journaled_raw_row_for_test(1), raw_row_before);
    assert_eq!(journaled_index_entries_for_test(), indexes_before);
    assert_eq!(
        JOURNALED_SESSION_SQL_DATA_STORE.with_borrow(DataStore::len),
        data_len_before,
    );
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        journal_len_before,
    );
    assert_eq!(
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
            journal
                .fold_watermark()
                .expect("revision-exhaustion final watermark should remain readable")
        }),
        watermark_before,
    );
    assert!(
        !crate::db::commit::commit_marker_present()
            .expect("revision-exhaustion final marker state should be readable"),
    );
}

#[test]
fn trusted_resumable_update_resume_rejects_changed_accepted_schema_before_row_access() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1006, sql)
        .expect("resumable schema-binding fixture should prepare");

    session
        .execute_admin_sql_ddl::<JournaledSessionSqlEntity>(
            &super::sql_surface::ddl_transition_sql(
                "ALTER TABLE JournaledSessionSqlEntity \
                 ADD COLUMN score nat64 NOT NULL DEFAULT 7",
                1,
            ),
        )
        .expect("schema-binding fixture DDL should publish");
    let (result, rows_scanned) =
        capture_rows_scanned_for_entity(JournaledSessionSqlEntity::PATH, || {
            resume_journaled(&session, 0x210_1006, sql, &initial)
        });
    let err = result.expect_err("old accepted-schema continuation must reject");

    assert_sql_write_boundary_detail(
        err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationSchemaMismatch,
    );
    assert_eq!(
        rows_scanned, 0,
        "schema mismatch must reject before row access"
    );
}

#[test]
fn trusted_resumable_update_continuation_decode_is_bounded_and_fail_closed() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    let initial = prepare_journaled(
        &session,
        0x210_1003,
        "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21",
    )
    .expect("resumable continuation fixture should prepare");

    let round_trip =
        crate::db::TrustedResumableUpdateContinuation::try_from_bytes(initial.as_bytes().to_vec())
            .expect("current resumable continuation should round-trip");
    assert_eq!(round_trip, initial);

    for malformed in [
        Vec::new(),
        initial.as_bytes()[..initial.as_bytes().len() - 1].to_vec(),
        vec![0; 2 * 1024 + 1],
    ] {
        let err = crate::db::TrustedResumableUpdateContinuation::try_from_bytes(malformed)
            .expect_err("malformed resumable continuation must reject");
        assert_sql_write_boundary_detail(
            err,
            SqlWriteBoundaryCode::ResumableUpdateContinuationMalformed,
        );
    }

    let future_format = mutate_continuation_payload(&initial, |payload| payload[4] = 2);
    let err = crate::db::TrustedResumableUpdateContinuation::try_from_bytes(future_format)
        .expect_err("future resumable continuation format must reject");
    assert_sql_write_boundary_detail(
        err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationMalformed,
    );
}

#[test]
fn trusted_resumable_update_resume_rejects_another_target_identity() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1007, sql)
        .expect("target-binding resumable update should prepare");
    let target_identity_offset = 4 + 1 + 16 + size_of::<u64>();
    let wrong_target = mutate_continuation_payload(&initial, |payload| {
        payload[target_identity_offset] ^= 1;
    });
    let wrong_target = crate::db::TrustedResumableUpdateContinuation::try_from_bytes(wrong_target)
        .expect("structurally valid alternate target token should decode");
    let (result, rows_scanned) =
        capture_rows_scanned_for_entity(JournaledSessionSqlEntity::PATH, || {
            resume_journaled(&session, 0x210_1007, sql, &wrong_target)
        });
    let err = result.expect_err("another resumable target identity must reject");

    assert_sql_write_boundary_detail(
        err,
        SqlWriteBoundaryCode::ResumableUpdateContinuationTargetMismatch,
    );
    assert_eq!(rows_scanned, 0, "target mismatch must precede row access");
}

#[test]
fn trusted_resumable_update_forward_retries_before_marker_persistence() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=2).map(|id| JournaledSessionSqlEntity {
            id,
            name: format!("row-{id}"),
            age: 21,
        }))
        .expect("resumable interruption fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1004, sql)
        .expect("resumable interruption fixture should prepare");

    arm_commit_failpoint_for_tests(
        CommitFailpoint::BeforeMarkerWrite,
        CommitFailpointMode::ReturnError,
    );
    resume_journaled(&session, 0x210_1004, sql, &initial)
        .expect_err("pre-marker interruption must reject without mutation authority");
    clear_commit_failpoint_for_tests();
    assert!(
        !crate::db::commit::commit_marker_present()
            .expect("pre-marker interruption state should remain readable"),
    );
    let before_retry = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity ORDER BY id ASC",
    )
    .expect("pre-marker rows should remain readable");
    assert_eq!(
        before_retry,
        vec![
            vec![Value::Text("row-1".to_string())],
            vec![Value::Text("row-2".to_string())],
        ],
    );

    let replay = resume_journaled(&session, 0x210_1004, sql, &initial)
        .expect("the same pre-marker continuation should remain retryable");
    assert_eq!(
        replay.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(replay.keys_scanned(), 2);
    assert_eq!(replay.rows_updated(), 2);
}

#[test]
fn trusted_resumable_update_forward_recovers_marker_authorized_batch_before_replay() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=64).map(|id| JournaledSessionSqlEntity {
            id,
            name: format!("row-{id}"),
            age: 21,
        }))
        .expect("resumable recovery fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_1005, sql)
        .expect("resumable recovery fixture should prepare");
    let journal_sequence_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
        journal
            .next_append_sequence()
            .expect("baseline journal sequence should remain readable")
            .get()
    });

    arm_commit_failpoint_for_tests(
        CommitFailpoint::AfterMarkerWrite,
        CommitFailpointMode::ReturnError,
    );
    resume_journaled(&session, 0x210_1005, sql, &initial)
        .expect_err("post-marker interruption must return an unknown call outcome");
    clear_commit_failpoint_for_tests();
    assert!(
        crate::db::commit::commit_marker_present()
            .expect("post-marker interruption marker should remain readable"),
    );
    let (marker_control_bytes, journal_batch_bytes) =
        crate::db::commit::persisted_commit_marker_lengths_for_tests()
            .expect("post-marker encoded lengths should remain readable");
    assert!(marker_control_bytes > journal_batch_bytes);
    eprintln!(
        "resumable 64-row Forward marker/control={marker_control_bytes} bytes, journal batch={journal_batch_bytes} bytes"
    );

    let replay = resume_journaled(&session, 0x210_1005, sql, &initial)
        .expect("replay should recover the authorized batch before rescanning");
    assert_eq!(
        replay.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(replay.keys_scanned(), 64);
    assert_eq!(replay.rows_updated(), 0);
    assert!(
        !crate::db::commit::commit_marker_present()
            .expect("successful replay should leave no marker"),
    );
    let journal_sequence_after = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
        journal
            .next_append_sequence()
            .expect("recovered journal sequence should remain readable")
            .get()
    });
    assert_eq!(
        journal_sequence_after,
        journal_sequence_before + 1,
        "recovery must advance durable sequence authority exactly once",
    );
    let rows = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity ORDER BY id ASC",
    )
    .expect("recovered resumable rows should load");
    assert_eq!(rows.len(), 64);
    assert!(
        rows.iter()
            .all(|row| row == &[Value::Text("Updated".to_string())])
    );
}

#[test]
fn trusted_resumable_update_verify_completes_only_after_a_stable_full_sweep() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=300).map(|id| JournaledSessionSqlEntity {
            id,
            name: "Already".to_string(),
            age: 21,
        }))
        .expect("resumable verification fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Already' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_2000, sql)
        .expect("resumable verification fixture should prepare");

    let forward_page = resume_journaled(&session, 0x210_2000, sql, &initial)
        .expect("first clean Forward page should advance");
    let forward_final = resume_journaled(
        &session,
        0x210_2000,
        sql,
        forward_page
            .continuation()
            .expect("clean Forward page should remain in progress"),
    )
    .expect("second clean Forward page should enter Verify");
    assert_eq!(
        forward_final.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert!(!forward_final.complete());

    let verify_page = resume_journaled(
        &session,
        0x210_2000,
        sql,
        forward_final
            .continuation()
            .expect("new Verify sweep should carry continuation"),
    )
    .expect("first Verify page should remain bounded");
    assert_eq!(
        verify_page.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(verify_page.keys_scanned(), 256);
    assert_eq!(verify_page.rows_updated(), 0);
    assert_eq!(verify_page.restart_reason(), None);
    assert!(!verify_page.complete());

    let final_verify_token = verify_page
        .continuation()
        .expect("partial Verify sweep should carry continuation")
        .clone();
    let complete = resume_journaled(&session, 0x210_2000, sql, &final_verify_token)
        .expect("stable final Verify page should complete");
    assert_eq!(
        complete.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );
    assert_eq!(complete.keys_scanned(), 44);
    assert_eq!(complete.rows_updated(), 0);
    assert_eq!(complete.restart_reason(), None);
    assert!(complete.complete());
    assert!(complete.continuation().is_none());

    let replay = resume_journaled(&session, 0x210_2000, sql, &final_verify_token)
        .expect("replaying the final stable Verify token should remain complete");
    assert!(replay.complete());
    assert!(replay.continuation().is_none());
}

#[test]
fn trusted_resumable_update_verify_revision_change_restarts_from_forward_start() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=300).map(|id| JournaledSessionSqlEntity {
            id,
            name: "Already".to_string(),
            age: 21,
        }))
        .expect("resumable revision fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Already' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_2001, sql)
        .expect("resumable revision fixture should prepare");
    let forward_page = resume_journaled(&session, 0x210_2001, sql, &initial)
        .expect("first revision fixture Forward page should advance");
    let verify_start = resume_journaled(
        &session,
        0x210_2001,
        sql,
        forward_page
            .continuation()
            .expect("partial Forward progress should carry continuation"),
    )
    .expect("revision fixture should enter Verify");
    let verify_page = resume_journaled(
        &session,
        0x210_2001,
        sql,
        verify_start
            .continuation()
            .expect("Verify start should carry continuation"),
    )
    .expect("first revision fixture Verify page should advance");
    assert_eq!(verify_page.keys_scanned(), 256);

    session
        .insert(JournaledSessionSqlEntity {
            id: 0,
            name: "Needs-patch".to_string(),
            age: 21,
        })
        .expect("behind-checkpoint write should succeed");
    let restarted = resume_journaled(
        &session,
        0x210_2001,
        sql,
        verify_page
            .continuation()
            .expect("partial Verify sweep should carry continuation"),
    )
    .expect("changed durable revision should return bounded restart progress");
    assert_eq!(
        restarted.phase(),
        crate::db::TrustedResumableUpdatePhase::Forward
    );
    assert_eq!(restarted.keys_scanned(), 0);
    assert_eq!(restarted.rows_updated(), 0);
    assert_eq!(
        restarted.restart_reason(),
        Some(crate::db::TrustedResumableUpdateRestartReason::RevisionChanged),
    );
    assert!(!restarted.complete());

    let resumed_forward = resume_journaled(
        &session,
        0x210_2001,
        sql,
        restarted
            .continuation()
            .expect("revision restart should carry Forward-start continuation"),
    )
    .expect("restarted Forward scan should revisit behind-checkpoint rows");
    assert_eq!(resumed_forward.rows_updated(), 1);
    let row = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity WHERE id = 0",
    )
    .expect("behind-checkpoint row should remain readable");
    assert_eq!(row, vec![vec![Value::Text("Already".to_string())]]);
}

#[test]
fn trusted_resumable_update_verify_restarts_after_an_out_of_scope_write() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Already".to_string(),
            age: 21,
        })
        .expect("out-of-scope revision fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Already' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_2002, sql)
        .expect("out-of-scope revision update should prepare");
    let verify_start = resume_journaled(&session, 0x210_2002, sql, &initial)
        .expect("clean Forward scan should enter Verify");
    assert_eq!(
        verify_start.phase(),
        crate::db::TrustedResumableUpdatePhase::Verify
    );

    session
        .insert(JournaledSessionSqlEntity {
            id: 2,
            name: "Outside".to_string(),
            age: 99,
        })
        .expect("out-of-scope store write should succeed");
    let restarted = resume_journaled(
        &session,
        0x210_2002,
        sql,
        verify_start
            .continuation()
            .expect("Verify start should carry continuation"),
    )
    .expect("store-wide revision drift should return bounded restart progress");

    assert_eq!(
        restarted.phase(),
        crate::db::TrustedResumableUpdatePhase::Forward
    );
    assert_eq!(restarted.keys_scanned(), 0);
    assert_eq!(restarted.rows_updated(), 0);
    assert_eq!(
        restarted.restart_reason(),
        Some(crate::db::TrustedResumableUpdateRestartReason::RevisionChanged),
    );
}

#[test]
fn trusted_resumable_update_verify_preserves_revision_across_journal_fold() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Needs-patch".to_string(),
            age: 21,
        })
        .expect("journal-fold fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Updated' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_2003, sql)
        .expect("journal-fold resumable update should prepare");
    let verify_start = resume_journaled(&session, 0x210_2003, sql, &initial)
        .expect("one-row Forward batch should enter Verify");
    assert_eq!(verify_start.rows_updated(), 1);
    let revision_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
        journal
            .next_append_sequence()
            .expect("pre-fold revision should remain readable")
    });
    let watermark_before = JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
        journal
            .fold_watermark()
            .expect("pre-fold watermark should remain readable")
    });

    crate::db::commit::clear_recovery_runtime_state_for_tests(&JOURNALED_SESSION_SQL_DB)
        .expect("journal-fold fixture should reopen recovery");
    ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect("journal-fold fixture should fold through recovery");
    let (revision_after, watermark_after) =
        JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow(|journal| {
            (
                journal
                    .next_append_sequence()
                    .expect("post-fold revision should remain readable"),
                journal
                    .fold_watermark()
                    .expect("post-fold watermark should remain readable"),
            )
        });
    assert_eq!(revision_after, revision_before);
    assert!(
        watermark_after.highest_folded_journal_sequence()
            >= watermark_before.highest_folded_journal_sequence()
    );

    let complete = resume_journaled(
        &session,
        0x210_2003,
        sql,
        verify_start
            .continuation()
            .expect("post-Forward Verify should carry continuation"),
    )
    .expect("journal folding alone must preserve verification stability");
    assert!(complete.complete());
    assert_eq!(complete.restart_reason(), None);
}

#[test]
fn trusted_resumable_update_verify_detects_residual_work_without_revision_drift() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert(JournaledSessionSqlEntity {
            id: 1,
            name: "Already".to_string(),
            age: 21,
        })
        .expect("residual-work fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity SET name = 'Already' WHERE age = 21";
    let initial = prepare_journaled(&session, 0x210_2004, sql)
        .expect("residual-work resumable update should prepare");
    let verify_start = resume_journaled(&session, 0x210_2004, sql, &initial)
        .expect("clean Forward scan should enter Verify");

    insert_raw_journaled_row_without_revision(&JournaledSessionSqlEntity {
        id: 1,
        name: "Bypassed".to_string(),
        age: 21,
    });
    let restarted = resume_journaled(
        &session,
        0x210_2004,
        sql,
        verify_start
            .continuation()
            .expect("residual-work Verify should carry continuation"),
    )
    .expect("residual work must restart rather than report false completion");

    assert_eq!(
        restarted.phase(),
        crate::db::TrustedResumableUpdatePhase::Forward
    );
    assert_eq!(restarted.keys_scanned(), 1);
    assert_eq!(restarted.rows_updated(), 0);
    assert_eq!(
        restarted.restart_reason(),
        Some(crate::db::TrustedResumableUpdateRestartReason::ResidualWork),
    );
    assert!(!restarted.complete());
}

#[test]
fn trusted_resumable_update_toko_shaped_tier_reset_converges_only_its_scope() {
    reset_journaled_session_sql_store();
    let session = journaled_sql_session();
    session
        .insert_many_atomic((1..=140).map(|id| {
            JournaledSessionSqlEntity {
                id,
                name: if id <= 130 {
                    if id % 2 == 0 {
                        "default-tier"
                    } else {
                        "custom-tier"
                    }
                } else {
                    "outside-collection"
                }
                .to_string(),
                age: if id <= 130 { 7 } else { 8 },
            }
        }))
        .expect("Toko-shaped tier fixture insert should succeed");
    let sql = "UPDATE JournaledSessionSqlEntity \
               SET name = 'default-tier' WHERE age = 7";
    let mut continuation = prepare_journaled(&session, 0x210_3000, sql)
        .expect("Toko-shaped tier reset should prepare");
    let mut rows_updated = 0_u32;
    let mut complete = false;

    for _ in 0..8 {
        let receipt = resume_journaled(&session, 0x210_3000, sql, &continuation)
            .expect("Toko-shaped tier reset step should succeed");
        rows_updated = rows_updated.saturating_add(receipt.rows_updated());
        if receipt.complete() {
            complete = true;
            break;
        }
        continuation = receipt
            .into_continuation()
            .expect("in-progress tier reset should carry continuation");
    }

    assert!(complete, "Toko-shaped tier reset should verify completely");
    assert_eq!(rows_updated, 65);
    let target_rows = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity WHERE age = 7 ORDER BY id ASC",
    )
    .expect("Toko-shaped target rows should load");
    assert_eq!(target_rows.len(), 130);
    assert!(
        target_rows
            .iter()
            .all(|row| row == &[Value::Text("default-tier".to_string())])
    );
    let outside_rows = statement_projection_rows::<JournaledSessionSqlEntity>(
        &session,
        "SELECT name FROM JournaledSessionSqlEntity WHERE age = 8 ORDER BY id ASC",
    )
    .expect("Toko-shaped out-of-scope rows should load");
    assert_eq!(outside_rows.len(), 10);
    assert!(
        outside_rows
            .iter()
            .all(|row| row == &[Value::Text("outside-collection".to_string())])
    );
}
