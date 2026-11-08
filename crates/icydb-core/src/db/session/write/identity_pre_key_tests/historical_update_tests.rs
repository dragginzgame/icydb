//! Resumable backfills compare historical values through Forward and Verify.

use super::{
    DbSession, ENTITY_TAG, JOURNALED_STORE_PATH, JournaledTestCanister,
    drive_journaled_recovery_to_completion, initialize_journaled, insert_exact_key_fixture,
};
use crate::{
    db::{
        MutationJobAdvanceRequest, MutationJobId, MutationJobIdempotencyKey, MutationJobPhase,
        MutationJobStatus, SqlStatementResult, commit::forget_recovered_domain_for_tests,
        data::DecodedDataStoreKey,
    },
    value::{OutputValue, Value},
};

fn row_bytes(session: &DbSession<JournaledTestCanister>, id: u64) -> Vec<u8> {
    let key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(id))
        .unwrap()
        .to_raw()
        .unwrap();
    session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .unwrap()
        .with_data(|data| data.get(&key).unwrap().as_bytes().to_vec())
}

fn historical_backfill(default: &str, target: &str, expected: Value, changed: bool) {
    let session = initialize_journaled();
    for payload in [10, 20, 30] {
        insert_exact_key_fixture(&session, payload);
    }
    session.execute_admin_sql_ddl(&format!(
        "ALTER TABLE IdentityRow ADD COLUMN score nat64 {default} EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
    )).unwrap();
    // Future insertion policy must not determine the value of historical rows.
    session.execute_admin_sql_ddl(
        "ALTER TABLE IdentityRow ALTER COLUMN score SET DEFAULT 99 EXPECT SCHEMA VERSION 2 SET SCHEMA VERSION 3",
    ).unwrap();
    let before = [
        row_bytes(&session, 1),
        row_bytes(&session, 2),
        row_bytes(&session, 3),
    ];
    let job_id = MutationJobId::try_from_bytes([0xc1; 32]).unwrap();
    let mut state = session
        .start_trusted_sql_mutation_job(
            job_id,
            &format!("UPDATE IdentityRow SET score = {target} WHERE payload < 30"),
        )
        .unwrap();
    let mut verified = false;
    for _ in 0..8 {
        let request = MutationJobAdvanceRequest::new(
            job_id,
            state.sequence,
            MutationJobIdempotencyKey::new(format!("historical-{}", state.sequence)).unwrap(),
        );
        let receipt = session.advance_trusted_mutation_job(&request).unwrap();
        assert_eq!(
            session.advance_trusted_mutation_job(&request).unwrap(),
            receipt
        );
        assert!(matches!(
            receipt.status,
            MutationJobStatus::Active | MutationJobStatus::Completed
        ));
        if receipt.phase == MutationJobPhase::Verify && !verified {
            verified = true;
            // Reopen between phases; Verify must read durable historical rows
            // when the target was already satisfied and no rewrite occurred.
            forget_recovered_domain_for_tests(&session.db).unwrap();
            drive_journaled_recovery_to_completion(&session);
        }
        state = session.mutation_job_state(job_id).unwrap();
        if state.status == MutationJobStatus::Completed {
            break;
        }
    }
    assert!(verified);
    assert_eq!(state.status, MutationJobStatus::Completed);
    assert_eq!(state.rows_updated_total, if changed { 2 } else { 0 });
    assert_eq!(state.verify_restarts_total, 0);
    assert_eq!(
        row_bytes(&session, 3),
        before[2],
        "out-of-scope row must not change"
    );
    for (index, id) in [1, 2].into_iter().enumerate() {
        assert_eq!(row_bytes(&session, id) != before[index], changed);
    }
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(
            "SELECT score FROM IdentityRow WHERE payload < 30 ORDER BY id ASC",
        )
        .unwrap()
    else {
        panic!("projection expected");
    };
    let expected = match expected {
        Value::Null => OutputValue::null(),
        Value::Nat64(value) => OutputValue::nat64(value),
        _ => panic!("numeric fixture"),
    };
    assert_eq!(rows, vec![vec![expected.clone()], vec![expected]]);
}

#[test]
fn historical_update_backfills_null_fields() {
    historical_backfill("", "7", Value::Nat64(7), true);
}

#[test]
fn historical_update_backfills_frozen_defaults() {
    historical_backfill("DEFAULT 7", "9", Value::Nat64(9), true);
}

#[test]
fn historical_update_verifies_matching_defaults_without_rewriting() {
    historical_backfill("DEFAULT 7", "7", Value::Nat64(7), false);
}

#[test]
fn historical_update_verifies_matching_nulls_without_rewriting() {
    historical_backfill("", "NULL", Value::Null, false);
}
