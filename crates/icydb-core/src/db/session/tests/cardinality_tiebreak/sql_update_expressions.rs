//! Scalar SET expressions use original rows and preserve atomic write admission.

use super::*;
use crate::db::{MutationJobError, MutationJobId, session::sql::sql_statement_dispatch};

#[test]
fn sql_update_expressions_copy_increment_and_swap_original_values() {
    let session = sql_not_null::initialize_nullable_rows();
    let before = projection_rows(
        &session,
        "SELECT id, status, peer, qty, other FROM PlannerRow ORDER BY id",
    );
    session.execute_trusted_sql_exact_update(
        "UPDATE PlannerRow AS p SET p.status = p.peer, p.peer = p.status, p.qty = p.other, p.other = p.qty + 1 WHERE p.id >= 0", 6,
    ).unwrap();
    let expected = before
        .into_iter()
        .map(|row| {
            let increment = match row[3].as_public() {
                crate::value::PublicValue::Nat64(value) => OutputValue::nat64(value + 1),
                crate::value::PublicValue::Null => OutputValue::null(),
                value => panic!("unexpected numeric input {value:?}"),
            };
            vec![
                row[0].clone(),
                row[2].clone(),
                row[1].clone(),
                row[4].clone(),
                increment,
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        projection_rows(
            &session,
            "SELECT id, status, peer, qty, other FROM PlannerRow ORDER BY id"
        ),
        expected
    );
}

#[test]
fn sql_update_expressions_public_lanes_use_the_same_evaluator_and_returning() {
    let session = sql_not_null::initialize_nullable_rows();
    let dispatch = sql_statement_dispatch(
        "UPDATE PlannerRow SET qty = qty + 1, status = peer WHERE id = 2 RETURNING id, qty, status",
    )
    .unwrap();
    let result = session
        .execute_sql_public_primary_key_update(&dispatch)
        .unwrap();
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("expected RETURNING");
    };
    assert_eq!(
        rows,
        vec![vec![
            OutputValue::nat64(2),
            OutputValue::nat64(3),
            OutputValue::text("closed".into())
        ]]
    );
    let dispatch = sql_statement_dispatch("UPDATE PlannerRow SET qty = COALESCE(qty, 0) + 1 WHERE id >= 3 ORDER BY id ASC LIMIT 2 RETURNING id, qty").unwrap();
    let result = session
        .execute_sql_public_bounded_update(&dispatch)
        .unwrap();
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("expected RETURNING");
    };
    assert_eq!(
        rows,
        vec![
            vec![OutputValue::nat64(3), OutputValue::nat64(1)],
            vec![OutputValue::nat64(4), OutputValue::nat64(4)]
        ]
    );
    assert_eq!(
        projection_rows(&session, "SELECT qty FROM PlannerRow WHERE id = 5"),
        vec![vec![OutputValue::nat64(0)]]
    );
}

#[test]
fn sql_update_expressions_reject_invalid_results_without_partial_writes() {
    let session = sql_not_null::initialize_nullable_rows();
    let read = "SELECT id, status, qty, marked FROM PlannerRow ORDER BY id";
    let before = projection_rows(&session, read);
    for (index, sql) in [
        // Earlier rows have valid results; a later row fails accepted admission.
        "UPDATE PlannerRow SET status = peer, qty = CASE WHEN id < 4 THEN qty + 1 ELSE -1 END WHERE id >= 0",
        "UPDATE PlannerRow SET status = peer, qty = qty + 0.5 WHERE id >= 0",
        "UPDATE PlannerRow SET status = peer, qty = 18446744073709551615 + qty WHERE id >= 0",
        "UPDATE PlannerRow SET status = peer, qty = peer WHERE id >= 0",
        "UPDATE PlannerRow SET status = peer, marked = flag WHERE id >= 0",
        "UPDATE PlannerRow SET status = peer, qty = typo + 1 WHERE id >= 0",
        "UPDATE PlannerRow SET status = peer, typo = qty + 1 WHERE id >= 0",
        "UPDATE PlannerRow SET id = id + 1 WHERE id = 1",
    ].into_iter().enumerate() {
        let error = session.execute_trusted_sql_exact_update(sql, 6).unwrap_err();
        if index < 4 {
            assert_eq!(error.diagnostic().detail(), Some(&icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary { boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::InvalidFieldLiteral }), "{sql}");
        }
        assert_eq!(projection_rows(&session, read), before, "{sql}");
    }
}

#[test]
fn sql_update_expressions_preserve_literals_null_default_and_authored_order() {
    let session = sql_not_null::initialize_nullable_rows();
    session.execute_trusted_sql_exact_update("UPDATE PlannerRow SET qty = qty + 1, qty = 9, status = DEFAULT, peer = NULL WHERE id = 1", 1).unwrap();
    assert_eq!(
        projection_rows(
            &session,
            "SELECT qty, status, peer FROM PlannerRow WHERE id = 1"
        ),
        vec![vec![
            OutputValue::nat64(9),
            OutputValue::null(),
            OutputValue::null()
        ]]
    );
    session
        .execute_trusted_sql_exact_update(
            "UPDATE PlannerRow SET qty = 9, qty = qty + 1 WHERE id = 2",
            1,
        )
        .unwrap();
    assert_eq!(
        projection_rows(&session, "SELECT qty FROM PlannerRow WHERE id = 2"),
        vec![vec![OutputValue::nat64(3)]]
    );
}

#[test]
fn sql_update_expressions_reject_resumable_jobs_without_job_or_row_effects() {
    let session = initialize_journaled();
    insert_row(&session, 1, "before", "group-a");
    let before = projection_rows(
        &session,
        "SELECT id, common, rare FROM PlannerRow ORDER BY id",
    );
    let inventory = session.progress_job_inventory().unwrap();
    for (index, rhs) in ["rare", "LOWER(common)", "1 + 1"].into_iter().enumerate() {
        let job = MutationJobId::try_from_bytes([u8::try_from(index + 1).unwrap(); 32]).unwrap();
        assert_eq!(
            session.start_trusted_sql_mutation_job(
                job,
                &format!("UPDATE PlannerRow SET common = {rhs} WHERE id >= 0")
            ),
            Err(MutationJobError::IneligibleIntent)
        );
        assert_eq!(session.progress_job_inventory().unwrap(), inventory);
        assert_eq!(
            projection_rows(
                &session,
                "SELECT id, common, rare FROM PlannerRow ORDER BY id"
            ),
            before
        );
    }
}
