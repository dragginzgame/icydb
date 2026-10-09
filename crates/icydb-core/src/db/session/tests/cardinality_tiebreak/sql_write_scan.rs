//! Bounded SQL writes reject scan exhaustion before changing any selected row.

use super::*;
use crate::db::{QueryError, session::sql::sql_statement_dispatch};

fn bounded_update(
    session: &DbSession<TestCanister>,
    sql: &str,
) -> Result<SqlStatementResult, QueryError> {
    session.execute_sql_public_bounded_update(&sql_statement_dispatch(sql)?)
}

fn bounded_delete(
    session: &DbSession<TestCanister>,
    sql: &str,
) -> Result<SqlStatementResult, QueryError> {
    session.execute_sql_public_bounded_delete(&sql_statement_dispatch(sql)?)
}
use icydb_diagnostic_code::{DiagnosticDetail, DiagnosticFactTag, SqlWriteBoundaryCode};

fn seed_scan_rows(last_id: u64) {
    let _ = initialize();
    let rows: Vec<_> = (0..=last_id)
        .map(|id| {
            row(
                id,
                if id == 4_095 {
                    "edge"
                } else if id == 4_096 {
                    "late"
                } else {
                    "early"
                },
                "unchanged",
            )
        })
        .collect();
    for chunk in rows.chunks(64) {
        let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        session
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, chunk.to_vec())
            .unwrap();
    }
}

#[test]
fn sql_write_scan_rejects_late_and_missing_matches_without_effects() {
    seed_scan_rows(4_096);
    for predicate in ["common = 'late'", "common = 'missing'"] {
        for delete in [false, true] {
            let session =
                new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
            let result = if delete {
                bounded_delete(
                    &session,
                    &format!("DELETE FROM PlannerRow WHERE {predicate} ORDER BY id ASC LIMIT 1"),
                )
            } else {
                bounded_update(
                    &session,
                    &format!(
                        "UPDATE PlannerRow SET rare = 'changed' WHERE {predicate} ORDER BY id ASC LIMIT 1"
                    ),
                )
            };
            let error = result.unwrap_err();
            assert_eq!(
                error.diagnostic().detail(),
                Some(&DiagnosticDetail::SqlWriteBoundary {
                    boundary: SqlWriteBoundaryCode::WriteScanBudgetExceeded
                })
            );
            assert_eq!(
                error.diagnostic_facts(),
                vec![
                    (DiagnosticFactTag::ActualCount, 4_097),
                    (DiagnosticFactTag::Limit, 4_096)
                ]
            );
            let rows = projection_rows(&session, "SELECT id, rare FROM PlannerRow WHERE id = 4096");
            assert_eq!(
                rows,
                vec![vec![
                    OutputValue::nat64(4_096),
                    OutputValue::text("unchanged".to_owned())
                ]]
            );
        }
    }
}

#[test]
fn sql_write_scan_admits_the_exact_ceiling_and_small_prefixes() {
    seed_scan_rows(4_095);
    for delete in [false, true] {
        let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        let missing = if delete {
            bounded_delete(&session, "DELETE FROM PlannerRow WHERE common = 'missing' ORDER BY id ASC LIMIT 1")
        } else {
            bounded_update(&session, "UPDATE PlannerRow SET rare = 'changed' WHERE common = 'missing' ORDER BY id ASC LIMIT 1")
        }.unwrap();
        assert!(matches!(
            missing,
            SqlStatementResult::Count { row_count: 0 }
        ));
        let result = if delete {
            bounded_delete(&session, "DELETE FROM PlannerRow WHERE common = 'edge' ORDER BY id ASC LIMIT 1")
        } else {
            bounded_update(&session, "UPDATE PlannerRow SET rare = 'changed' WHERE common = 'edge' ORDER BY id ASC LIMIT 1")
        }.unwrap();
        assert!(matches!(result, SqlStatementResult::Count { row_count: 1 }));
    }
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    assert!(matches!(session.execute_trusted_sql_prefix_update("UPDATE PlannerRow SET rare = 'changed' WHERE common = 'early' ORDER BY id ASC LIMIT 1").unwrap(), SqlStatementResult::Count { row_count: 1 }));
}
