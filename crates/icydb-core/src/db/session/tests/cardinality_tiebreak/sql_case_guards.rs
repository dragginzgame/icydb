//! CASE keeps arithmetic and function errors confined to the selected branch.

use super::*;
use crate::{
    db::{
        executor::{StructuralProjectionRequest, execute_structural_projection_rows},
        query::admission::{QueryAdmissionPolicy, QueryAdmissionRejection, QueryAdmissionSummary},
    },
    value::Value,
};
use icydb_diagnostic_code::ErrorCode;

const GUARDED: &str = "CASE WHEN qty = 0 THEN FALSE ELSE 6 / qty > 2 END";

#[test]
fn guarded_case_reads_preserve_lazy_results_and_selected_errors() {
    let _ = sql_not_null::initialize_nullable_rows();
    for predicate in [
        GUARDED,
        "CASE WHEN qty <> 0 THEN 6 / qty > 2 ELSE FALSE END",
        "CASE WHEN qty <> 0 THEN 6 / qty > 2 END",
        "CASE WHEN qty = 0 THEN FALSE WHEN 6 / qty > 2 THEN TRUE ELSE FALSE END",
        "CASE WHEN qty = 0 THEN FALSE ELSE MOD(6, qty) = 0 END",
        "CASE WHEN qty = 0 THEN FALSE ELSE CASE WHEN qty IS NULL THEN FALSE ELSE 6 / qty > 2 END END",
    ] {
        let expected = if predicate.contains("MOD") {
            vec![1, 2, 4]
        } else {
            vec![1, 2]
        };
        for direction in ["ASC", "DESC"] {
            for _ in 0..2 {
                let session =
                    new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
                let mut expected = expected.clone();
                if direction == "DESC" {
                    expected.reverse();
                }
                assert_eq!(
                    projection_rows(
                        &session,
                        &format!(
                            "SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id {direction}"
                        )
                    ),
                    expected
                        .into_iter()
                        .map(|id| vec![OutputValue::nat64(id)])
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    // Selected errors remain typed failures; keeping CASE must not swallow them.
    for predicate in [
        "6 / qty > 2",
        "CASE WHEN qty = 0 THEN 6 / qty > 2 ELSE FALSE END",
    ] {
        let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        let error = session
            .execute_trusted_sql_query(&format!(
                "SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id"
            ))
            .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE
        );
    }
}

#[test]
fn guarded_case_delete_preserves_nonmatches_and_errors_are_atomic() {
    let _ = sql_not_null::initialize_nullable_rows();
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    let error = session
        .execute_trusted_sql_mutation("DELETE FROM PlannerRow WHERE 6 / qty > 2")
        .unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE
    );
    assert_eq!(
        projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id").len(),
        6
    );

    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    session
        .execute_trusted_sql_mutation(&format!("DELETE FROM PlannerRow WHERE {GUARDED}"))
        .unwrap();
    assert_eq!(
        projection_rows(&session, "SELECT id FROM PlannerRow ORDER BY id"),
        [3, 4, 5, 6]
            .into_iter()
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn guarded_case_preserves_public_admission_and_trusted_expression_execution() {
    let _ = sql_not_null::initialize_nullable_rows();
    for (predicate, ids, expression_backed) in [
        (GUARDED, &[1, 2][..], true),
        (
            "CASE WHEN id <= 2 THEN TRUE ELSE FALSE END",
            &[1, 2][..],
            false,
        ),
        (
            "CASE WHEN LOWER(status) = 'active' THEN TRUE ELSE FALSE END",
            &[2][..],
            false,
        ),
    ] {
        for lane in [
            DiagnosticExecutionLane::PublicRead,
            DiagnosticExecutionLane::TrustedRead,
        ] {
            for _ in 0..2 {
                let session =
                    new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
                let query = missing_path_filters::structural_select(
                    &session,
                    &format!(
                        "SELECT id FROM PlannerRow WHERE id IN (1,2,3,4,5,6) AND ({predicate}) ORDER BY id LIMIT 6"
                    ),
                );
                let catalog = session
                    .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
                    .unwrap();
                let plan = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog(
                        catalog.accepted_entity_authority(),
                        &catalog,
                        &query,
                        lane,
                    )
                    .unwrap();
                let policy = QueryAdmissionPolicy::default_bounded_read();
                let rejection = policy
                    .evaluate(
                        QueryAdmissionSummary::from_plan(policy.lane(), plan.logical_plan())
                            .unwrap(),
                    )
                    .rejection();
                assert_eq!(
                    rejection,
                    expression_backed.then_some(QueryAdmissionRejection::UnboundedFullScanRejected)
                );
                // Expression-only filters retain the existing public refusal;
                // trusted execution and fully predicate-backed reads stay distinct.
                if lane == DiagnosticExecutionLane::PublicRead && rejection.is_some() {
                    continue;
                }
                let rows = execute_structural_projection_rows(
                    &session.db,
                    StructuralProjectionRequest::new(plan, lane),
                )
                .unwrap()
                .into_value_rows();
                assert_eq!(
                    rows,
                    ids.iter()
                        .map(|id| vec![Value::Nat64(*id)])
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn guarded_case_projection_and_having_preserve_nulls_and_first_match() {
    let _ = sql_not_null::initialize_nullable_rows();
    for _ in 0..2 {
        let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        assert_eq!(
            projection_rows(
                &session,
                &format!("SELECT {GUARDED} FROM PlannerRow ORDER BY id")
            ),
            [Some(true), Some(true), None, Some(false), Some(false), None]
                .map(|value| vec![value.map_or_else(OutputValue::null, OutputValue::boolean)])
        );
        let SqlStatementResult::Grouped {rows, ..} = session.execute_trusted_sql_query("SELECT flag, COUNT(*) FROM PlannerRow GROUP BY flag HAVING CASE WHEN COUNT(qty) = 0 THEN FALSE ELSE 10 / COUNT(qty) > 1 END").unwrap() else {panic!("expected grouped results")};
        assert_eq!(rows.len(), 2);
        for (row, flag) in rows.iter().zip([false, true]) {
            assert_eq!(row.group_key(), &[OutputValue::boolean(flag)]);
            assert_eq!(row.aggregate_values(), &[OutputValue::nat64(2)]);
        }
        let SqlStatementResult::Grouped { rows: omitted, .. } = session.execute_trusted_sql_query("SELECT flag, COUNT(*) FROM PlannerRow GROUP BY flag HAVING CASE WHEN COUNT(qty) > 0 THEN 10 / COUNT(qty) > 1 END").unwrap() else {panic!("expected grouped results")};
        assert_eq!(omitted, rows);
        let error = session.execute_trusted_sql_query("SELECT flag, COUNT(*) FROM PlannerRow GROUP BY flag HAVING CASE WHEN COUNT(qty) = 0 THEN 10 / COUNT(qty) > 1 ELSE FALSE END").unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::QUERY_NUMERIC_NOT_REPRESENTABLE
        );
    }
}

#[test]
fn guarded_case_global_having_preserves_omitted_else() {
    let _ = sql_not_null::initialize_nullable_rows();
    for otherwise in ["", " ELSE FALSE"] {
        let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        assert_eq!(
            projection_rows(
                &session,
                &format!(
                    "SELECT COUNT(*) + 0 FROM PlannerRow HAVING CASE WHEN COUNT(qty) > 0 THEN 10 / COUNT(qty) > 1{otherwise} END"
                )
            ),
            vec![vec![OutputValue::decimal(crate::types::Decimal::from(6))]]
        );
    }
    // Omitted ELSE retains its existing eight-arm admission bound.
    let arms = "WHEN COUNT(qty) > 0 THEN TRUE ".repeat(9);
    for (grouping, error_code) in [
        ("", ErrorCode::SQL_LOWERING_HAVING_REQUIRES_GROUP_BY),
        (
            " GROUP BY flag",
            ErrorCode::SQL_LOWERING_SELECT_HAVING_SHAPE,
        ),
    ] {
        let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
        let admitted = "WHEN COUNT(qty) > 0 THEN TRUE ".repeat(8);
        session
            .execute_trusted_sql_query(&format!(
                "SELECT COUNT(*) + 0 FROM PlannerRow{grouping} HAVING CASE {admitted} END"
            ))
            .unwrap();
        let error = session
            .execute_trusted_sql_query(&format!(
                "SELECT COUNT(*) + 0 FROM PlannerRow{grouping} HAVING CASE {arms} END"
            ))
            .unwrap_err();
        assert_eq!(error.diagnostic().error_code(), error_code);
    }
}
