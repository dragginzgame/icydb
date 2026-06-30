//! Session-level read-admission enforcement tests.

use std::num::NonZeroU32;

use super::{
    FilteredIndexedSessionSqlEntity, IndexedSessionSqlEntity, SessionSqlEntity,
    indexed_sql_session, reset_indexed_session_sql_store, reset_session_sql_store,
    seed_filtered_composite_indexed_session_sql_entities, seed_indexed_session_sql_entities,
    seed_session_sql_entities, sql_session,
};
use crate::db::{
    GroupedAdmissionPolicy, QueryAdmissionDecision, QueryAdmissionPolicy, QueryAdmissionRejection,
    QueryAdmissionSummary, QueryBoundKind, QueryError, SqlStatementResult,
};
use icydb_diagnostic_code::{DiagnosticCode, DiagnosticDetail, QueryReadAdmissionCode};

#[test]
fn public_read_sql_rejects_missing_limit_before_execution() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%'",
            &public_read_policy(10),
        )
        .expect_err("public read SQL should reject missing LIMIT");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::PublicQueryRequiresLimit,
        "missing LIMIT",
    );
}

#[test]
fn public_read_sql_rejects_full_scan_even_with_limit() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
            &public_read_policy(10),
        )
        .expect_err("public read SQL should reject full scan");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::UnboundedFullScanRejected,
        "full scan",
    );
}

#[test]
fn public_read_sql_rejects_global_count_full_scan() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            "SELECT COUNT(*) FROM SessionSqlEntity",
            &public_read_policy(10),
        )
        .expect_err("public read SQL should reject global COUNT over full scan");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::UnboundedFullScanRejected,
        "global count full scan",
    );
}

#[test]
fn public_read_sql_admits_indexed_bounded_scalar_select() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let result = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' \
             ORDER BY name ASC, id ASC LIMIT 2",
            &public_read_policy(10),
        )
        .expect("public read SQL should admit indexed bounded SELECT");
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("indexed bounded SELECT should return projection rows");
    };

    assert_eq!(row_count, 2);
    assert_eq!(rows.len(), 2);
}

#[test]
fn public_read_fluent_admission_rejects_missing_limit_before_execution() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let summary = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .read_admission(&public_read_policy(10))
        .expect("fluent public read admission should produce a summary");

    assert_admission_summary_rejection(
        &summary,
        QueryAdmissionRejection::PublicQueryRequiresLimit,
        "fluent missing LIMIT",
    );
}

#[test]
fn public_read_fluent_ensure_admission_returns_shared_query_error_on_rejection() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .ensure_read_admission(&public_read_policy(10))
        .expect_err("fluent ensure admission should fail closed for missing LIMIT");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::PublicQueryRequiresLimit,
        "fluent ensure missing LIMIT",
    );
}

#[test]
fn public_read_fluent_admission_admits_indexed_bounded_scalar_query() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let summary = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .read_admission(&public_read_policy(10))
        .expect("fluent indexed bounded admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert!(
        summary.selected_access().is_secondary_index(),
        "bounded typed/fluent public reads should prove an index-backed route",
    );
    assert_eq!(summary.returned_row_bound(), Some(2));
    assert_eq!(summary.response_byte_bound(), None);
    assert_eq!(
        summary.response_byte_bound_kind(),
        QueryBoundKind::Unavailable,
        "non-executing fluent admission should not claim response-byte proof",
    );
}

#[test]
fn public_read_session_ensure_admission_returns_admitted_summary() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let load = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2);
    let summary = session
        .ensure_query_read_admission_policy(load.query(), &public_read_policy(10))
        .expect("session ensure admission should return the admitted summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.returned_row_bound(), Some(2));
}

#[test]
fn public_read_sql_rejects_non_zero_offset() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' \
             ORDER BY name ASC, id ASC LIMIT 1 OFFSET 1",
            &public_read_policy(10),
        )
        .expect_err("public read SQL should reject non-zero OFFSET");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::PublicQueryOffsetRejected,
        "non-zero OFFSET",
    );
}

#[test]
fn public_read_sql_rejects_returned_row_bound_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' \
             ORDER BY name ASC, id ASC LIMIT 2",
            &public_read_policy(1),
        )
        .expect_err("public read SQL should reject LIMIT above returned-row policy");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy,
        "returned-row cap",
    );
}

#[test]
fn public_read_sql_rejects_response_bytes_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' \
             ORDER BY name ASC, id ASC LIMIT 1",
            &public_read_policy_with_response_bytes(10, 1),
        )
        .expect_err("public read SQL should reject responses above byte policy");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::ProjectionResponseMayExceedLimit,
        "response-byte cap",
    );
}

#[test]
fn public_read_sql_rejects_unresolved_order_materialized_sort() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (1, "Sam", true, "gold", "sam", 30),
            (2, "Sasha", true, "gold", "sasha", 24),
            (3, "Mira", true, "silver", "mira", 40),
        ],
    );

    let err = session
        .execute_sql_query_with_read_admission_policy::<FilteredIndexedSessionSqlEntity>(
            "SELECT name FROM FilteredIndexedSessionSqlEntity \
             WHERE active = true AND tier = 'gold' ORDER BY age ASC, id ASC LIMIT 2",
            &public_read_policy(10),
        )
        .expect_err("public read SQL should reject materialized sort");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::SortRequiresMaterialization,
        "materialized sort",
    );
}

#[test]
fn public_read_fluent_admission_rejects_unresolved_order_materialized_sort() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (1, "Sam", true, "gold", "sam", 30),
            (2, "Sasha", true, "gold", "sasha", 24),
            (3, "Mira", true, "silver", "mira", 40),
        ],
    );

    let summary = session
        .load::<FilteredIndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("active").eq(true))
        .filter(crate::db::FieldRef::new("tier").eq("gold"))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .read_admission(&public_read_policy(10))
        .expect("fluent materialized-sort admission should produce a summary");

    assert_admission_summary_rejection(
        &summary,
        QueryAdmissionRejection::SortRequiresMaterialization,
        "fluent materialized sort",
    );
}

#[test]
fn public_read_sql_rejects_grouped_query_without_group_budgets() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) FROM IndexedSessionSqlEntity \
             WHERE name LIKE 'S%' GROUP BY name",
            &public_read_policy(10),
        )
        .expect_err("public read SQL should reject grouped query without group budgets");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::GroupedQueryRequiresLimits,
        "missing grouped budgets",
    );
}

#[test]
fn public_read_sql_admits_grouped_query_with_group_budgets_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let result = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) FROM IndexedSessionSqlEntity \
             WHERE name LIKE 'S%' GROUP BY name",
            &public_grouped_read_policy(10, 10, 8192, None, 32_768),
        )
        .expect("public read SQL should admit grouped query with explicit group budgets");
    let SqlStatementResult::Grouped {
        row_count, rows, ..
    } = result
    else {
        panic!("grouped public SELECT should return grouped rows");
    };

    assert_eq!(row_count, 2);
    assert_eq!(rows.len(), 2);
}

#[test]
fn public_read_sql_rejects_distinct_grouped_query_without_distinct_budget() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sam", 31), ("Sasha", 24)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(DISTINCT age) FROM IndexedSessionSqlEntity \
             WHERE name LIKE 'S%' GROUP BY name",
            &public_grouped_read_policy(10, 10, 8192, None, 32_768),
        )
        .expect_err("public read SQL should reject distinct grouped query without distinct budget");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::GroupedQueryRequiresLimits,
        "missing grouped distinct budget",
    );
}

#[test]
fn public_read_sql_rejects_grouped_response_bytes_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) FROM IndexedSessionSqlEntity \
             WHERE name LIKE 'S%' GROUP BY name",
            &public_grouped_read_policy(10, 10, 8192, None, 1),
        )
        .expect_err("public read SQL should reject grouped responses above byte policy");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::ProjectionResponseMayExceedLimit,
        "grouped response-byte cap",
    );
}

#[test]
fn trusted_sql_query_path_keeps_existing_unbounded_admin_behavior() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let result = session
        .execute_sql_query::<SessionSqlEntity>("SELECT name FROM SessionSqlEntity")
        .expect("trusted SQL query path should keep existing behavior");
    let SqlStatementResult::Projection { row_count, .. } = result else {
        panic!("trusted SQL query should return projection rows");
    };

    assert_eq!(row_count, 2);
}

const fn public_read_policy(max_rows: u32) -> QueryAdmissionPolicy {
    public_read_policy_with_response_bytes(max_rows, 32_768)
}

const fn public_read_policy_with_response_bytes(
    max_rows: u32,
    max_response_bytes: u32,
) -> QueryAdmissionPolicy {
    QueryAdmissionPolicy::public_read(
        NonZeroU32::new(max_rows).expect("test max rows should be non-zero"),
        NonZeroU32::new(max_response_bytes).expect("test byte cap should be non-zero"),
    )
}

const fn public_grouped_read_policy(
    max_rows: u32,
    max_groups: u32,
    max_group_bytes: u32,
    max_distinct_entries: Option<NonZeroU32>,
    max_response_bytes: u32,
) -> QueryAdmissionPolicy {
    QueryAdmissionPolicy::public_read(
        NonZeroU32::new(max_rows).expect("test max rows should be non-zero"),
        NonZeroU32::new(max_response_bytes).expect("test byte cap should be non-zero"),
    )
    .with_grouped_policy(GroupedAdmissionPolicy::bounded(
        NonZeroU32::new(max_groups).expect("test group cap should be non-zero"),
        NonZeroU32::new(max_group_bytes).expect("test group byte cap should be non-zero"),
        max_distinct_entries,
    ))
}

fn assert_read_admission_rejection(err: QueryError, reason: QueryReadAdmissionCode, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryReadAdmission,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryReadAdmission { reason }),
        "{context}: diagnostic detail drifted",
    );
}

fn assert_admission_summary_rejection(
    summary: &QueryAdmissionSummary,
    reason: QueryAdmissionRejection,
    context: &str,
) {
    assert_eq!(
        summary.decision(),
        QueryAdmissionDecision::Rejected,
        "{context}: admission decision drifted",
    );
    assert_eq!(
        summary.rejection(),
        Some(reason),
        "{context}: admission rejection drifted",
    );
}
