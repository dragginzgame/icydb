//! Session-level read-admission enforcement tests.

use std::num::NonZeroU32;

use super::{
    IndexedSessionSqlEntity, SessionSqlEntity, indexed_sql_session,
    reset_indexed_session_sql_store, reset_session_sql_store, seed_indexed_session_sql_entities,
    seed_session_sql_entities, sql_session,
};
use crate::db::{QueryAdmissionPolicy, QueryError, SqlStatementResult};
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
    QueryAdmissionPolicy::public_read(
        NonZeroU32::new(max_rows).expect("test max rows should be non-zero"),
        NonZeroU32::new(32_768).expect("test byte cap should be non-zero"),
    )
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
