//! Session-level read-admission enforcement tests.

use std::num::NonZeroU32;

use super::{
    FilteredIndexedSessionSqlEntity, HeapSessionSqlEntity, IndexedSessionSqlEntity,
    JournaledSessionSqlEntity, SessionPrincipalKeyEntity, SessionSqlCompositeWriteEntity,
    SessionSqlEntity, SessionSqlSignedWriteEntity, SessionSqlWriteEntity,
    SessionUniquePrefixOffsetEntity, assert_query_plan_expr_unknown_field,
    assert_query_plan_predicate_invalid_field, assert_sql_lowering_detail, heap_sql_session,
    indexed_sql_session, journaled_sql_session, reset_heap_session_sql_store,
    reset_indexed_session_sql_store, reset_journaled_session_sql_store, reset_session_sql_store,
    seed_filtered_composite_indexed_session_sql_entities, seed_indexed_session_sql_entities,
    seed_session_sql_entities, seed_unique_prefix_offset_session_entities, sql_session,
};
use crate::db::{
    QueryAdmissionAccessKind, QueryAdmissionDecision, QueryAdmissionRejection,
    QueryAdmissionSummary, QueryBoundKind, QueryError, ResponseCardinalityExt, SqlStatementResult,
    query::{
        admission::{GroupedAdmissionPolicy, QueryAdmissionPolicy},
        intent::IntentError,
    },
};
use icydb_diagnostic_code::{
    DiagnosticCode, DiagnosticDetail, QueryErrorKind, QueryReadAdmissionCode, SqlLoweringCode,
};

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

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
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

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"));
    let err = session
        .ensure_query_read_admission_policy(query.query(), &public_read_policy(10))
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

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2);
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
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
fn public_read_fluent_admission_admits_primary_key_lookup_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(18_801);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .by_id(crate::types::Id::from_key(id));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("fluent primary-key lookup admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.returned_row_bound(), Some(1));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn public_read_fluent_admission_admits_primary_key_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(18_802);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(id));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("primary-key filter admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.returned_row_bound(), Some(1));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn public_read_fluent_admission_admits_primary_key_filter_with_residual_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(18_806);

    let query = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(id),
            crate::db::FieldRef::new("age").gt(30_u64),
        ]));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("primary-key filter plus residual admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(1));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn public_read_fluent_admission_fails_invalid_residual_after_primary_key_filter() {
    reset_session_sql_store();
    let session = sql_session();
    let existing_id = crate::types::Ulid::from_u128(18_807);
    let missing_id = crate::types::Ulid::from_u128(18_808);
    session
        .insert(SessionSqlEntity {
            id: existing_id,
            name: "Mira".to_string(),
            age: 40,
        })
        .expect("test row should insert");

    for (id, context) in [
        (
            existing_id,
            "existing exact primary key plus invalid residual",
        ),
        (
            missing_id,
            "missing exact primary key plus invalid residual",
        ),
    ] {
        let query = session
            .load::<SessionSqlEntity>()
            .filter(crate::db::FilterExpr::and(vec![
                crate::db::FieldRef::new("id").eq(id),
                crate::db::FieldRef::new("unknown_field").eq("x"),
            ]));
        let err = session
            .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
            .expect_err("invalid residual fields must fail validation before admission");

        assert_query_plan_expr_unknown_field(err, "unknown_field", context);
    }
}

#[test]
fn public_read_fluent_admission_admits_primary_key_in_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(18_810);
    let second_id = crate::types::Ulid::from_u128(18_811);
    let third_id = crate::types::Ulid::from_u128(18_812);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([second_id, first_id, second_id, third_id]));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("primary-key IN admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(3));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(3));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn public_read_fluent_rejects_primary_key_in_deduped_count_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_780);
    let second_id = crate::types::Ulid::from_u128(19_781);
    let third_id = crate::types::Ulid::from_u128(19_782);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([third_id, first_id, second_id, second_id]));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(2))
        .expect("primary-key IN admission should produce a summary before policy rejection");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        summary.rejection(),
        Some(QueryAdmissionRejection::ReturnedRowBoundExceedsPolicy)
    );
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(3));
    assert_eq!(summary.returned_row_bound(), Some(3));
}

#[test]
fn public_read_fluent_rejects_primary_key_in_input_terms_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_783);
    let second_id = crate::types::Ulid::from_u128(19_784);
    let third_id = crate::types::Ulid::from_u128(19_785);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([third_id, first_id, second_id]));
    let summary = session
        .evaluate_query_read_admission_policy(
            query.query(),
            &public_read_policy_with_primary_key_input_caps(10, 2, 128),
        )
        .expect("primary-key IN admission should produce a summary before input rejection");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        summary.rejection(),
        Some(QueryAdmissionRejection::PrimaryKeyInputExceedsPolicy)
    );
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(3));
    assert_eq!(summary.returned_row_bound(), Some(3));
    assert_eq!(summary.primary_key_input_terms(), Some(3));
    assert_eq!(summary.primary_key_input_payload_bytes(), Some(48));
}

#[test]
fn public_read_fluent_by_ids_rejects_duplicate_raw_input_terms_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(19_789);

    let query = session.load::<IndexedSessionSqlEntity>().by_ids([
        crate::types::Id::from_key(id),
        crate::types::Id::from_key(id),
        crate::types::Id::from_key(id),
    ]);
    let summary = session
        .evaluate_query_read_admission_policy(
            query.query(),
            &public_read_policy_with_primary_key_input_caps(10, 2, 128),
        )
        .expect("typed by_ids admission should produce a summary before input rejection");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        summary.rejection(),
        Some(QueryAdmissionRejection::PrimaryKeyInputExceedsPolicy)
    );
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.returned_row_bound(), Some(1));
    assert_eq!(summary.primary_key_input_terms(), Some(3));
    assert_eq!(summary.primary_key_input_payload_bytes(), Some(48));
}

#[test]
fn public_read_sql_rejects_primary_key_in_payload_bytes_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_786);
    let second_id = crate::types::Ulid::from_u128(19_787);
    let third_id = crate::types::Ulid::from_u128(19_788);
    let sql = format!(
        "SELECT name FROM IndexedSessionSqlEntity WHERE id IN ('{first_id}', '{second_id}', '{third_id}')"
    );

    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            &sql,
            &public_read_policy_with_primary_key_input_caps(10, 10, 32),
        )
        .expect_err("SQL primary-key IN payload budget should reject before execution");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy,
        "primary-key input payload cap",
    );
}

#[test]
fn public_read_fluent_admission_canonicalizes_empty_primary_key_filters_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(18_817);
    let second_id = crate::types::Ulid::from_u128(18_818);

    for (query, context) in [
        (
            session
                .load::<SessionSqlEntity>()
                .filter(crate::db::FilterExpr::and(vec![
                    crate::db::FieldRef::new("id").eq(first_id),
                    crate::db::FieldRef::new("id").eq(second_id),
                ])),
            "contradictory primary-key equality filters",
        ),
        (
            session.load::<SessionSqlEntity>().filter(
                crate::db::FieldRef::new("id").in_list(std::iter::empty::<crate::types::Ulid>()),
            ),
            "empty primary-key IN filter",
        ),
        (
            session
                .load::<SessionSqlEntity>()
                .filter(crate::db::FilterExpr::and(vec![
                    crate::db::FieldRef::new("id").eq(first_id),
                    crate::db::FieldRef::new("id").in_list([second_id]),
                ])),
            "primary-key equality excluded by IN filter",
        ),
    ] {
        let summary = session
            .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
            .unwrap_or_else(|err| panic!("{context}: admission should produce a summary: {err}"));

        assert_eq!(
            summary.decision(),
            QueryAdmissionDecision::Admitted,
            "{context}"
        );
        assert_eq!(summary.rejection(), None, "{context}");
        assert_eq!(
            summary.selected_access(),
            QueryAdmissionAccessKind::ByKeys,
            "{context}: proven-empty key filters should use empty ByKeys access",
        );
        assert_eq!(summary.limit(), None, "{context}");
        assert_eq!(summary.scan_bound(), Some(0), "{context}");
        assert_eq!(
            summary.scan_bound_kind(),
            QueryBoundKind::Exact,
            "{context}"
        );
        assert_eq!(summary.returned_row_bound(), Some(0), "{context}");
        assert_eq!(
            summary.returned_row_bound_kind(),
            QueryBoundKind::ConservativeUpperBound,
            "{context}",
        );
    }
}

#[test]
fn public_read_fluent_admission_narrows_primary_key_eq_and_in_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(18_819);
    let second_id = crate::types::Ulid::from_u128(18_820);

    let query = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(first_id),
            crate::db::FieldRef::new("id").in_list([second_id, first_id, second_id]),
        ]));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("primary-key equality intersected with IN should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKey);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(1));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(1));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn public_read_fluent_admission_keeps_unique_secondary_equality_off_primary_key_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_unique_prefix_offset_session_entities(
        &session,
        &[
            (19_720, "gold", "amber", "A"),
            (19_721, "gold", "bravo", "B"),
            (19_722, "silver", "charlie", "C"),
        ],
    );

    let query =
        session
            .load::<SessionUniquePrefixOffsetEntity>()
            .filter(crate::db::FilterExpr::and(vec![
                crate::db::FieldRef::new("tier").eq("gold"),
                crate::db::FieldRef::new("handle").eq("amber"),
            ]));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("unique secondary equality admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Rejected);
    assert_eq!(
        summary.rejection(),
        Some(QueryAdmissionRejection::PublicQueryRequiresLimit),
    );
    assert!(
        summary.selected_access().is_secondary_index(),
        "unique secondary equality should remain a secondary-index access path even when public read admission requires a limit",
    );
    assert_ne!(
        summary.selected_access(),
        QueryAdmissionAccessKind::ByKey,
        "unique secondary equality must not masquerade as scalar primary-key access",
    );
    assert_ne!(
        summary.selected_access(),
        QueryAdmissionAccessKind::ByKeys,
        "unique secondary equality must not masquerade as primary-key set access",
    );
    assert_eq!(summary.limit(), None);
}

#[test]
fn public_read_fluent_admission_rejects_partial_composite_primary_key_as_full_scan() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .load::<SessionSqlCompositeWriteEntity>()
        .filter(crate::db::FieldRef::new("tenant_id").eq(7_u64))
        .order_term(crate::db::asc("tenant_id"))
        .order_term(crate::db::asc("local_id"))
        .limit(1);
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("partial composite primary-key admission should produce a summary");

    assert_admission_summary_rejection(
        &summary,
        QueryAdmissionRejection::UnboundedFullScanRejected,
        "partial composite primary-key filter",
    );
    assert_eq!(
        summary.selected_access(),
        QueryAdmissionAccessKind::FullScan,
        "a single composite primary-key component must not lower to ByKey access",
    );
}

#[test]
fn public_read_fluent_admission_admits_primary_key_in_filter_with_residual_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(18_813);
    let second_id = crate::types::Ulid::from_u128(18_814);
    let third_id = crate::types::Ulid::from_u128(18_815);

    let query = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").in_list([second_id, first_id, second_id, third_id]),
            crate::db::FieldRef::new("age").gte(30_u64),
        ]));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("primary-key IN filter plus residual admission should produce a summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(3));
    assert_eq!(summary.scan_bound_kind(), QueryBoundKind::Exact);
    assert_eq!(summary.returned_row_bound(), Some(3));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound
    );
}

#[test]
fn default_fluent_try_entity_admits_primary_key_lookup_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(18_803);
    session
        .insert(IndexedSessionSqlEntity {
            id,
            name: "Sam".to_string(),
            age: 30,
        })
        .expect("test row should insert");

    let entity = session
        .load::<IndexedSessionSqlEntity>()
        .by_id(crate::types::Id::from_key(id))
        .execute_rows()
        .expect("primary-key try_entity should be admitted without explicit LIMIT")
        .try_entity()
        .expect("primary-key response should satisfy optional-entity cardinality")
        .expect("inserted row should exist");

    assert_eq!(entity.id, id);
    assert_eq!(entity.name, "Sam");
}

#[test]
fn default_fluent_try_entity_admits_primary_key_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(18_804);
    session
        .insert(IndexedSessionSqlEntity {
            id,
            name: "Sasha".to_string(),
            age: 24,
        })
        .expect("test row should insert");

    let entity = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(id))
        .execute_rows()
        .expect("primary-key filter should be admitted without explicit LIMIT")
        .try_entity()
        .expect("primary-key filter response should satisfy optional-entity cardinality")
        .expect("inserted row should exist");

    assert_eq!(entity.id, id);
    assert_eq!(entity.name, "Sasha");
}

#[test]
fn default_fluent_filter_expr_admits_signed_primary_key_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSignedWriteEntity { id: 197, delta: 11 })
        .expect("signed primary-key row should insert");

    let query = session
        .load::<SessionSqlSignedWriteEntity>()
        .filter(crate::db::FilterExpr::eq("id", 197_i64));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("signed primary-key filter admission should produce a summary");

    assert_primary_key_exact_admission_summary(&summary, "signed primary-key filter");

    let entity = query
        .execute_rows()
        .expect("signed primary-key filter should execute as bounded exact-key access")
        .try_entity()
        .expect("signed primary-key filter should satisfy optional cardinality")
        .expect("signed primary-key filter should find the inserted row");

    assert_eq!(entity.id, 197);
    assert_eq!(entity.delta, 11);
}

#[test]
fn public_read_fluent_admission_admits_heap_and_journaled_primary_key_filters_without_limit() {
    reset_heap_session_sql_store();
    let heap_session = heap_sql_session();
    let heap_query = heap_session
        .load::<HeapSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(2_u64));
    let heap_summary = heap_session
        .evaluate_query_read_admission_policy(heap_query.query(), &public_read_policy(10))
        .expect("heap primary-key filter admission should produce a summary");

    assert_primary_key_exact_admission_summary(&heap_summary, "heap primary-key filter");

    reset_journaled_session_sql_store();
    let journaled_session = journaled_sql_session();
    let journaled_query = journaled_session
        .load::<JournaledSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(2_u64));
    let journaled_summary = journaled_session
        .evaluate_query_read_admission_policy(journaled_query.query(), &public_read_policy(10))
        .expect("journaled primary-key filter admission should produce a summary");

    assert_primary_key_exact_admission_summary(&journaled_summary, "journaled primary-key filter");
}

#[test]
fn default_fluent_try_entity_returns_none_for_missing_heap_and_journaled_primary_key_filters() {
    reset_heap_session_sql_store();
    let heap_session = heap_sql_session();
    let heap_entity = heap_session
        .load::<HeapSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(99_u64))
        .execute_rows()
        .expect("missing heap primary-key filter should execute as bounded exact-key access")
        .try_entity()
        .expect("missing heap primary-key filter should satisfy optional cardinality");

    assert!(
        heap_entity.is_none(),
        "missing heap exact-key filter should return no row",
    );

    reset_journaled_session_sql_store();
    let journaled_session = journaled_sql_session();
    let journaled_entity = journaled_session
        .load::<JournaledSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(99_u64))
        .execute_rows()
        .expect("missing journaled primary-key filter should execute as bounded exact-key access")
        .try_entity()
        .expect("missing journaled primary-key filter should satisfy optional cardinality");

    assert!(
        journaled_entity.is_none(),
        "missing journaled exact-key filter should return no row",
    );
}

#[test]
fn default_fluent_try_entity_returns_none_for_deleted_heap_and_journaled_primary_key_filters() {
    reset_heap_session_sql_store();
    let heap_session = heap_sql_session();
    heap_session
        .insert(HeapSessionSqlEntity {
            id: 2,
            name: "Beryl".to_string(),
            age: 30,
        })
        .expect("heap row should insert before deletion");
    let heap_deleted = heap_session
        .delete::<HeapSessionSqlEntity>()
        .by_id(crate::types::Id::from_key(2_u64))
        .execute()
        .expect("heap row should delete by primary key");
    let heap_entity = heap_session
        .load::<HeapSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(2_u64))
        .execute_rows()
        .expect("deleted heap primary-key filter should execute as bounded exact-key access")
        .try_entity()
        .expect("deleted heap primary-key filter should satisfy optional cardinality");

    assert_eq!(heap_deleted, 1);
    assert!(
        heap_entity.is_none(),
        "deleted heap exact-key filter should not return a stale row",
    );

    reset_journaled_session_sql_store();
    let journaled_session = journaled_sql_session();
    journaled_session
        .insert(JournaledSessionSqlEntity {
            id: 2,
            name: "Beryl".to_string(),
            age: 30,
        })
        .expect("journaled row should insert before deletion");
    let journaled_deleted = journaled_session
        .delete::<JournaledSessionSqlEntity>()
        .by_id(crate::types::Id::from_key(2_u64))
        .execute()
        .expect("journaled row should delete by primary key");
    let journaled_entity = journaled_session
        .load::<JournaledSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(2_u64))
        .execute_rows()
        .expect("deleted journaled primary-key filter should execute as bounded exact-key access")
        .try_entity()
        .expect("deleted journaled primary-key filter should satisfy optional cardinality");

    assert_eq!(journaled_deleted, 1);
    assert!(
        journaled_entity.is_none(),
        "deleted journaled exact-key filter should not return a stale row",
    );
}

#[test]
fn default_fluent_execute_rows_applies_residual_after_primary_key_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(18_816);
    session
        .insert(SessionSqlEntity {
            id,
            name: "Sasha".to_string(),
            age: 24,
        })
        .expect("test row should insert");

    let rejected_by_residual = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(id),
            crate::db::FieldRef::new("age").gt(30_u64),
        ]))
        .execute_rows()
        .expect("primary-key filter with false residual should still be admitted");
    let accepted_by_residual = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(id),
            crate::db::FieldRef::new("age").gte(24_u64),
        ]))
        .execute_rows()
        .expect("primary-key filter with true residual should still be admitted");

    assert_eq!(
        rejected_by_residual.count(),
        0,
        "primary-key exact access must not bypass a false residual predicate",
    );
    assert_eq!(accepted_by_residual.count(), 1);
    assert_eq!(accepted_by_residual.entities()[0].name, "Sasha");
}

#[test]
fn default_fluent_execute_rows_returns_empty_for_empty_primary_key_filters_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(18_828);
    let second_id = crate::types::Ulid::from_u128(18_829);
    session
        .insert(SessionSqlEntity {
            id: first_id,
            name: "Sam".to_string(),
            age: 30,
        })
        .expect("test row should insert");

    let contradictory = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(first_id),
            crate::db::FieldRef::new("id").eq(second_id),
        ]))
        .execute_rows()
        .expect("contradictory primary-key filters should execute as bounded empty access");
    let empty_in = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list(std::iter::empty::<crate::types::Ulid>()))
        .execute_rows()
        .expect("empty primary-key IN should execute as bounded empty access");
    let narrowed_out = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(first_id),
            crate::db::FieldRef::new("id").in_list([second_id]),
        ]))
        .execute_rows()
        .expect("primary-key equality excluded by IN should execute as bounded empty access");

    assert_eq!(contradictory.count(), 0);
    assert_eq!(empty_in.count(), 0);
    assert_eq!(narrowed_out.count(), 0);
}

#[test]
fn default_fluent_count_returns_zero_for_empty_primary_key_filters_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(19_750);
    let second_id = crate::types::Ulid::from_u128(19_751);
    session
        .insert(SessionSqlEntity {
            id: first_id,
            name: "Sam".to_string(),
            age: 30,
        })
        .expect("test row should insert");

    let contradictory = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(first_id),
            crate::db::FieldRef::new("id").eq(second_id),
        ]))
        .count()
        .expect("contradictory primary-key filters should count as bounded empty access");
    let empty_in = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list(std::iter::empty::<crate::types::Ulid>()))
        .count()
        .expect("empty primary-key IN should count as bounded empty access");
    let narrowed_out = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").eq(first_id),
            crate::db::FieldRef::new("id").in_list([second_id]),
        ]))
        .count()
        .expect("primary-key equality excluded by IN should count as bounded empty access");

    assert_eq!(contradictory, 0);
    assert_eq!(empty_in, 0);
    assert_eq!(narrowed_out, 0);
}

#[test]
fn default_fluent_count_exact_counts_primary_key_filters_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(19_756);
    session
        .insert(SessionSqlEntity {
            id,
            name: "Sam".to_string(),
            age: 30,
        })
        .expect("test row should insert");

    let existing = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(id))
        .count_exact()
        .expect("primary-key exact count should not require a raw limit");
    let missing = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(crate::types::Ulid::from_u128(19_757)))
        .count_exact()
        .expect("missing primary-key exact count should still be bounded");

    assert_eq!(existing, 1);
    assert_eq!(missing, 0);
}

#[test]
fn default_fluent_sum_exact_sums_primary_key_filters_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(19_758);
    session
        .insert(SessionSqlEntity {
            id,
            name: "Sam".to_string(),
            age: 30,
        })
        .expect("test row should insert");

    let existing = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(id))
        .sum_exact("age")
        .expect("primary-key exact sum should not require a raw limit");
    let missing = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(crate::types::Ulid::from_u128(19_759)))
        .sum_exact("age")
        .expect("missing primary-key exact sum should still be bounded");

    assert_eq!(existing, Some(crate::types::Decimal::from(30_u64)));
    assert_eq!(missing, None);
}

#[test]
fn default_fluent_collect_complete_returns_indexed_small_set_without_raw_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let names: Vec<String> = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .collect_complete()
        .expect("collect_complete() should return admitted complete small sets")
        .into_iter()
        .map(|entity| entity.name)
        .collect();

    assert_eq!(names, ["Sam".to_string(), "Sasha".to_string()]);
}

#[test]
fn default_fluent_collect_complete_rejects_over_cap_sets_instead_of_truncating() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    for offset in 0_u64..=100 {
        session
            .insert(IndexedSessionSqlEntity {
                id: crate::types::Ulid::from_u128(20_000 + u128::from(offset)),
                name: format!("S{offset:03}"),
                age: offset,
            })
            .expect("test row should insert");
    }

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .collect_complete()
        .expect_err("collect_complete() should fail when lookahead proves truncation");

    assert_complete_read_too_many_rows(err, "default fluent collect_complete over cap");
}

#[test]
fn default_fluent_require_one_reports_not_found_for_empty_primary_key_filters_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(19_752);
    let second_id = crate::types::Ulid::from_u128(19_753);
    session
        .insert(SessionSqlEntity {
            id: first_id,
            name: "Sam".to_string(),
            age: 30,
        })
        .expect("test row should insert");

    for (query, context) in [
        (
            session
                .load::<SessionSqlEntity>()
                .filter(crate::db::FilterExpr::and(vec![
                    crate::db::FieldRef::new("id").eq(first_id),
                    crate::db::FieldRef::new("id").eq(second_id),
                ])),
            "contradictory primary-key equality filters",
        ),
        (
            session.load::<SessionSqlEntity>().filter(
                crate::db::FieldRef::new("id").in_list(std::iter::empty::<crate::types::Ulid>()),
            ),
            "empty primary-key IN filter",
        ),
        (
            session
                .load::<SessionSqlEntity>()
                .filter(crate::db::FilterExpr::and(vec![
                    crate::db::FieldRef::new("id").eq(first_id),
                    crate::db::FieldRef::new("id").in_list([second_id]),
                ])),
            "primary-key equality excluded by IN filter",
        ),
    ] {
        let err = query
            .require_one()
            .expect_err("required-one over proven-empty exact-key access should report not found");

        assert_query_not_found(err, context);
    }
}

#[test]
fn default_fluent_execute_rows_dedups_primary_key_in_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(18_820);
    let second_id = crate::types::Ulid::from_u128(18_821);
    let third_id = crate::types::Ulid::from_u128(18_822);
    for (id, name, age) in [
        (first_id, "Sam", 30),
        (second_id, "Sasha", 24),
        (third_id, "Mira", 40),
        (crate::types::Ulid::from_u128(18_823), "Quinn", 55),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let response = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([third_id, first_id, first_id, second_id]))
        .execute_rows()
        .expect("primary-key IN filter should be admitted without explicit LIMIT");
    let mut ids: Vec<_> = response
        .entities()
        .into_iter()
        .map(|entity| entity.id)
        .collect();
    ids.sort_unstable();

    assert_eq!(ids, vec![first_id, second_id, third_id]);
}

#[test]
fn default_fluent_execute_rows_orders_primary_key_in_filters_deterministically_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(18_837);
    let second_id = crate::types::Ulid::from_u128(18_838);
    let third_id = crate::types::Ulid::from_u128(18_839);
    for (id, name, age) in [
        (first_id, "first", 30),
        (second_id, "second", 24),
        (third_id, "third", 40),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let unsorted_response = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([third_id, first_id, first_id, second_id]))
        .execute_rows()
        .expect("unsorted primary-key IN filter should be admitted without explicit LIMIT");
    let canonical_response = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([first_id, second_id, third_id]))
        .execute_rows()
        .expect("canonical primary-key IN filter should be admitted without explicit LIMIT");
    let unsorted_ids: Vec<_> = unsorted_response
        .entities()
        .into_iter()
        .map(|entity| entity.id)
        .collect();
    let canonical_ids: Vec<_> = canonical_response
        .entities()
        .into_iter()
        .map(|entity| entity.id)
        .collect();

    assert_eq!(unsorted_ids, vec![first_id, second_id, third_id]);
    assert_eq!(
        unsorted_ids, canonical_ids,
        "primary-key IN result order must follow deterministic encoded key order, not input-list order",
    );
}

#[test]
fn default_fluent_primary_key_in_filter_materializes_finite_non_key_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_760);
    let second_id = crate::types::Ulid::from_u128(19_761);
    let third_id = crate::types::Ulid::from_u128(19_762);
    for (id, name, age) in [
        (first_id, "first", 30),
        (second_id, "second", 24),
        (third_id, "third", 40),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").in_list([third_id, first_id, first_id, second_id]))
        .order_term(crate::db::desc("age"))
        .order_term(crate::db::asc("id"));
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(3))
        .expect("finite primary-key IN plus non-key order should produce an admission summary");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.selected_access(), QueryAdmissionAccessKind::ByKeys);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.scan_bound(), Some(3));
    assert_eq!(summary.returned_row_bound(), Some(3));

    let names: Vec<String> = query
        .execute_rows()
        .expect("finite primary-key IN plus non-key order should execute without explicit LIMIT")
        .entities()
        .into_iter()
        .map(|entity| entity.name)
        .collect();

    assert_eq!(names, vec!["third", "first", "second"]);
}

#[test]
fn default_fluent_execute_rows_applies_residual_after_primary_key_in_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(18_824);
    let second_id = crate::types::Ulid::from_u128(18_825);
    let third_id = crate::types::Ulid::from_u128(18_826);
    let outside_id = crate::types::Ulid::from_u128(18_827);
    for (id, name, age) in [
        (first_id, "Sam", 30),
        (second_id, "Sasha", 24),
        (third_id, "Mira", 40),
        (outside_id, "Quinn", 55),
    ] {
        session
            .insert(SessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let response = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("id").in_list([third_id, first_id, second_id, second_id]),
            crate::db::FieldRef::new("age").gte(30_u64),
        ]))
        .execute_rows()
        .expect("primary-key IN filter with residual should be admitted without explicit LIMIT");
    let mut names: Vec<String> = response
        .entities()
        .into_iter()
        .map(|entity| entity.name)
        .collect();
    names.sort();

    assert_eq!(names, vec!["Mira".to_string(), "Sam".to_string()]);
}

#[test]
fn public_read_sql_admits_primary_key_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(18_805);
    session
        .insert(IndexedSessionSqlEntity {
            id,
            name: "Mira".to_string(),
            age: 40,
        })
        .expect("test row should insert");

    let sql = format!("SELECT name FROM IndexedSessionSqlEntity WHERE id = '{id}'");
    let result = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(10),
        )
        .expect("public read SQL primary-key filter should not need explicit LIMIT");
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("primary-key SQL filter should return projection rows");
    };

    assert_eq!(row_count, 1);
    assert_eq!(
        rows,
        vec![vec![crate::value::OutputValue::Text("Mira".to_string())]],
    );
}

#[test]
fn public_read_sql_admits_commuted_primary_key_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let id = crate::types::Ulid::from_u128(19_740);
    session
        .insert(IndexedSessionSqlEntity {
            id,
            name: "Mira".to_string(),
            age: 40,
        })
        .expect("test row should insert");

    let sql = format!("SELECT name FROM IndexedSessionSqlEntity WHERE '{id}' = id");
    let result = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(10),
        )
        .expect("commuted SQL primary-key filter should not need explicit LIMIT");
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("commuted primary-key SQL filter should return projection rows");
    };

    assert_eq!(row_count, 1);
    assert_eq!(
        rows,
        vec![vec![crate::value::OutputValue::Text("Mira".to_string())]],
    );
}

#[test]
fn public_read_sql_applies_residual_after_primary_key_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(18_833);
    session
        .insert(SessionSqlEntity {
            id,
            name: "Mira".to_string(),
            age: 40,
        })
        .expect("test row should insert");

    let sql = format!("SELECT name FROM SessionSqlEntity WHERE id = '{id}' AND age > 99");
    let result = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(1),
        )
        .expect("public read SQL primary-key filter plus residual should not need explicit LIMIT");
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("primary-key SQL filter plus residual should return projection rows");
    };

    assert_eq!(row_count, 0);
    assert!(
        rows.is_empty(),
        "SQL primary-key exact access must not bypass a false residual predicate",
    );
}

#[test]
fn public_read_sql_applies_residual_after_commuted_primary_key_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(19_741);
    session
        .insert(SessionSqlEntity {
            id,
            name: "Mira".to_string(),
            age: 40,
        })
        .expect("test row should insert");

    let sql = format!("SELECT name FROM SessionSqlEntity WHERE '{id}' = id AND age > 99");
    let result = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(1),
        )
        .expect("commuted SQL primary-key filter plus residual should not need explicit LIMIT");
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("commuted primary-key SQL filter plus residual should return projection rows");
    };

    assert_eq!(row_count, 0);
    assert!(
        rows.is_empty(),
        "commuted SQL primary-key exact access must not bypass a false residual predicate",
    );
}

#[test]
fn public_read_sql_primary_key_parameter_shape_fails_before_admission() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE id = ?",
            &public_read_policy(10),
        )
        .expect_err("SQL parameter placeholders are not a supported exact-key binding surface");

    assert_sql_lowering_detail(err, SqlLoweringCode::ParameterPlacement);
}

#[test]
fn public_read_sql_primary_key_wrong_type_literal_fails_before_admission() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE id = 'not-a-ulid' LIMIT 1",
            &public_read_policy(10),
        )
        .expect_err("wrong-type primary-key literal should fail validation before admission");

    assert_query_plan_predicate_invalid_field(err, "id", "wrong-type SQL primary-key literal");
}

#[test]
fn public_read_sql_commuted_primary_key_wrong_type_literal_fails_before_admission() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE 'not-a-ulid' = id LIMIT 1",
            &public_read_policy(10),
        )
        .expect_err(
            "commuted wrong-type primary-key literal should fail validation before admission",
        );

    assert_query_plan_predicate_invalid_field(
        err,
        "id",
        "commuted wrong-type SQL primary-key literal",
    );
}

#[cfg(feature = "sql-explain")]
#[test]
fn sql_explain_expression_wrapped_primary_key_does_not_canonicalize_to_exact_key() {
    reset_session_sql_store();
    let session = sql_session();

    let result = session
        .execute_sql_query::<SessionSqlWriteEntity>(
            "EXPLAIN EXECUTION SELECT name FROM SessionSqlWriteEntity \
             WHERE id + 0 = 1 ORDER BY id ASC LIMIT 1",
        )
        .expect("expression-wrapped primary-key explain should build");
    let SqlStatementResult::Explain(explain) = result else {
        panic!("EXPLAIN EXECUTION should return explain text");
    };

    assert!(
        !explain.contains("ByKeyLookup") && !explain.contains("planner_primary_key_lookup"),
        "expression-wrapped primary-key predicates must not explain as exact-key canonicalization: {explain}",
    );
}

#[cfg(feature = "sql-explain")]
#[test]
fn sql_explain_commuted_primary_key_filter_canonicalizes_to_exact_key() {
    reset_session_sql_store();
    let session = sql_session();
    let id = crate::types::Ulid::from_u128(19_742);

    let result = session
        .execute_sql_query::<SessionSqlEntity>(
            format!("EXPLAIN EXECUTION SELECT name FROM SessionSqlEntity WHERE '{id}' = id")
                .as_str(),
        )
        .expect("commuted primary-key explain should build");
    let SqlStatementResult::Explain(explain) = result else {
        panic!("EXPLAIN EXECUTION should return explain text");
    };

    assert!(
        explain.contains("ByKeyLookup") && explain.contains("planner_primary_key_lookup"),
        "commuted primary-key predicates should explain as exact-key canonicalization: {explain}",
    );
}

#[test]
fn public_read_sql_admits_primary_key_in_filter_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(18_830);
    let second_id = crate::types::Ulid::from_u128(18_831);
    let third_id = crate::types::Ulid::from_u128(18_832);
    for (id, name, age) in [
        (first_id, "Sam", 30),
        (second_id, "Sasha", 24),
        (third_id, "Mira", 40),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let sql = format!(
        "SELECT name FROM IndexedSessionSqlEntity WHERE id IN ('{third_id}', '{first_id}', '{first_id}')"
    );
    let result = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(2),
        )
        .expect("public read SQL primary-key IN filter should not need explicit LIMIT");
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("primary-key SQL IN filter should return projection rows");
    };
    let mut names = text_projection_values(rows, "primary-key SQL IN projection");
    names.sort();

    assert_eq!(row_count, 2);
    assert_eq!(names, vec!["Mira".to_string(), "Sam".to_string()]);
}

#[test]
fn public_read_sql_rejects_primary_key_in_deduped_count_above_policy() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_783);
    let second_id = crate::types::Ulid::from_u128(19_784);
    let third_id = crate::types::Ulid::from_u128(19_785);

    let sql = format!(
        "SELECT name FROM IndexedSessionSqlEntity \
         WHERE id IN ('{third_id}', '{first_id}', '{second_id}', '{second_id}')"
    );
    let err = session
        .execute_sql_query_with_read_admission_policy::<IndexedSessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(2),
        )
        .expect_err("public read SQL should reject deduped primary-key IN count above policy");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy,
        "primary-key SQL IN deduped count above returned-row policy",
    );
}

#[test]
fn public_read_sql_primary_key_in_filter_orders_deterministically_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_750);
    let second_id = crate::types::Ulid::from_u128(19_751);
    let third_id = crate::types::Ulid::from_u128(19_752);
    for (id, name, age) in [
        (first_id, "first", 30),
        (second_id, "second", 24),
        (third_id, "third", 40),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    for (direction, expected) in [
        ("ASC", vec!["first", "second", "third"]),
        ("DESC", vec!["third", "second", "first"]),
    ] {
        let unsorted_sql = format!(
            "SELECT name FROM IndexedSessionSqlEntity \
             WHERE id IN ('{third_id}', '{first_id}', '{first_id}', '{second_id}') \
             ORDER BY id {direction}"
        );
        let canonical_sql = format!(
            "SELECT name FROM IndexedSessionSqlEntity \
             WHERE id IN ('{first_id}', '{second_id}', '{third_id}') \
             ORDER BY id {direction}"
        );
        let unsorted_names = public_read_sql_text_projection_values::<IndexedSessionSqlEntity>(
            &session,
            unsorted_sql.as_str(),
            3,
            "unsorted primary-key SQL IN ordered projection",
        );
        let canonical_names = public_read_sql_text_projection_values::<IndexedSessionSqlEntity>(
            &session,
            canonical_sql.as_str(),
            3,
            "canonical primary-key SQL IN ordered projection",
        );

        assert_eq!(unsorted_names, expected, "ORDER BY id {direction}");
        assert_eq!(
            unsorted_names, canonical_names,
            "primary-key SQL IN result order must be independent of input-list order and duplicates",
        );
    }
}

#[test]
fn public_read_sql_primary_key_in_filter_materializes_finite_non_key_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let first_id = crate::types::Ulid::from_u128(19_770);
    let second_id = crate::types::Ulid::from_u128(19_771);
    let third_id = crate::types::Ulid::from_u128(19_772);
    for (id, name, age) in [
        (first_id, "first", 30),
        (second_id, "second", 24),
        (third_id, "third", 40),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let sql = format!(
        "SELECT name FROM IndexedSessionSqlEntity \
         WHERE id IN ('{third_id}', '{first_id}', '{first_id}', '{second_id}') \
         ORDER BY age DESC, id ASC"
    );
    let names = public_read_sql_text_projection_values::<IndexedSessionSqlEntity>(
        &session,
        sql.as_str(),
        3,
        "finite primary-key SQL IN plus non-key ordered projection",
    );

    assert_eq!(names, vec!["third", "first", "second"]);
}

#[test]
fn public_read_sql_applies_residual_after_primary_key_in_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_id = crate::types::Ulid::from_u128(18_834);
    let second_id = crate::types::Ulid::from_u128(18_835);
    let third_id = crate::types::Ulid::from_u128(18_836);
    for (id, name, age) in [
        (first_id, "Sam", 30),
        (second_id, "Sasha", 24),
        (third_id, "Mira", 40),
    ] {
        session
            .insert(SessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("test row should insert");
    }

    let sql = format!(
        "SELECT name FROM SessionSqlEntity \
         WHERE id IN ('{third_id}', '{first_id}', '{second_id}', '{second_id}') AND age >= 30"
    );
    let result = session
        .execute_sql_query_with_read_admission_policy::<SessionSqlEntity>(
            sql.as_str(),
            &public_read_policy(3),
        )
        .expect(
            "public read SQL primary-key IN filter plus residual should not need explicit LIMIT",
        );
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("primary-key SQL IN filter plus residual should return projection rows");
    };
    let mut names = text_projection_values(rows, "primary-key SQL IN residual projection");
    names.sort();

    assert_eq!(row_count, 2);
    assert_eq!(names, vec!["Mira".to_string(), "Sam".to_string()]);
}

#[test]
fn public_read_fluent_admission_admits_external_primary_key_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let pid = crate::types::Principal::from_slice(&[1, 9, 7]);

    let filter_query = session
        .load::<SessionPrincipalKeyEntity>()
        .filter(crate::db::FieldRef::new("pid").eq(pid));
    let filter_summary = session
        .evaluate_query_read_admission_policy(filter_query.query(), &public_read_policy(10))
        .expect("external primary-key filter admission should produce a summary");
    let by_id_query = session
        .load::<SessionPrincipalKeyEntity>()
        .by_id(crate::types::Id::from_key(pid));
    let by_id_summary = session
        .evaluate_query_read_admission_policy(by_id_query.query(), &public_read_policy(10))
        .expect("external by_id admission should produce a summary");

    assert_eq!(filter_summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(filter_summary.rejection(), None);
    assert_eq!(
        filter_summary.selected_access(),
        QueryAdmissionAccessKind::ByKey
    );
    assert_eq!(filter_summary.limit(), None);
    assert_eq!(filter_summary.scan_bound(), Some(1));
    assert_eq!(filter_summary.returned_row_bound(), Some(1));
    assert_eq!(
        filter_summary.selected_access(),
        by_id_summary.selected_access()
    );
    assert_eq!(filter_summary.scan_bound(), by_id_summary.scan_bound());
    assert_eq!(
        filter_summary.returned_row_bound(),
        by_id_summary.returned_row_bound(),
    );
}

#[test]
fn default_fluent_try_entity_matches_by_id_for_external_primary_key_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let pid = crate::types::Principal::from_slice(&[1, 9, 7, 1]);
    let user_id = crate::types::Ulid::from_u128(19_701);
    session
        .insert(SessionPrincipalKeyEntity {
            pid,
            user_id,
            label: "mapping".to_string(),
        })
        .expect("external primary-key row should insert");

    let filter_entity = session
        .load::<SessionPrincipalKeyEntity>()
        .filter(crate::db::FieldRef::new("pid").eq(pid))
        .execute_rows()
        .expect("external primary-key filter should be admitted without explicit LIMIT")
        .try_entity()
        .expect("external primary-key filter should satisfy optional cardinality")
        .expect("external primary-key filter should find the inserted row");
    let by_id_entity = session
        .load::<SessionPrincipalKeyEntity>()
        .by_id(crate::types::Id::from_key(pid))
        .execute_rows()
        .expect("external by_id lookup should be admitted without explicit LIMIT")
        .try_entity()
        .expect("external by_id lookup should satisfy optional cardinality")
        .expect("external by_id lookup should find the inserted row");

    assert_eq!(filter_entity, by_id_entity);
    assert_eq!(filter_entity.pid, pid);
    assert_eq!(filter_entity.user_id, user_id);
}

#[test]
fn public_read_fluent_admission_admits_external_primary_key_in_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_pid = crate::types::Principal::from_slice(&[1, 9, 7, 2]);
    let second_pid = crate::types::Principal::from_slice(&[1, 9, 7, 3]);

    let filter_query = session
        .load::<SessionPrincipalKeyEntity>()
        .filter(crate::db::FieldRef::new("pid").in_list([second_pid, first_pid, second_pid]));
    let filter_summary = session
        .evaluate_query_read_admission_policy(filter_query.query(), &public_read_policy(10))
        .expect("external primary-key IN admission should produce a summary");
    let by_ids_query = session.load::<SessionPrincipalKeyEntity>().by_ids([
        crate::types::Id::from_key(second_pid),
        crate::types::Id::from_key(first_pid),
        crate::types::Id::from_key(second_pid),
    ]);
    let by_ids_summary = session
        .evaluate_query_read_admission_policy(by_ids_query.query(), &public_read_policy(10))
        .expect("external by_ids admission should produce a summary");

    assert_eq!(filter_summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(filter_summary.rejection(), None);
    assert_eq!(
        filter_summary.selected_access(),
        QueryAdmissionAccessKind::ByKeys
    );
    assert_eq!(filter_summary.limit(), None);
    assert_eq!(filter_summary.scan_bound(), Some(2));
    assert_eq!(filter_summary.returned_row_bound(), Some(2));
    assert_eq!(
        filter_summary.selected_access(),
        by_ids_summary.selected_access()
    );
    assert_eq!(filter_summary.scan_bound(), by_ids_summary.scan_bound());
    assert_eq!(
        filter_summary.returned_row_bound(),
        by_ids_summary.returned_row_bound(),
    );
}

#[test]
fn default_fluent_execute_rows_matches_by_ids_for_external_primary_key_in_filter_without_limit() {
    reset_session_sql_store();
    let session = sql_session();
    let first_pid = crate::types::Principal::from_slice(&[1, 9, 7, 4]);
    let second_pid = crate::types::Principal::from_slice(&[1, 9, 7, 5]);
    let third_pid = crate::types::Principal::from_slice(&[1, 9, 7, 6]);
    for (pid, user_id, label) in [
        (first_pid, crate::types::Ulid::from_u128(19_711), "first"),
        (second_pid, crate::types::Ulid::from_u128(19_712), "second"),
        (third_pid, crate::types::Ulid::from_u128(19_713), "third"),
    ] {
        session
            .insert(SessionPrincipalKeyEntity {
                pid,
                user_id,
                label: label.to_string(),
            })
            .expect("external primary-key row should insert");
    }

    let filter_response = session
        .load::<SessionPrincipalKeyEntity>()
        .filter(
            crate::db::FieldRef::new("pid").in_list([second_pid, first_pid, second_pid, third_pid]),
        )
        .execute_rows()
        .expect("external primary-key IN filter should be admitted without explicit LIMIT");
    let by_ids_response = session
        .load::<SessionPrincipalKeyEntity>()
        .by_ids([
            crate::types::Id::from_key(second_pid),
            crate::types::Id::from_key(first_pid),
            crate::types::Id::from_key(second_pid),
            crate::types::Id::from_key(third_pid),
        ])
        .execute_rows()
        .expect("external by_ids lookup should be admitted without explicit LIMIT");
    let mut filter_labels: Vec<String> = filter_response
        .entities()
        .into_iter()
        .map(|entity| entity.label)
        .collect();
    let mut by_ids_labels: Vec<String> = by_ids_response
        .entities()
        .into_iter()
        .map(|entity| entity.label)
        .collect();
    filter_labels.sort();
    by_ids_labels.sort();

    assert_eq!(filter_labels, vec!["first", "second", "third"]);
    assert_eq!(filter_labels, by_ids_labels);
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
fn default_fluent_execute_rejects_unindexed_full_scan() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let err = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .limit(1)
        .execute()
        .expect_err("default fluent execute should reject unindexed full scans");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::UnboundedFullScanRejected,
        "default fluent execute full scan",
    );
}

#[test]
fn default_fluent_execute_rows_rejects_unindexed_full_scan_without_policy_setup() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let err = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .limit(1)
        .execute_rows()
        .expect_err("default fluent execute_rows should reject unindexed full scans");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::UnboundedFullScanRejected,
        "default fluent execute_rows full scan",
    );
}

#[test]
fn default_fluent_execute_rejects_non_zero_offset_without_policy_setup() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .offset(1)
        .execute()
        .expect_err("default fluent execute should reject non-zero OFFSET");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::PublicQueryOffsetRejected,
        "default fluent execute offset",
    );
}

#[test]
fn default_fluent_execute_rejects_materialized_sort_without_policy_setup() {
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
        .load::<FilteredIndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("active").eq(true))
        .filter(crate::db::FieldRef::new("tier").eq("gold"))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute()
        .expect_err("default fluent execute should reject materialized sorts");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::SortRequiresMaterialization,
        "default fluent execute materialized sort",
    );
}

#[test]
fn default_fluent_execute_rejects_grouped_query_without_hard_limits() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count())
        .execute()
        .expect_err("default fluent execute should reject grouped reads without hard limits");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::GroupedQueryRequiresLimits,
        "default fluent execute missing grouped limits",
    );
}

#[test]
fn default_fluent_execute_admits_grouped_query_with_hard_limits() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let grouped = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count())
        .grouped_limits(10, 8192)
        .execute()
        .and_then(crate::db::LoadQueryResult::into_grouped)
        .expect("default fluent execute should admit grouped reads with hard limits");

    assert_eq!(grouped.rows().len(), 2);
}

#[test]
fn default_fluent_terminal_rejects_unindexed_full_scan() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let err = session
        .load::<SessionSqlEntity>()
        .count()
        .expect_err("default fluent terminal should reject unindexed full scans");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::UnboundedFullScanRejected,
        "default fluent terminal full scan",
    );
}

#[test]
fn default_fluent_exists_rejects_prior_raw_limit_before_admission() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .exists()
        .expect_err("exists() should own existence bounds instead of accepting raw limits");

    assert_raw_limit_before_exists_terminal(err, "default fluent exists raw limit");
}

#[test]
fn default_fluent_not_exists_rejects_prior_raw_limit_before_admission() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .not_exists()
        .expect_err("not_exists() should share the exists raw-limit intent gate");

    assert_raw_limit_before_exists_terminal(err, "default fluent not_exists raw limit");
}

#[test]
fn default_fluent_explain_exists_rejects_prior_raw_limit_before_planning() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .explain_exists()
        .expect_err("explain_exists() should report the same raw-limit intent conflict");

    assert_raw_limit_before_exists_terminal(err, "default fluent explain_exists raw limit");
}

#[test]
fn default_fluent_page_request_rejects_prior_raw_limit_before_admission() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .execute_paged(crate::db::PageRequest::first(2))
        .expect_err("PageRequest should own page size instead of accepting raw limits");

    assert_raw_limit_before_page_terminal(err, "default fluent page raw limit");
}

#[test]
fn default_fluent_page_request_clamps_requested_limit_to_public_cap() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    for offset in 0_u64..=100 {
        session
            .insert(IndexedSessionSqlEntity {
                id: crate::types::Ulid::from_u128(21_000 + u128::from(offset)),
                name: format!("S{offset:03}"),
                age: offset,
            })
            .expect("test row should insert");
    }

    let page = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .execute_paged(crate::db::PageRequest::first(1_000))
        .expect("PageRequest should clamp oversized public page limits");

    assert_eq!(
        page.response().count(),
        100,
        "oversized page request should clamp to the default public page cap",
    );
    assert_eq!(
        page.read_intent(),
        crate::db::ReadIntentKind::PublicPage,
        "paged responses should report the public-page read intent",
    );
    assert!(
        page.continuation_cursor().is_some(),
        "clamped public page should expose continuation when more rows exist",
    );
}

#[test]
fn default_fluent_page_request_uses_request_cursor_for_continuation() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Soren", 31)]);

    let first_page = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .execute_paged(crate::db::PageRequest::first(2))
        .expect("first PageRequest page should execute");
    let first_names = first_page
        .response()
        .iter()
        .map(|row| row.entity_ref().name.as_str())
        .collect::<Vec<_>>();
    let cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first PageRequest page should emit a continuation cursor"),
    );

    let second_page = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .execute_paged(crate::db::PageRequest::next(2, cursor))
        .expect("second PageRequest page should execute");
    let second_names = second_page
        .response()
        .iter()
        .map(|row| row.entity_ref().name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(first_names, ["Sam", "Sasha"]);
    assert_eq!(second_names, ["Soren"]);
    assert!(second_page.continuation_cursor().is_none());
}

#[test]
fn default_fluent_admin_batch_requires_trusted_read_unchecked() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .admin_batch(crate::db::AdminBatchRequest::new())
        .expect_err("admin_batch should be unavailable on the public read lane");

    assert_admin_batch_requires_trusted_read(err, "default fluent admin_batch");
}

#[test]
fn trusted_fluent_admin_batch_rejects_prior_raw_limit_before_planning() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .trusted_read_unchecked()
        .admin_batch(crate::db::AdminBatchRequest::new())
        .expect_err("admin_batch should own the trusted batch size");

    assert_raw_limit_before_admin_batch_terminal(err, "trusted fluent admin_batch raw limit");
}

#[test]
fn trusted_fluent_admin_batch_uses_hardcoded_batch_and_cursor_continuation() {
    reset_session_sql_store();
    let session = sql_session();
    for offset in 0_u64..=100 {
        session
            .insert(SessionSqlEntity {
                id: crate::types::Ulid::from_u128(31_000 + u128::from(offset)),
                name: format!("Admin{offset:03}"),
                age: offset,
            })
            .expect("admin batch fixture row should insert");
    }

    let first_batch = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("id"))
        .trusted_read_unchecked()
        .admin_batch(crate::db::AdminBatchRequest::new())
        .expect("trusted admin batch should execute");
    let cursor = crate::db::encode_cursor(
        first_batch
            .continuation_cursor()
            .expect("first admin batch should emit a continuation cursor"),
    );

    let second_batch = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("id"))
        .trusted_read_unchecked()
        .admin_batch(crate::db::AdminBatchRequest::next(cursor))
        .expect("trusted admin batch continuation should execute");

    assert_eq!(
        first_batch.response().count(),
        100,
        "trusted admin batch should use the engine-owned batch size",
    );
    assert_eq!(
        first_batch.read_intent(),
        crate::db::ReadIntentKind::TrustedAdminBatch,
        "admin batches should report the trusted-admin-batch read intent",
    );
    assert_eq!(
        second_batch.response().count(),
        1,
        "admin batch continuation should resume after the first batch",
    );
    assert_eq!(
        second_batch.read_intent(),
        crate::db::ReadIntentKind::TrustedAdminBatch,
        "admin batch continuations should keep the trusted-admin-batch read intent",
    );
    assert!(second_batch.continuation_cursor().is_none());
}

#[test]
fn default_fluent_execute_admits_indexed_bounded_query() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let response = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute()
        .expect("default fluent execute should admit indexed bounded reads");

    assert_eq!(response.count(), 2);
}

#[test]
fn default_fluent_execute_rows_keeps_low_level_raw_limit_window() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let response = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute_rows()
        .expect("execute_rows() remains the low-level bounded row-window terminal");

    assert_eq!(response.count(), 2);
}

#[test]
fn default_fluent_count_keeps_low_level_raw_limit_window() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let count = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .count()
        .expect("count() keeps the existing effective-window aggregate contract");

    assert_eq!(count, 2);
}

#[test]
fn default_fluent_count_exact_rejects_prior_raw_limit_before_admission() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .count_exact()
        .expect_err("count_exact() should reject raw row-window limits");

    assert_raw_limit_before_count_exact_terminal(err, "default fluent count_exact raw limit");
}

#[test]
fn default_fluent_sum_by_keeps_low_level_raw_limit_window() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let sum = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .sum_by("age")
        .expect("sum_by() keeps the existing effective-window aggregate contract");

    assert_eq!(sum, Some(crate::types::Decimal::from(54_u64)));
}

#[test]
fn default_fluent_sum_exact_rejects_prior_raw_limit_before_admission() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .sum_exact("age")
        .expect_err("sum_exact() should reject raw row-window limits");

    assert_raw_limit_before_sum_exact_terminal(err, "default fluent sum_exact raw limit");
}

#[test]
fn default_fluent_collect_complete_rejects_prior_raw_limit_before_admission() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let err = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .collect_complete()
        .expect_err("collect_complete() should reject raw row-window limits");

    assert_raw_limit_before_collect_complete_terminal(
        err,
        "default fluent collect_complete raw limit",
    );
}

#[test]
fn trusted_fluent_execute_keeps_existing_unbounded_behavior() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let response = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .limit(1)
        .execute_trusted()
        .expect("trusted fluent execute should keep existing behavior");

    assert_eq!(response.count(), 1);
}

#[test]
fn trusted_read_unchecked_execute_rows_bypasses_default_admission_explicitly() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Alice", 30), ("Bob", 24)]);

    let response = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .limit(1)
        .trusted_read_unchecked()
        .execute_rows()
        .expect("trusted_read_unchecked should explicitly bypass default read admission");

    assert_eq!(response.count(), 1);
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

    let query = session
        .load::<FilteredIndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("active").eq(true))
        .filter(crate::db::FieldRef::new("tier").eq("gold"))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2);
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
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
fn public_read_fluent_admission_rejects_grouped_query_without_policy_group_budgets() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count())
        .grouped_limits(10, 8192);
    let summary = session
        .evaluate_query_read_admission_policy(query.query(), &public_read_policy(10))
        .expect("fluent grouped admission should produce a summary");

    assert_admission_summary_rejection(
        &summary,
        QueryAdmissionRejection::GroupedQueryRequiresLimits,
        "fluent grouped missing policy budgets",
    );
}

#[test]
fn public_read_fluent_admission_rejects_grouped_query_without_query_hard_limits() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count());
    let err = session
        .ensure_query_read_admission_policy(
            query.query(),
            &public_grouped_read_policy(10, 10, 8192, None, 32_768),
        )
        .expect_err("fluent grouped admission should require query hard limits");

    assert_read_admission_rejection(
        err,
        QueryReadAdmissionCode::GroupedQueryRequiresLimits,
        "fluent grouped missing query hard limits",
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
fn public_read_fluent_admission_admits_grouped_query_with_group_budgets_without_limit() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count())
        .grouped_limits(10, 8192);
    let summary = session
        .evaluate_query_read_admission_policy(
            query.query(),
            &public_grouped_read_policy(10, 10, 8192, None, 32_768),
        )
        .expect("fluent grouped admission should produce a summary");
    let grouped = summary
        .grouped()
        .expect("grouped fluent admission should carry grouped facts");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(summary.limit(), None);
    assert_eq!(summary.returned_row_bound(), Some(10));
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound,
    );
    assert_eq!(grouped.group_field_count(), 1);
    assert_eq!(grouped.aggregate_count(), 1);
    assert_eq!(grouped.distinct_aggregate_count(), 0);
    assert_eq!(grouped.max_groups(), 10);
    assert_eq!(grouped.max_group_bytes(), 8192);
}

#[test]
fn public_read_fluent_admission_rejects_grouped_query_above_policy_budgets() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count())
        .grouped_limits(11, 8193);
    let summary = session
        .evaluate_query_read_admission_policy(
            query.query(),
            &public_grouped_read_policy(10, 10, 8192, None, 32_768),
        )
        .expect("fluent grouped admission should produce a summary");

    assert_admission_summary_rejection(
        &summary,
        QueryAdmissionRejection::GroupedQueryExceedsBudget,
        "fluent grouped budget above policy",
    );
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
fn public_read_fluent_admission_rejects_distinct_grouped_query_without_distinct_budget() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sam", 31), ("Sasha", 24)]);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count_by("age").distinct())
        .grouped_limits(10, 8192);
    let summary = session
        .evaluate_query_read_admission_policy(
            query.query(),
            &public_grouped_read_policy(10, 10, 8192, None, 32_768),
        )
        .expect("fluent grouped distinct admission should produce a summary");
    let grouped = summary
        .grouped()
        .expect("grouped distinct admission should carry grouped facts");

    assert_eq!(grouped.distinct_aggregate_count(), 1);
    assert_admission_summary_rejection(
        &summary,
        QueryAdmissionRejection::GroupedQueryRequiresLimits,
        "fluent grouped missing distinct budget",
    );
}

#[test]
fn public_read_fluent_admission_admits_distinct_grouped_query_with_distinct_budget() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sam", 31), ("Sasha", 24)]);

    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(crate::db::FieldRef::new("name").text_starts_with("S"))
        .group_by("name")
        .expect("group_by(name) should resolve")
        .aggregate(crate::db::count_by("age").distinct())
        .grouped_limits(10, 8192);
    let summary = session
        .evaluate_query_read_admission_policy(
            query.query(),
            &public_grouped_read_policy(10, 10, 8192, NonZeroU32::new(64), 32_768),
        )
        .expect("fluent grouped distinct admission should produce a summary");
    let grouped = summary
        .grouped()
        .expect("grouped distinct admission should carry grouped facts");

    assert_eq!(summary.decision(), QueryAdmissionDecision::Admitted);
    assert_eq!(summary.rejection(), None);
    assert_eq!(grouped.distinct_aggregate_count(), 1);
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

const fn public_read_policy_with_primary_key_input_caps(
    max_rows: u32,
    max_primary_key_input_terms: u32,
    max_primary_key_input_bytes: u32,
) -> QueryAdmissionPolicy {
    public_read_policy(max_rows).with_primary_key_input_caps(
        NonZeroU32::new(max_primary_key_input_terms)
            .expect("test primary-key input term cap should be non-zero"),
        NonZeroU32::new(max_primary_key_input_bytes)
            .expect("test primary-key input byte cap should be non-zero"),
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

fn assert_query_not_found(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryNotFound,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: icydb_diagnostic_code::QueryErrorKind::NotFound,
        }),
        "{context}: diagnostic detail drifted",
    );
}

fn assert_raw_limit_before_exists_terminal(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeExistsTerminal),
        "{context}: intent error variant drifted",
    );
}

fn assert_raw_limit_before_page_terminal(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforePageTerminal),
        "{context}: intent error variant drifted",
    );
}

fn assert_raw_limit_before_count_exact_terminal(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeCountExactTerminal),
        "{context}: intent error variant drifted",
    );
}

fn assert_raw_limit_before_sum_exact_terminal(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeSumExactTerminal),
        "{context}: intent error variant drifted",
    );
}

fn assert_raw_limit_before_collect_complete_terminal(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeCollectCompleteTerminal),
        "{context}: intent error variant drifted",
    );
}

fn assert_raw_limit_before_admin_batch_terminal(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::RawLimitBeforeAdminBatchTerminal),
        "{context}: intent error variant drifted",
    );
}

fn assert_admin_batch_requires_trusted_read(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::AdminBatchRequiresTrustedRead),
        "{context}: intent error variant drifted",
    );
}

fn assert_complete_read_too_many_rows(err: QueryError, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryIntent,
        "{context}: diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&DiagnosticDetail::QueryKind {
            kind: QueryErrorKind::Intent,
        }),
        "{context}: diagnostic detail drifted",
    );
    std::assert_matches!(
        err,
        QueryError::Intent(IntentError::CompleteReadTooManyRows),
        "{context}: intent error variant drifted",
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

fn assert_primary_key_exact_admission_summary(summary: &QueryAdmissionSummary, context: &str) {
    assert_eq!(
        summary.decision(),
        QueryAdmissionDecision::Admitted,
        "{context}: primary-key exact filter should be admitted",
    );
    assert_eq!(summary.rejection(), None, "{context}");
    assert_eq!(
        summary.selected_access(),
        QueryAdmissionAccessKind::ByKey,
        "{context}: primary-key equality filter should canonicalize to ByKey",
    );
    assert_eq!(summary.limit(), None, "{context}");
    assert_eq!(
        summary.scan_bound(),
        Some(1),
        "{context}: exact-key access should scan at most one row",
    );
    assert_eq!(
        summary.scan_bound_kind(),
        QueryBoundKind::Exact,
        "{context}: exact-key access should have an exact scan bound",
    );
    assert_eq!(
        summary.returned_row_bound(),
        Some(1),
        "{context}: exact-key access should return at most one row",
    );
    assert_eq!(
        summary.returned_row_bound_kind(),
        QueryBoundKind::ConservativeUpperBound,
        "{context}",
    );
}

fn text_projection_values(rows: Vec<Vec<crate::value::OutputValue>>, context: &str) -> Vec<String> {
    rows.into_iter()
        .map(|mut row| {
            assert!(
                row.len() == 1,
                "{context}: expected one projected column, got {row:?}",
            );
            match row.pop() {
                Some(crate::value::OutputValue::Text(value)) => value,
                value => panic!("{context}: expected text projection value, got {value:?}"),
            }
        })
        .collect()
}

fn public_read_sql_text_projection_values<E>(
    session: &crate::db::DbSession<super::SessionSqlCanister>,
    sql: &str,
    max_rows: u32,
    context: &str,
) -> Vec<String>
where
    E: crate::db::PersistedRow<Canister = super::SessionSqlCanister> + crate::traits::EntityValue,
{
    let result = session
        .execute_sql_query_with_read_admission_policy::<E>(sql, &public_read_policy(max_rows))
        .unwrap_or_else(|err| {
            panic!("{context}: SQL should execute as bounded public read: {err}")
        });
    let SqlStatementResult::Projection {
        row_count, rows, ..
    } = result
    else {
        panic!("{context}: SQL should return projection rows");
    };
    assert_eq!(row_count as usize, rows.len(), "{context}");

    text_projection_values(rows, context)
}
