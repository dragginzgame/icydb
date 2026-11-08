//! Public SQL uses the maintained structural page and admission contracts.

use super::*;
use crate::db::{RequestExecutionRoot, SqlStatementPageResult, sql_statement_dispatch};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource as Resource, ErrorCode,
    QueryReadAdmissionCode,
};

#[test]
fn public_sql_distinct_rejects_before_reading_and_remains_trusted() {
    seed();
    for sql in [
        "SELECT DISTINCT code FROM PlannerRow WHERE code >= 0 AND code < 10 ORDER BY code ASC",
        "SELECT DISTINCT code, MOD(code, 2) AS residue FROM PlannerRow WHERE code >= 0 AND code < 10 ORDER BY code ASC",
        "SELECT DISTINCT MOD(code, 2) AS residue FROM PlannerRow WHERE id IN (0, 1, 2, 3, 4)",
        "SELECT DISTINCT MOD(code, 3) AS residue FROM PlannerRow WHERE id IN (0, 1, 2, 3)",
    ] {
        let root = RequestExecutionRoot::__new_runtime_root();
        let session = new_request_session(&root);
        let dispatch = sql_statement_dispatch(sql).unwrap();
        let error = session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], None)
            .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_UNSUPPORTED
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert!(session.execute_trusted_sql_query(sql).is_ok());
    }
}

fn seed() {
    initialize_field_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "code", 1, AcceptedFieldKind::Nat64),
            field(3, "marker", 2, AcceptedFieldKind::Nat64),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "code_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::from_field_paths(vec![
                PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["code".into()],
                    AcceptedFieldKind::Nat64,
                    false,
                ),
            ]),
            None,
        )],
    );
    for id in 0..10 {
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "code".into(),
                        DynamicWriteCell::Value(InputValue::nat64(id)),
                    ),
                    (
                        "marker".into(),
                        DynamicWriteCell::Value(InputValue::nat64(u64::from(id == 9))),
                    ),
                ])],
            )
            .unwrap();
    }
}

fn scalar(sql: &str, continuation: Option<&str>) -> crate::db::LiveQueryPageOutput {
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    let dispatch = sql_statement_dispatch(sql).unwrap();
    let result = session
        .execute_sql_public_query_page_dispatch(&dispatch, &[], continuation)
        .unwrap();
    let SqlStatementPageResult::Scalar { page, .. } = result else {
        panic!("scalar SELECT must return scalar page progress");
    };
    page
}

fn collect(sql: &str) -> Vec<Vec<OutputValue>> {
    let mut rows = Vec::new();
    let mut cursor = None;
    for _ in 0..32 {
        let page = scalar(sql, cursor.as_deref());
        assert!(page.row_count <= 2);
        assert_ne!(page.work.envelope_identity, 0);
        assert_eq!(page.work.result_rows, page.row_count);
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return rows;
        };
        assert_ne!(cursor.as_ref(), Some(&next));
        cursor = Some(next);
    }
    panic!("bounded fixture traversal must exhaust");
}

#[test]
fn public_sql_matches_dynamic_pages_and_retains_total_limit() {
    seed();
    let sql = "SELECT id, code FROM PlannerRow WHERE code >= 0 AND code < 10 ORDER BY code ASC, id ASC LIMIT 7";
    let dynamic = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "code"])
        .filter(FilterExpr::and(vec![
            FieldRef::new("code").gte(InputValue::nat64(0)),
            FieldRef::new("code").lt(InputValue::nat64(10)),
        ]))
        .order_by(asc("code"))
        .order_by(asc("id"))
        .limit(7);
    let (mut sql_cursor, mut dynamic_cursor) = (None, None);
    let mut count = 0;
    for _ in 0..10 {
        let sql_page = scalar(sql, sql_cursor.as_deref());
        let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
        let dynamic_page = session
            .execute_public_live_page(&dynamic, dynamic_cursor.as_deref())
            .unwrap();
        assert_eq!(sql_page.rows, dynamic_page.rows);
        assert_eq!(sql_page.work, dynamic_page.work);
        count += sql_page.row_count;
        sql_cursor = sql_page.continuation;
        dynamic_cursor = dynamic_page.continuation;
        assert_eq!(sql_cursor.is_some(), dynamic_cursor.is_some());
        if sql_cursor.is_none() {
            break;
        }
    }
    assert_eq!(count, 7);
    assert!(sql_cursor.is_none());
}

#[test]
fn public_sql_keeps_expression_and_order_semantics() {
    seed();
    for sql in [
        "SELECT id + 1 AS next_id FROM PlannerRow WHERE code >= 0 AND code < 10 ORDER BY code DESC, id DESC LIMIT 5",
        "SELECT code * 2 AS doubled FROM PlannerRow WHERE id IN (0, 1, 2, 3) ORDER BY id DESC",
    ] {
        let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
        let SqlStatementResult::Projection { rows, .. } =
            session.execute_trusted_sql_query(sql).unwrap()
        else {
            panic!("trusted scalar reference must remain available");
        };
        assert_eq!(collect(sql), rows);
    }
}

#[test]
fn public_sql_grouped_pages_keep_the_existing_cursor_and_empty_result() {
    seed();
    let sql = "SELECT code, COUNT(*) AS total FROM PlannerRow WHERE code >= 0 AND code < 10 GROUP BY code ORDER BY code ASC LIMIT 2";
    let dispatch = sql_statement_dispatch(sql).unwrap();
    let mut cursor = None;
    let mut all = Vec::new();
    for _ in 0..10 {
        let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
        let result = session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], cursor.as_deref())
            .unwrap();
        let SqlStatementPageResult::Statement {
            result:
                SqlStatementResult::Grouped {
                    rows,
                    row_count,
                    next_cursor,
                    ..
                },
            ..
        } = result
        else {
            panic!("grouped SQL must retain its grouped cursor contract")
        };
        assert!(row_count <= 2);
        all.extend(rows);
        if next_cursor.is_none() {
            cursor = None;
            break;
        }
        assert_ne!(cursor.as_ref(), next_cursor.as_ref());
        cursor = next_cursor;
    }
    assert!(cursor.is_none());
    assert_eq!(
        all,
        (0..10)
            .map(|code| crate::db::GroupedRow::new(
                [OutputValue::nat64(code)],
                [OutputValue::nat64(1)],
            ))
            .collect::<Vec<_>>()
    );
    let empty = sql_statement_dispatch(
        "SELECT code, COUNT(*) FROM PlannerRow WHERE code = 99 GROUP BY code LIMIT 2",
    )
    .unwrap();
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    assert!(matches!(
        session
            .execute_sql_public_query_page_dispatch(&empty, &[], None)
            .unwrap(),
        SqlStatementPageResult::Statement {
            result: SqlStatementResult::Grouped {
                row_count: 0,
                next_cursor: None,
                ..
            },
            ..
        }
    ));
}

#[test]
fn public_sql_sparse_and_empty_pages_preserve_physical_progress() {
    seed();
    for (marker, expected) in [(1, vec![vec![OutputValue::nat64(9)]]), (2, vec![])] {
        let sql = format!(
            "SELECT id FROM PlannerRow WHERE id IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND marker = {marker} ORDER BY id ASC LIMIT 1"
        );
        let first = scalar(&sql, None);
        assert!(first.rows.is_empty(), "first sparse page: {first:?}");
        assert!(first.continuation.is_some());
        assert_eq!(first.work.entries_visited, 4);
        assert_eq!(collect(&sql), expected);
    }
}

#[test]
fn trusted_cache_warmth_never_grants_public_full_scan_admission() {
    seed();
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    let sql = "SELECT id FROM PlannerRow ORDER BY id LIMIT 1";
    assert!(session.execute_trusted_sql_query(sql).is_ok());
    let dispatch = sql_statement_dispatch(sql).unwrap();
    for _ in 0..2 {
        let error = session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], None)
            .unwrap_err();
        assert_eq!(
            error.diagnostic().detail(),
            Some(&DiagnosticDetail::QueryReadAdmission {
                reason: QueryReadAdmissionCode::UnboundedFullScanRejected,
            })
        );
    }
    assert!(session.execute_trusted_sql_query(sql).is_ok());
}

#[test]
fn public_sql_rejects_nonpublic_routes_and_metadata_continuations() {
    seed();
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    for sql in [
        "SELECT id FROM PlannerRow WHERE code = 1 ORDER BY id LIMIT 1 OFFSET 1",
        "SELECT COUNT(*) FROM PlannerRow WHERE code = 1",
        "EXPLAIN EXECUTION SELECT id FROM PlannerRow WHERE code = 1",
    ] {
        let dispatch = sql_statement_dispatch(sql).unwrap();
        let error = session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], None)
            .unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_UNSUPPORTED
        );
        assert!(session.execute_trusted_sql_query(sql).is_ok());
    }
    let dispatch = sql_statement_dispatch("SHOW ENTITIES").unwrap();
    assert!(matches!(
        session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], None)
            .unwrap(),
        SqlStatementPageResult::Statement {
            result: SqlStatementResult::ShowEntities { .. },
            ..
        }
    ));
    assert!(
        session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], Some("not-a-metadata-cursor"))
            .is_err()
    );
    for sql in [
        "DELETE FROM PlannerRow WHERE id = 1",
        "UPDATE PlannerRow SET code = 99 WHERE id = 1",
    ] {
        let dispatch = sql_statement_dispatch(sql).unwrap();
        assert!(
            session
                .execute_sql_public_query_page_dispatch(&dispatch, &[], None)
                .is_err()
        );
    }
    assert_eq!(
        collect(
            "SELECT id FROM PlannerRow WHERE code >= 0 AND code < 10 ORDER BY code ASC, id ASC"
        )
        .len(),
        10
    );
}

#[test]
fn public_sql_continuation_rejects_changed_query_and_tampered_bytes() {
    seed();
    let sql = "SELECT id FROM PlannerRow WHERE code >= 0 AND code < 10 ORDER BY code ASC, id ASC";
    let first = scalar(sql, None);
    let token = first.continuation.unwrap();
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    let different = sql_statement_dispatch(
        "SELECT id FROM PlannerRow WHERE code >= 1 AND code < 10 ORDER BY code ASC, id ASC",
    )
    .unwrap();
    assert!(
        session
            .execute_sql_public_query_page_dispatch(&different, &[], Some(&token))
            .is_err()
    );
    let dispatch = sql_statement_dispatch(sql).unwrap();
    let mut changed = token.into_bytes();
    changed[0] = if changed[0] == b'A' { b'B' } else { b'A' };
    let changed = String::from_utf8(changed).unwrap();
    assert!(
        session
            .execute_sql_public_query_page_dispatch(&dispatch, &[], Some(&changed))
            .is_err()
    );
}
