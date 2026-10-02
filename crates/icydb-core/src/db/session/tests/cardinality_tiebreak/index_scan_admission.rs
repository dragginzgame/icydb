//! Public access proofs survive the shared planner-to-admission boundary.

use super::materialized_sort_admission::summary;
use super::*;
use crate::db::{
    ExhaustiveReadError, RequestExecutionRoot,
    commit::cursor_authentication_key,
    count,
    cursor::{ScalarPageToken, ScalarPageTokenWindow, decode_optional_cursor_token, encode_cursor},
    desc,
    executor::PageWorkEnvelope,
    query::admission::{QueryAdmissionAccessKind, QueryAdmissionPolicy, QueryAdmissionRejection},
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource as Resource, QueryReadAdmissionCode,
};

fn assert_full_scan_rejection(detail: Option<&DiagnosticDetail>) {
    assert_eq!(
        detail,
        Some(&DiagnosticDetail::QueryReadAdmission {
            reason: QueryReadAdmissionCode::UnboundedFullScanRejected,
        }),
    );
}

fn whole_index_query() -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("wide_branch").eq(InputValue::text("y".into())))
        .select(["id"])
        .order_by(asc("common"))
        .limit(5)
}

// Keep the runtime-minted authority, query, order and progress. Bind the test
// token to the public envelope through the current canonical encoder so resume
// exercises admission rather than the trusted/public window mismatch.
fn public_cursor(trusted: &str) -> String {
    let key = cursor_authentication_key().unwrap();
    let bytes = decode_optional_cursor_token(Some(trusted))
        .unwrap()
        .unwrap();
    let token = ScalarPageToken::decode(&bytes, &key).unwrap();
    let window = token.window();
    let public = ScalarPageToken::new(
        token.mode(),
        token.signature(),
        token.authority(),
        token.route_pin(),
        ScalarPageTokenWindow::new(
            window.initial_offset(),
            window.total_limit(),
            PageWorkEnvelope::public_scalar().profile_identity(),
        ),
        token.order_terms().to_vec(),
        token.progress().clone(),
    );
    encode_cursor(&public.encode(&key).unwrap())
}

#[test]
fn whole_index_admission_rejects_order_only_predicate_and_expression_shapes() {
    let setup = initialize();
    seed_rows(&setup);
    for filter in [
        None,
        Some(FieldRef::new("wide_branch").eq(InputValue::text("y".into()))),
        Some(FieldRef::new("common").eq_field("rare")),
    ] {
        for order in [asc("common"), desc("common")] {
            for limit in [1, 5] {
                let root = RequestExecutionRoot::__new_runtime_root();
                let session = new_request_session(&root);
                let mut query = DynamicQuery::new(ENTITY_NAME)
                    .select(["id"])
                    .order_by(order.clone())
                    .limit(limit);
                if let Some(filter) = &filter {
                    query = query.filter(filter.clone());
                }
                for _ in 0..2 {
                    let facts = summary(&session, &query);
                    assert_eq!(facts.selected_access(), QueryAdmissionAccessKind::FullScan);
                    assert_eq!(facts.selected_index(), Some("a_common_idx"));
                    assert_eq!(facts.scan_bound(), None);
                    assert_eq!(
                        QueryAdmissionPolicy::default_bounded_read()
                            .evaluate(facts)
                            .rejection(),
                        Some(QueryAdmissionRejection::UnboundedFullScanRejected),
                    );
                    let error = session.execute_public_live_page(&query, None).unwrap_err();
                    assert_full_scan_rejection(error.diagnostic().detail());
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn whole_index_admission_rejects_authenticated_live_resume_before_execution() {
    let session = initialize();
    seed_rows(&session);
    let query = whole_index_query();
    let first = session.execute_trusted_live_page(&query, None).unwrap();
    let cursor = first.continuation.unwrap();
    assert!(
        session
            .execute_trusted_live_page(&query, Some(&cursor))
            .is_ok()
    );
    let cursor = public_cursor(&cursor);
    let root = RequestExecutionRoot::__new_runtime_root();
    let public = new_request_session(&root);
    let error = public
        .execute_public_live_page(&query, Some(&cursor))
        .unwrap_err();
    assert_full_scan_rejection(error.diagnostic().detail());
    assert_eq!(root.observed(Resource::RowsVisited), 0);
}

#[test]
fn whole_index_admission_rejects_exhaustive_and_grouped_consumers() {
    let session = initialize();
    seed_rows(&session);
    let query = whole_index_query();
    let first = session
        .execute_trusted_exhaustive_page(&query, None, None)
        .unwrap();
    let cursor = public_cursor(first.continuation.as_deref().unwrap());
    for cursor in [None, Some(cursor.as_str())] {
        let root = RequestExecutionRoot::__new_runtime_root();
        let public = new_request_session(&root);
        let error = public
            .execute_public_exhaustive_page(&query, cursor, cursor.map(|_| &first.proof))
            .unwrap_err();
        let ExhaustiveReadError::Query(error) = error else {
            panic!("expected shared query admission rejection");
        };
        assert_full_scan_rejection(error.diagnostic().detail());
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
    let grouped = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("wide_branch").eq(InputValue::text("y".into())))
        .group_by("common")
        .aggregate(count())
        .order_by(asc("common"))
        .grouped_limits(4, 16 * 1024)
        .limit(1);
    let trusted = session
        .execute_trusted_dynamic_grouped_query(&grouped)
        .unwrap();
    assert_eq!(trusted.rows[0].aggregate_values(), &[OutputValue::nat64(6)]);
    for _ in 0..2 {
        let error = session
            .execute_public_dynamic_grouped_query(&grouped)
            .unwrap_err();
        assert_full_scan_rejection(error.diagnostic().detail());
    }
}

fn collect_pages(
    session: &DbSession<TestCanister>,
    query: &DynamicQuery,
    public: bool,
) -> Vec<Vec<OutputValue>> {
    let mut cursor = None;
    let mut rows = Vec::new();
    for _ in 0..16 {
        let page = if public {
            session.execute_public_live_page(query, cursor.as_deref())
        } else {
            session.execute_trusted_live_page(query, cursor.as_deref())
        }
        .unwrap();
        rows.extend(page.rows);
        cursor = page.continuation;
        if cursor.is_none() {
            return rows;
        }
    }
    panic!("fixture traversal must exhaust");
}

#[test]
fn selective_index_admission_preserves_prefix_and_range_page_equivalence() {
    let session = initialize();
    seed_rows(&session);
    let cases = [
        (
            FieldRef::new("common").eq(InputValue::text("everyone".into())),
            QueryAdmissionAccessKind::IndexPrefix,
        ),
        (
            FieldRef::new("common").gt(InputValue::text("a".into())),
            QueryAdmissionAccessKind::IndexRange,
        ),
        (
            FieldRef::new("common").gte(InputValue::text("everyone".into())),
            QueryAdmissionAccessKind::IndexRange,
        ),
        (
            FieldRef::new("common").lt(InputValue::text("z".into())),
            QueryAdmissionAccessKind::IndexRange,
        ),
        (
            FieldRef::new("common").lte(InputValue::text("everyone".into())),
            QueryAdmissionAccessKind::IndexRange,
        ),
    ];
    for (filter, kind) in cases {
        for order in [asc("common"), desc("common")] {
            let query = DynamicQuery::new(ENTITY_NAME)
                .filter(filter.clone())
                .select(["id"])
                .order_by(order)
                .limit(5);
            for _ in 0..2 {
                let facts = summary(&session, &query);
                assert_eq!(facts.selected_access(), kind);
                assert_eq!(
                    QueryAdmissionPolicy::default_bounded_read()
                        .evaluate(facts)
                        .rejection(),
                    None
                );
                let expected = collect_pages(&session, &query, false);
                assert_eq!(expected.len(), 5);
                assert_eq!(collect_pages(&session, &query, true), expected);
            }
        }
    }
}

#[test]
fn whole_index_admission_explain_keeps_physical_index_and_logical_scan_facts() {
    let session = initialize();
    seed_rows(&session);
    for _ in 0..2 {
        for prefix in ["EXPLAIN EXECUTION VERBOSE", "EXPLAIN EXECUTION JSON"] {
            let sql = format!(
                "{prefix} SELECT id FROM PlannerRow WHERE wide_branch = 'y' ORDER BY common LIMIT 1"
            );
            let SqlStatementResult::Explain(output) =
                session.execute_trusted_sql_query(&sql).unwrap()
            else {
                panic!("expected EXPLAIN output");
            };
            if prefix.ends_with("JSON") {
                assert!(output.contains("\"selected_access\":\"full_scan\""));
                assert!(output.contains("\"selected_index\":\"a_common_idx\""));
            } else {
                assert!(output.contains("selected_access=full_scan"));
                assert!(output.contains("selected_index=a_common_idx"));
                assert!(output.contains("access_strategy=IndexRange(a_common_idx)"));
            }
        }
    }
}
