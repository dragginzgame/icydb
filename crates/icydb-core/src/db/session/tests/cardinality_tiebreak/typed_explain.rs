//! Current typed authority and ordinary planner freshness, not forced replanning.

use super::*;
use crate::db::{
    DynamicTypedEntityBinding, ExplainPlan, TypedEntityDescriptor, TypedFieldDescriptor,
    TypedFieldType,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

const DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &["db::session::tests::cardinality_tiebreak::PlannerRow::id"],
    &[TypedFieldDescriptor::new(
        "db::session::tests::cardinality_tiebreak::PlannerRow::id",
        TypedFieldType::Scalar(icydb_schema::ScalarType::Nat64),
        false,
    )],
);

fn binding<C: CanisterKind>(session: &DbSession<C>) -> DynamicTypedEntityBinding {
    session.issue_typed_entity_binding(&DESCRIPTOR).unwrap()
}

fn request() -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::and(vec![
            FieldRef::new("common").eq(InputValue::text("everyone".to_string())),
            FieldRef::new("rare").eq(InputValue::text("group-a".to_string())),
        ]))
        .order_by(asc("id"))
        .limit(20)
}

fn inspect<C: CanisterKind>(
    session: &DbSession<C>,
    binding: &DynamicTypedEntityBinding,
) -> ExplainPlan {
    session
        .explain_query_for_typed_binding(binding, &request())
        .unwrap()
        .unwrap()
}

#[test]
fn typed_explain_rechecks_accepted_binding_before_returning_warm_diagnostics() {
    let session = initialize();
    seed_rows(&session);
    let binding = binding(&session);
    let first = inspect(&session, &binding);
    assert_eq!(inspect(&session, &binding), first);
    let store = session.db.store_handle(STORE_PATH).unwrap();
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::INITIAL,
        &schema_candidate_at(STORE_PATH, AcceptedSchemaRevision::new(2)),
    )
    .unwrap();
    assert!(
        session
            .explain_query_for_typed_binding(&binding, &request())
            .unwrap()
            .is_none()
    );
    let current = session.issue_typed_entity_binding(&DESCRIPTOR).unwrap();
    assert!(
        session
            .explain_query_for_typed_binding(&current, &request())
            .unwrap()
            .is_some()
    );
    assert!(first.render_json_canonical().is_ok());
}

#[test]
fn typed_explain_follows_metadata_lifecycle_without_scanning_result_rows() {
    let session = initialize_journaled();
    seed_rows(&session);
    let binding = binding(&session);
    let unavailable = inspect(&session, &binding);
    assert_eq!(
        unavailable.access_decision().selected.index_name.as_deref(),
        Some("a_common_idx")
    );
    assert_eq!(
        unavailable.access_decision().cardinality_evidence_state,
        "unavailable"
    );
    drive_journaled_cardinality_to_ready(&session);
    let ready = inspect(&session, &binding);
    assert_eq!(
        ready.access_decision().selected.index_name.as_deref(),
        Some("z_rare_idx")
    );
    assert_eq!(
        ready.access_decision().cardinality_evidence_state,
        "exact_at_selection"
    );
    assert_eq!(inspect(&session, &binding), ready);
}

#[test]
fn typed_explain_preserves_exact_selection_reuse_and_never_executes_rows() {
    let session = initialize();
    seed_rows(&session);
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let reader = new_request_session(&root);
    let binding = binding(&reader);
    let initial = inspect(&reader, &binding);
    let compilations = root.observed(Resource::PlanCompilations);
    assert!(compilations > 0);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    insert_row(&session, 100, "everyone", "group-a");
    assert_eq!(inspect(&reader, &binding), initial);
    assert_eq!(root.observed(Resource::PlanCompilations), compilations);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    let SqlStatementResult::Explain(sql) = reader.execute_trusted_sql_query(
        "EXPLAIN JSON SELECT * FROM PlannerRow WHERE common = 'everyone' AND rare = 'group-a' ORDER BY id LIMIT 20"
    ).unwrap() else { panic!("logical SQL explain result required") };
    assert_eq!(initial.render_json_canonical().unwrap(), sql);
}

#[test]
fn live_range_pagination_advances_without_diagnostic_warmup() {
    use crate::db::desc;

    for descending in [false, true] {
        let session = initialize();
        seed_rows(&session);
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .filter(FilterExpr::and(vec![
                FieldRef::new("rare").gte("group-a"),
                FieldRef::new("rare").lt("group-b"),
            ]))
            .order_by(if descending { desc("id") } else { asc("id") })
            .limit(20);
        let mut expected = (0..6_u64)
            .map(|id| vec![OutputValue::nat64(id)])
            .collect::<Vec<_>>();
        if descending {
            expected.reverse();
        }
        let mut continuation = None;
        let mut actual = Vec::new();
        for _ in 0..8 {
            let page = session
                .execute_trusted_live_page(&query, continuation.as_deref())
                .unwrap();
            actual.extend(page.rows);
            assert_eq!(actual, expected[..actual.len().min(expected.len())]);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        assert!(continuation.is_none());
        assert_eq!(actual, expected);
    }
}

#[test]
fn typed_explain_point_range_and_large_membership_preserve_reports_and_rows() {
    let setup = initialize();
    seed_rows(&setup);
    let members = (0..1024_u64)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    for (filter, predicate, expected_rows) in [
        (FieldRef::new("id").eq(1_u64), "id = 1".to_string(), 1),
        (
            FilterExpr::and(vec![
                FieldRef::new("rare").gte("group-a"),
                FieldRef::new("rare").lt("group-b"),
            ]),
            "rare >= 'group-a' AND rare < 'group-b'".to_string(),
            6,
        ),
        (
            FieldRef::new("id").in_list(0..1024_u64),
            format!("id IN ({members})"),
            12,
        ),
    ] {
        let root = crate::db::RequestExecutionRoot::__new_runtime_root();
        let reader = new_request_session(&root);
        // Earlier cases may share a parameterized template with this shape.
        // Explicitly start cold before asserting this request compiled a plan.
        reader.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let binding = binding(&reader);
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(filter)
            .order_by(asc("id"))
            .limit(20);
        let explain = || {
            reader
                .explain_query_for_typed_binding(&binding, &query)
                .unwrap()
                .unwrap()
        };
        let cold = explain();
        let compilations = root.observed(Resource::PlanCompilations);
        assert!(compilations > 0);
        for _ in 0..2 {
            assert_eq!(explain(), cold);
            assert_eq!(root.observed(Resource::PlanCompilations), compilations);
        }
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        let sql = format!("SELECT * FROM PlannerRow WHERE {predicate} ORDER BY id LIMIT 20");
        let SqlStatementResult::Explain(report) = reader
            .execute_trusted_sql_query(&format!("EXPLAIN JSON {sql}"))
            .unwrap()
        else {
            panic!("logical SQL explain result required")
        };
        assert_eq!(cold.render_json_canonical().unwrap(), report);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        let expected = projection_rows(&setup, &sql);
        assert_eq!(expected.len(), expected_rows);
        for _ in 0..2 {
            // The native fixture deliberately returns small live pages; compare
            // the complete traversal, not its first page, with SQL's result.
            // Missing IN keys also consume its four-entry work envelope. Each
            // resumed endpoint invocation gets a fresh request budget.
            let mut continuation = None;
            let mut actual = Vec::new();
            for _ in 0..=1024 {
                let page_reader =
                    new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
                let page = page_reader
                    .execute_trusted_live_page(&query, continuation.as_deref())
                    .unwrap();
                actual.extend(page.rows);
                assert_eq!(
                    actual,
                    expected[..actual.len().min(expected.len())],
                    "{predicate}: every page must extend the expected result prefix",
                );
                if page.continuation.is_some() {
                    assert_ne!(page.continuation, continuation);
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            assert!(continuation.is_none(), "{predicate}: traversal must finish");
            assert_eq!(actual, expected);
        }
        // The detached diagnostic survives ordinary execution and stays unchanged.
        assert_eq!(cold.render_json_canonical().unwrap(), report);
    }
}
