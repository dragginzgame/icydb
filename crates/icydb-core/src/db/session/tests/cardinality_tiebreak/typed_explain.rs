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
