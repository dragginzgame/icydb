use super::*;

#[test]
fn explain_does_not_evaluate_order_pushdown() {
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_index(PUSHDOWN_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    assert_eq!(
        plan.explain().order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
fn explain_does_not_evaluate_descending_pushdown() {
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_index(PUSHDOWN_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });

    assert_eq!(
        plan.explain().order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
fn explain_does_not_evaluate_composite_pushdown_rejections() {
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "id",
                    OrderDirection::Asc,
                )],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::index_range(
                PUSHDOWN_INDEX,
                vec![],
                Bound::Included(Value::Text("alpha".to_string())),
                Bound::Excluded(Value::Text("omega".to_string())),
            )),
            AccessPlan::path(AccessPath::FullScan),
        ]),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    };

    assert_eq!(
        plan.explain().order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
fn explain_without_model_reports_missing_model_context() {
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_index(PUSHDOWN_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    assert_eq!(
        plan.explain().order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}
