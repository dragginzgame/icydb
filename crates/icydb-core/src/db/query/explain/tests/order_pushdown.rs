use super::*;

#[test]
fn explain_does_not_evaluate_order_pushdown() {
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
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
            index: PUSHDOWN_INDEX,
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
            index: PUSHDOWN_INDEX,
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
#[expect(clippy::too_many_lines)]
fn explain_pushdown_conversion_covers_all_variants() {
    let cases = vec![
        (
            SecondaryOrderPushdownEligibility::Eligible {
                index: "explain::pushdown_tag",
                prefix_len: 1,
            },
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index: "explain::pushdown_tag",
                prefix_len: 1,
            },
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
            ExplainOrderPushdown::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: 3,
                    index_field_len: 2,
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: 3,
                    index_field_len: 2,
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                    field: "id".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                    field: "id".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                    field: "id".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                    field: "id".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: "rank".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: "rank".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                    expected_suffix: vec!["rank".to_string()],
                    expected_full: vec!["group".to_string(), "rank".to_string()],
                    actual: vec!["other".to_string()],
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                    expected_suffix: vec!["rank".to_string()],
                    expected_full: vec!["group".to_string(), "rank".to_string()],
                    actual: vec!["other".to_string()],
                },
            ),
        ),
    ];

    for (input, expected) in cases {
        assert_eq!(ExplainOrderPushdown::from(input), expected);
    }
}
