//! Module: db::query::plan::tests::pushdown
//! Covers access pushdown and planner pushdown eligibility behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        executor::planning::route::derive_secondary_pushdown_applicability_from_contract,
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, LoadSpec, LogicalPlan,
            LogicalPushdownEligibility, OrderDirection, OrderSpec, PlannerRouteProfile, QueryMode,
        },
    },
    model::{
        entity::EntityModel,
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::generated("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);
const EXPRESSION_INDEX_FIELDS: [&str; 1] = ["name"];
const EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
const EXPRESSION_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
    "test::idx_name_lower",
    "test::ExpressionIndexStore",
    &EXPRESSION_INDEX_FIELDS,
    &EXPRESSION_INDEX_KEY_ITEMS,
    false,
);

crate::test_entity! {
    ident = PlanValidatePushdownEntity,
    id = Ulid,
    entity_name = "IndexedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanValidateExpressionPushdownEntity,
    id = Ulid,
    entity_name = "ExpressionIndexedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&EXPRESSION_INDEX_MODEL],
}

fn model_with_index() -> &'static EntityModel {
    <PlanValidatePushdownEntity as EntitySchema>::MODEL
}

fn model_with_expression_index() -> &'static EntityModel {
    <PlanValidateExpressionPushdownEntity as EntitySchema>::MODEL
}

fn load_plan(access: AccessPlan<Value>, order: Option<OrderSpec>) -> AccessPlannedQuery {
    AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate: None,
            order,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    }
}

fn load_union_plan(
    children: Vec<AccessPlan<Value>>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery {
    load_plan(AccessPlan::Union(children), order)
}

fn load_intersection_plan(
    children: Vec<AccessPlan<Value>>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery {
    load_plan(AccessPlan::Intersection(children), order)
}

fn order_spec(fields: &[(&str, OrderDirection)]) -> OrderSpec {
    OrderSpec {
        fields: fields
            .iter()
            .map(|(field, direction)| crate::db::query::plan::OrderTerm::field(*field, *direction))
            .collect(),
    }
}

fn load_index_prefix_plan(values: Vec<Value>, order: Option<OrderSpec>) -> AccessPlannedQuery {
    load_plan(
        AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values,
        }),
        order,
    )
}

fn load_index_range_plan(
    prefix: Vec<Value>,
    lower: Bound<Value>,
    upper: Bound<Value>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery {
    load_plan(
        AccessPlan::path(AccessPath::index_range(INDEX_MODEL, prefix, lower, upper)),
        order,
    )
}

fn load_expression_index_range_plan(
    prefix: Vec<Value>,
    lower: Bound<Value>,
    upper: Bound<Value>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery {
    load_plan(
        AccessPlan::path(AccessPath::index_range(
            EXPRESSION_INDEX_MODEL,
            prefix,
            lower,
            upper,
        )),
        order,
    )
}

fn contract_pushdown_applicability(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> PushdownApplicability {
    let mut finalized = plan.clone();
    finalized.finalize_planner_route_profile_for_model(model);
    let planner_route_profile = finalized.planner_route_profile();

    derive_secondary_pushdown_applicability_from_contract(&finalized, planner_route_profile)
}

#[test]
#[expect(clippy::too_many_lines)]
fn secondary_order_pushdown_core_cases() {
    struct Case {
        name: &'static str,
        plan: AccessPlannedQuery,
        expected: PushdownApplicability,
    }

    let cases = vec![
        Case {
            name: "eligible_pk_only_order",
            plan: load_index_prefix_plan(
                vec![Value::Text("a".to_string())],
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 1,
                },
            ),
        },
        Case {
            name: "reject_non_index_order_field",
            plan: load_index_prefix_plan(
                vec![Value::Text("a".to_string())],
                Some(order_spec(&[
                    ("rank", OrderDirection::Asc),
                    ("id", OrderDirection::Asc),
                ])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                        index: INDEX_MODEL.name(),
                        prefix_len: 1,
                        expected_suffix: vec![],
                        expected_full: vec!["tag".to_string()],
                        actual: vec!["rank".to_string()],
                    },
                ),
            ),
        },
        Case {
            name: "reject_full_scan_access",
            plan: load_plan(
                AccessPlan::path(AccessPath::FullScan),
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "reject_index_range_access_explicitly",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: INDEX_MODEL.name(),
                        prefix_len: 0,
                    },
                ),
            ),
        },
        Case {
            name: "accept_index_range_when_order_matches_range_field_plus_pk",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(order_spec(&[
                    ("tag", OrderDirection::Asc),
                    ("id", OrderDirection::Asc),
                ])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 0,
                },
            ),
        },
        Case {
            name: "accept_index_range_when_order_matches_range_field_plus_pk_desc",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(order_spec(&[
                    ("tag", OrderDirection::Desc),
                    ("id", OrderDirection::Desc),
                ])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 0,
                },
            ),
        },
        Case {
            name: "reject_composite_access_when_child_is_index_range",
            plan: load_union_plan(
                vec![
                    AccessPlan::path(AccessPath::index_range(
                        INDEX_MODEL,
                        vec![],
                        Bound::Included(Value::Text("a".to_string())),
                        Bound::Excluded(Value::Text("z".to_string())),
                    )),
                    AccessPlan::path(AccessPath::FullScan),
                ],
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: INDEX_MODEL.name(),
                        prefix_len: 0,
                    },
                ),
            ),
        },
        Case {
            name: "accept_descending_primary_key",
            plan: load_index_prefix_plan(
                vec![Value::Text("a".to_string())],
                Some(order_spec(&[("id", OrderDirection::Desc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 1,
                },
            ),
        },
    ];

    let model = model_with_index();
    for case in cases {
        assert_eq!(
            contract_pushdown_applicability(model, &case.plan),
            case.expected,
            "unexpected pushdown applicability for case '{}'",
            case.name
        );
    }
}

#[test]
#[expect(clippy::too_many_lines)]
fn secondary_order_pushdown_contract_matrix_is_exhaustive() {
    struct Case {
        name: &'static str,
        plan: AccessPlannedQuery,
        expected: PushdownApplicability,
    }

    let cases = vec![
        Case {
            name: "no_order_by_none",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                None,
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "no_order_by_empty_fields",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec { fields: vec![] }),
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "access_path_not_single_index_prefix",
            plan: load_plan(
                AccessPlan::path(AccessPath::FullScan),
                Some(OrderSpec {
                    fields: vec![crate::db::query::plan::OrderTerm::field(
                        "id",
                        OrderDirection::Asc,
                    )],
                }),
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "access_path_index_range_unsupported",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(OrderSpec {
                    fields: vec![crate::db::query::plan::OrderTerm::field(
                        "id",
                        OrderDirection::Asc,
                    )],
                }),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: INDEX_MODEL.name(),
                        prefix_len: 0,
                    },
                ),
            ),
        },
        Case {
            name: "access_path_index_range_supported_when_order_matches_suffix_plus_pk",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(OrderSpec {
                    fields: vec![
                        crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                        crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                    ],
                }),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 0,
                },
            ),
        },
        Case {
            name: "access_path_index_range_supported_when_order_matches_suffix_plus_pk_desc",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(OrderSpec {
                    fields: vec![
                        crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Desc),
                        crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
                    ],
                }),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 0,
                },
            ),
        },
        Case {
            name: "composite_access_path_contains_index_range_unsupported",
            plan: load_intersection_plan(
                vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![Value::Ulid(Ulid::from_u128(1))])),
                    AccessPlan::path(AccessPath::index_range(
                        INDEX_MODEL,
                        vec![],
                        Bound::Included(Value::Text("a".to_string())),
                        Bound::Excluded(Value::Text("z".to_string())),
                    )),
                ],
                Some(OrderSpec {
                    fields: vec![crate::db::query::plan::OrderTerm::field(
                        "id",
                        OrderDirection::Asc,
                    )],
                }),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: INDEX_MODEL.name(),
                        prefix_len: 0,
                    },
                ),
            ),
        },
        Case {
            name: "invalid_index_prefix_bounds",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![crate::db::query::plan::OrderTerm::field(
                        "id",
                        OrderDirection::Asc,
                    )],
                }),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                        prefix_len: 2,
                        index_field_len: 1,
                    },
                ),
            ),
        },
        Case {
            name: "missing_primary_key_tie_break",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![crate::db::query::plan::OrderTerm::field(
                        "tag",
                        OrderDirection::Asc,
                    )],
                }),
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "mixed_direction_not_eligible",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![
                        crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Desc),
                        crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                    ],
                }),
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "order_fields_do_not_match_index",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![
                        crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                        crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                    ],
                }),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                        index: INDEX_MODEL.name(),
                        prefix_len: 1,
                        expected_suffix: vec![],
                        expected_full: vec!["tag".to_string()],
                        actual: vec!["rank".to_string()],
                    },
                ),
            ),
        },
    ];

    let model = model_with_index();
    for case in cases {
        assert_eq!(
            contract_pushdown_applicability(model, &case.plan),
            case.expected,
            "unexpected pushdown contract outcome for case '{}'",
            case.name
        );
    }
}

#[test]
fn secondary_order_pushdown_contract_cases() {
    struct Case {
        name: &'static str,
        plan: AccessPlannedQuery,
        expected: PushdownApplicability,
    }

    let cases = vec![
        Case {
            name: "not_applicable_no_order",
            plan: load_index_prefix_plan(vec![Value::Text("a".to_string())], None),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "not_applicable_full_scan",
            plan: load_plan(
                AccessPlan::path(AccessPath::FullScan),
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::NotApplicable,
        },
        Case {
            name: "applicable_descending_direction_is_eligible",
            plan: load_index_prefix_plan(
                vec![Value::Text("a".to_string())],
                Some(order_spec(&[("id", OrderDirection::Desc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Eligible {
                    index: INDEX_MODEL.name(),
                    prefix_len: 1,
                },
            ),
        },
        Case {
            name: "applicable_index_range_explicit_rejection",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: INDEX_MODEL.name(),
                        prefix_len: 0,
                    },
                ),
            ),
        },
        Case {
            name: "applicable_composite_with_index_range_child_explicit_rejection",
            plan: load_union_plan(
                vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![Value::Ulid(Ulid::from_u128(2))])),
                    AccessPlan::path(AccessPath::index_range(
                        INDEX_MODEL,
                        vec![],
                        Bound::Included(Value::Text("a".to_string())),
                        Bound::Excluded(Value::Text("z".to_string())),
                    )),
                ],
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: PushdownApplicability::Applicable(
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: INDEX_MODEL.name(),
                        prefix_len: 0,
                    },
                ),
            ),
        },
    ];

    let model = model_with_index();
    for case in cases {
        assert_eq!(
            contract_pushdown_applicability(model, &case.plan),
            case.expected,
            "unexpected pushdown applicability for case '{}'",
            case.name
        );
    }
}

#[test]
fn secondary_order_pushdown_contract_honors_planner_logical_gate() {
    let model = model_with_index();
    let mut plan = load_index_prefix_plan(
        vec![Value::Text("a".to_string())],
        Some(order_spec(&[("id", OrderDirection::Asc)])),
    );
    plan.finalize_planner_route_profile_for_model(model);
    let finalized = plan;
    let planner_route_profile = finalized.planner_route_profile();
    let gated_profile = PlannerRouteProfile::new(
        ContinuationPolicy::new(false, false, true),
        LogicalPushdownEligibility::new(false, false, false),
        planner_route_profile.secondary_order_contract().cloned(),
    );

    assert_eq!(
        derive_secondary_pushdown_applicability_from_contract(&finalized, &gated_profile),
        PushdownApplicability::NotApplicable
    );
}

#[test]
fn secondary_order_pushdown_contract_rejects_non_deterministic_tie_break_shape() {
    let model = model_with_index();
    let mut plan = load_index_prefix_plan(
        vec![Value::Text("a".to_string())],
        Some(order_spec(&[("tag", OrderDirection::Asc)])),
    );
    plan.finalize_planner_route_profile_for_model(model);
    let finalized = plan;
    let planner_route_profile = finalized.planner_route_profile();

    assert_eq!(
        derive_secondary_pushdown_applicability_from_contract(&finalized, planner_route_profile),
        PushdownApplicability::NotApplicable,
        "route pushdown must not activate when ORDER BY omits deterministic PK tie-break",
    );
}

#[test]
fn secondary_order_pushdown_contract_rejects_mixed_direction_shape() {
    let model = model_with_index();
    let mut plan = load_index_prefix_plan(
        vec![Value::Text("a".to_string())],
        Some(order_spec(&[
            ("tag", OrderDirection::Desc),
            ("id", OrderDirection::Asc),
        ])),
    );
    plan.finalize_planner_route_profile_for_model(model);
    let finalized = plan;
    let planner_route_profile = finalized.planner_route_profile();

    assert_eq!(
        derive_secondary_pushdown_applicability_from_contract(&finalized, planner_route_profile),
        PushdownApplicability::NotApplicable,
        "route pushdown must not activate when ORDER BY direction contract is mixed",
    );
}

#[test]
fn secondary_order_pushdown_contract_accepts_expression_index_order_terms() {
    let model = model_with_expression_index();

    assert_eq!(
        contract_pushdown_applicability(
            model,
            &load_expression_index_range_plan(
                vec![],
                Bound::Unbounded,
                Bound::Unbounded,
                Some(order_spec(&[
                    ("LOWER(name)", OrderDirection::Asc),
                    ("id", OrderDirection::Asc),
                ])),
            ),
        ),
        PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Eligible {
            index: EXPRESSION_INDEX_MODEL.name(),
            prefix_len: 0,
        }),
        "expression ORDER BY should activate the same pushdown contract when one matching expression index path is selected",
    );
}
