use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        contracts::ReadConsistency,
        query::{
            intent::{LoadSpec, QueryMode},
            plan::{
                AccessPlannedQuery, LogicalPlan, OrderDirection, OrderSpec,
                assess_secondary_order_pushdown, assess_secondary_order_pushdown_if_applicable,
                assess_secondary_order_pushdown_if_applicable_validated,
            },
        },
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity! {
    ident = PlanValidatePushdownEntity,
    id = Ulid,
    entity_name = "IndexedEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

fn model_with_index() -> &'static EntityModel {
    <PlanValidatePushdownEntity as EntitySchema>::MODEL
}

fn load_plan(access: AccessPlan<Value>, order: Option<OrderSpec>) -> AccessPlannedQuery<Value> {
    AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access,
    }
}

fn load_union_plan(
    children: Vec<AccessPlan<Value>>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery<Value> {
    load_plan(AccessPlan::Union(children), order)
}

fn load_intersection_plan(
    children: Vec<AccessPlan<Value>>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery<Value> {
    load_plan(AccessPlan::Intersection(children), order)
}

fn order_spec(fields: &[(&str, OrderDirection)]) -> OrderSpec {
    OrderSpec {
        fields: fields
            .iter()
            .map(|(field, direction)| ((*field).to_string(), *direction))
            .collect(),
    }
}

fn load_index_prefix_plan(
    values: Vec<Value>,
    order: Option<OrderSpec>,
) -> AccessPlannedQuery<Value> {
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
) -> AccessPlannedQuery<Value> {
    load_plan(
        AccessPlan::path(AccessPath::index_range(INDEX_MODEL, prefix, lower, upper)),
        order,
    )
}

#[test]
#[expect(clippy::too_many_lines)]
fn secondary_order_pushdown_core_cases() {
    struct Case {
        name: &'static str,
        plan: AccessPlannedQuery<Value>,
        expected: SecondaryOrderPushdownEligibility,
    }

    let cases = vec![
        Case {
            name: "eligible_pk_only_order",
            plan: load_index_prefix_plan(
                vec![Value::Text("a".to_string())],
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: SecondaryOrderPushdownEligibility::Eligible {
                index: INDEX_MODEL.name,
                prefix_len: 1,
            },
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
            expected: SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: INDEX_MODEL.name,
                    prefix_len: 1,
                    expected_suffix: vec![],
                    expected_full: vec!["tag".to_string()],
                    actual: vec!["rank".to_string()],
                },
            ),
        },
        Case {
            name: "reject_full_scan_access",
            plan: load_plan(
                AccessPlan::path(AccessPath::FullScan),
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
        },
        Case {
            name: "reject_index_range_access_explicitly",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(order_spec(&[("id", OrderDirection::Asc)])),
            ),
            expected: SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: INDEX_MODEL.name,
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
            expected: SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: INDEX_MODEL.name,
                    prefix_len: 0,
                },
            ),
        },
        Case {
            name: "accept_descending_primary_key",
            plan: load_index_prefix_plan(
                vec![Value::Text("a".to_string())],
                Some(order_spec(&[("id", OrderDirection::Desc)])),
            ),
            expected: SecondaryOrderPushdownEligibility::Eligible {
                index: INDEX_MODEL.name,
                prefix_len: 1,
            },
        },
    ];

    let model = model_with_index();
    for case in cases {
        assert_eq!(
            assess_secondary_order_pushdown(model, &case.plan),
            case.expected,
            "unexpected pushdown eligibility for case '{}'",
            case.name
        );
    }
}

#[test]
#[expect(clippy::too_many_lines)]
fn secondary_order_pushdown_rejection_matrix_is_exhaustive() {
    struct RejectionCase {
        name: &'static str,
        plan: AccessPlannedQuery<Value>,
        expected: SecondaryOrderPushdownRejection,
    }

    let cases = vec![
        RejectionCase {
            name: "no_order_by_none",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                None,
            ),
            expected: SecondaryOrderPushdownRejection::NoOrderBy,
        },
        RejectionCase {
            name: "no_order_by_empty_fields",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec { fields: vec![] }),
            ),
            expected: SecondaryOrderPushdownRejection::NoOrderBy,
        },
        RejectionCase {
            name: "access_path_not_single_index_prefix",
            plan: load_plan(
                AccessPlan::path(AccessPath::FullScan),
                Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Asc)],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        },
        RejectionCase {
            name: "access_path_index_range_unsupported",
            plan: load_index_range_plan(
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
                Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Asc)],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index: INDEX_MODEL.name,
                prefix_len: 0,
            },
        },
        RejectionCase {
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
                    fields: vec![("id".to_string(), OrderDirection::Asc)],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index: INDEX_MODEL.name,
                prefix_len: 0,
            },
        },
        RejectionCase {
            name: "invalid_index_prefix_bounds",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Asc)],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                prefix_len: 2,
                index_field_len: 1,
            },
        },
        RejectionCase {
            name: "missing_primary_key_tie_break",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![("tag".to_string(), OrderDirection::Asc)],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: "id".to_string(),
            },
        },
        RejectionCase {
            name: "mixed_direction_not_eligible",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![
                        ("tag".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                field: "tag".to_string(),
            },
        },
        RejectionCase {
            name: "order_fields_do_not_match_index",
            plan: load_plan(
                AccessPlan::path(AccessPath::IndexPrefix {
                    index: INDEX_MODEL,
                    values: vec![Value::Text("a".to_string())],
                }),
                Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Asc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
            ),
            expected: SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                index: INDEX_MODEL.name,
                prefix_len: 1,
                expected_suffix: vec![],
                expected_full: vec!["tag".to_string()],
                actual: vec!["rank".to_string()],
            },
        },
    ];

    let model = model_with_index();
    for case in cases {
        let actual = assess_secondary_order_pushdown(model, &case.plan);
        assert_eq!(
            actual,
            SecondaryOrderPushdownEligibility::Rejected(case.expected),
            "unexpected rejection for case '{}'",
            case.name
        );
    }
}

#[test]
fn secondary_order_pushdown_if_applicable_cases() {
    struct Case {
        name: &'static str,
        plan: AccessPlannedQuery<Value>,
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
                    index: INDEX_MODEL.name,
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
                        index: INDEX_MODEL.name,
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
                        index: INDEX_MODEL.name,
                        prefix_len: 0,
                    },
                ),
            ),
        },
    ];

    let model = model_with_index();
    for case in cases {
        assert_eq!(
            assess_secondary_order_pushdown_if_applicable(model, &case.plan),
            case.expected,
            "unexpected pushdown applicability for case '{}'",
            case.name
        );
    }
}

#[test]
fn secondary_order_pushdown_if_applicable_validated_matches_defensive_assessor() {
    let model = model_with_index();

    let descending_plan = load_plan(
        AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string())],
        }),
        Some(order_spec(&[("id", OrderDirection::Desc)])),
    );
    assert_eq!(
        assess_secondary_order_pushdown_if_applicable_validated(model, &descending_plan),
        assess_secondary_order_pushdown_if_applicable(model, &descending_plan),
    );

    let non_applicable_plan = load_plan(
        AccessPlan::path(AccessPath::FullScan),
        Some(order_spec(&[("id", OrderDirection::Asc)])),
    );
    assert_eq!(
        assess_secondary_order_pushdown_if_applicable_validated(model, &non_applicable_plan),
        assess_secondary_order_pushdown_if_applicable(model, &non_applicable_plan),
    );

    let index_range_plan = load_index_range_plan(
        vec![],
        Bound::Included(Value::Text("a".to_string())),
        Bound::Excluded(Value::Text("z".to_string())),
        Some(order_spec(&[("id", OrderDirection::Asc)])),
    );
    assert_eq!(
        assess_secondary_order_pushdown_if_applicable_validated(model, &index_range_plan),
        assess_secondary_order_pushdown_if_applicable(model, &index_range_plan),
    );

    let composite_index_range_plan = load_union_plan(
        vec![
            AccessPlan::path(AccessPath::ByKeys(vec![Value::Ulid(Ulid::from_u128(3))])),
            AccessPlan::path(AccessPath::index_range(
                INDEX_MODEL,
                vec![],
                Bound::Included(Value::Text("a".to_string())),
                Bound::Excluded(Value::Text("z".to_string())),
            )),
        ],
        Some(order_spec(&[("id", OrderDirection::Asc)])),
    );
    assert_eq!(
        assess_secondary_order_pushdown_if_applicable_validated(model, &composite_index_range_plan),
        assess_secondary_order_pushdown_if_applicable(model, &composite_index_range_plan),
    );
}
