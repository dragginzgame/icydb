// NOTE: Invalid helpers remain only for intentionally invalid schemas.
#![expect(clippy::too_many_lines)]
use super::{
    AccessPlanError, CursorPlanError, OrderPlanError, PlanError, PolicyPlanError,
    PushdownApplicability, SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
    assess_secondary_order_pushdown, assess_secondary_order_pushdown_if_applicable,
    assess_secondary_order_pushdown_if_applicable_validated, validate_logical_plan_model,
};
use crate::{
    db::query::{
        ReadConsistency,
        intent::{LoadSpec, QueryMode},
        plan::{AccessPath, AccessPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
        predicate::{SchemaInfo, ValidateError},
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::IndexModel,
    },
    test_fixtures::InvalidEntityModelBuilder,
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};
use std::ops::Bound;

fn field(name: &'static str, kind: FieldKind) -> FieldModel {
    FieldModel { name, kind }
}

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity_schema! {
    PlanValidateIndexedEntity,
    id = Ulid,
    path = "plan_validate::IndexedEntity",
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

crate::test_entity_schema! {
    PlanValidateListEntity,
    id = Ulid,
    path = "plan_validate::ListEntity",
    entity_name = "ListEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tags", FieldKind::List(&FieldKind::Text)),
    ],
    indexes = [],
}

// Helper for tests that need the indexed model derived from a typed schema.
fn model_with_index() -> &'static EntityModel {
    <PlanValidateIndexedEntity as EntitySchema>::MODEL
}

fn load_plan(access: AccessPlan<Value>, order: Option<OrderSpec>) -> LogicalPlan<Value> {
    LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access,
        predicate: None,
        order,
        delete_limit: None,
        page: None,
        consistency: ReadConsistency::MissingOk,
    }
}

fn load_union_plan(
    children: Vec<AccessPlan<Value>>,
    order: Option<OrderSpec>,
) -> LogicalPlan<Value> {
    load_plan(AccessPlan::Union(children), order)
}

fn load_intersection_plan(
    children: Vec<AccessPlan<Value>>,
    order: Option<OrderSpec>,
) -> LogicalPlan<Value> {
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

fn load_index_prefix_plan(values: Vec<Value>, order: Option<OrderSpec>) -> LogicalPlan<Value> {
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
) -> LogicalPlan<Value> {
    load_plan(
        AccessPlan::path(AccessPath::IndexRange {
            index: INDEX_MODEL,
            prefix,
            lower,
            upper,
        }),
        order,
    )
}

#[test]
fn model_rejects_missing_primary_key() {
    // Invalid test scaffolding: models are hand-built to exercise
    // validation failures that helpers intentionally prevent.
    let fields: &'static [FieldModel] =
        Box::leak(vec![field("id", FieldKind::Ulid)].into_boxed_slice());
    let missing_pk = Box::leak(Box::new(field("missing", FieldKind::Ulid)));

    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        missing_pk,
        fields,
        &[],
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidPrimaryKey { .. })
    ));
}

#[test]
fn model_rejects_duplicate_fields() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![field("dup", FieldKind::Text), field("dup", FieldKind::Text)],
        0,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::DuplicateField { .. })
    ));
}

#[test]
fn model_rejects_invalid_primary_key_type() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![field("pk", FieldKind::List(&FieldKind::Text))],
        0,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidPrimaryKeyType { .. })
    ));
}

#[test]
fn model_rejects_index_unknown_field() {
    const INDEX_FIELDS: [&str; 1] = ["missing"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "test::idx_missing",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] =
        Box::leak(vec![field("id", FieldKind::Ulid)].into_boxed_slice());
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldUnknown { .. })
    ));
}

#[test]
fn model_rejects_index_non_queryable_field() {
    const INDEX_FIELDS: [&str; 1] = ["broken"];
    const INDEX_MODEL: IndexModel =
        IndexModel::new("test::idx_broken", "test::IndexStore", &INDEX_FIELDS, false);
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("broken", FieldKind::Structured { queryable: false }),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldNotQueryable { .. })
    ));
}

#[test]
fn model_rejects_index_map_field_in_0_7_x() {
    const INDEX_FIELDS: [&str; 1] = ["attributes"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "test::idx_attributes",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field(
                "attributes",
                FieldKind::Map {
                    key: &FieldKind::Text,
                    value: &FieldKind::Uint,
                },
            ),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldMapNotQueryable { .. })
    ));
}

#[test]
fn model_rejects_duplicate_index_names() {
    const INDEX_FIELDS_A: [&str; 1] = ["id"];
    const INDEX_FIELDS_B: [&str; 1] = ["other"];
    const INDEX_A: IndexModel = IndexModel::new(
        "test::dup_index",
        "test::IndexStore",
        &INDEX_FIELDS_A,
        false,
    );
    const INDEX_B: IndexModel = IndexModel::new(
        "test::dup_index",
        "test::IndexStore",
        &INDEX_FIELDS_B,
        false,
    );
    const INDEXES: [&IndexModel; 2] = [&INDEX_A, &INDEX_B];

    let fields: &'static [FieldModel] = Box::leak(
        vec![
            field("id", FieldKind::Ulid),
            field("other", FieldKind::Text),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::DuplicateIndexName { .. })
    ));
}

#[test]
fn plan_rejects_unorderable_field() {
    let model = <PlanValidateListEntity as EntitySchema>::MODEL;

    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("tags".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan).expect_err("unorderable field");
    assert!(matches!(err, PlanError::Order(inner) if matches!(
        inner.as_ref(),
        OrderPlanError::UnorderableField { .. }
    )));
}

#[test]
fn plan_rejects_index_prefix_too_long() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
        }),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err =
        validate_logical_plan_model(&schema, model, &plan).expect_err("index prefix too long");
    assert!(matches!(err, PlanError::Access(inner) if matches!(
        inner.as_ref(),
        AccessPlanError::IndexPrefixTooLong { .. }
    )));
}

#[test]
fn plan_rejects_empty_index_prefix() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![],
        }),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan).expect_err("index prefix empty");
    assert!(matches!(err, PlanError::Access(inner) if matches!(
        inner.as_ref(),
        AccessPlanError::IndexPrefixEmpty
    )));
}

#[test]
fn plan_accepts_model_based_validation() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::nil()))),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    validate_logical_plan_model(&schema, model, &plan).expect("valid plan");
}

#[test]
fn plan_rejects_unordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::FullScan),
        predicate: None,
        order: None,
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(10),
            offset: 2,
        }),
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan)
        .expect_err("pagination without ordering must be rejected");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::UnorderedPagination
    )));
}

#[test]
fn plan_accepts_ordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(10),
            offset: 2,
        }),
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    validate_logical_plan_model(&schema, model, &plan).expect("ordered pagination is valid");
}

#[test]
fn plan_rejects_order_without_terminal_primary_key_tie_break() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: QueryMode::Load(LoadSpec::new()),
        access: AccessPlan::path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("tag".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan).expect_err("missing PK tie-break");
    assert!(matches!(err, PlanError::Order(inner) if matches!(
        inner.as_ref(),
        OrderPlanError::MissingPrimaryKeyTieBreak { .. }
    )));
}

#[test]
fn secondary_order_pushdown_core_cases() {
    struct Case {
        name: &'static str,
        plan: LogicalPlan<Value>,
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
                    AccessPlan::path(AccessPath::IndexRange {
                        index: INDEX_MODEL,
                        prefix: vec![],
                        lower: Bound::Included(Value::Text("a".to_string())),
                        upper: Bound::Excluded(Value::Text("z".to_string())),
                    }),
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
        plan: LogicalPlan<Value>,
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
                    AccessPlan::path(AccessPath::IndexRange {
                        index: INDEX_MODEL,
                        prefix: vec![],
                        lower: Bound::Included(Value::Text("a".to_string())),
                        upper: Bound::Excluded(Value::Text("z".to_string())),
                    }),
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
            name: "non_ascending_direction",
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
            expected: SecondaryOrderPushdownRejection::NonAscendingDirection {
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
        plan: LogicalPlan<Value>,
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
                    AccessPlan::path(AccessPath::IndexRange {
                        index: INDEX_MODEL,
                        prefix: vec![],
                        lower: Bound::Included(Value::Text("a".to_string())),
                        upper: Bound::Excluded(Value::Text("z".to_string())),
                    }),
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
            AccessPlan::path(AccessPath::IndexRange {
                index: INDEX_MODEL,
                prefix: vec![],
                lower: Bound::Included(Value::Text("a".to_string())),
                upper: Bound::Excluded(Value::Text("z".to_string())),
            }),
        ],
        Some(order_spec(&[("id", OrderDirection::Asc)])),
    );
    assert_eq!(
        assess_secondary_order_pushdown_if_applicable_validated(model, &composite_index_range_plan),
        assess_secondary_order_pushdown_if_applicable(model, &composite_index_range_plan),
    );
}

#[test]
fn plan_error_from_order_maps_to_order_domain_variant() {
    let err = PlanError::from(OrderPlanError::UnorderableField {
        field: "rank".to_string(),
    });

    assert!(matches!(
        err,
        PlanError::Order(inner)
            if matches!(
                inner.as_ref(),
                OrderPlanError::UnorderableField { field } if field == "rank"
            )
    ));
}

#[test]
fn plan_error_from_access_maps_to_access_domain_variant() {
    let err = PlanError::from(AccessPlanError::InvalidKeyRange);

    assert!(matches!(err, PlanError::Access(inner) if matches!(
        inner.as_ref(),
        AccessPlanError::InvalidKeyRange
    )));
}

#[test]
fn plan_error_from_policy_maps_to_policy_domain_variant() {
    let err = PlanError::from(PolicyPlanError::UnorderedPagination);

    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::UnorderedPagination
    )));
}

#[test]
fn plan_error_from_cursor_maps_to_cursor_domain_variant() {
    let err = PlanError::from(CursorPlanError::ContinuationCursorBoundaryArityMismatch {
        expected: 2,
        found: 1,
    });

    assert!(matches!(
        err,
        PlanError::Cursor(inner)
            if matches!(
                inner.as_ref(),
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
    ));
}
