use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        contracts::{CompareOp, MissingRowPolicy, SchemaInfo},
        query::{
            intent::{LoadSpec, QueryMode},
            plan::{
                AccessPlannedQuery, DeleteLimitSpec, FieldSlot, GroupAggregateKind,
                GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
                GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, GroupPlanError, GroupSpec,
                GroupedExecutionConfig, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
                global_distinct_field_aggregate_admissibility, grouped_distinct_admissibility,
                grouped_executor_handoff, is_global_distinct_field_aggregate_candidate,
                validate::{PlanError, PolicyPlanError, validate_query_semantics},
                validate_group_query_semantics,
            },
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity! {
    ident = PlanValidateGroupedEntity,
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

fn load_plan(access: AccessPlan<Value>) -> AccessPlannedQuery<Value> {
    load_plan_with_order_and_distinct(access, None, false)
}

fn load_plan_with_order_and_distinct(
    access: AccessPlan<Value>,
    order: Option<OrderSpec>,
    distinct: bool,
) -> AccessPlannedQuery<Value> {
    load_plan_with_order_distinct_and_limit(access, order, distinct, None)
}

fn load_plan_with_order_distinct_and_limit(
    access: AccessPlan<Value>,
    order: Option<OrderSpec>,
    distinct: bool,
    limit: Option<u32>,
) -> AccessPlannedQuery<Value> {
    AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order,
            distinct,
            delete_limit: None,
            page: limit.map(|limit| PageSpec {
                limit: Some(limit),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access,
    }
}

fn grouped_plan(
    base: AccessPlannedQuery<Value>,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
) -> AccessPlannedQuery<Value> {
    grouped_plan_with_having(base, group_fields, aggregates, None)
}

fn grouped_plan_with_having(
    base: AccessPlannedQuery<Value>,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
    having: Option<GroupHavingSpec>,
) -> AccessPlannedQuery<Value> {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    base.into_grouped_with_having(
        GroupSpec {
            group_fields: group_fields
                .into_iter()
                .map(|field| {
                    FieldSlot::resolve(model, field).unwrap_or_else(|| {
                        FieldSlot::from_parts_for_test(usize::MAX, field.to_string())
                    })
                })
                .collect(),
            aggregates,
            execution: GroupedExecutionConfig::unbounded(),
        },
        having,
    )
}

#[test]
fn grouped_plan_rejects_empty_group_fields() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("empty group-fields spec must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::EmptyGroupFields
    )));
}

#[test]
fn grouped_plan_accepts_global_distinct_count_field_without_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: Some("tag".to_string()),
            distinct: true,
        }],
    );

    validate_group_query_semantics(&schema, model, &grouped)
        .expect("global grouped count(distinct field) should be accepted");
}

#[test]
fn grouped_plan_accepts_global_distinct_sum_field_without_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Sum,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    validate_group_query_semantics(&schema, model, &grouped)
        .expect("global grouped sum(distinct field) should be accepted");
}

#[test]
fn grouped_plan_rejects_global_distinct_sum_non_numeric_target() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Sum,
            target_field: Some("tag".to_string()),
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("global grouped sum(distinct non-numeric) should fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
            if *index == 0 && field == "tag"
    )));
}

#[test]
fn grouped_plan_rejects_global_distinct_unsupported_kind() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Exists,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("global grouped distinct should reject unsupported aggregate kinds");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
            if *index == 0 && kind == "Exists"
    )));
}

#[test]
fn grouped_plan_rejects_global_distinct_mixed_aggregate_shape() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![
            GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: Some("tag".to_string()),
                distinct: true,
            },
            GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: None,
                distinct: false,
            },
        ],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("global grouped distinct shape should reject mixed aggregate list");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::GlobalDistinctAggregateShapeUnsupported
    )));
}

#[test]
fn grouped_plan_rejects_unknown_group_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("unknown group field must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_plan_rejects_duplicate_group_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank", "rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("duplicate group field must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::DuplicateGroupField { field } if field == "rank"
    )));
}

#[test]
fn grouped_plan_rejects_distinct_without_adjacency_proof() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan_with_order_and_distinct(AccessPlan::path(AccessPath::FullScan), None, true),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped distinct should fail without ordered-group adjacency eligibility");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::DistinctAdjacencyEligibilityRequired
    )));
}

#[test]
fn grouped_plan_rejects_order_prefix_not_aligned_with_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("tag".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped order should fail when grouped-key prefix is missing");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
    )));
}

#[test]
fn grouped_plan_rejects_order_without_limit() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan_with_order_and_distinct(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped order should fail when LIMIT is omitted");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::OrderRequiresLimit
    )));
}

#[test]
fn grouped_plan_accepts_order_prefix_aligned_with_group_keys_when_limited() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    validate_group_query_semantics(&schema, model, &grouped).expect(
        "grouped order should be accepted when grouped keys lead ORDER BY and LIMIT is explicit",
    );
}

#[test]
fn grouped_plan_rejects_empty_aggregate_spec_list() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        Vec::new(),
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("empty grouped aggregate list must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::EmptyAggregates
    )));
}

#[test]
fn grouped_plan_rejects_unknown_aggregate_target_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Min,
            target_field: Some("missing_target".to_string()),
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("unknown grouped aggregate target field must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::UnknownAggregateTargetField { index, field }
            if *index == 0 && field == "missing_target"
    )));
}

#[test]
fn grouped_plan_rejects_field_target_aggregates_in_grouped_v1() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Min,
            target_field: Some("rank".to_string()),
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("field-target grouped terminal must fail while grouped v1 lacks support");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::FieldTargetAggregatesUnsupported { index, kind, field }
            if *index == 0 && kind == "Min" && field == "rank"
    )));
}

#[test]
fn grouped_plan_accepts_distinct_count_aggregate_terminal() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: true,
        }],
    );

    validate_group_query_semantics(&schema, model, &grouped)
        .expect("grouped distinct count should be accepted in grouped v1");
}

#[test]
fn grouped_plan_rejects_distinct_exists_aggregate_terminal() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Exists,
            target_field: None,
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("distinct exists should be rejected until grouped distinct support expands");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
            if *index == 0 && kind == "Exists"
    )));
}

#[test]
fn grouped_plan_rejects_distinct_field_target_aggregate_terminal() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Max,
            target_field: Some("rank".to_string()),
            distinct: true,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("distinct field-target grouped terminals should remain rejected in grouped v1");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::DistinctAggregateFieldTargetUnsupported { index, kind, field }
            if *index == 0 && kind == "Max" && field == "rank"
    )));
}

#[test]
fn grouped_plan_rejects_having_with_distinct() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan_with_having(
        load_plan_with_order_and_distinct(AccessPlan::path(AccessPath::FullScan), None, true),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(0),
            }],
        }),
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped having with distinct should be rejected");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::DistinctHavingUnsupported
    )));
}

#[test]
fn grouped_distinct_policy_contract_rejects_distinct_without_adjacency_proof() {
    assert_eq!(
        grouped_distinct_admissibility(true, false),
        GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired
        ),
        "grouped DISTINCT policy contract should classify adjacency-proof gating explicitly",
    );
}

#[test]
fn grouped_global_distinct_policy_contract_matches_candidate_and_having_rules() {
    let aggregates = vec![GroupAggregateSpec {
        kind: GroupAggregateKind::Count,
        target_field: Some("rank".to_string()),
        distinct: true,
    }];
    let having = GroupHavingSpec {
        clauses: vec![GroupHavingClause {
            symbol: GroupHavingSymbol::AggregateIndex(0),
            op: CompareOp::Gt,
            value: Value::Uint(1),
        }],
    };

    assert!(
        is_global_distinct_field_aggregate_candidate(&[], aggregates.as_slice()),
        "global grouped DISTINCT contract should detect field-target aggregate candidates",
    );
    assert_eq!(
        global_distinct_field_aggregate_admissibility(aggregates.as_slice(), None),
        GroupDistinctAdmissibility::Allowed,
        "candidate global DISTINCT shape should be admissible without HAVING",
    );
    assert_eq!(
        global_distinct_field_aggregate_admissibility(aggregates.as_slice(), Some(&having)),
        GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
        ),
        "global DISTINCT contract should reject HAVING consistently",
    );
}

#[test]
fn grouped_plan_rejects_having_group_field_outside_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::GroupField(
                    FieldSlot::resolve(model, "tag")
                        .expect("having group field slot should resolve for test"),
                ),
                op: CompareOp::Eq,
                value: Value::Text("alpha".to_string()),
            }],
        }),
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("having should reject group-field symbols not declared in group keys");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::HavingNonGroupFieldReference { field, .. } if field == "tag"
    )));
}

#[test]
fn grouped_plan_rejects_having_aggregate_index_out_of_bounds() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(1),
                op: CompareOp::Eq,
                value: Value::Uint(1),
            }],
        }),
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("having should reject aggregate indexes beyond declared aggregate count");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::HavingAggregateIndexOutOfBounds { aggregate_index, aggregate_count, .. }
            if *aggregate_index == 1 && *aggregate_count == 1
    )));
}

#[test]
fn grouped_plan_accepts_having_over_group_and_aggregate_symbols() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![
                GroupHavingClause {
                    symbol: GroupHavingSymbol::GroupField(
                        FieldSlot::resolve(model, "rank")
                            .expect("group field slot should resolve for test"),
                    ),
                    op: CompareOp::Gte,
                    value: Value::Int(1),
                },
                GroupHavingClause {
                    symbol: GroupHavingSymbol::AggregateIndex(0),
                    op: CompareOp::Gt,
                    value: Value::Uint(0),
                },
            ],
        }),
    );

    validate_group_query_semantics(&schema, model, &grouped)
        .expect("having over grouped keys and grouped aggregate symbols should be accepted");
}

#[test]
fn grouped_executor_handoff_preserves_group_fields_aggregates_and_execution_config() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = base.into_grouped(GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                .expect("rank field must resolve"),
            FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "tag")
                .expect("tag field must resolve"),
        ],
        aggregates: vec![
            GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: None,
                distinct: false,
            },
            GroupAggregateSpec {
                kind: GroupAggregateKind::Max,
                target_field: Some("rank".to_string()),
                distinct: false,
            },
        ],
        execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
    });

    let handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");

    assert_eq!(
        handoff
            .group_fields()
            .iter()
            .map(|field| field.field().to_string())
            .collect::<Vec<_>>(),
        vec!["rank".to_string(), "tag".to_string()]
    );
    assert_eq!(handoff.aggregates().len(), 2);
    assert_eq!(handoff.aggregates()[0].kind, GroupAggregateKind::Count);
    assert_eq!(handoff.aggregates()[0].target_field, None);
    assert_eq!(handoff.aggregates()[1].kind, GroupAggregateKind::Max);
    assert_eq!(
        handoff.aggregates()[1].target_field.as_deref(),
        Some("rank")
    );
    assert_eq!(handoff.execution().max_groups(), 11);
    assert_eq!(handoff.execution().max_group_bytes(), 2048);
    assert_eq!(
        handoff.base().scalar_plan().consistency,
        grouped.scalar_plan().consistency
    );
}

#[test]
fn grouped_executor_handoff_preserves_having_clause_contract() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan_with_having(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
        Some(GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::AggregateIndex(0),
                op: CompareOp::Gt,
                value: Value::Uint(1),
            }],
        }),
    );

    let handoff =
        grouped_executor_handoff(&grouped).expect("grouped logical plans should build handoff");
    let having = handoff
        .having()
        .expect("grouped handoff should preserve having clause payload");
    assert_eq!(having.clauses().len(), 1);
    assert!(matches!(
        having.clauses()[0].symbol(),
        GroupHavingSymbol::AggregateIndex(0)
    ));
    assert_eq!(having.clauses()[0].op(), CompareOp::Gt);
    assert_eq!(having.clauses()[0].value(), &Value::Uint(1));
}

#[test]
fn grouped_executor_handoff_contract_matrix_vectors_are_frozen() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped_cases = [
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                    .expect("rank field must resolve"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "tag")
                    .expect("tag field must resolve"),
                FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                    .expect("rank field must resolve"),
            ],
            aggregates: vec![
                GroupAggregateSpec {
                    kind: GroupAggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    distinct: false,
                },
                GroupAggregateSpec {
                    kind: GroupAggregateKind::Min,
                    target_field: None,
                    distinct: false,
                },
            ],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        },
    ];

    #[expect(clippy::type_complexity)]
    let actual_vectors: Vec<(
        Vec<String>,
        Vec<(GroupAggregateKind, Option<String>)>,
        u64,
        u64,
    )> = grouped_cases
        .iter()
        .map(|group| {
            let grouped = base.clone().into_grouped(group.clone());
            let handoff = grouped_executor_handoff(&grouped)
                .expect("grouped logical plans should build handoff");
            let aggregate_vector = handoff
                .aggregates()
                .iter()
                .map(|aggregate| (aggregate.kind, aggregate.target_field.clone()))
                .collect::<Vec<_>>();

            (
                handoff
                    .group_fields()
                    .iter()
                    .map(|field| field.field().to_string())
                    .collect::<Vec<_>>(),
                aggregate_vector,
                handoff.execution().max_groups(),
                handoff.execution().max_group_bytes(),
            )
        })
        .collect();
    let expected_vectors = vec![
        (
            vec!["rank".to_string()],
            vec![(GroupAggregateKind::Count, None::<String>)],
            u64::MAX,
            u64::MAX,
        ),
        (
            vec!["tag".to_string(), "rank".to_string()],
            vec![
                (GroupAggregateKind::Max, Some("rank".to_string())),
                (GroupAggregateKind::Min, None::<String>),
            ],
            11,
            2048,
        ),
    ];

    assert_eq!(actual_vectors, expected_vectors);
}

#[test]
fn grouped_invalid_spec_does_not_change_scalar_plan_validation_outcome() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base.clone(),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    validate_query_semantics(&schema, model, &base)
        .expect("scalar plan validation must not require grouped spec");
    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped validation must enforce grouped spec");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_validation_preserves_scalar_policy_errors_on_base_plan() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    base.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec { max_rows: 1 });
    let grouped = grouped_plan(
        base.clone(),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
            distinct: false,
        }],
    );

    let scalar_err = validate_query_semantics(&schema, model, &base)
        .expect_err("invalid scalar base plan must fail scalar policy validation");
    assert!(matches!(scalar_err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
    let grouped_err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped validation must preserve scalar base-plan policy failures");
    assert!(matches!(grouped_err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
}
