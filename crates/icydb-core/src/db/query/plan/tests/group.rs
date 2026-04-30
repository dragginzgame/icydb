//! Module: db::query::plan::tests::group
//! Covers grouped query-plan semantics and grouped validation behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        predicate::{CompareOp, MissingRowPolicy},
        query::plan::{
            AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec, FieldSlot,
            GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
            GroupPlanError, GroupSpec, GroupedCursorPolicyViolation,
            GroupedDistinctExecutionStrategy, GroupedExecutionConfig, GroupedExecutionRoute,
            LoadSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec, PlanPolicyError,
            PlanUserError, QueryMode,
            expr::{
                Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSelection,
                ProjectionSpec,
            },
            global_distinct_field_aggregate_admissibility,
            global_distinct_group_spec_for_aggregate_identity, group_aggregate_spec_expr,
            grouped_cursor_policy_violation, grouped_distinct_admissibility,
            grouped_executor_handoff, grouped_having_compare_expr,
            is_global_distinct_field_aggregate_candidate,
            validate::{
                ExprPlanError, PlanError, PolicyPlanError,
                grouped::validate_group_projection_expr_compatibility, validate_query_semantics,
            },
            validate_group_query_semantics,
        },
        schema::SchemaInfo,
    },
    model::{field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::generated("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

type GroupedCaseBuilder = fn() -> AccessPlannedQuery;
type GroupedPlanErrorCase<'a> = (&'a str, GroupedCaseBuilder, fn(&GroupPlanError) -> bool);

crate::test_entity! {
    ident = PlanValidateGroupedEntity,
    id = Ulid,
    entity_name = "IndexedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text { max_len: None }),
        ("rank", FieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

fn load_plan(access: AccessPlan<Value>) -> AccessPlannedQuery {
    load_plan_with_order_and_distinct(access, None, false)
}

fn load_plan_with_order_and_distinct(
    access: AccessPlan<Value>,
    order: Option<OrderSpec>,
    distinct: bool,
) -> AccessPlannedQuery {
    load_plan_with_order_distinct_and_limit(access, order, distinct, None)
}

fn load_plan_with_order_distinct_and_limit(
    access: AccessPlan<Value>,
    order: Option<OrderSpec>,
    distinct: bool,
    limit: Option<u32>,
) -> AccessPlannedQuery {
    AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
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
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    }
}

fn grouped_plan(
    base: AccessPlannedQuery,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
) -> AccessPlannedQuery {
    grouped_plan_with_having(base, group_fields, aggregates, None)
}

fn grouped_plan_with_having(
    base: AccessPlannedQuery,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
    having_expr: Option<Expr>,
) -> AccessPlannedQuery {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let group = GroupSpec {
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
    };

    base.into_grouped_with_having_expr(group, having_expr)
}

fn aggregate_having_expr(group: &GroupSpec, index: usize, op: CompareOp, value: Value) -> Expr {
    grouped_having_compare_expr(
        Expr::Aggregate(group_aggregate_spec_expr(
            group
                .aggregates
                .get(index)
                .expect("grouped HAVING aggregate should exist"),
        )),
        op,
        value,
    )
}

fn group_field_having_expr(field_slot: &FieldSlot, op: CompareOp, value: Value) -> Expr {
    grouped_having_compare_expr(Expr::Field(FieldId::new(field_slot.field())), op, value)
}

fn grouped_spec_for_projection_expr_tests(group_fields: Vec<&str>) -> GroupSpec {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    GroupSpec {
        group_fields: group_fields
            .into_iter()
            .map(|field| {
                FieldSlot::resolve(model, field)
                    .expect("grouped projection compatibility tests require valid field slots")
            })
            .collect(),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    }
}

fn finalized_grouped_plan(plan: &AccessPlannedQuery) -> AccessPlannedQuery {
    let mut finalized = plan.clone();
    finalized
        .finalize_static_planning_shape_for_model(
            <PlanValidateGroupedEntity as EntitySchema>::MODEL,
        )
        .expect("grouped plan tests require planner-frozen execution shape");

    finalized
}

fn additive_rank_order_term(direction: OrderDirection) -> crate::db::query::plan::OrderTerm {
    crate::db::query::plan::OrderTerm::new(
        Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        direction,
    )
}

fn subtractive_rank_order_term(direction: OrderDirection) -> crate::db::query::plan::OrderTerm {
    crate::db::query::plan::OrderTerm::new(
        Expr::Binary {
            op: BinaryOp::Sub,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        },
        direction,
    )
}

// Assert one grouped aggregate terminal remains semantically admissible for the
// shared grouped-v1 contract.
fn assert_grouped_terminal_accepts(
    label: &str,
    kind: AggregateKind,
    target_field: Option<&str>,
    distinct: bool,
) {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind,
            target_field: target_field.map(str::to_string),
            input_expr: None,
            filter_expr: None,
            distinct,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .unwrap_or_else(|_| panic!("{label} should be accepted in grouped v1"));
}

// Assert one global DISTINCT grouped handoff lowers onto the expected executor
// strategy for the aggregate kind under test.
fn assert_global_distinct_execution_strategy(label: &str, kind: AggregateKind, target_field: &str) {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec![],
        vec![GroupAggregateSpec {
            kind,
            target_field: Some(target_field.to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    let expected_target_slot = FieldSlot::resolve(model, target_field)
        .expect("grouped DISTINCT target slot should resolve");
    let expected_strategy = match kind {
        AggregateKind::Count => GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount {
            target_field: target_field.to_string(),
            target_slot: expected_target_slot,
        },
        AggregateKind::Sum => GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum {
            target_field: target_field.to_string(),
            target_slot: expected_target_slot,
        },
        AggregateKind::Avg => GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg {
            target_field: target_field.to_string(),
            target_slot: expected_target_slot,
        },
        _ => unreachable!("helper only covers supported grouped DISTINCT field strategies"),
    };
    assert_eq!(
        handoff.group_fields().len(),
        0,
        "{label}: wrong group field shape"
    );
    assert_eq!(
        handoff.aggregate_specs().len(),
        1,
        "{label}: wrong aggregate projection count",
    );
    assert_eq!(
        handoff.distinct_execution_strategy(),
        &expected_strategy,
        "{label}: wrong DISTINCT execution strategy",
    );
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        None,
        "{label}: executor lowering should not project scalar DISTINCT policy violations",
    );
}

// Assert one global DISTINCT grouped aggregate shape remains semantically
// admissible without group keys.
fn assert_global_distinct_accepts(label: &str, kind: AggregateKind, target_field: &str) {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind,
            target_field: Some(target_field.to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .unwrap_or_else(|_| panic!("{label} should be accepted"));
}

// Assert the semantic helper and aggregate-expression builder stay in lockstep
// for supported global DISTINCT grouped aggregate kinds.
fn assert_global_distinct_shape_helper_matches_expr(
    label: &str,
    kind: AggregateKind,
    target_field: &str,
) {
    let execution = GroupedExecutionConfig::with_hard_limits(64, 4096);
    let helper = global_distinct_group_spec_for_aggregate_identity(kind, target_field, execution)
        .unwrap_or_else(|_| panic!("{label}: helper shape should build"));
    let builder = match kind {
        AggregateKind::Count => GroupSpec::global_distinct_shape_from_aggregate_expr(
            &crate::db::count_by(target_field).distinct(),
            execution,
        ),
        AggregateKind::Sum => GroupSpec::global_distinct_shape_from_aggregate_expr(
            &crate::db::sum(target_field).distinct(),
            execution,
        ),
        AggregateKind::Avg => GroupSpec::global_distinct_shape_from_aggregate_expr(
            &crate::db::avg(target_field).distinct(),
            execution,
        ),
        _ => unreachable!("helper only covers supported grouped DISTINCT builder kinds"),
    };

    assert_eq!(
        helper, builder,
        "{label}: distinct shape helper must match aggregate-expression semantic path",
    );
}

#[test]
fn grouped_extrema_distinct_dedupes_by_aggregate_identity() {
    let plain_min = GroupAggregateSpec::from_aggregate_expr(&crate::db::min_by("rank"));
    let distinct_min =
        GroupAggregateSpec::from_aggregate_expr(&crate::db::min_by("rank").distinct());

    assert_eq!(
        plain_min, distinct_min,
        "grouped extrema identity must ignore DISTINCT through AggregateIdentity",
    );
}

#[test]
fn grouped_count_distinct_stays_distinct_in_aggregate_identity() {
    let plain_count = GroupAggregateSpec::from_aggregate_expr(&crate::db::count_by("rank"));
    let distinct_count =
        GroupAggregateSpec::from_aggregate_expr(&crate::db::count_by("rank").distinct());

    assert_ne!(
        plain_count, distinct_count,
        "grouped COUNT identity must retain observable DISTINCT semantics",
    );
}

#[test]
fn grouped_aggregate_filter_expr_stays_part_of_semantic_key() {
    let active_sum = GroupAggregateSpec::from_aggregate_expr(
        &crate::db::sum("rank").with_filter_expr(Expr::Literal(Value::Bool(true))),
    );
    let archived_sum = GroupAggregateSpec::from_aggregate_expr(
        &crate::db::sum("rank").with_filter_expr(Expr::Literal(Value::Bool(false))),
    );

    assert_ne!(
        active_sum, archived_sum,
        "grouped aggregate semantic keys must include aggregate-local FILTER",
    );
}

// Assert one grouped identity shape is rejected with the expected grouped-plan
// error contract.
fn assert_grouped_semantics_error_case(
    label: &str,
    grouped: &AccessPlannedQuery,
    predicate: fn(&GroupPlanError) -> bool,
) {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let err = validate_group_query_semantics(schema, model, grouped)
        .expect_err(&format!("{label} should be rejected"));

    assert!(
        is_group_plan_error(&err, predicate),
        "{label}: grouped semantic error contract drifted",
    );
}

fn grouped_global_distinct_sum_non_numeric_target_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Sum,
            target_field: Some("tag".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
    )
}

fn grouped_global_distinct_unsupported_kind_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Exists,
            target_field: Some("rank".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
    )
}

fn grouped_global_distinct_mixed_aggregate_shape_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![
            GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: Some("tag".to_string()),
                input_expr: None,
                filter_expr: None,
                distinct: true,
            },
            GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            },
        ],
    )
}

fn grouped_global_distinct_with_having_clause_case() -> AccessPlannedQuery {
    let group = GroupSpec {
        group_fields: Vec::new(),
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("rank".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    };
    grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        group.aggregates.clone(),
        Some(aggregate_having_expr(
            &group,
            0,
            CompareOp::Gt,
            Value::Uint(1),
        )),
    )
}

fn is_global_distinct_sum_target_not_numeric(err: &GroupPlanError) -> bool {
    matches!(
        err,
        GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
            if *index == 0 && field == "tag"
    )
}

fn is_distinct_aggregate_kind_unsupported_exists(err: &GroupPlanError) -> bool {
    matches!(
        err,
        GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
            if *index == 0 && kind == "Exists"
    )
}

fn is_global_distinct_shape_unsupported(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::GlobalDistinctAggregateShapeUnsupported)
}

fn grouped_unknown_group_field_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    )
}

fn grouped_duplicate_group_field_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank", "rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    )
}

fn grouped_distinct_without_adjacency_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan_with_order_and_distinct(AccessPlan::path(AccessPath::FullScan), None, true),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    )
}

fn grouped_order_prefix_misaligned_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    )
}

fn grouped_order_without_limit_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan_with_order_and_distinct(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    )
}

fn is_unknown_group_field_missing(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::UnknownGroupField { field } if field == "missing_group_field")
}

fn is_duplicate_group_field_rank(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::DuplicateGroupField { field } if field == "rank")
}

fn is_distinct_adjacency_required(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::DistinctAdjacencyEligibilityRequired)
}

fn is_order_prefix_not_aligned(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::OrderPrefixNotAlignedWithGroupKeys)
}

fn is_order_requires_limit(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::OrderRequiresLimit)
}

fn grouped_field_compare_predicate_case() -> AccessPlannedQuery {
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.scalar_plan_mut().predicate = Some(crate::db::Predicate::gt_fields(
        "rank".to_string(),
        "rank".to_string(),
    ));

    grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    )
}

fn grouped_distinct_exists_terminal_case() -> AccessPlannedQuery {
    grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Exists,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
    )
}

fn grouped_having_with_distinct_case() -> AccessPlannedQuery {
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    };
    grouped_plan_with_having(
        load_plan_with_order_and_distinct(AccessPlan::path(AccessPath::FullScan), None, true),
        vec!["rank"],
        group.aggregates.clone(),
        Some(aggregate_having_expr(
            &group,
            0,
            CompareOp::Gt,
            Value::Uint(0),
        )),
    )
}

fn is_distinct_aggregate_kind_unsupported_exists_terminal(err: &GroupPlanError) -> bool {
    matches!(
        err,
        GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
            if *index == 0 && kind == "Exists"
    )
}

fn is_distinct_having_unsupported(err: &GroupPlanError) -> bool {
    matches!(err, GroupPlanError::DistinctHavingUnsupported)
}

fn is_group_plan_error(err: &PlanError, predicate: impl FnOnce(&GroupPlanError) -> bool) -> bool {
    match err {
        PlanError::User(inner) => match inner.as_ref() {
            PlanUserError::Group(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        PlanError::Policy(inner) => match inner.as_ref() {
            PlanPolicyError::Group(inner) => predicate(inner.as_ref()),
            PlanPolicyError::Policy(_) => false,
        },
        PlanError::Cursor(_) => false,
    }
}

fn is_policy_plan_error(err: &PlanError, predicate: impl FnOnce(&PolicyPlanError) -> bool) -> bool {
    match err {
        PlanError::Policy(inner) => match inner.as_ref() {
            PlanPolicyError::Policy(inner) => predicate(inner.as_ref()),
            PlanPolicyError::Group(_) => false,
        },
        PlanError::User(_) | PlanError::Cursor(_) => false,
    }
}

fn is_expr_plan_error(err: &PlanError, predicate: impl FnOnce(&ExprPlanError) -> bool) -> bool {
    match err {
        PlanError::User(inner) => match inner.as_ref() {
            PlanUserError::Expr(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        PlanError::Policy(_) | PlanError::Cursor(_) => false,
    }
}

#[test]
fn grouped_plan_accepts_global_aggregate_without_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped)
        .expect("zero-key aggregate spec should now plan as one implicit global group");
}

#[test]
fn grouped_plan_accepts_global_distinct_field_without_group_keys_matrix() {
    let cases = [
        (
            "global grouped COUNT(distinct field)",
            AggregateKind::Count,
            "tag",
        ),
        (
            "global grouped SUM(distinct field)",
            AggregateKind::Sum,
            "rank",
        ),
        (
            "global grouped AVG(distinct field)",
            AggregateKind::Avg,
            "rank",
        ),
    ];

    for (label, kind, target_field) in cases {
        assert_global_distinct_accepts(label, kind, target_field);
    }
}

#[test]
fn global_distinct_shape_helper_matches_aggregate_expr_path_matrix() {
    let cases = [
        ("count distinct shape", AggregateKind::Count, "tag"),
        ("sum distinct shape", AggregateKind::Sum, "rank"),
        ("avg distinct shape", AggregateKind::Avg, "rank"),
    ];

    for (label, kind, target_field) in cases {
        assert_global_distinct_shape_helper_matches_expr(label, kind, target_field);
    }
}

#[test]
fn global_distinct_shape_helper_rejects_unsupported_kinds_structurally() {
    let execution = GroupedExecutionConfig::with_hard_limits(64, 4096);

    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let result = global_distinct_group_spec_for_aggregate_identity(kind, "tag", execution);

        assert!(
            matches!(
                result,
                Err(GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind)
            ),
            "unsupported grouped distinct kind should be rejected by semantic helper: {kind:?}",
        );
    }
}

#[test]
fn grouped_cursor_policy_violation_contract_is_shared_for_limit_and_global_distinct_cases() {
    let grouped_without_limit = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
            None,
        ),
        vec!["tag"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );
    let grouped_without_limit_plan = grouped_without_limit
        .grouped_plan()
        .expect("grouped plan should be present");
    assert_eq!(
        grouped_cursor_policy_violation(grouped_without_limit_plan, true)
            .map(GroupedCursorPolicyViolation::invariant_message),
        Some("grouped continuation cursors require an explicit LIMIT"),
        "grouped cursor contract should require explicit limit when continuation is present",
    );

    let grouped_global_distinct = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: Some("tag".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: true,
        }],
    );
    let grouped_global_distinct_plan = grouped_global_distinct
        .grouped_plan()
        .expect("grouped plan should be present");
    assert_eq!(
        grouped_cursor_policy_violation(grouped_global_distinct_plan, true)
            .map(GroupedCursorPolicyViolation::invariant_message),
        Some("global DISTINCT grouped aggregates do not support continuation cursors"),
        "global DISTINCT grouped cursor policy should reject continuation reuse",
    );
}

#[test]
fn grouped_plan_rejects_global_distinct_invalid_shape_matrix() {
    let cases: &[GroupedPlanErrorCase<'_>] = &[
        (
            "global DISTINCT SUM non-numeric target",
            grouped_global_distinct_sum_non_numeric_target_case,
            is_global_distinct_sum_target_not_numeric,
        ),
        (
            "global DISTINCT unsupported kind",
            grouped_global_distinct_unsupported_kind_case,
            is_distinct_aggregate_kind_unsupported_exists,
        ),
        (
            "global DISTINCT mixed aggregate shape",
            grouped_global_distinct_mixed_aggregate_shape_case,
            is_global_distinct_shape_unsupported,
        ),
        (
            "global DISTINCT with HAVING clause",
            grouped_global_distinct_with_having_clause_case,
            is_global_distinct_shape_unsupported,
        ),
    ];

    for (label, build_grouped, check_error) in cases.iter().copied() {
        let grouped = build_grouped();
        assert_grouped_semantics_error_case(label, &grouped, check_error);
    }
}

#[test]
fn grouped_plan_rejects_validation_shape_matrix() {
    let cases: &[GroupedPlanErrorCase<'_>] = &[
        (
            "unknown group field",
            grouped_unknown_group_field_case,
            is_unknown_group_field_missing,
        ),
        (
            "duplicate group field",
            grouped_duplicate_group_field_case,
            is_duplicate_group_field_rank,
        ),
        (
            "distinct without adjacency proof",
            grouped_distinct_without_adjacency_case,
            is_distinct_adjacency_required,
        ),
        (
            "order prefix misaligned with group keys",
            grouped_order_prefix_misaligned_case,
            is_order_prefix_not_aligned,
        ),
        (
            "order without limit",
            grouped_order_without_limit_case,
            is_order_requires_limit,
        ),
    ];

    for (label, build_grouped, check_error) in cases.iter().copied() {
        let grouped = build_grouped();
        assert_grouped_semantics_error_case(label, &grouped, check_error);
    }
}

#[test]
fn grouped_plan_accepts_field_to_field_predicate() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_field_compare_predicate_case();

    validate_group_query_semantics(schema, model, &grouped).expect(
        "grouped field-to-field predicates should reuse the grouped residual filter path instead of failing closed at policy validation",
    );
}

#[test]
fn grouped_plan_accepts_order_prefix_aligned_with_group_keys_when_limited() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped).expect(
        "grouped order should be accepted when grouped keys lead ORDER BY and LIMIT is explicit",
    );
}

#[test]
fn grouped_plan_accepts_additive_group_key_order_when_limited() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    additive_rank_order_term(OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped).expect(
        "grouped additive ORDER BY over the grouped key should be accepted when LIMIT is explicit",
    );
}

#[test]
fn grouped_plan_accepts_subtractive_group_key_order_when_limited() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    subtractive_rank_order_term(OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    validate_group_query_semantics(schema, model, &grouped).expect(
        "grouped subtractive ORDER BY over the grouped key should be accepted when LIMIT is explicit",
    );
}

#[test]
fn grouped_plan_accepts_multi_key_aggregate_order_with_group_tie_breakers_when_limited() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let aggregates = vec![GroupAggregateSpec {
        kind: AggregateKind::Count,
        target_field: None,
        input_expr: None,
        filter_expr: None,
        distinct: false,
    }];
    let grouped = grouped_plan(
        load_plan_with_order_distinct_and_limit(
            AccessPlan::path(AccessPath::FullScan),
            Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::new(
                        Expr::Aggregate(group_aggregate_spec_expr(
                            aggregates
                                .first()
                                .expect("count aggregate should exist for grouped order test"),
                        )),
                        OrderDirection::Desc,
                    ),
                    crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                ],
            }),
            false,
            Some(1),
        ),
        vec!["tag", "rank"],
        aggregates,
    );

    validate_group_query_semantics(schema, model, &grouped).expect(
        "bounded grouped aggregate ORDER BY should admit grouped-key tie-breakers without requiring canonical prefix ordering",
    );
}

#[test]
fn grouped_plan_having_order_limit_composition_enforces_bounded_policy() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);

    let build = |order: Option<OrderSpec>, limit: Option<u32>| {
        grouped_plan_with_having(
            load_plan_with_order_distinct_and_limit(
                AccessPlan::path(AccessPath::FullScan),
                order,
                false,
                limit,
            ),
            vec!["rank"],
            vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            Some(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Uint(0))),
            }),
        )
    };

    // Accepted shape: grouped HAVING + grouped-key-aligned ORDER + explicit LIMIT.
    let accepted = build(
        Some(OrderSpec {
            fields: vec![
                crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
            ],
        }),
        Some(1),
    );
    validate_group_query_semantics(schema, model, &accepted).expect(
        "grouped HAVING + ORDER should be accepted when ORDER prefix is aligned and LIMIT is explicit",
    );

    // Rejected shape: grouped HAVING + ORDER without LIMIT.
    let missing_limit = build(
        Some(OrderSpec {
            fields: vec![
                crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
            ],
        }),
        None,
    );
    let err = validate_group_query_semantics(schema, model, &missing_limit)
        .expect_err("grouped HAVING + ORDER without LIMIT should fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderRequiresLimit
    )));

    // Rejected shape: grouped HAVING + LIMIT but ORDER prefix not aligned with grouped keys.
    let prefix_mismatch = build(
        Some(OrderSpec {
            fields: vec![
                crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
            ],
        }),
        Some(1),
    );
    let err = validate_group_query_semantics(schema, model, &prefix_mismatch)
        .expect_err("grouped HAVING + misaligned ORDER should fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
    )));
}

#[test]
fn grouped_plan_rejects_empty_aggregate_spec_list() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        Vec::new(),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("empty grouped aggregate list must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::EmptyAggregates
    )));
}

#[test]
fn grouped_plan_rejects_unknown_aggregate_target_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Min,
            target_field: Some("missing_target".to_string()),
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("unknown grouped aggregate target field must fail");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::UnknownAggregateTargetField { index, field }
            if *index == 0 && field == "missing_target"
    )));
}

#[test]
fn grouped_plan_accepts_grouped_v1_aggregate_terminal_matrix() {
    let cases = [
        (
            "grouped MIN(field)",
            AggregateKind::Min,
            Some("rank"),
            false,
        ),
        (
            "grouped MAX(field)",
            AggregateKind::Max,
            Some("rank"),
            false,
        ),
        (
            "grouped COUNT(field)",
            AggregateKind::Count,
            Some("rank"),
            false,
        ),
        (
            "grouped SUM(field)",
            AggregateKind::Sum,
            Some("rank"),
            false,
        ),
        (
            "grouped AVG(field)",
            AggregateKind::Avg,
            Some("rank"),
            false,
        ),
        (
            "grouped distinct COUNT(*)",
            AggregateKind::Count,
            None,
            true,
        ),
    ];

    for (label, kind, target_field, distinct) in cases {
        assert_grouped_terminal_accepts(label, kind, target_field, distinct);
    }
}

#[test]
fn grouped_plan_accepts_distinct_field_aggregate_terminal_matrix() {
    let cases = [
        ("distinct COUNT(field)", AggregateKind::Count),
        ("distinct SUM(field)", AggregateKind::Sum),
        ("distinct AVG(field)", AggregateKind::Avg),
        ("semantic DISTINCT MIN(field)", AggregateKind::Min),
        ("semantic DISTINCT MAX(field)", AggregateKind::Max),
    ];

    for (label, kind) in cases {
        assert_grouped_terminal_accepts(label, kind, Some("rank"), true);
    }
}

#[test]
fn grouped_plan_rejects_distinct_terminal_shape_matrix() {
    let cases: &[GroupedPlanErrorCase<'_>] = &[
        (
            "distinct EXISTS terminal",
            grouped_distinct_exists_terminal_case,
            is_distinct_aggregate_kind_unsupported_exists_terminal,
        ),
        (
            "grouped HAVING with DISTINCT",
            grouped_having_with_distinct_case,
            is_distinct_having_unsupported,
        ),
    ];

    for (label, build_grouped, check_error) in cases.iter().copied() {
        let grouped = build_grouped();
        assert_grouped_semantics_error_case(label, &grouped, check_error);
    }
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
        kind: AggregateKind::Count,
        target_field: Some("rank".to_string()),
        input_expr: None,
        filter_expr: None,
        distinct: true,
    }];
    let having = grouped_having_compare_expr(
        Expr::Aggregate(group_aggregate_spec_expr(
            aggregates
                .first()
                .expect("global DISTINCT grouped HAVING test needs one aggregate"),
        )),
        CompareOp::Gt,
        Value::Uint(1),
    );

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
        global_distinct_field_aggregate_admissibility(aggregates.as_slice(), Some(&having),),
        GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
        ),
        "global DISTINCT contract should reject HAVING consistently",
    );
}

#[test]
fn grouped_plan_rejects_having_group_field_outside_group_keys() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = grouped_plan_with_having(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        Some(group_field_having_expr(
            &FieldSlot::resolve(model, "tag")
                .expect("having group field slot should resolve for test"),
            CompareOp::Eq,
            Value::Text("alpha".to_string()),
        )),
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("having should reject group-field symbols not declared in group keys");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::HavingNonGroupFieldReference { field, .. } if field == "tag"
    )));
}

#[test]
fn grouped_plan_rejects_having_aggregate_index_out_of_bounds() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let grouped = load_plan(AccessPlan::path(AccessPath::FullScan)).into_grouped_with_having_expr(
        GroupSpec {
            group_fields: vec![
                FieldSlot::resolve(model, "rank")
                    .expect("group field slot should resolve for test"),
            ],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        Some(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Aggregate(crate::db::sum("rank"))),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        }),
    );

    let err = validate_group_query_semantics(schema, model, &grouped).expect_err(
        "having should reject aggregate references outside declared grouped aggregates",
    );
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::HavingAggregateIndexOutOfBounds { aggregate_count, .. }
            if *aggregate_count == 1
    )));
}

#[test]
fn grouped_plan_accepts_having_over_group_and_aggregate_symbols() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(model, "rank").expect("group field slot should resolve for test"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    };
    let grouped = load_plan(AccessPlan::path(AccessPath::FullScan)).into_grouped_with_having_expr(
        group.clone(),
        Some(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(group_field_having_expr(
                &FieldSlot::resolve(model, "rank")
                    .expect("group field slot should resolve for test"),
                CompareOp::Gte,
                Value::Int(1),
            )),
            right: Box::new(aggregate_having_expr(
                &group,
                0,
                CompareOp::Gt,
                Value::Uint(0),
            )),
        }),
    );

    validate_group_query_semantics(schema, model, &grouped)
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
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            },
            GroupAggregateSpec {
                kind: AggregateKind::Max,
                target_field: Some("rank".to_string()),
                input_expr: None,
                filter_expr: None,
                distinct: false,
            },
        ],
        execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
    });

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert!(
        handoff.projection_is_identity(),
        "planner grouped handoff should carry the canonical identity-projection contract",
    );
    assert_eq!(
        handoff
            .group_fields()
            .iter()
            .map(|field| field.field().to_string())
            .collect::<Vec<_>>(),
        vec!["rank".to_string(), "tag".to_string()]
    );
    assert_eq!(handoff.aggregate_specs().len(), 2);
    assert_eq!(handoff.aggregate_specs()[0].kind(), AggregateKind::Count);
    assert_eq!(handoff.aggregate_specs()[0].target_field(), None);
    assert_eq!(handoff.aggregate_specs()[1].kind(), AggregateKind::Max);
    assert_eq!(handoff.aggregate_specs()[1].target_field(), Some("rank"));
    assert_eq!(handoff.execution().max_groups(), 11);
    assert_eq!(handoff.execution().max_group_bytes(), 2048);
    assert_eq!(handoff.projection_layout().group_field_positions(), &[0, 1]);
    assert_eq!(handoff.projection_layout().aggregate_positions(), &[2, 3]);
    assert_eq!(
        handoff.grouped_execution_route(),
        GroupedExecutionRoute::GenericFull,
        "non-count grouped handoff shapes must stay on the generic grouped execution route",
    );
    assert!(matches!(
        handoff.distinct_execution_strategy(),
        GroupedDistinctExecutionStrategy::None
    ));
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        None,
        "grouped handoff should not project DISTINCT policy violations for non-DISTINCT grouped plans",
    );
    assert_eq!(
        handoff.base().scalar_plan().consistency,
        grouped.scalar_plan().consistency
    );
}

#[test]
fn grouped_executor_handoff_global_distinct_execution_strategy_matrix() {
    let cases = [
        ("global DISTINCT COUNT(field)", AggregateKind::Count, "tag"),
        ("global DISTINCT SUM(field)", AggregateKind::Sum, "rank"),
        ("global DISTINCT AVG(field)", AggregateKind::Avg, "rank"),
    ];

    for (label, kind, target_field) in cases {
        assert_global_distinct_execution_strategy(label, kind, target_field);
    }
}

#[test]
fn grouped_executor_handoff_projects_dedicated_count_fold_path_for_single_count_rows() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(
        handoff.grouped_execution_route(),
        GroupedExecutionRoute::CountRowsDedicated,
        "single grouped COUNT(*) shapes must project the dedicated grouped count execution route",
    );
}

#[test]
fn grouped_executor_handoff_rejects_dedicated_count_fold_path_for_filtered_count_rows() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: Some(Box::new(Expr::Binary {
                left: Box::new(Expr::Field(FieldId::from("tag"))),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(Value::Text("alpha".to_string()))),
            })),
            distinct: false,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(
        handoff.grouped_execution_route(),
        GroupedExecutionRoute::GenericFull,
        "filtered grouped COUNT(*) shapes must stay on the generic grouped execution route",
    );
}

#[test]
fn grouped_executor_handoff_projects_scalar_distinct_policy_violation_for_executor() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let mut grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );
    grouped.scalar_plan_mut().distinct = true;

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    assert_eq!(
        handoff.distinct_policy_violation_for_executor(),
        Some(GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired),
        "grouped handoff should project scalar DISTINCT policy violations for executor boundaries",
    );
    assert!(
        matches!(
            handoff.distinct_execution_strategy(),
            GroupedDistinctExecutionStrategy::None
        ),
        "scalar DISTINCT policy violations should remain independent from global DISTINCT execution strategy lowering",
    );
}

#[test]
fn grouped_executor_handoff_preserves_having_clause_contract() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let group = GroupSpec {
        group_fields: vec![
            FieldSlot::resolve(<PlanValidateGroupedEntity as EntitySchema>::MODEL, "rank")
                .expect("group field should resolve"),
        ],
        aggregates: vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
        execution: GroupedExecutionConfig::unbounded(),
    };
    let grouped = grouped_plan_with_having(
        base,
        vec!["rank"],
        group.aggregates.clone(),
        Some(aggregate_having_expr(
            &group,
            0,
            CompareOp::Gt,
            Value::Uint(1),
        )),
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    let group = finalized
        .grouped_plan()
        .expect("grouped handoff test should keep grouped plan shape")
        .group
        .clone();
    assert_eq!(
        handoff.having_expr(),
        Some(&aggregate_having_expr(
            &group,
            0,
            CompareOp::Gt,
            Value::Uint(1)
        )),
    );
}

type GroupedExecutorHandoffSnapshotVector = (
    Vec<String>,
    Vec<(AggregateKind, Option<String>)>,
    Vec<usize>,
    Vec<usize>,
    String,
    String,
    u64,
    u64,
);

fn grouped_executor_handoff_snapshot_vector(
    base: &AccessPlannedQuery,
    group: &GroupSpec,
) -> GroupedExecutorHandoffSnapshotVector {
    let grouped = base.clone().into_grouped(group.clone());
    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");
    let aggregate_vector = handoff
        .aggregate_specs()
        .iter()
        .map(|aggregate| {
            (
                aggregate.kind(),
                aggregate.target_field().map(str::to_string),
            )
        })
        .collect::<Vec<_>>();

    (
        handoff
            .group_fields()
            .iter()
            .map(|field| field.field().to_string())
            .collect::<Vec<_>>(),
        aggregate_vector,
        handoff.projection_layout().group_field_positions().to_vec(),
        handoff.projection_layout().aggregate_positions().to_vec(),
        format!("{:?}", handoff.grouped_execution_route()),
        format!("{:?}", handoff.distinct_execution_strategy()),
        handoff.execution().max_groups(),
        handoff.execution().max_group_bytes(),
    )
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
                kind: AggregateKind::Count,
                target_field: None,
                input_expr: None,
                filter_expr: None,
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
                    kind: AggregateKind::Max,
                    target_field: Some("rank".to_string()),
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                },
                GroupAggregateSpec {
                    kind: AggregateKind::Min,
                    target_field: None,
                    input_expr: None,
                    filter_expr: None,
                    distinct: false,
                },
            ],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        },
    ];

    let actual_vectors: Vec<GroupedExecutorHandoffSnapshotVector> = grouped_cases
        .iter()
        .map(|group| grouped_executor_handoff_snapshot_vector(&base, group))
        .collect();
    let expected_vectors: Vec<GroupedExecutorHandoffSnapshotVector> = vec![
        (
            vec!["rank".to_string()],
            vec![(AggregateKind::Count, None::<String>)],
            vec![0],
            vec![1],
            "CountRowsDedicated".to_string(),
            "None".to_string(),
            u64::MAX,
            u64::MAX,
        ),
        (
            vec!["tag".to_string(), "rank".to_string()],
            vec![
                (AggregateKind::Max, Some("rank".to_string())),
                (AggregateKind::Min, None::<String>),
            ],
            vec![0, 1],
            vec![2, 3],
            "GenericFull".to_string(),
            "None".to_string(),
            11,
            2048,
        ),
    ];

    assert_eq!(actual_vectors, expected_vectors);
}

#[test]
fn grouped_invalid_spec_does_not_change_scalar_plan_validation_outcome() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base.clone(),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    validate_query_semantics(schema, model, &base)
        .expect("scalar plan validation must not require grouped spec");
    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped validation must enforce grouped spec");
    assert!(is_group_plan_error(&err, |inner| matches!(
        inner,
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_validation_preserves_scalar_policy_errors_on_base_plan() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    base.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(1),
        offset: 0,
    });
    let grouped = grouped_plan(
        base.clone(),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    let scalar_err = validate_query_semantics(schema, model, &base)
        .expect_err("invalid scalar base plan must fail scalar policy validation");
    assert!(is_policy_plan_error(&scalar_err, |inner| matches!(
        inner,
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
    let grouped_err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("grouped validation must preserve scalar base-plan policy failures");
    assert!(is_policy_plan_error(&grouped_err, |inner| matches!(
        inner,
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
}

#[test]
fn grouped_validation_rejects_delete_mode_grouped_shape_as_policy_error() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    base.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    let err = validate_group_query_semantics(schema, model, &grouped)
        .expect_err("delete grouped shape must fail planner policy validation");
    assert!(is_policy_plan_error(&err, |inner| matches!(
        inner,
        PolicyPlanError::DeletePlanWithGrouping
    )));
}

#[test]
fn grouped_projection_expr_compatibility_accepts_group_fields_and_aggregates_with_alias_wrapping() {
    let group = grouped_spec_for_projection_expr_tests(vec!["rank"]);
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Field(FieldId::new("rank"))),
                name: Alias::new("group_key"),
            },
            alias: Some(Alias::new("group_key_out")),
        },
        ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Aggregate(crate::db::count())),
                name: Alias::new("count_alias"),
            },
            alias: None,
        },
    ]);

    validate_group_projection_expr_compatibility(&group, &projection).expect(
        "grouped projection compatibility should allow grouped fields, aliases, and aggregates",
    );
}

#[test]
fn grouped_executor_handoff_deduplicates_repeated_aggregate_leaves_in_projection_expr() {
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.projection_selection = ProjectionSelection::Exprs(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Aggregate(crate::db::count())),
            },
            alias: None,
        },
    ]);

    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            input_expr: None,
            filter_expr: None,
            distinct: false,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(handoff.projection_layout().group_field_positions(), &[0]);
    assert_eq!(handoff.projection_layout().aggregate_positions(), &[1]);
    assert_eq!(handoff.aggregate_specs().len(), 1);
    assert_eq!(handoff.grouped_aggregate_execution_specs().len(), 1);
    assert_eq!(handoff.aggregate_specs()[0].kind(), AggregateKind::Count);
    assert_eq!(handoff.aggregate_specs()[0].target_field(), None);
}

#[test]
fn grouped_executor_handoff_deduplicates_repeated_aggregate_input_leaves_in_projection_expr() {
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.projection_selection = ProjectionSelection::Exprs(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(
                    crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(
                        AggregateKind::Avg,
                        Expr::Binary {
                            op: BinaryOp::Add,
                            left: Box::new(Expr::Field(FieldId::new("rank"))),
                            right: Box::new(Expr::Literal(Value::Int(1))),
                        },
                    ),
                )),
                right: Box::new(Expr::Aggregate(
                    crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(
                        AggregateKind::Avg,
                        Expr::Binary {
                            op: BinaryOp::Add,
                            left: Box::new(Expr::Field(FieldId::new("rank"))),
                            right: Box::new(Expr::Literal(Value::Int(1))),
                        },
                    ),
                )),
            },
            alias: None,
        },
    ]);

    let grouped = grouped_plan(
        base,
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: AggregateKind::Avg,
            target_field: Some("rank".to_string()),
            input_expr: Some(Box::new(Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Field(FieldId::new("rank"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            })),
            filter_expr: None,
            distinct: false,
        }],
    );

    let finalized = finalized_grouped_plan(&grouped);
    let handoff =
        grouped_executor_handoff(&finalized).expect("grouped logical plans should build handoff");

    assert_eq!(handoff.projection_layout().group_field_positions(), &[0]);
    assert_eq!(handoff.projection_layout().aggregate_positions(), &[1]);
    assert_eq!(handoff.aggregate_specs().len(), 1);
    assert_eq!(handoff.grouped_aggregate_execution_specs().len(), 1);
    assert_eq!(handoff.aggregate_specs()[0].kind(), AggregateKind::Avg);
    assert_eq!(
        handoff.aggregate_specs()[0].input_expr(),
        Some(&Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Decimal(crate::types::Decimal::from(
                1_u64
            ),))),
        }),
        "repeated grouped aggregate-input leaves should reuse one canonical normalized grouped aggregate projection spec",
    );
}

#[test]
fn grouped_projection_expr_compatibility_rejects_non_group_field_reference() {
    let group = grouped_spec_for_projection_expr_tests(vec!["rank"]);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("tag"))),
            name: Alias::new("not_grouped"),
        },
        alias: None,
    }]);

    let err = validate_group_projection_expr_compatibility(&group, &projection)
        .expect_err("grouped projection compatibility should reject non-group field references");
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::GroupedProjectionReferencesNonGroupField { index } if *index == 0
    )));
}

#[test]
fn grouped_projection_expr_compatibility_rejects_non_group_field_in_mixed_aggregate_expression() {
    let group = grouped_spec_for_projection_expr_tests(vec!["rank"]);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(crate::db::count())),
            right: Box::new(Expr::Field(FieldId::new("tag"))),
        },
        alias: None,
    }]);

    let err = validate_group_projection_expr_compatibility(&group, &projection).expect_err(
        "mixed expressions must still reject non-group field references outside aggregate nodes",
    );
    assert!(is_expr_plan_error(&err, |inner| matches!(
        inner,
        ExprPlanError::GroupedProjectionReferencesNonGroupField { index } if *index == 0
    )));
}
