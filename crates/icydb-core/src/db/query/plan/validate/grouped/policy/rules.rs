//! Module: db::query::plan::validate::grouped::policy::rules
//! Defines grouped-policy validation rules enforced during plan validation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    contracts::first_violated_rule,
    query::plan::{
        AggregateKind, GroupAggregateSpec, GroupHavingExpr, grouped_having_compare_op_supported,
        validate::{GroupPlanError, resolve_group_aggregate_target_field_type},
    },
    schema::SchemaInfo,
};

type GroupedHavingPolicyRule = for<'a> fn(GroupedHavingPolicyContext<'a>) -> Option<GroupPlanError>;
type GroupedAggregatePolicyRule =
    for<'a> fn(GroupedAggregatePolicyContext<'a>) -> Option<GroupPlanError>;
type GlobalDistinctAggregatePolicyRule =
    for<'a> fn(GlobalDistinctAggregatePolicyContext<'a>) -> Option<GroupPlanError>;

const GROUPED_HAVING_POLICY_RULES: &[GroupedHavingPolicyRule] = &[grouped_having_compare_op_rule];
const GROUPED_AGGREGATE_POLICY_RULES: &[GroupedAggregatePolicyRule] = &[
    grouped_aggregate_distinct_kind_supported_rule,
    grouped_aggregate_distinct_field_target_unsupported_rule,
    grouped_aggregate_field_target_unsupported_rule,
];
const GLOBAL_DISTINCT_AGGREGATE_POLICY_RULES: &[GlobalDistinctAggregatePolicyRule] = &[
    global_distinct_target_field_known_rule,
    global_distinct_numeric_target_rule,
];

///
/// GroupedHavingPolicyContext
///

#[derive(Clone, Copy)]
struct GroupedHavingPolicyContext<'a> {
    index: usize,
    clause: &'a crate::db::query::plan::GroupHavingClause,
}

impl<'a> GroupedHavingPolicyContext<'a> {
    #[must_use]
    const fn new(index: usize, clause: &'a crate::db::query::plan::GroupHavingClause) -> Self {
        Self { index, clause }
    }
}

///
/// GroupedAggregatePolicyContext
///

#[derive(Clone, Copy)]
struct GroupedAggregatePolicyContext<'a> {
    index: usize,
    aggregate: &'a GroupAggregateSpec,
}

impl<'a> GroupedAggregatePolicyContext<'a> {
    #[must_use]
    const fn new(index: usize, aggregate: &'a GroupAggregateSpec) -> Self {
        Self { index, aggregate }
    }
}

///
/// GlobalDistinctAggregatePolicyContext
///

#[derive(Clone, Copy)]
struct GlobalDistinctAggregatePolicyContext<'a> {
    schema: &'a SchemaInfo,
    aggregate_kind: AggregateKind,
    target_field: &'a str,
}

impl<'a> GlobalDistinctAggregatePolicyContext<'a> {
    #[must_use]
    const fn new(
        schema: &'a SchemaInfo,
        aggregate_kind: AggregateKind,
        target_field: &'a str,
    ) -> Self {
        Self {
            schema,
            aggregate_kind,
            target_field,
        }
    }
}

pub(super) fn first_grouped_having_expr_policy_violation(
    index: usize,
    expr: &GroupHavingExpr,
) -> Option<GroupPlanError> {
    fn walk(expr: &GroupHavingExpr, next_index: &mut usize) -> Option<GroupPlanError> {
        match expr {
            GroupHavingExpr::Compare { op, .. } => {
                let current = *next_index;
                *next_index = next_index.saturating_add(1);
                (!grouped_having_compare_op_supported(*op)).then(|| {
                    GroupPlanError::having_unsupported_compare_op(current, format!("{op:?}"))
                })
            }
            GroupHavingExpr::And(children) => {
                children.iter().find_map(|child| walk(child, next_index))
            }
        }
    }

    let mut next_index = index;
    walk(expr, &mut next_index)
}

pub(super) fn first_grouped_having_policy_violation(
    index: usize,
    clause: &crate::db::query::plan::GroupHavingClause,
) -> Option<GroupPlanError> {
    first_violated_rule(
        GROUPED_HAVING_POLICY_RULES,
        GroupedHavingPolicyContext::new(index, clause),
    )
}

fn grouped_having_compare_op_rule(ctx: GroupedHavingPolicyContext<'_>) -> Option<GroupPlanError> {
    (!grouped_having_compare_op_supported(ctx.clause.op())).then(|| {
        GroupPlanError::having_unsupported_compare_op(ctx.index, format!("{:?}", ctx.clause.op()))
    })
}

pub(super) fn first_grouped_aggregate_policy_violation(
    index: usize,
    aggregate: &GroupAggregateSpec,
) -> Option<GroupPlanError> {
    first_violated_rule(
        GROUPED_AGGREGATE_POLICY_RULES,
        GroupedAggregatePolicyContext::new(index, aggregate),
    )
}

fn grouped_aggregate_distinct_kind_supported_rule(
    ctx: GroupedAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    (ctx.aggregate.distinct() && !ctx.aggregate.kind().supports_grouped_distinct_v1()).then(|| {
        GroupPlanError::distinct_aggregate_kind_unsupported(
            ctx.index,
            format!("{:?}", ctx.aggregate.kind()),
        )
    })
}

fn grouped_aggregate_distinct_field_target_unsupported_rule(
    ctx: GroupedAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    ctx.aggregate
        .target_field
        .as_ref()
        .filter(|_| {
            ctx.aggregate.distinct()
                && !matches!(
                    ctx.aggregate.kind(),
                    AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
                )
        })
        .map(|target_field| {
            GroupPlanError::distinct_aggregate_field_target_unsupported(
                ctx.index,
                format!("{:?}", ctx.aggregate.kind()),
                target_field,
            )
        })
}

fn grouped_aggregate_field_target_unsupported_rule(
    ctx: GroupedAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    ctx.aggregate
        .target_field
        .as_ref()
        .filter(|_| {
            !ctx.aggregate.distinct()
                && !matches!(
                    ctx.aggregate.kind(),
                    AggregateKind::Count
                        | AggregateKind::Sum
                        | AggregateKind::Avg
                        | AggregateKind::Min
                        | AggregateKind::Max
                )
        })
        .map(|target_field| {
            GroupPlanError::field_target_aggregates_unsupported(
                ctx.index,
                format!("{:?}", ctx.aggregate.kind()),
                target_field,
            )
        })
}

pub(super) fn first_global_distinct_aggregate_policy_violation(
    schema: &SchemaInfo,
    aggregate_kind: AggregateKind,
    target_field: &str,
) -> Option<GroupPlanError> {
    first_violated_rule(
        GLOBAL_DISTINCT_AGGREGATE_POLICY_RULES,
        GlobalDistinctAggregatePolicyContext::new(schema, aggregate_kind, target_field),
    )
}

// Resolve the grouped global-DISTINCT target field once so the rule family can
// share the same schema lookup and unknown-field error mapping.
fn resolve_global_distinct_target_field_type(
    ctx: GlobalDistinctAggregatePolicyContext<'_>,
) -> Result<&crate::db::schema::FieldType, GroupPlanError> {
    resolve_group_aggregate_target_field_type(ctx.schema, ctx.target_field, 0)
}

fn global_distinct_target_field_known_rule(
    ctx: GlobalDistinctAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    resolve_global_distinct_target_field_type(ctx).err()
}

fn global_distinct_numeric_target_rule(
    ctx: GlobalDistinctAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    if !ctx.aggregate_kind.is_sum() {
        return None;
    }

    resolve_global_distinct_target_field_type(ctx)
        .ok()
        .filter(|field_type| !field_type.supports_numeric_coercion())
        .map(|_| GroupPlanError::global_distinct_sum_target_not_numeric(0, ctx.target_field))
}
