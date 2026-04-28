//! Module: db::query::plan::validate::grouped::policy::rules
//! Defines grouped-policy validation rules enforced during plan validation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    query::plan::{
        AggregateKind, GroupAggregateSpec,
        expr::Expr,
        grouped_having_binary_compare_op, grouped_having_compare_op_supported,
        validate::{GroupPlanError, resolve_group_aggregate_target_field_type},
    },
    schema::SchemaInfo,
};

type GroupedAggregatePolicyRule =
    for<'a> fn(GroupedAggregatePolicyContext<'a>) -> Option<GroupPlanError>;
type GlobalDistinctAggregatePolicyRule =
    for<'a> fn(GlobalDistinctAggregatePolicyContext<'a>) -> Option<GroupPlanError>;

const GROUPED_AGGREGATE_POLICY_RULES: &[GroupedAggregatePolicyRule] = &[
    grouped_aggregate_distinct_kind_supported_rule,
    grouped_aggregate_distinct_field_target_unsupported_rule,
    grouped_aggregate_field_target_unsupported_rule,
];
const GLOBAL_DISTINCT_AGGREGATE_POLICY_RULES: &[GlobalDistinctAggregatePolicyRule] = &[
    global_distinct_target_field_known_rule,
    global_distinct_numeric_target_rule,
];

// Return the first violated grouped-policy rule in declaration order.
fn first_violated_rule<R, C, E>(rules: &[R], ctx: C) -> Option<E>
where
    C: Copy,
    R: Fn(C) -> Option<E>,
{
    for rule in rules {
        if let Some(err) = rule(ctx) {
            return Some(err);
        }
    }

    None
}

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
    expr: &Expr,
) -> Option<GroupPlanError> {
    let mut next_index = index;
    expr.try_for_each_tree_expr_with_compare_index(&mut next_index, &mut |compare_index, node| {
        let Expr::Binary { op, .. } = node else {
            return Ok(());
        };

        let Some(compare_op) = grouped_having_binary_compare_op(*op) else {
            return Ok(());
        };

        grouped_having_compare_op_supported(compare_op)
            .then_some(())
            .ok_or_else(|| {
                GroupPlanError::having_unsupported_compare_op(
                    compare_index,
                    format!("{compare_op:?}"),
                )
            })
    })
    .err()
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
        .target_field()
        .filter(|_| {
            ctx.aggregate.distinct()
                && !ctx.aggregate.identity().uses_grouped_distinct_value_dedup()
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
        .target_field()
        .filter(|_| !ctx.aggregate.distinct() && !ctx.aggregate.kind().supports_field_target_v1())
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
