//! Module: db::query::plan::validate::grouped::policy::rules
//! Defines grouped-policy validation rules enforced during plan validation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    contracts::first_violated_rule,
    query::plan::{
        AggregateKind, GroupAggregateSpec,
        expr::{BinaryOp, Expr},
        grouped_having_compare_op_supported,
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
    fn walk(expr: &Expr, next_index: &mut usize) -> Option<GroupPlanError> {
        match expr {
            Expr::Field(_) | Expr::Aggregate(_) | Expr::Literal(_) => None,
            Expr::FunctionCall { args, .. } => args.iter().find_map(|arg| walk(arg, next_index)),
            Expr::Unary { expr, .. } => walk(expr, next_index),
            Expr::Case {
                when_then_arms,
                else_expr,
            } => when_then_arms
                .iter()
                .find_map(|arm| {
                    walk(arm.condition(), next_index).or_else(|| walk(arm.result(), next_index))
                })
                .or_else(|| walk(else_expr, next_index)),
            Expr::Binary { op, left, right } => match op {
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Lte
                | BinaryOp::Gt
                | BinaryOp::Gte => {
                    let compare_op = match op {
                        BinaryOp::Eq => crate::db::predicate::CompareOp::Eq,
                        BinaryOp::Ne => crate::db::predicate::CompareOp::Ne,
                        BinaryOp::Lt => crate::db::predicate::CompareOp::Lt,
                        BinaryOp::Lte => crate::db::predicate::CompareOp::Lte,
                        BinaryOp::Gt => crate::db::predicate::CompareOp::Gt,
                        BinaryOp::Gte => crate::db::predicate::CompareOp::Gte,
                        _ => unreachable!("non-compare operator excluded above"),
                    };

                    let current = *next_index;
                    *next_index = next_index.saturating_add(1);
                    (!grouped_having_compare_op_supported(compare_op)).then(|| {
                        GroupPlanError::having_unsupported_compare_op(
                            current,
                            format!("{compare_op:?}"),
                        )
                    })
                }
                BinaryOp::And => walk(left, next_index).or_else(|| walk(right, next_index)),
                BinaryOp::Or | BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                    walk(left, next_index).or_else(|| walk(right, next_index))
                }
            },
            #[cfg(test)]
            Expr::Alias { expr, name: _ } => walk(expr, next_index),
        }
    }

    let mut next_index = index;
    walk(expr, &mut next_index)
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
        .target_field()
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
