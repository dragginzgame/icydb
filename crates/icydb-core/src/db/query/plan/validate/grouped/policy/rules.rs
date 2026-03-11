//! Module: db::query::plan::validate::grouped::policy::rules
//! Responsibility: module-local ownership and contracts for db::query::plan::validate::grouped::policy::rules.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    contracts::first_violated_rule,
    query::plan::{
        AggregateKind, GroupAggregateSpec, grouped_having_compare_op_supported,
        validate::GroupPlanError,
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
        GroupPlanError::HavingUnsupportedCompareOp {
            index: ctx.index,
            op: format!("{:?}", ctx.clause.op()),
        }
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
        GroupPlanError::DistinctAggregateKindUnsupported {
            index: ctx.index,
            kind: format!("{:?}", ctx.aggregate.kind()),
        }
    })
}

fn grouped_aggregate_distinct_field_target_unsupported_rule(
    ctx: GroupedAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    ctx.aggregate
        .target_field
        .as_ref()
        .filter(|_| ctx.aggregate.distinct())
        .map(
            |target_field| GroupPlanError::DistinctAggregateFieldTargetUnsupported {
                index: ctx.index,
                kind: format!("{:?}", ctx.aggregate.kind()),
                field: target_field.clone(),
            },
        )
}

fn grouped_aggregate_field_target_unsupported_rule(
    ctx: GroupedAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    ctx.aggregate
        .target_field
        .as_ref()
        .filter(|_| !ctx.aggregate.distinct())
        .map(
            |target_field| GroupPlanError::FieldTargetAggregatesUnsupported {
                index: ctx.index,
                kind: format!("{:?}", ctx.aggregate.kind()),
                field: target_field.clone(),
            },
        )
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

fn global_distinct_target_field_known_rule(
    ctx: GlobalDistinctAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    ctx.schema.field(ctx.target_field).is_none().then(|| {
        GroupPlanError::UnknownAggregateTargetField {
            index: 0,
            field: ctx.target_field.to_string(),
        }
    })
}

fn global_distinct_numeric_target_rule(
    ctx: GlobalDistinctAggregatePolicyContext<'_>,
) -> Option<GroupPlanError> {
    ctx.aggregate_kind
        .is_sum()
        .then_some(())
        .and_then(|()| ctx.schema.field(ctx.target_field))
        .filter(|field_type| !field_type.supports_numeric_coercion())
        .map(|_| GroupPlanError::GlobalDistinctSumTargetNotNumeric {
            index: 0,
            field: ctx.target_field.to_string(),
        })
}
