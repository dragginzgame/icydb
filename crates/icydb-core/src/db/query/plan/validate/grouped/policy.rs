//! Module: query::plan::validate::grouped::policy
//! Responsibility: grouped-plan policy checks and grouped DISTINCT admissibility mapping.
//! Does not own: grouped runtime execution guards or aggregate runtime evaluation.
//! Boundary: maps grouped policy reasons into planner-domain grouped plan errors.

use crate::db::{
    contracts::first_violated_rule,
    predicate::SchemaInfo,
    query::plan::{
        AggregateKind, GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
        GroupHavingSpec, GroupSpec, ScalarPlan, grouped_distinct_admissibility,
        grouped_having_compare_op_supported, resolve_global_distinct_field_aggregate,
        validate::{GroupPlanError, PlanError},
    },
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
    global_distinct_sum_target_numeric_rule,
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

// Validate grouped policy gates independent from structural shape checks.
pub(in crate::db::query::plan::validate) fn validate_group_policy(
    schema: &SchemaInfo,
    logical: &ScalarPlan,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    validate_grouped_distinct_policy(logical, having.is_some())?;
    validate_grouped_having_policy(having)?;
    validate_group_spec_policy(schema, group, having)?;

    Ok(())
}

// Validate grouped DISTINCT policy gates for grouped v1 hardening.
fn validate_grouped_distinct_policy(
    logical: &ScalarPlan,
    has_having: bool,
) -> Result<(), PlanError> {
    match grouped_distinct_admissibility(logical.distinct, has_having) {
        GroupDistinctAdmissibility::Allowed => Ok(()),
        GroupDistinctAdmissibility::Disallowed(reason) => Err(PlanError::from(
            group_plan_error_from_distinct_policy_reason(reason, None),
        )),
    }
}

// Validate grouped HAVING policy gates and operator support.
fn validate_grouped_having_policy(having: Option<&GroupHavingSpec>) -> Result<(), PlanError> {
    let Some(having) = having else {
        return Ok(());
    };

    having
        .clauses()
        .iter()
        .enumerate()
        .find_map(|(index, clause)| first_grouped_having_policy_violation(index, clause))
        .map_or(Ok(()), |reason| Err(PlanError::from(reason)))
}

// Validate grouped execution policy over a structurally valid grouped spec.
fn validate_group_spec_policy(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        validate_global_distinct_aggregate_without_group_keys(schema, group, having)
    } else {
        group
            .aggregates
            .iter()
            .enumerate()
            .find_map(|(index, aggregate)| {
                first_grouped_aggregate_policy_violation(index, aggregate)
            })
            .map_or(Ok(()), |reason| Err(PlanError::from(reason)))
    }
}

// Validate the restricted global DISTINCT aggregate shape (`GROUP BY` omitted).
fn validate_global_distinct_aggregate_without_group_keys(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    let aggregate = match resolve_global_distinct_field_aggregate(
        group.group_fields.as_slice(),
        group.aggregates.as_slice(),
        having,
    ) {
        Ok(Some(aggregate)) => aggregate,
        Ok(None) => {
            return Err(PlanError::from(
                GroupPlanError::GlobalDistinctAggregateShapeUnsupported,
            ));
        }
        Err(reason) => {
            let aggregate = group.aggregates.first();
            return Err(PlanError::from(
                group_plan_error_from_distinct_policy_reason(reason, aggregate),
            ));
        }
    };

    first_global_distinct_aggregate_policy_violation(
        schema,
        aggregate.kind(),
        aggregate.target_field(),
    )
    .map_or(Ok(()), |reason| Err(PlanError::from(reason)))
}

fn first_grouped_having_policy_violation(
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

fn first_grouped_aggregate_policy_violation(
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

fn first_global_distinct_aggregate_policy_violation(
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

fn global_distinct_sum_target_numeric_rule(
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

// Map one grouped DISTINCT policy reason to planner-visible grouped plan errors.
fn group_plan_error_from_distinct_policy_reason(
    reason: GroupDistinctPolicyReason,
    aggregate: Option<&GroupAggregateSpec>,
) -> GroupPlanError {
    match reason {
        GroupDistinctPolicyReason::DistinctHavingUnsupported => {
            GroupPlanError::DistinctHavingUnsupported
        }
        GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired => {
            GroupPlanError::DistinctAdjacencyEligibilityRequired
        }
        GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported
        | GroupDistinctPolicyReason::GlobalDistinctRequiresSingleAggregate
        | GroupDistinctPolicyReason::GlobalDistinctRequiresFieldTargetAggregate
        | GroupDistinctPolicyReason::GlobalDistinctRequiresDistinctAggregateTerminal => {
            GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        }
        GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind => {
            let kind = aggregate.map_or_else(
                || "Unknown".to_string(),
                |aggregate| format!("{:?}", aggregate.kind()),
            );
            GroupPlanError::DistinctAggregateKindUnsupported { index: 0, kind }
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        predicate::CompareOp,
        predicate::MissingRowPolicy,
        query::plan::{
            DeleteSpec, FieldSlot, GroupHavingClause, GroupHavingSpec, GroupHavingSymbol, LoadSpec,
            LogicalPlan, OrderDirection, OrderSpec, QueryMode,
        },
    };
    use crate::value::Value;

    fn scalar_plan(distinct: bool) -> ScalarPlan {
        ScalarPlan {
            mode: QueryMode::Load(LoadSpec {
                limit: None,
                offset: 0,
            }),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }
    }

    #[test]
    fn grouped_distinct_without_adjacency_proof_fails_in_planner_policy() {
        let err = validate_grouped_distinct_policy(&scalar_plan(true), false)
            .expect_err("grouped DISTINCT without adjacency proof must fail in planner policy");

        assert!(matches!(
            err,
            PlanError::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::PlanPolicyError::Group(group)
                        if matches!(
                            group.as_ref(),
                            GroupPlanError::DistinctAdjacencyEligibilityRequired
                        )
                )
        ));
    }

    #[test]
    fn grouped_distinct_with_having_fails_in_planner_policy() {
        let err = validate_grouped_distinct_policy(&scalar_plan(true), true)
            .expect_err("grouped DISTINCT + HAVING must fail in planner policy");

        assert!(matches!(
            err,
            PlanError::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::PlanPolicyError::Group(group)
                        if matches!(group.as_ref(), GroupPlanError::DistinctHavingUnsupported)
                )
        ));
    }

    #[test]
    fn grouped_non_distinct_shape_passes_planner_distinct_policy_gate() {
        validate_grouped_distinct_policy(&scalar_plan(false), false)
            .expect("non-distinct grouped shapes should pass planner distinct policy gate");
    }

    #[test]
    fn grouped_having_contains_operator_fails_in_planner_policy() {
        let having = GroupHavingSpec {
            clauses: vec![GroupHavingClause {
                symbol: GroupHavingSymbol::GroupField(FieldSlot {
                    index: 0,
                    field: "team".to_string(),
                }),
                op: CompareOp::Contains,
                value: Value::Text("A".to_string()),
            }],
        };

        let err = validate_grouped_having_policy(Some(&having))
            .expect_err("grouped HAVING with unsupported compare operator must fail in planner");

        assert!(matches!(
            err,
            PlanError::Policy(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::PlanPolicyError::Group(group)
                        if matches!(
                            group.as_ref(),
                            GroupPlanError::HavingUnsupportedCompareOp { index: 0, .. }
                        )
                )
        ));
    }

    #[test]
    fn grouped_policy_tests_track_planner_logical_mode_contract() {
        // Keep grouped-policy tests compile-time linked to logical mode contracts.
        let _ = LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Delete(DeleteSpec { limit: Some(1) }),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        });
    }
}
