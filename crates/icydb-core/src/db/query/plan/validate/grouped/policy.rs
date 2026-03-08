use crate::db::{
    predicate::SchemaInfo,
    query::plan::{
        AggregateKind, GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
        GroupHavingSpec, GroupSpec, ScalarPlan, grouped_distinct_admissibility,
        grouped_having_compare_op_supported, resolve_global_distinct_field_aggregate,
        validate::{GroupPlanError, PlanError},
    },
};

#[derive(Clone, Copy)]
enum GroupedHavingPolicyRule {
    CompareOperatorSupported,
}

const GROUPED_HAVING_POLICY_RULES: &[GroupedHavingPolicyRule] =
    &[GroupedHavingPolicyRule::CompareOperatorSupported];

#[derive(Clone, Copy)]
enum GroupedAggregatePolicyRule {
    DistinctKindSupported,
    DistinctFieldTargetUnsupported,
    FieldTargetUnsupported,
}

const GROUPED_AGGREGATE_POLICY_RULES: &[GroupedAggregatePolicyRule] = &[
    GroupedAggregatePolicyRule::DistinctKindSupported,
    GroupedAggregatePolicyRule::DistinctFieldTargetUnsupported,
    GroupedAggregatePolicyRule::FieldTargetUnsupported,
];

#[derive(Clone, Copy)]
enum GlobalDistinctAggregatePolicyRule {
    TargetFieldKnown,
    SumTargetNumeric,
}

const GLOBAL_DISTINCT_AGGREGATE_POLICY_RULES: &[GlobalDistinctAggregatePolicyRule] = &[
    GlobalDistinctAggregatePolicyRule::TargetFieldKnown,
    GlobalDistinctAggregatePolicyRule::SumTargetNumeric,
];

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

    for (index, clause) in having.clauses().iter().enumerate() {
        if let Some(reason) = first_grouped_having_policy_violation(index, clause) {
            return Err(PlanError::from(reason));
        }
    }

    Ok(())
}

// Validate grouped execution policy over a structurally valid grouped spec.
fn validate_group_spec_policy(
    schema: &SchemaInfo,
    group: &GroupSpec,
    having: Option<&GroupHavingSpec>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        validate_global_distinct_aggregate_without_group_keys(schema, group, having)?;
        return Ok(());
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        if let Some(reason) = first_grouped_aggregate_policy_violation(index, aggregate) {
            return Err(PlanError::from(reason));
        }
    }

    Ok(())
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

    if let Some(reason) = first_global_distinct_aggregate_policy_violation(
        schema,
        aggregate.kind(),
        aggregate.target_field(),
    ) {
        return Err(PlanError::from(reason));
    }

    Ok(())
}

fn first_grouped_having_policy_violation(
    index: usize,
    clause: &crate::db::query::plan::GroupHavingClause,
) -> Option<GroupPlanError> {
    for rule in GROUPED_HAVING_POLICY_RULES {
        if let Some(reason) = evaluate_grouped_having_policy_rule(*rule, index, clause) {
            return Some(reason);
        }
    }

    None
}

fn evaluate_grouped_having_policy_rule(
    rule: GroupedHavingPolicyRule,
    index: usize,
    clause: &crate::db::query::plan::GroupHavingClause,
) -> Option<GroupPlanError> {
    match rule {
        GroupedHavingPolicyRule::CompareOperatorSupported => {
            if grouped_having_compare_op_supported(clause.op()) {
                None
            } else {
                Some(GroupPlanError::HavingUnsupportedCompareOp {
                    index,
                    op: format!("{:?}", clause.op()),
                })
            }
        }
    }
}

fn first_grouped_aggregate_policy_violation(
    index: usize,
    aggregate: &GroupAggregateSpec,
) -> Option<GroupPlanError> {
    for rule in GROUPED_AGGREGATE_POLICY_RULES {
        if let Some(reason) = evaluate_grouped_aggregate_policy_rule(*rule, index, aggregate) {
            return Some(reason);
        }
    }

    None
}

fn evaluate_grouped_aggregate_policy_rule(
    rule: GroupedAggregatePolicyRule,
    index: usize,
    aggregate: &GroupAggregateSpec,
) -> Option<GroupPlanError> {
    match rule {
        GroupedAggregatePolicyRule::DistinctKindSupported => {
            if aggregate.distinct() && !aggregate.kind().supports_grouped_distinct_v1() {
                Some(GroupPlanError::DistinctAggregateKindUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                })
            } else {
                None
            }
        }
        GroupedAggregatePolicyRule::DistinctFieldTargetUnsupported => aggregate
            .target_field
            .as_ref()
            .filter(|_| aggregate.distinct())
            .map(
                |target_field| GroupPlanError::DistinctAggregateFieldTargetUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                    field: target_field.clone(),
                },
            ),
        GroupedAggregatePolicyRule::FieldTargetUnsupported => aggregate
            .target_field
            .as_ref()
            .filter(|_| !aggregate.distinct())
            .map(
                |target_field| GroupPlanError::FieldTargetAggregatesUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                    field: target_field.clone(),
                },
            ),
    }
}

fn first_global_distinct_aggregate_policy_violation(
    schema: &SchemaInfo,
    aggregate_kind: AggregateKind,
    target_field: &str,
) -> Option<GroupPlanError> {
    for rule in GLOBAL_DISTINCT_AGGREGATE_POLICY_RULES {
        if let Some(reason) = evaluate_global_distinct_aggregate_policy_rule(
            *rule,
            schema,
            aggregate_kind,
            target_field,
        ) {
            return Some(reason);
        }
    }

    None
}

fn evaluate_global_distinct_aggregate_policy_rule(
    rule: GlobalDistinctAggregatePolicyRule,
    schema: &SchemaInfo,
    aggregate_kind: AggregateKind,
    target_field: &str,
) -> Option<GroupPlanError> {
    match rule {
        GlobalDistinctAggregatePolicyRule::TargetFieldKnown => schema
            .field(target_field)
            .is_none()
            .then(|| GroupPlanError::UnknownAggregateTargetField {
                index: 0,
                field: target_field.to_string(),
            }),
        GlobalDistinctAggregatePolicyRule::SumTargetNumeric => {
            if !aggregate_kind.is_sum() {
                return None;
            }

            schema
                .field(target_field)
                .filter(|field_type| !field_type.supports_numeric_coercion())
                .map(|_| GroupPlanError::GlobalDistinctSumTargetNotNumeric {
                    index: 0,
                    field: target_field.to_string(),
                })
        }
    }
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
