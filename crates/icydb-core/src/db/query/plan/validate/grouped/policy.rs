use crate::db::{
    predicate::SchemaInfo,
    query::plan::{
        GroupAggregateSpec, GroupDistinctAdmissibility, GroupDistinctPolicyReason, GroupHavingSpec,
        GroupSpec, ScalarPlan, grouped_distinct_admissibility, grouped_having_compare_op_supported,
        resolve_global_distinct_field_aggregate,
        validate::{GroupPlanError, PlanError},
    },
};

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
        if !grouped_having_compare_op_supported(clause.op()) {
            return Err(PlanError::from(
                GroupPlanError::HavingUnsupportedCompareOp {
                    index,
                    op: format!("{:?}", clause.op()),
                },
            ));
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
        if aggregate.distinct() && !aggregate.kind().supports_grouped_distinct_v1() {
            return Err(PlanError::from(
                GroupPlanError::DistinctAggregateKindUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                },
            ));
        }

        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        if aggregate.distinct() {
            return Err(PlanError::from(
                GroupPlanError::DistinctAggregateFieldTargetUnsupported {
                    index,
                    kind: format!("{:?}", aggregate.kind()),
                    field: target_field.clone(),
                },
            ));
        }
        return Err(PlanError::from(
            GroupPlanError::FieldTargetAggregatesUnsupported {
                index,
                kind: format!("{:?}", aggregate.kind()),
                field: target_field.clone(),
            },
        ));
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

    let target_field = aggregate.target_field();
    let Some(field_type) = schema.field(target_field) else {
        return Err(PlanError::from(
            GroupPlanError::UnknownAggregateTargetField {
                index: 0,
                field: target_field.to_string(),
            },
        ));
    };
    if aggregate.kind().is_sum() && !field_type.supports_numeric_coercion() {
        return Err(PlanError::from(
            GroupPlanError::GlobalDistinctSumTargetNotNumeric {
                index: 0,
                field: target_field.to_string(),
            },
        ));
    }

    Ok(())
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
