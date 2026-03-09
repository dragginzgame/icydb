//! Module: query::plan::validate::grouped::policy
//! Responsibility: grouped-plan policy checks and grouped DISTINCT admissibility mapping.
//! Does not own: grouped runtime execution guards or aggregate runtime evaluation.
//! Boundary: maps grouped policy reasons into planner-domain grouped plan errors.

mod rules;

use crate::db::{
    predicate::SchemaInfo,
    query::plan::{
        GroupAggregateSpec, GroupDistinctAdmissibility, GroupHavingSpec, GroupSpec, ScalarPlan,
        grouped_distinct_admissibility, resolve_global_distinct_field_aggregate,
        validate::{GroupPlanError, PlanError},
    },
};

use crate::db::query::plan::validate::grouped::policy::rules::{
    first_global_distinct_aggregate_policy_violation, first_grouped_aggregate_policy_violation,
    first_grouped_having_policy_violation,
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
        GroupDistinctAdmissibility::Disallowed(reason) => {
            Err(PlanError::from(reason.planner_group_plan_error(None)))
        }
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
                reason.planner_group_plan_error(aggregate.map(GroupAggregateSpec::kind)),
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
