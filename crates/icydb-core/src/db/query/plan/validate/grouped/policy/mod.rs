//! Module: query::plan::validate::grouped::policy
//! Responsibility: grouped-plan policy checks and grouped DISTINCT admissibility mapping.
//! Does not own: grouped runtime execution guards or aggregate runtime evaluation.
//! Boundary: maps grouped policy reasons into planner-domain grouped plan errors.

mod rules;

use crate::db::{
    predicate::Predicate,
    query::plan::{
        GroupAggregateSpec, GroupDistinctAdmissibility, GroupHavingSpec, GroupSpec, ScalarPlan,
        grouped_distinct_admissibility, resolve_global_distinct_field_aggregate,
        validate::{GroupPlanError, PlanError},
    },
    schema::SchemaInfo,
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
    validate_grouped_predicate_policy(logical.predicate.as_ref())?;
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

// Reject predicate leaves that grouped execution does not interpret in this
// slice so grouped lanes fail closed before route/runtime handoff.
fn validate_grouped_predicate_policy(predicate: Option<&Predicate>) -> Result<(), PlanError> {
    let Some(predicate) = predicate else {
        return Ok(());
    };

    if predicate_contains_compare_fields(predicate) {
        return Err(PlanError::from(
            GroupPlanError::predicate_field_compare_unsupported(),
        ));
    }

    Ok(())
}

// Compare-fields leaves are scalar residual semantics only in 0.79 and must
// not be silently reinterpreted by grouped execution.
fn predicate_contains_compare_fields(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().any(predicate_contains_compare_fields)
        }
        Predicate::Not(inner) => predicate_contains_compare_fields(inner),
        Predicate::CompareFields(_) => true,
        Predicate::True
        | Predicate::False
        | Predicate::Compare(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => false,
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
    group.group_fields.is_empty().then_some(()).map_or_else(
        || {
            group
                .aggregates
                .iter()
                .enumerate()
                .find_map(|(index, aggregate)| {
                    first_grouped_aggregate_policy_violation(index, aggregate)
                })
                .map_or(Ok(()), |reason| Err(PlanError::from(reason)))
        },
        |()| validate_global_distinct_aggregate_without_group_keys(schema, group, having),
    )
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
                GroupPlanError::global_distinct_aggregate_shape_unsupported(),
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
