//! Module: query::plan::validate::semantic_gates
//! Responsibility: planner semantic gate orchestration for scalar/grouped plans.
//! Does not own: executor defensive runtime checks or cursor token protocol concerns.
//! Boundary: coordinates planner validation gates into typed plan errors.

use crate::db::{
    QueryError,
    access::validate_access_runtime_invariants_with_schema,
    query::plan::{
        AccessPlannedQuery, LogicalPlan, ScalarPlan,
        expr::ProjectionSpec,
        validate::{
            GroupPlanError, PlanError,
            grouped::{
                validate_group_cursor_constraints, validate_group_policy, validate_group_structure,
                validate_projection_expr_types,
            },
            order::{
                validate_no_duplicate_non_pk_order_fields, validate_order,
                validate_primary_key_tie_break,
            },
            validate_plan_shape,
        },
    },
    query::predicate::validate_predicate,
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::QueryFieldRole;

fn validate_accepted_access_structure_for_plan(
    schema: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Result<(), PlanError> {
    validate_access_runtime_invariants_with_schema(schema, &plan.access).map_err(PlanError::from)
}

/// Validate one scalar query entirely from accepted schema authority.
pub(in crate::db::query) fn validate_query_semantics_with_schema(
    schema: &SchemaInfo,
    plan: &AccessPlannedQuery,
    projection: &ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let logical = plan.scalar_plan();

    validate_scalar_plan_semantic_gates(schema, logical, plan, true, work)?;
    validate_projection_expr_types(schema, projection)?;

    Ok(())
}

/// Validate one grouped query entirely from accepted schema authority.
pub(in crate::db::query) fn validate_group_query_semantics_with_schema(
    schema: &SchemaInfo,
    plan: &AccessPlannedQuery,
    projection: &ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let (logical, group, having_expr) = match &plan.logical {
        LogicalPlan::Grouped(grouped) => (
            &grouped.scalar,
            &grouped.group,
            grouped.having_expr.as_ref(),
        ),
        LogicalPlan::Scalar(_) => {
            return Err(PlanError::from(GroupPlanError::grouped_logical_plan_required()).into());
        }
    };

    validate_scalar_plan_semantic_gates(schema, logical, plan, false, work)?;
    validate_group_structure(schema, group, projection, having_expr)?;
    validate_group_policy(schema, logical, group, having_expr)?;
    validate_group_cursor_constraints(logical, group)?;
    validate_projection_expr_types(schema, projection)?;

    Ok(())
}

// Shared scalar-plan semantic gates owned by planner validation.
fn validate_scalar_plan_semantic_gates(
    schema: &SchemaInfo,
    logical: &ScalarPlan,
    plan: &AccessPlannedQuery,
    require_primary_key_tie_break: bool,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if let Some(predicate) = &logical.predicate {
        validate_predicate(schema, predicate).map_err(|error| {
            PlanError::from(error).attach_query_field(QueryFieldRole::Predicate)
        })?;
    }

    if let Some(order) = &logical.order {
        validate_order(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(schema.primary_key_names(), order, work)?;
        if require_primary_key_tie_break {
            validate_primary_key_tie_break(schema.primary_key_names(), order, work)?;
        }
    }

    validate_accepted_access_structure_for_plan(schema, plan)?;
    validate_plan_shape(&plan.logical).map_err(PlanError::from)?;

    Ok(())
}
