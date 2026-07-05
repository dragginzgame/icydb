//! Module: query::plan::validate::semantic_gates
//! Responsibility: planner semantic gate orchestration for scalar/grouped plans.
//! Does not own: executor defensive runtime checks or cursor token protocol concerns.
//! Boundary: coordinates planner validation gates into typed plan errors.

use crate::{
    db::{
        access::validate_access_structure_model as validate_access_structure_model_shared,
        query::plan::{
            AccessPlannedQuery, LogicalPlan, OrderSpec, ScalarPlan,
            validate::{
                GroupPlanError, PlanError,
                grouped::{
                    validate_group_cursor_constraints, validate_group_policy,
                    validate_group_structure, validate_projection_expr_types,
                },
                order::{
                    validate_no_duplicate_non_pk_order_fields, validate_order,
                    validate_primary_key_tie_break,
                },
                validate_plan_shape,
            },
        },
        query::predicate::validate_predicate,
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
};

// Lift the shared access-structure validation bridge into one named helper so
// scalar and grouped semantic entry points do not each restate the same
// planner-to-access validation mapping closure.
fn validate_access_structure_for_plan(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<(), PlanError> {
    validate_access_structure_model_shared(schema, model, &plan.access).map_err(PlanError::from)
}

/// Validate a logical plan with model-level key values.
///
/// Ownership:
/// - semantic owner for user-facing query validity at planning boundaries
/// - failures here are user-visible planning failures (`PlanError`)
///
/// New user-facing validation rules must be introduced here first, then mirrored
/// defensively in downstream layers without changing semantics.
pub(in crate::db::query) fn validate_query_semantics(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<(), PlanError> {
    let logical = plan.scalar_plan();
    let projection = plan.projection_spec(model);

    validate_scalar_plan_semantic_gates(
        schema,
        model,
        logical,
        plan,
        validate_order,
        validate_access_structure_for_plan,
        true,
    )?;
    validate_projection_expr_types(schema, &projection)?;

    Ok(())
}

/// Validate grouped query semantics for one grouped plan wrapper.
///
/// Ownership:
/// - semantic owner for GROUP BY wrapper validation
/// - failures here are user-visible planning failures (`PlanError`)
pub(in crate::db::query) fn validate_group_query_semantics(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<(), PlanError> {
    let (logical, group, having_expr) = match &plan.logical {
        LogicalPlan::Grouped(grouped) => (
            &grouped.scalar,
            &grouped.group,
            grouped.having_expr.as_ref(),
        ),
        LogicalPlan::Scalar(_) => {
            return Err(PlanError::from(
                GroupPlanError::grouped_logical_plan_required(),
            ));
        }
    };
    let projection = plan.projection_spec(model);

    validate_scalar_plan_semantic_gates(
        schema,
        model,
        logical,
        plan,
        validate_order,
        validate_access_structure_for_plan,
        false,
    )?;
    validate_group_structure(schema, group, &projection, having_expr)?;
    validate_group_policy(schema, logical, group, having_expr)?;
    validate_group_cursor_constraints(logical, group)?;
    validate_projection_expr_types(schema, &projection)?;

    Ok(())
}

// Shared scalar-plan semantic gates owned by planner validation.
fn validate_scalar_plan_semantic_gates<FOrder, FAccess>(
    schema: &SchemaInfo,
    model: &EntityModel,
    logical: &ScalarPlan,
    plan: &AccessPlannedQuery,
    validate_order_fn: FOrder,
    validate_access_fn: FAccess,
    require_primary_key_tie_break: bool,
) -> Result<(), PlanError>
where
    FOrder: Fn(&SchemaInfo, &OrderSpec) -> Result<(), PlanError>,
    FAccess: Fn(&SchemaInfo, &EntityModel, &AccessPlannedQuery) -> Result<(), PlanError>,
{
    if let Some(predicate) = &logical.predicate {
        validate_predicate(schema, predicate)?;
    }

    if let Some(order) = &logical.order {
        validate_order_fn(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(schema, order)?;
        if require_primary_key_tie_break {
            validate_primary_key_tie_break(schema, order)?;
        }
    }

    validate_access_fn(schema, model, plan)?;
    validate_plan_shape(&plan.logical)?;

    Ok(())
}
