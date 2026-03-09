//! Module: query::plan::validate::core
//! Responsibility: core planner semantic validation orchestration for scalar/grouped plans.
//! Does not own: executor defensive runtime checks or cursor token protocol concerns.
//! Boundary: coordinates planner validation gates into typed plan errors.

use crate::{
    db::{
        access::validate_access_structure_model as validate_access_structure_model_shared,
        predicate::{SchemaInfo, validate},
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
    },
    model::entity::EntityModel,
    value::Value,
};

/// Validate a logical plan with model-level key values.
///
/// Ownership:
/// - semantic owner for user-facing query validity at planning boundaries
/// - failures here are user-visible planning failures (`PlanError`)
///
/// New user-facing validation rules must be introduced here first, then mirrored
/// defensively in downstream layers without changing semantics.
pub(crate) fn validate_query_semantics(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery<Value>,
) -> Result<(), PlanError> {
    let logical = plan.scalar_plan();
    let projection = plan.projection_spec(model);

    validate_plan_core(
        schema,
        model,
        logical,
        plan,
        validate_order,
        |schema, model, plan| {
            validate_access_structure_model_shared(schema, model, &plan.access)
                .map_err(PlanError::from)
        },
    )?;
    validate_projection_expr_types(schema, &projection)?;

    Ok(())
}

/// Validate grouped query semantics for one grouped plan wrapper.
///
/// Ownership:
/// - semantic owner for GROUP BY wrapper validation
/// - failures here are user-visible planning failures (`PlanError`)
pub(crate) fn validate_group_query_semantics(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &AccessPlannedQuery<Value>,
) -> Result<(), PlanError> {
    let (logical, group, having) = match &plan.logical {
        LogicalPlan::Grouped(grouped) => (&grouped.scalar, &grouped.group, grouped.having.as_ref()),
        LogicalPlan::Scalar(_) => {
            return Err(PlanError::from(GroupPlanError::GroupedLogicalPlanRequired));
        }
    };
    let projection = plan.projection_spec(model);

    validate_plan_core(
        schema,
        model,
        logical,
        plan,
        validate_order,
        |schema, model, plan| {
            validate_access_structure_model_shared(schema, model, &plan.access)
                .map_err(PlanError::from)
        },
    )?;
    validate_group_structure(schema, model, group, &projection, having)?;
    validate_group_policy(schema, logical, group, having)?;
    validate_group_cursor_constraints(logical, group)?;
    validate_projection_expr_types(schema, &projection)?;

    Ok(())
}

// Shared logical plan validation core owned by planner semantics.
fn validate_plan_core<K, FOrder, FAccess>(
    schema: &SchemaInfo,
    model: &EntityModel,
    logical: &ScalarPlan,
    plan: &AccessPlannedQuery<K>,
    validate_order_fn: FOrder,
    validate_access_fn: FAccess,
) -> Result<(), PlanError>
where
    FOrder: Fn(&SchemaInfo, &OrderSpec) -> Result<(), PlanError>,
    FAccess: Fn(&SchemaInfo, &EntityModel, &AccessPlannedQuery<K>) -> Result<(), PlanError>,
{
    if let Some(predicate) = &logical.predicate {
        validate(schema, predicate)?;
    }

    if let Some(order) = &logical.order {
        validate_order_fn(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(model, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    validate_access_fn(schema, model, plan)?;
    validate_plan_shape(&plan.logical)?;

    Ok(())
}
