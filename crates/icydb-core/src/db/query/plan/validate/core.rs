//! Module: query::plan::validate::core
//! Responsibility: core planner semantic validation orchestration for scalar/grouped plans.
//! Does not own: executor defensive runtime checks or cursor token protocol concerns.
//! Boundary: coordinates planner validation gates into typed plan errors.

use crate::{
    db::{
        access::validate_access_structure_model as validate_access_structure_model_shared,
        query::plan::{
            AccessPlannedQuery, LogicalPlan, OrderSpec, ScalarPlan,
            expr::{
                parse_supported_order_expr, supported_order_expr_requires_index_satisfied_access,
            },
            validate::{
                GroupPlanError, PlanError, PolicyPlanError,
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
        schema::{SchemaInfo, validate},
    },
    model::entity::EntityModel,
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
    plan: &AccessPlannedQuery,
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
    plan: &AccessPlannedQuery,
) -> Result<(), PlanError> {
    let (logical, group, having) = match &plan.logical {
        LogicalPlan::Grouped(grouped) => (&grouped.scalar, &grouped.group, grouped.having.as_ref()),
        LogicalPlan::Scalar(_) => {
            return Err(PlanError::from(
                GroupPlanError::grouped_logical_plan_required(),
            ));
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
fn validate_plan_core<FOrder, FAccess>(
    schema: &SchemaInfo,
    model: &EntityModel,
    logical: &ScalarPlan,
    plan: &AccessPlannedQuery,
    validate_order_fn: FOrder,
    validate_access_fn: FAccess,
) -> Result<(), PlanError>
where
    FOrder: Fn(&SchemaInfo, &OrderSpec) -> Result<(), PlanError>,
    FAccess: Fn(&SchemaInfo, &EntityModel, &AccessPlannedQuery) -> Result<(), PlanError>,
{
    if let Some(predicate) = &logical.predicate {
        validate(schema, predicate)?;
    }

    if let Some(order) = &logical.order {
        validate_order_fn(schema, order)?;
        validate_no_duplicate_non_pk_order_fields(model, order)?;
        validate_primary_key_tie_break(model, order)?;
        validate_expression_order_support(model, plan, order)?;
    }

    validate_access_fn(schema, model, plan)?;
    validate_plan_shape(&plan.logical)?;

    Ok(())
}

fn validate_expression_order_support(
    _model: &EntityModel,
    plan: &AccessPlannedQuery,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    let expressions_requiring_index_support = order
        .fields
        .iter()
        .filter_map(|(field, _)| parse_supported_order_expr(field))
        .any(|expr| supported_order_expr_requires_index_satisfied_access(&expr));

    if !expressions_requiring_index_support {
        return Ok(());
    }

    if plan.access.is_singleton_or_empty_primary_key_access() || plan.access.is_explicit_empty() {
        return Ok(());
    }

    let access_class = plan.access_strategy().class();
    let planner_route_profile = plan.planner_route_profile();
    let logical_pushdown_eligibility = planner_route_profile.logical_pushdown_eligibility();
    let index_prefix_details = access_class.single_path_index_prefix_details();
    let index_range_details = access_class.single_path_index_range_details();
    let secondary_contract_active = logical_pushdown_eligibility.secondary_order_allowed()
        && !logical_pushdown_eligibility.requires_full_materialization();
    let has_index_path = index_prefix_details.is_some() || index_range_details.is_some();
    let prefix_order_contract_safe =
        index_prefix_details.is_none() || access_class.prefix_order_contract_safe();
    let secondary_pushdown_eligible = planner_route_profile
        .secondary_order_contract()
        .is_some_and(|order_contract| {
            access_class
                .secondary_order_pushdown_applicability(order_contract)
                .is_eligible()
        });

    if secondary_contract_active
        && has_index_path
        && prefix_order_contract_safe
        && secondary_pushdown_eligible
    {
        return Ok(());
    }

    Err(PlanError::from(
        PolicyPlanError::expression_order_requires_index_satisfied_access(),
    ))
}
