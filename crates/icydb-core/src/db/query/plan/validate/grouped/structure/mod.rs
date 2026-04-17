//! Module: query::plan::validate::grouped::structure
//! Responsibility: grouped structural validation before grouped policy gates.
//! Does not own: grouped policy admissibility rules or runtime grouped execution checks.
//! Boundary: validates grouped spec and HAVING symbol structure at planner boundary.

use crate::{
    db::{
        query::plan::{
            GroupAggregateSpec, GroupHavingExpr, GroupHavingValueExpr, GroupSpec,
            expr::ProjectionSpec,
            validate::grouped::projection_expr::validate_group_projection_expr_compatibility,
            validate::{GroupPlanError, PlanError, resolve_group_aggregate_target_field_type},
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
};

// Validate grouped structural invariants before policy/cursor gates.
pub(in crate::db::query::plan::validate) fn validate_group_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
    projection: &ProjectionSpec,
    having_expr: Option<&GroupHavingExpr>,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() && having_expr.is_some() {
        return Err(PlanError::from(
            GroupPlanError::global_distinct_aggregate_shape_unsupported(),
        ));
    }

    validate_group_spec_structure(schema, model, group)?;
    validate_group_projection_expr_compatibility(group, projection)?;
    validate_grouped_having_structure(group, having_expr)?;

    Ok(())
}

// Validate grouped HAVING structural symbol/reference compatibility.
fn validate_grouped_having_structure(
    group: &GroupSpec,
    having_expr: Option<&GroupHavingExpr>,
) -> Result<(), PlanError> {
    if let Some(having_expr) = having_expr {
        let mut compare_index = 0;
        validate_grouped_having_expr_structure(group, having_expr, &mut compare_index)?;
    }

    Ok(())
}

// Validate that HAVING group-field references are a subset of declared GROUP BY keys.
fn validate_having_group_field_reference(
    group: &GroupSpec,
    field_slot: &crate::db::query::plan::FieldSlot,
    index: usize,
) -> Result<(), PlanError> {
    group
        .group_fields
        .iter()
        .any(|group_field| group_field.index() == field_slot.index())
        .then_some(())
        .ok_or_else(|| {
            PlanError::from(GroupPlanError::having_non_group_field_reference(
                index,
                field_slot.field(),
            ))
        })
}

// Validate that HAVING aggregate symbols point at declared aggregate slots.
fn validate_having_aggregate_index(
    group: &GroupSpec,
    aggregate_index: usize,
    index: usize,
) -> Result<(), PlanError> {
    (aggregate_index < group.aggregates.len())
        .then_some(())
        .ok_or_else(|| {
            PlanError::from(GroupPlanError::having_aggregate_index_out_of_bounds(
                index,
                aggregate_index,
                group.aggregates.len(),
            ))
        })
}

// Validate grouped structural declarations against model/schema shape.
fn validate_group_spec_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    match (
        group.group_fields.is_empty(),
        group.aggregates.iter().any(GroupAggregateSpec::distinct),
    ) {
        (true, true) => return Ok(()),
        (true, false) => return Err(PlanError::from(GroupPlanError::empty_group_fields())),
        (false, _) => {}
    }
    (!group.aggregates.is_empty())
        .then_some(())
        .ok_or_else(|| PlanError::from(GroupPlanError::empty_aggregates()))?;

    let mut seen_group_slots = Vec::<usize>::with_capacity(group.group_fields.len());
    for field_slot in &group.group_fields {
        model.fields.get(field_slot.index()).ok_or_else(|| {
            PlanError::from(GroupPlanError::unknown_group_field(field_slot.field()))
        })?;
        seen_group_slots
            .iter()
            .any(|seen| *seen == field_slot.index())
            .then_some(())
            .map_or_else(
                || {
                    seen_group_slots.push(field_slot.index());
                    Ok(())
                },
                |()| {
                    Err(PlanError::from(GroupPlanError::duplicate_group_field(
                        field_slot.field(),
                    )))
                },
            )?;
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        let Some(target_field) = aggregate.target_field.as_ref() else {
            continue;
        };
        resolve_group_aggregate_target_field_type(schema, target_field, index)
            .map_err(PlanError::from)?;
    }

    Ok(())
}

fn validate_grouped_having_expr_structure(
    group: &GroupSpec,
    expr: &GroupHavingExpr,
    compare_index: &mut usize,
) -> Result<(), PlanError> {
    match expr {
        GroupHavingExpr::Compare { left, right, .. } => {
            validate_grouped_having_value_expr_structure(group, left, *compare_index)?;
            validate_grouped_having_value_expr_structure(group, right, *compare_index)?;
            *compare_index = compare_index.saturating_add(1);
            Ok(())
        }
        GroupHavingExpr::And(children) => {
            for child in children {
                validate_grouped_having_expr_structure(group, child, compare_index)?;
            }
            Ok(())
        }
    }
}

fn validate_grouped_having_value_expr_structure(
    group: &GroupSpec,
    expr: &GroupHavingValueExpr,
    compare_index: usize,
) -> Result<(), PlanError> {
    match expr {
        GroupHavingValueExpr::GroupField(field_slot) => {
            validate_having_group_field_reference(group, field_slot, compare_index)
        }
        GroupHavingValueExpr::AggregateIndex(aggregate_index) => {
            validate_having_aggregate_index(group, *aggregate_index, compare_index)
        }
        GroupHavingValueExpr::Literal(_) => Ok(()),
        GroupHavingValueExpr::FunctionCall { args, .. } => {
            for arg in args {
                validate_grouped_having_value_expr_structure(group, arg, compare_index)?;
            }
            Ok(())
        }
        GroupHavingValueExpr::Unary { expr, .. } => {
            validate_grouped_having_value_expr_structure(group, expr, compare_index)
        }
        GroupHavingValueExpr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                validate_grouped_having_value_expr_structure(
                    group,
                    arm.condition(),
                    compare_index,
                )?;
                validate_grouped_having_value_expr_structure(group, arm.result(), compare_index)?;
            }

            validate_grouped_having_value_expr_structure(group, else_expr, compare_index)
        }
        GroupHavingValueExpr::Binary { left, right, .. } => {
            validate_grouped_having_value_expr_structure(group, left, compare_index)?;
            validate_grouped_having_value_expr_structure(group, right, compare_index)
        }
    }
}
