//! Module: query::plan::validate::grouped::structure
//! Responsibility: grouped structural validation before grouped policy gates.
//! Does not own: grouped policy admissibility rules or runtime grouped execution checks.
//! Boundary: validates grouped spec and HAVING symbol structure at planner boundary.

use crate::{
    db::{
        query::plan::{
            GroupSpec,
            expr::{Expr, ProjectionSpec},
            validate::grouped::projection_expr::validate_group_projection_expr_compatibility,
            validate::{GroupPlanError, PlanError, resolve_group_aggregate_target_field_type},
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
};

// Validate grouped structural invariants before policy/cursor gates.
pub(crate) fn validate_group_structure(
    schema: &SchemaInfo,
    model: &EntityModel,
    group: &GroupSpec,
    projection: &ProjectionSpec,
    having_expr: Option<&Expr>,
) -> Result<(), PlanError> {
    validate_group_spec_structure(schema, model, group)?;
    validate_group_projection_expr_compatibility(group, projection)?;
    validate_grouped_having_structure(group, having_expr)?;

    Ok(())
}

// Validate grouped HAVING structural symbol/reference compatibility.
fn validate_grouped_having_structure(
    group: &GroupSpec,
    having_expr: Option<&Expr>,
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
    if group.group_fields.is_empty() {
        (!group.aggregates.is_empty())
            .then_some(())
            .ok_or_else(|| PlanError::from(GroupPlanError::empty_aggregates()))?;

        for (index, aggregate) in group.aggregates.iter().enumerate() {
            let Some(target_field) = aggregate.target_field() else {
                continue;
            };
            resolve_group_aggregate_target_field_type(schema, target_field, index)
                .map_err(PlanError::from)?;
        }

        return Ok(());
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
        let Some(target_field) = aggregate.target_field() else {
            continue;
        };
        resolve_group_aggregate_target_field_type(schema, target_field, index)
            .map_err(PlanError::from)?;
    }

    Ok(())
}

fn validate_grouped_having_expr_structure(
    group: &GroupSpec,
    expr: &Expr,
    compare_index: &mut usize,
) -> Result<(), PlanError> {
    expr.try_for_each_tree_expr_with_compare_index(compare_index, &mut |compare_index, node| {
        match node {
            Expr::Field(field_id) => {
                let field_name = field_id.as_str();
                let Some(field_slot) = group
                    .group_fields
                    .iter()
                    .find(|group_field| group_field.field() == field_name)
                else {
                    return Err(PlanError::from(
                        GroupPlanError::having_non_group_field_reference(compare_index, field_name),
                    ));
                };

                validate_having_group_field_reference(group, field_slot, compare_index)
            }
            Expr::Aggregate(aggregate_expr) => {
                let Some(aggregate_index) =
                    resolve_group_having_aggregate_index(group, aggregate_expr)
                else {
                    return Err(PlanError::from(
                        GroupPlanError::having_aggregate_index_out_of_bounds(
                            compare_index,
                            group.aggregates.len(),
                            group.aggregates.len(),
                        ),
                    ));
                };

                validate_having_aggregate_index(group, aggregate_index, compare_index)
            }
            Expr::Literal(_)
            | Expr::FunctionCall { .. }
            | Expr::Unary { .. }
            | Expr::Case { .. }
            | Expr::Binary { .. } => Ok(()),
            #[cfg(test)]
            Expr::Alias { .. } => Ok(()),
        }
    })
}

fn resolve_group_having_aggregate_index(
    group: &GroupSpec,
    aggregate_expr: &crate::db::query::builder::AggregateExpr,
) -> Option<usize> {
    group.aggregates.iter().position(|aggregate| {
        let distinct_matches = aggregate.distinct() == aggregate_expr.is_distinct();

        aggregate.kind() == aggregate_expr.kind()
            && aggregate.target_field() == aggregate_expr.target_field()
            && aggregate.semantic_input_expr_owned().as_ref() == aggregate_expr.input_expr()
            && distinct_matches
    })
}
