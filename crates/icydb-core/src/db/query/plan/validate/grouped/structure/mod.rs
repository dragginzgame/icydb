//! Module: query::plan::validate::grouped::structure
//! Responsibility: grouped structural validation before grouped policy gates.
//! Does not own: grouped policy admissibility rules or runtime grouped execution checks.
//! Boundary: validates grouped spec and HAVING symbol structure at planner boundary.

use crate::db::{
    query::plan::{
        AggregateSemanticKey, GroupField, GroupSpec,
        expr::{Expr, ProjectionSpec},
        validate::grouped::projection_expr::validate_group_projection_expr_compatibility,
        validate::{GroupPlanError, PlanError, resolve_group_aggregate_target_field_type},
    },
    schema::SchemaInfo,
};
use icydb_diagnostic_code::QueryFieldRole;

// Validate grouped structural invariants before policy/cursor gates.
pub(in crate::db::query) fn validate_group_structure(
    schema: &SchemaInfo,
    group: &GroupSpec,
    projection: &ProjectionSpec,
    having_expr: Option<&Expr>,
) -> Result<(), PlanError> {
    validate_group_spec_structure(schema, group)?;
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
fn validate_group_spec_structure(schema: &SchemaInfo, group: &GroupSpec) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        (!group.aggregates.is_empty())
            .then_some(())
            .ok_or_else(|| PlanError::from(GroupPlanError::empty_aggregates()))?;

        for (index, aggregate) in group.aggregates.iter().enumerate() {
            let Some(target_field) = aggregate.target_field() else {
                continue;
            };
            resolve_group_aggregate_target_field_type(schema, target_field, index).map_err(
                |error| PlanError::from(error).attach_query_field(QueryFieldRole::AggregateTarget),
            )?;
        }

        return Ok(());
    }
    (!group.aggregates.is_empty())
        .then_some(())
        .ok_or_else(|| PlanError::from(GroupPlanError::empty_aggregates()))?;

    for (group_index, group_field) in group.group_fields.iter().enumerate() {
        let Some(resolved) = GroupField::resolve_with_schema(schema, group_field.field()) else {
            return Err(PlanError::from(GroupPlanError::unknown_group_field_at(
                group_index,
                group_field.field(),
            ))
            .attach_query_field(QueryFieldRole::GroupBy));
        };
        if group_field.root_slot() != resolved.root_slot() || !group_field.same_identity(&resolved)
        {
            return Err(PlanError::from(GroupPlanError::unknown_group_field_at(
                group_index,
                group_field.field(),
            ))
            .attach_query_field(QueryFieldRole::GroupBy));
        }

        for seen_index in 0..group_index {
            let Some(seen) = group.group_fields.get(seen_index) else {
                return Err(PlanError::from(GroupPlanError::unknown_group_field_at(
                    group_index,
                    group_field.field(),
                ))
                .attach_query_field(QueryFieldRole::GroupBy));
            };
            if seen.same_identity(&resolved) {
                return Err(PlanError::from(GroupPlanError::duplicate_group_field(
                    group_index,
                    group_field.field(),
                )));
            }
        }
    }

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        let Some(target_field) = aggregate.target_field() else {
            continue;
        };
        resolve_group_aggregate_target_field_type(schema, target_field, index).map_err(
            |error| PlanError::from(error).attach_query_field(QueryFieldRole::AggregateTarget),
        )?;
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
            Expr::Field(_) | Expr::FieldPath(_) => {
                if !group.group_fields.contains_expr(node) {
                    return Err(
                        PlanError::from(GroupPlanError::having_non_group_field_reference(
                            compare_index,
                            crate::db::query::builder::scalar_projection::render_scalar_projection_expr_plan_label(node),
                        ))
                        .attach_query_field(QueryFieldRole::Having),
                    );
                }

                Ok(())
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
    let semantic_key = AggregateSemanticKey::from_aggregate_expr(aggregate_expr);

    group
        .aggregates
        .iter()
        .position(|aggregate| aggregate.semantic_key() == semantic_key)
}
