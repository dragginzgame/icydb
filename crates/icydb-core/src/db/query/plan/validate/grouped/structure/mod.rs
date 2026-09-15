//! Module: query::plan::validate::grouped::structure
//! Responsibility: grouped structural validation before grouped policy gates.
//! Does not own: grouped policy admissibility rules or runtime grouped execution checks.
//! Boundary: validates grouped spec and HAVING symbol structure at planner boundary.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    query::builder::scalar_projection::write_scalar_projection_expr_plan_label,
    query::plan::{
        AggregateSemanticKeyRef, GroupSpec,
        expr::{Expr, ProjectionSpec},
        validate::grouped::projection_expr::validate_group_projection_expr_compatibility,
        validate::{GroupPlanError, PlanError, resolve_group_aggregate_target_field_type},
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, QueryFieldRole};

// Validate grouped structural invariants before policy/cursor gates.
pub(in crate::db::query) fn validate_group_structure(
    schema: &SchemaInfo,
    group: &GroupSpec,
    projection: &ProjectionSpec,
    having_expr: Option<&Expr>,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    validate_group_spec_structure(schema, group)?;
    validate_group_projection_expr_compatibility(group, projection, work)?;
    validate_grouped_having_structure(group, having_expr, work)?;

    Ok(())
}

// Validate grouped HAVING structural symbol/reference compatibility.
fn validate_grouped_having_structure(
    group: &GroupSpec,
    having_expr: Option<&Expr>,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if let Some(having_expr) = having_expr {
        let mut compare_index = 0;
        validate_grouped_having_expr_structure(group, having_expr, &mut compare_index, work)?;
    }

    Ok(())
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
        if !group_field.matches_schema_identity(schema) {
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
            if seen.same_identity(group_field) {
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
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    expr.try_for_each_tree_expr_with_compare_index(compare_index, &mut |compare_index, node| {
        // Admit the reference walk itself before any aggregate candidate scan.
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        match node {
            Expr::Field(_) | Expr::FieldPath(_) => {
                if !group.group_fields.try_contains_expr(node, &mut |steps| {
                    work.charge(Resource::PredicateExpressionSteps, steps)
                })? {
                    return Err(
                        PlanError::from(GroupPlanError::having_non_group_field_reference(
                            compare_index,
                            work.render_text(|out| {
                                write_scalar_projection_expr_plan_label(node, out)
                            })?,
                        ))
                        .attach_query_field(QueryFieldRole::Having)
                        .into(),
                    );
                }

                Ok(())
            }
            Expr::Aggregate(aggregate_expr) => {
                if resolve_group_having_aggregate_index(group, aggregate_expr, work)?.is_none() {
                    return Err(PlanError::from(
                        GroupPlanError::having_aggregate_index_out_of_bounds(
                            compare_index,
                            group.aggregates.len(),
                            group.aggregates.len(),
                        ),
                    )
                    .into());
                }

                Ok(())
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
    work: &PreparationWork<'_>,
) -> Result<Option<usize>, QueryError> {
    let semantic_key = AggregateSemanticKeyRef::from_aggregate_expr(aggregate_expr);

    for (index, aggregate) in group.aggregates.iter().enumerate() {
        if aggregate
            .semantic_key()
            .try_eq_for_preparation(semantic_key, work)
            .map_err(QueryError::execute)?
        {
            return Ok(Some(index));
        }
    }
    Ok(None)
}
