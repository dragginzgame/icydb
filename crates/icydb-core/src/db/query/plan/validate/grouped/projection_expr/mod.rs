//! Module: query::plan::validate::grouped::projection_expr
//! Responsibility: grouped projection-expression compatibility validation at planner boundary.
//! Does not own: runtime grouped projection evaluation or executor fallback behavior.
//! Boundary: enforces grouped projection expression field/symbol compatibility rules.

use crate::db::{
    QueryError,
    query::plan::{
        GroupSpec,
        expr::{ProjectionSpec, infer_expr_type},
        validate::{ExprPlanError, PlanError},
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, QueryFieldRole};

// Validate GROUP BY expression compatibility over canonical projection semantics.
pub(in crate::db::query) fn validate_group_projection_expr_compatibility(
    group: &GroupSpec,
    projection: &ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if group.group_fields.is_empty() {
        return Ok(());
    }
    for (index, field) in projection.fields().enumerate() {
        if !group
            .group_fields
            .try_contains_all_expr_references(field.expr(), &mut |steps| {
                work.charge(Resource::PredicateExpressionSteps, steps)
            })?
        {
            return Err(PlanError::from(
                ExprPlanError::grouped_projection_references_non_group_field(index),
            )
            .into());
        }
    }
    Ok(())
}

// Validate deterministic planner expression typing over one canonical projection shape.
pub(in crate::db::query) fn validate_projection_expr_types(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    for field in projection.fields() {
        infer_expr_type(field.expr(), schema, work)
            .map_err(|error| error.attach_query_field(QueryFieldRole::Projection))?;
    }

    Ok(())
}
