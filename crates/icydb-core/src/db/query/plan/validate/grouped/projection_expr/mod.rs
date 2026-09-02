//! Module: query::plan::validate::grouped::projection_expr
//! Responsibility: grouped projection-expression compatibility validation at planner boundary.
//! Does not own: runtime grouped projection evaluation or executor fallback behavior.
//! Boundary: enforces grouped projection expression field/symbol compatibility rules.

use crate::db::{
    query::plan::{
        GroupSpec,
        expr::{ProjectionSpec, infer_expr_type},
        validate::{ExprPlanError, PlanError},
    },
    schema::SchemaInfo,
};
use icydb_diagnostic_code::QueryFieldRole;

// Validate GROUP BY expression compatibility over canonical projection semantics.
pub(in crate::db::query) fn validate_group_projection_expr_compatibility(
    group: &GroupSpec,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    group.group_fields.is_empty().then_some(()).map_or_else(
        || {
            for (index, field) in projection.fields().enumerate() {
                group
                    .group_fields
                    .contains_all_expr_references(field.expr())
                    .then_some(())
                    .ok_or_else(|| {
                        PlanError::from(
                            ExprPlanError::grouped_projection_references_non_group_field(index),
                        )
                    })?;
            }

            Ok(())
        },
        |()| Ok(()),
    )
}

// Validate deterministic planner expression typing over one canonical projection shape.
pub(in crate::db::query) fn validate_projection_expr_types(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    for field in projection.fields() {
        infer_expr_type(field.expr(), schema)
            .map_err(|error| error.attach_query_field(QueryFieldRole::Projection))?;
    }

    Ok(())
}
