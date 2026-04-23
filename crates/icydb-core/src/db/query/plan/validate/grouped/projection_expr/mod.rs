//! Module: query::plan::validate::grouped::projection_expr
//! Responsibility: grouped projection-expression compatibility validation at planner boundary.
//! Does not own: runtime grouped projection evaluation or executor fallback behavior.
//! Boundary: enforces grouped projection expression field/symbol compatibility rules.

use crate::db::{
    query::plan::{
        FieldSlot, GroupSpec,
        expr::{ProjectionSpec, infer_expr_type},
        validate::{ExprPlanError, PlanError},
    },
    schema::SchemaInfo,
};

// Validate GROUP BY expression compatibility over canonical projection semantics.
pub(crate) fn validate_group_projection_expr_compatibility(
    group: &GroupSpec,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    group.group_fields.is_empty().then_some(()).map_or_else(
        || {
            let grouped_fields = group
                .group_fields
                .iter()
                .map(FieldSlot::field)
                .collect::<Vec<_>>();

            for (index, field) in projection.fields().enumerate() {
                field
                    .expr()
                    .references_only_fields(grouped_fields.as_slice())
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
pub(crate) fn validate_projection_expr_types(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    for field in projection.fields() {
        infer_expr_type(field.expr(), schema)?;
    }

    Ok(())
}
