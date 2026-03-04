use crate::db::{
    predicate::SchemaInfo,
    query::plan::{
        FieldSlot, GroupSpec,
        expr::{ProjectionField, ProjectionSpec, expr_references_only_fields, infer_expr_type},
        validate::{ExprPlanError, PlanError},
    },
};
use std::collections::HashSet;

// Validate GROUP BY expression compatibility over canonical projection semantics.
pub(in crate::db::query::plan::validate) fn validate_group_projection_expr_compatibility(
    group: &GroupSpec,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    if group.group_fields.is_empty() {
        return Ok(());
    }

    let grouped_fields = group
        .group_fields
        .iter()
        .map(FieldSlot::field)
        .collect::<HashSet<_>>();

    for (index, field) in projection.fields().enumerate() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                if !expr_references_only_fields(expr, &grouped_fields) {
                    return Err(PlanError::from(
                        ExprPlanError::GroupedProjectionReferencesNonGroupField { index },
                    ));
                }
            }
        }
    }

    Ok(())
}

// Validate deterministic planner expression typing over one canonical projection shape.
pub(in crate::db::query::plan::validate) fn validate_projection_expr_types(
    schema: &SchemaInfo,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                infer_expr_type(expr, schema)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
pub(in crate::db::query) fn validate_group_projection_expr_compatibility_for_test(
    group: &GroupSpec,
    projection: &ProjectionSpec,
) -> Result<(), PlanError> {
    validate_group_projection_expr_compatibility(group, projection)
}
