//! Module: query::plan::projection
//! Responsibility: planner-owned projection intent lowering into canonical semantic shape.
//! Does not own: expression evaluation or executor output materialization.
//! Boundary: converts logical query intent into `ProjectionSpec`.

use crate::{
    db::{
        query::{
            builder::aggregate::AggregateExpr,
            plan::{
                FieldSlot, GroupAggregateSpec, LogicalPlan,
                expr::{
                    Expr, FieldId, ProjectionField, ProjectionSelection, ProjectionSpec,
                    collect_unique_direct_projection_slots_with_schema,
                },
                semantics::group_aggregate_spec_expr,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, field::FieldModel},
};

/// Lower one logical plan into the canonical planner-owned projection semantic shape.
#[must_use]
pub(in crate::db::query) fn lower_projection_intent(
    model: &EntityModel,
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> ProjectionSpec {
    match logical {
        LogicalPlan::Scalar(_) => lower_scalar_projection(model, selection),
        LogicalPlan::Grouped(grouped) => match selection {
            ProjectionSelection::Exprs(fields) => ProjectionSpec::new(fields.clone()),
            ProjectionSelection::All | ProjectionSelection::Fields(_) => lower_grouped_projection(
                grouped.group.group_fields.as_slice(),
                grouped.group.aggregates.as_slice(),
            ),
        },
    }
}

/// Lower one already-validated global aggregate output field list into the
/// canonical planner-owned projection semantic shape.
#[must_use]
pub(in crate::db) const fn lower_global_aggregate_projection(
    fields: Vec<ProjectionField>,
) -> ProjectionSpec {
    ProjectionSpec::new(fields)
}

/// Lower scalar plans to one explicit field projection per declared entity field.
fn lower_scalar_projection(model: &EntityModel, selection: &ProjectionSelection) -> ProjectionSpec {
    let fields = match selection {
        ProjectionSelection::All => model
            .fields
            .iter()
            .map(|field| direct_field_projection(FieldId::new(field.name)))
            .collect(),
        ProjectionSelection::Fields(field_ids) => field_ids
            .iter()
            .cloned()
            .map(direct_field_projection)
            .collect(),
        ProjectionSelection::Exprs(fields) => fields.clone(),
    };

    ProjectionSpec::new(fields)
}

/// Lower a direct slot projection layout using explicit schema authority.
#[must_use]
pub(in crate::db::query) fn lower_direct_projection_slots_with_schema(
    model: &EntityModel,
    schema: &SchemaInfo,
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> Option<Vec<usize>> {
    match logical {
        LogicalPlan::Scalar(_) => match selection {
            ProjectionSelection::All => collect_unique_direct_projection_slots_with_schema(
                schema,
                model.fields().iter().map(FieldModel::name),
            ),
            ProjectionSelection::Fields(field_ids) => {
                collect_unique_direct_projection_slots_with_schema(
                    schema,
                    field_ids.iter().map(FieldId::as_str),
                )
            }
            ProjectionSelection::Exprs(fields) => {
                collect_unique_direct_projection_slots_with_schema(
                    schema,
                    fields
                        .iter()
                        .map(ProjectionField::direct_field_name)
                        .collect::<Option<Vec<_>>>()?,
                )
            }
        },
        LogicalPlan::Grouped(_) => None,
    }
}

/// Lower a duplicate-preserving direct slot projection layout for raw data-row
/// readers using explicit schema authority.
#[must_use]
pub(in crate::db::query) fn lower_data_row_direct_projection_slots_with_schema(
    model: &EntityModel,
    schema: &SchemaInfo,
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> Option<Vec<usize>> {
    match logical {
        LogicalPlan::Scalar(_) => match selection {
            ProjectionSelection::All => collect_direct_projection_slots_with_schema(
                schema,
                model.fields().iter().map(FieldModel::name),
            ),
            ProjectionSelection::Fields(field_ids) => collect_direct_projection_slots_with_schema(
                schema,
                field_ids.iter().map(FieldId::as_str),
            ),
            ProjectionSelection::Exprs(fields) => collect_direct_projection_slots_with_schema(
                schema,
                fields
                    .iter()
                    .map(ProjectionField::direct_field_name)
                    .collect::<Option<Vec<_>>>()?,
            ),
        },
        LogicalPlan::Grouped(_) => None,
    }
}

/// Lower one logical plan into the identity projection used by hash/fingerprint
/// surfaces when a full schema model is not available at the call boundary.
#[must_use]
pub(in crate::db::query) fn lower_projection_identity(
    logical: &LogicalPlan,
    selection: &ProjectionSelection,
) -> ProjectionSpec {
    match logical {
        LogicalPlan::Scalar(_) => match selection {
            ProjectionSelection::All => ProjectionSpec::new(vec![direct_field_projection(
                FieldId::new("__icydb_scalar_projection_default_v1__"),
            )]),
            ProjectionSelection::Fields(field_ids) => ProjectionSpec::new(
                field_ids
                    .iter()
                    .cloned()
                    .map(direct_field_projection)
                    .collect(),
            ),
            ProjectionSelection::Exprs(fields) => ProjectionSpec::new(fields.clone()),
        },
        LogicalPlan::Grouped(grouped) => lower_grouped_projection(
            grouped.group.group_fields.as_slice(),
            grouped.group.aggregates.as_slice(),
        ),
    }
}

/// Lower grouped plans to one explicit projection of grouped keys followed by
/// grouped aggregates, preserving declaration order.
fn lower_grouped_projection(
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) -> ProjectionSpec {
    let mut fields = Vec::with_capacity(group_fields.len().saturating_add(aggregates.len()));
    for group_field in group_fields {
        fields.push(direct_field_projection(FieldId::new(group_field.field())));
    }
    for aggregate in aggregates {
        fields.push(aggregate_projection(group_aggregate_spec_expr(aggregate)));
    }

    ProjectionSpec::new(fields)
}

// Build one direct-field projection node so scalar, grouped, and identity
// lowering keep the same projection-field shape in one place.
const fn direct_field_projection(field_id: FieldId) -> ProjectionField {
    ProjectionField::Scalar {
        expr: Expr::Field(field_id),
        alias: None,
    }
}

// Build one grouped aggregate projection node so grouped projection lowering
// does not restate the scalar aggregate projection envelope inline.
const fn aggregate_projection(aggregate_expr: AggregateExpr) -> ProjectionField {
    ProjectionField::Scalar {
        expr: Expr::Aggregate(aggregate_expr),
        alias: None,
    }
}

// Resolve one direct field-slot layout while preserving duplicate source slots.
// Raw data-row projection can borrow the same slot repeatedly, unlike retained
// slot readers that consume values through `Option::take()`.
fn collect_direct_projection_slots_with_schema<'a>(
    schema: &SchemaInfo,
    field_names: impl IntoIterator<Item = &'a str>,
) -> Option<Vec<usize>> {
    let mut field_slots = Vec::new();

    for field_name in field_names {
        field_slots.push(schema.field_slot_index(field_name)?);
    }

    Some(field_slots)
}
