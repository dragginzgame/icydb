//! Module: db::query::plan::validate::symbols
//! Responsibility: validate user-facing field and symbol references against
//! the model and grouped/query projection surfaces.
//! Does not own: ordering, cursor, or grouped policy enforcement outside symbol lookup.
//! Boundary: keeps symbol-resolution failures localized within query-plan validation.

use crate::{
    db::query::{
        intent::QueryError,
        plan::{
            FieldSlot, GroupField, GroupFieldSet,
            validate::{ExprPlanError, GroupPlanError, PlanError},
        },
        preparation::PreparationWork,
    },
    db::schema::{FieldType, SchemaInfo},
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, QueryFieldRole};

/// Materialize a complete grouping clause through the existing key resolver.
/// Select its final representation before allocation, avoiding prefix promotion.
/// Key payload copying and downstream rebinding are separate accounting owners.
pub(in crate::db) fn resolve_group_fields_with_schema(
    schema: &SchemaInfo,
    fields: &[String],
    work: &PreparationWork<'_>,
) -> Result<GroupFieldSet, QueryError> {
    let mut has_path = false;
    for field in fields {
        work.charge(Resource::PredicateExpressionSteps, 1 + field.len() as u64)?;
        has_path |= field.contains('.');
    }
    let mut resolved = if has_path {
        GroupFieldSet::PathAware(work.vec_with_capacity(fields.len())?)
    } else {
        GroupFieldSet::Direct(work.vec_with_capacity(fields.len())?)
    };
    for field in fields {
        let key = GroupField::resolve_with_schema(schema, field).ok_or_else(|| {
            PlanError::from(GroupPlanError::unknown_group_field(field))
                .attach_query_field(QueryFieldRole::GroupBy)
        })?;
        resolved.push(key);
    }
    Ok(resolved)
}

/// Resolve one aggregate target field through schema slot authority.
///
/// The physical slot, field label, and type metadata all come from the
/// selected accepted `SchemaInfo`.
pub(in crate::db) fn resolve_aggregate_target_field_slot_with_schema(
    schema: &SchemaInfo,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    FieldSlot::resolve_with_schema(schema, field).ok_or_else(|| {
        QueryError::from(
            PlanError::from(ExprPlanError::unknown_field(field))
                .attach_query_field(QueryFieldRole::AggregateTarget),
        )
    })
}

/// Resolve one grouped aggregate target field into one schema field type.
pub(in crate::db::query::plan::validate) fn resolve_group_aggregate_target_field_type<'a>(
    schema: &'a SchemaInfo,
    field: &str,
    index: usize,
) -> Result<&'a FieldType, GroupPlanError> {
    schema
        .field(field)
        .ok_or_else(|| GroupPlanError::unknown_aggregate_target_field(index, field))
}
