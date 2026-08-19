//! Module: db::query::plan::validate::symbols
//! Responsibility: validate user-facing field and symbol references against
//! the model and grouped/query projection surfaces.
//! Does not own: ordering, cursor, or grouped policy enforcement outside symbol lookup.
//! Boundary: keeps symbol-resolution failures localized within query-plan validation.

use crate::{
    db::query::{
        intent::QueryError,
        plan::{
            FieldSlot,
            validate::{GroupPlanError, PlanError},
        },
    },
    db::schema::{FieldType, SchemaInfo},
};
use icydb_diagnostic_code::QueryFieldRole;

/// Resolve one grouped field through schema slot authority.
pub(in crate::db) fn resolve_group_field_slot_with_schema(
    schema: &SchemaInfo,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    FieldSlot::resolve_with_schema(schema, field).ok_or_else(|| {
        PlanError::from(GroupPlanError::unknown_group_field(field))
            .attach_query_field(QueryFieldRole::GroupBy)
    })
}

/// Resolve one aggregate target field through schema slot authority.
///
/// The physical slot, field label, and type metadata all come from the
/// selected accepted `SchemaInfo`.
pub(in crate::db) fn resolve_aggregate_target_field_slot_with_schema(
    schema: &SchemaInfo,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    FieldSlot::resolve_with_schema(schema, field)
        .ok_or_else(|| QueryError::unknown_aggregate_target_field(field))
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
