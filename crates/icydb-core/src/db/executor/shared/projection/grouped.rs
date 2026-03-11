//! Module: db::executor::shared::projection::grouped
//! Responsibility: module-local ownership and contracts for db::executor::shared::projection::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::query::{builder::AggregateExpr, plan::FieldSlot},
    value::Value,
};

///
/// GroupedRowView
///
/// Read-only grouped-row adapter for expression evaluation over finalized
/// grouped-key and aggregate outputs.
///

pub(in crate::db::executor) struct GroupedRowView<'a> {
    pub(in crate::db::executor::shared::projection) key_values: &'a [Value],
    pub(in crate::db::executor::shared::projection) aggregate_values: &'a [Value],
    pub(in crate::db::executor::shared::projection) group_fields: &'a [FieldSlot],
    pub(in crate::db::executor::shared::projection) aggregate_exprs: &'a [AggregateExpr],
}

impl<'a> GroupedRowView<'a> {
    /// Build one grouped-row adapter from grouped finalization payloads.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        key_values: &'a [Value],
        aggregate_values: &'a [Value],
        group_fields: &'a [FieldSlot],
        aggregate_exprs: &'a [AggregateExpr],
    ) -> Self {
        Self {
            key_values,
            aggregate_values,
            group_fields,
            aggregate_exprs,
        }
    }
}

pub(in crate::db::executor::shared::projection) fn resolve_group_field_offset(
    grouped_row: &GroupedRowView<'_>,
    field_name: &str,
) -> Option<usize> {
    for (offset, group_field) in grouped_row.group_fields.iter().enumerate() {
        if group_field.field() == field_name {
            return Some(offset);
        }
    }

    None
}

pub(in crate::db::executor::shared::projection) fn resolve_grouped_aggregate_index(
    grouped_row: &GroupedRowView<'_>,
    aggregate_expr: &AggregateExpr,
) -> Option<usize> {
    for (index, candidate) in grouped_row.aggregate_exprs.iter().enumerate() {
        if candidate == aggregate_expr {
            return Some(index);
        }
    }

    None
}
