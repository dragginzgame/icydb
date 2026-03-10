//! Module: db::query::intent::order
//! Responsibility: module-local ownership and contracts for db::query::intent::order.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::query::plan::{OrderDirection, OrderSpec};

/// Helper to append an ordering field while preserving existing order spec.
pub(in crate::db::query::intent) fn push_order(
    order: Option<OrderSpec>,
    field: &str,
    direction: OrderDirection,
) -> OrderSpec {
    match order {
        Some(mut spec) => {
            spec.fields.push((field.to_string(), direction));
            spec
        }
        None => OrderSpec {
            fields: vec![(field.to_string(), direction)],
        },
    }
}
