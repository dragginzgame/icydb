//! Neutral execution-plan contracts shared across db subsystems.
//!
//! This module intentionally re-exports only execution-relevant plan
//! structs/enums. Query semantic validation remains owned by
//! `db::query::plan::validate`.

use crate::db::direction::Direction;

pub(in crate::db) use crate::db::query::plan::{
    AccessPlannedQuery, LogicalPlan, OrderDirection, OrderSpec, QueryMode,
};
#[cfg(test)]
pub(in crate::db) use crate::db::query::plan::{GroupedPlan, PageSpec};

///
/// OrderSlotPolicy
///
/// Slot-selection policy for deriving scan direction from canonical order specs.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum OrderSlotPolicy {
    First,
    Last,
}

/// Derive scan direction from an optional canonical order spec + slot policy.
#[must_use]
pub(in crate::db) fn derive_scan_direction(
    order: Option<&OrderSpec>,
    slot_policy: OrderSlotPolicy,
) -> Direction {
    let selected = order.and_then(|order| match slot_policy {
        OrderSlotPolicy::First => order.fields.first(),
        OrderSlotPolicy::Last => order.fields.last(),
    });

    selected.map_or(Direction::Asc, |(_, direction)| direction.as_direction())
}
