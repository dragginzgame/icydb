//! Neutral execution-plan contracts shared across db subsystems.
//!
//! This module intentionally re-exports only execution-relevant plan
//! structs/enums. Query semantic validation remains owned by
//! `db::query::plan::validate`.

use crate::db::{
    direction::Direction,
    query::plan::{AccessPlannedQuery, OrderSpec},
};

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

/// Derive canonical direction for primary-order execution surfaces.
#[must_use]
pub(in crate::db) fn derive_primary_scan_direction(order: Option<&OrderSpec>) -> Direction {
    derive_scan_direction(order, OrderSlotPolicy::First)
}

/// Derive canonical direction for secondary-index order pushdown surfaces.
#[must_use]
pub(in crate::db) fn derive_secondary_order_scan_direction(order: Option<&OrderSpec>) -> Direction {
    derive_scan_direction(order, OrderSlotPolicy::Last)
}

/// Derive the effective pagination offset for a plan under cursor-window semantics.
#[must_use]
pub(in crate::db) fn effective_page_offset_for_window<K>(
    plan: &AccessPlannedQuery<K>,
    cursor_boundary_present: bool,
) -> u32 {
    if cursor_boundary_present {
        return 0;
    }

    plan.page.as_ref().map_or(0, |page| page.offset)
}

/// Derive the effective keep-count (`offset + limit`) for one plan and limit.
#[must_use]
pub(in crate::db) fn effective_keep_count_for_limit<K>(
    plan: &AccessPlannedQuery<K>,
    cursor_boundary_present: bool,
    limit: u32,
) -> usize {
    let effective_offset = effective_page_offset_for_window(plan, cursor_boundary_present);
    usize::try_from(effective_offset)
        .unwrap_or(usize::MAX)
        .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
}
