//! Module: db::executor::traversal
//! Responsibility: module-local ownership and contracts for db::executor::traversal.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Traversal and pagination helpers shared across cursor and executor paths.
//!
//! This module owns effective page-window derivation under continuation
//! semantics. Query semantic validation remains
//! owned by `db::query::plan::validate`.

use crate::db::{
    predicate::MissingRowPolicy,
    query::plan::{AccessPlannedQuery, effective_offset_for_cursor_window},
};

/// Derive the effective pagination offset for a plan under cursor-window semantics.
#[must_use]
pub(in crate::db) fn effective_page_offset_for_window<K>(
    plan: &AccessPlannedQuery<K>,
    cursor_boundary_present: bool,
) -> u32 {
    let window_size = plan
        .scalar_plan()
        .page
        .as_ref()
        .map_or(0, |page| page.offset);

    effective_offset_for_cursor_window(window_size, cursor_boundary_present)
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

/// Derive row-read missing-row policy for one executor-consumed logical plan.
#[must_use]
pub(in crate::db::executor) const fn row_read_consistency_for_plan<K>(
    plan: &AccessPlannedQuery<K>,
) -> MissingRowPolicy {
    plan.scalar_plan().consistency
}
