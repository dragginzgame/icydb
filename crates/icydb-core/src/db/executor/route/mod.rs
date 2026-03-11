//! Module: db::executor::route
//! Responsibility: derive runtime route decisions from validated executor/query inputs.
//! Does not own: logical query semantics or stream/kernel execution internals.
//! Boundary: produces one immutable execution-route contract consumed by runtime dispatch.

mod capability;
mod contracts;
mod fast_path;
mod grouped_runtime;
mod guard;
mod hints;
mod mode;
mod planner;
mod pushdown;
mod semantics;
mod terminal;

pub(in crate::db::executor) use capability::derive_budget_safety_flags;
pub(in crate::db::executor::route) use capability::direction_allows_physical_fetch_hint;
#[cfg(test)]
pub(in crate::db::executor) use capability::stream_order_contract_safe;
pub(in crate::db::executor) use contracts::*;
pub(in crate::db::executor) use fast_path::try_first_verified_fast_path_hit;
pub(super) use guard::*;
#[cfg(test)]
pub(in crate::db::executor) use planner::grouped_ordered_runtime_revalidation_flag_count_guard;
pub(in crate::db::executor) use pushdown::access_order_satisfied_by_route_contract;
pub(in crate::db) use pushdown::derive_secondary_pushdown_applicability_from_contract;
pub(in crate::db::executor) use pushdown::secondary_order_contract_active;
pub(in crate::db::executor) use semantics::{
    aggregate_bounded_probe_fetch_hint, aggregate_extrema_direction,
    aggregate_materialized_fold_direction, aggregate_supports_bounded_probe_hint,
    direction_from_order,
};
pub(in crate::db::executor) use terminal::{
    BytesTerminalFastPathContract, CountTerminalFastPathContract, ExistsTerminalFastPathContract,
};
