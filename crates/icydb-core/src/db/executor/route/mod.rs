//! Module: db::executor::route
//! Responsibility: derive runtime route decisions from validated executor/query inputs.
//! Does not own: logical query semantics or stream/kernel execution internals.
//! Boundary: produces one immutable execution-route contract consumed by runtime dispatch.

mod capability;
mod contracts;
mod fast_path;
mod guard;
mod hints;
mod mode;
mod planner;
mod pushdown;
mod semantics;

pub(in crate::db::executor::route) use capability::direction_allows_physical_fetch_hint;
#[cfg(test)]
pub(in crate::db::executor) use capability::{
    derive_budget_safety_flags, streaming_access_shape_safe,
};
pub(in crate::db::executor) use capability::{
    primary_scan_fetch_hint_for_executable_access_path, supports_pk_stream_access_executable_path,
};
pub(in crate::db::executor) use contracts::*;
pub(in crate::db::executor) use fast_path::try_first_verified_fast_path_hit;
pub(super) use guard::*;
#[cfg(test)]
pub(in crate::db::executor) use planner::grouped_ordered_runtime_revalidation_flag_count_guard;
pub(in crate::db::executor) use pushdown::derive_secondary_pushdown_applicability_validated;
#[cfg(test)]
pub(in crate::db) use pushdown::{
    assess_secondary_order_pushdown, assess_secondary_order_pushdown_if_applicable,
    assess_secondary_order_pushdown_if_applicable_validated,
};
pub(in crate::db::executor) use semantics::{
    aggregate_bounded_probe_fetch_hint, aggregate_extrema_direction,
    aggregate_materialized_fold_direction, aggregate_supports_bounded_probe_hint,
    direction_from_order, order_direction_from_direction,
};
