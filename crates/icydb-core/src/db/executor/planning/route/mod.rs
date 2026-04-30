//! Module: db::executor::planning::route
//! Responsibility: derive runtime route decisions from validated executor/query inputs.
//! Does not own: logical query semantics or stream/kernel execution internals.
//! Boundary: produces one immutable execution-route contract consumed by runtime dispatch.

mod capability;
mod contracts;
mod fast_path;
mod guard;
mod hints;
mod mode;
mod order_pushdown;
mod planner;
mod pushdown;
mod terminal;

///
/// TESTS
///

#[cfg(test)]
mod tests;

pub(in crate::db::executor::planning::route) use capability::derive_execution_capabilities_for_model;
pub(in crate::db::executor::planning::route) use capability::direction_allows_physical_fetch_hint;
pub(in crate::db::executor) use capability::explain_access_order_satisfied_for_model;
pub(in crate::db::executor) use capability::{
    count_pushdown_shape_supported, direct_primary_key_lookup_shape_supported,
    ordered_key_stream_window_shape_supported, paged_primary_key_numeric_fold_shape_supported,
    primary_key_stream_window_shape_supported, primary_scan_fetch_hint_shape_supported,
    streaming_numeric_fold_shape_supported, top_n_seek_lookahead_required_for_shape,
};
pub(in crate::db) use contracts::AggregateRouteShape;
pub use contracts::RouteExecutionMode;
pub(in crate::db::executor) use contracts::*;
pub(in crate::db) use contracts::{LoadOrderRouteContract, LoadOrderRouteReason};
pub(in crate::db::executor::planning::route) use fast_path::aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation;
pub(in crate::db::executor::planning::route) use fast_path::pk_order_stream_fast_path_shape_supported;
pub(in crate::db::executor) use fast_path::try_first_verified_fast_path_hit;
pub(in crate::db::executor) use fast_path::verify_pk_stream_fast_path_access;
pub(in crate::db::executor) use guard::*;
pub(in crate::db::executor) use hints::widened_residual_filter_predicate_pushdown_fetch;
pub(in crate::db::executor::planning::route) use hints::{
    aggregate_probe_fetch_hint, aggregate_seek_spec, assess_index_range_limit_pushdown_for_model,
    bounded_probe_hint_is_safe, count_pushdown_fetch_hint, load_scan_budget_hint,
    top_n_seek_spec_for_model,
};
pub(in crate::db::executor) use mode::{
    aggregate_non_count_streaming_allowed, load_streaming_allowed,
};
pub(in crate::db::executor::planning::route) use mode::{
    derive_aggregate_route_direction, derive_load_route_direction,
};
pub(in crate::db) use order_pushdown::{PushdownApplicability, SecondaryOrderPushdownRejection};
pub(in crate::db::executor) use planner::{RoutePlanRequest, build_execution_route_plan};
pub(in crate::db::executor) use pushdown::access_order_satisfied_by_route_contract;
pub(in crate::db) use pushdown::derive_secondary_pushdown_applicability_from_contract;
pub(in crate::db::executor::planning::route) use pushdown::index_range_limit_pushdown_shape_supported_for_order_contract;
pub(in crate::db::executor) use pushdown::secondary_order_contract_active;
pub(in crate::db::executor) use terminal::{
    BytesTerminalFastPathContract, CountTerminalFastPathContract, ExistsTerminalFastPathContract,
    LoadTerminalFastPathContract, derive_count_terminal_fast_path_contract_for_model,
    derive_exists_terminal_fast_path_contract_for_model,
    derive_load_terminal_fast_path_contract_for_plan,
};

#[cfg(test)]
const fn route_capability_flag_count_guard() -> usize {
    6
}

#[cfg(test)]
const fn route_shape_kind_count_guard() -> usize {
    5
}

#[cfg(test)]
const fn grouped_ordered_runtime_revalidation_flag_count_guard() -> usize {
    // Runtime grouped revalidation should stay capability-focused and consume
    // only eligibility, rejection reason, and grouped execution mode.
    3
}
