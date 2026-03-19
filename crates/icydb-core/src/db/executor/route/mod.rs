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
pub(in crate::db::executor::route) use capability::derive_execution_capabilities_for_model;
pub(in crate::db::executor::route) use capability::direction_allows_physical_fetch_hint;
pub(in crate::db::executor) use contracts::*;
pub(in crate::db::executor::route) use fast_path::aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation;
pub(in crate::db::executor::route) use fast_path::pk_order_stream_fast_path_shape_supported_for_model;
pub(in crate::db::executor) use fast_path::try_first_verified_fast_path_hit;
pub(in crate::db::executor) use grouped_runtime::{
    grouped_plan_metrics_strategy_for_execution_strategy, grouped_route_observability_for_runtime,
};
pub(super) use guard::*;
pub(in crate::db::executor::route) use hints::{
    aggregate_probe_fetch_hint_for_model, aggregate_seek_spec_for_model,
    assess_index_range_limit_pushdown_for_model, bounded_probe_hint_is_safe,
    count_pushdown_fetch_hint, load_scan_budget_hint, top_n_seek_spec_for_model,
};
pub(in crate::db::executor) use mode::{
    aggregate_non_count_streaming_allowed, load_streaming_allowed,
};
pub(in crate::db::executor::route) use mode::{
    derive_aggregate_route_direction, derive_load_route_direction,
};
pub(in crate::db::executor) use pushdown::access_order_satisfied_by_route_contract_for_model;
pub(in crate::db) use pushdown::derive_secondary_pushdown_applicability_from_contract;
pub(in crate::db::executor) use pushdown::secondary_order_contract_active;
pub(in crate::db::executor) use semantics::{
    aggregate_bounded_probe_fetch_hint, aggregate_extrema_direction,
    aggregate_materialized_fold_direction, aggregate_supports_bounded_probe_hint,
};
pub(in crate::db::executor) use terminal::{
    BytesTerminalFastPathContract, CountTerminalFastPathContract, ExistsTerminalFastPathContract,
};
