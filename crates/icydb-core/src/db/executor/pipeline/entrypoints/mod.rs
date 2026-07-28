//! Module: executor::pipeline::entrypoints
//! Responsibility: structural scalar and grouped execution entrypoints.
//! Does not own: typed/dynamic response decoding or frontend orchestration.
//! Boundary: consumes accepted prepared plans and returns structural rows.

#[cfg(feature = "query")]
mod grouped;
#[cfg(feature = "query")]
mod scalar;

#[cfg(feature = "query")]
pub(in crate::db) use grouped::execute_shared_grouped_plan_for_canister;
#[cfg(all(feature = "query", feature = "diagnostics"))]
pub(in crate::db) use grouped::execute_shared_grouped_plan_for_canister_with_phase_attribution;
#[cfg(all(feature = "query", feature = "diagnostics"))]
pub(in crate::db) use grouped::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, GroupedRuntimeAttribution,
};
#[cfg(feature = "query")]
pub(in crate::db::executor) use scalar::execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister;
#[cfg(feature = "query")]
pub(in crate::db::executor) use scalar::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister;
