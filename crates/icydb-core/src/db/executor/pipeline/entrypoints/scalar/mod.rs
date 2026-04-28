//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint module wiring.
//! Does not own: runtime construction, execution loops, finalization, or diagnostics logic.
//! Boundary: re-exports scalar entrypoint surfaces to the executor pipeline root.

#[cfg(feature = "diagnostics")]
mod diagnostics;
mod entrypoints;
mod finalize;
mod hints;
mod materialized;
mod runtime;
mod streaming;

#[cfg(feature = "diagnostics")]
pub(in crate::db) use diagnostics::ScalarExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use entrypoints::execute_prepared_scalar_rows_for_canister_with_phase_attribution;
pub(in crate::db::executor) use entrypoints::{
    PreparedScalarMaterializedBoundary, execute_prepared_scalar_rows_for_canister,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use entrypoints::{
    execute_initial_scalar_retained_slot_page_from_runtime_parts_for_canister,
    execute_prepared_scalar_aggregate_kernel_row_sink_for_canister,
};
pub(in crate::db::executor) use materialized::execute_prepared_scalar_route_runtime;
pub(in crate::db::executor) use runtime::PreparedScalarRouteRuntime;
