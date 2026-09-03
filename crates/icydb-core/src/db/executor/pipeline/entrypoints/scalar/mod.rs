//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint module wiring.
//! Does not own: runtime construction or execution loops.
//! Boundary: re-exports scalar entrypoint surfaces to the executor pipeline root.

mod entrypoints;
mod execution;
mod hints;
mod materialized;
mod runtime;
#[cfg(feature = "sql")]
mod streaming;

pub(in crate::db::executor) use entrypoints::execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use entrypoints::execute_prepared_scalar_aggregate_kernel_row_sink_for_canister;
pub(in crate::db::executor) use entrypoints::execute_resumed_scalar_retained_slot_page_from_runtime_handoff_for_canister;
