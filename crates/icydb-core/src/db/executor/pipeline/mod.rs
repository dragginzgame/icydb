//! Module: db::executor::pipeline
//! Responsibility: execution pipeline orchestration boundaries shared by load entrypoints.
//! Does not own: scan-route execution details or terminal page shaping.
//! Boundary: owns pipeline-phase execution modules and compatibility export points.

pub(in crate::db) mod contracts;
pub(in crate::db) mod entrypoints;
pub(super) mod grouped_runtime;
pub(super) mod operators;
pub(super) mod orchestrator;
pub(super) mod runtime;
pub(super) mod timing;

#[cfg(feature = "sql")]
pub(in crate::db::executor) use entrypoints::execute_initial_scalar_retained_slot_page_from_runtime_parts_for_canister;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use entrypoints::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, ScalarExecutePhaseAttribution,
};
