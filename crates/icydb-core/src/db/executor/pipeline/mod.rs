//! Module: db::executor::pipeline
//! Responsibility: execution pipeline orchestration boundaries shared by load entrypoints.
//! Does not own: scan-route execution details or terminal page shaping.
//! Boundary: owns pipeline-phase execution modules and compatibility export points.

pub(in crate::db) mod contracts;
pub(in crate::db) mod entrypoints;
pub(super) mod grouped_runtime;
pub(super) mod operators;
pub(super) mod runtime;
pub(super) mod timing;
pub(in crate::db::executor) use entrypoints::execute_initial_scalar_retained_slot_page_from_runtime_handoff_for_canister;
pub(in crate::db::executor) use entrypoints::execute_resumed_scalar_retained_slot_page_from_runtime_handoff_for_canister;
pub(in crate::db) use entrypoints::execute_shared_grouped_plan_for_canister;
