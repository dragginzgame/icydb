//! Module: db::executor::pipeline
//! Responsibility: execution pipeline orchestration boundaries shared by load entrypoints.
//! Does not own: scan-route execution details or terminal page shaping.
//! Boundary: owns pipeline-phase execution modules and compatibility export points.

pub(super) mod contracts;
pub(super) mod entrypoints;
pub(super) mod execute;
pub(super) mod grouped_route;
pub(super) mod grouped_runtime;
