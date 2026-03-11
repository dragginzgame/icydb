//! Module: db::executor::shared
//! Responsibility: cross-layer executor contracts shared by load, scan, pipeline, and aggregate helpers.
//! Does not own: route planning policies or terminal/materialization behavior.
//! Boundary: exports internal executor contract surfaces to keep layer wiring explicit.

pub(super) mod execution_contracts;
mod load_context;
pub(super) mod load_contracts;
pub(super) mod projection;
