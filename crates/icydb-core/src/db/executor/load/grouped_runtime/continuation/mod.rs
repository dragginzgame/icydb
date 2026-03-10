//! Module: db::executor::load::grouped_runtime::continuation
//! Responsibility: module-local ownership and contracts for db::executor::load::grouped_runtime::continuation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod capabilities;
mod context;
mod window;

pub(in crate::db::executor::load) use capabilities::GroupedContinuationCapabilities;
pub(in crate::db::executor::load) use context::GroupedContinuationContext;
pub(in crate::db::executor::load) use window::GroupedPaginationWindow;
