//! Module: db::executor::continuation::grouped
//! Responsibility: continuation-owned grouped pagination/continuation runtime contracts.
//! Does not own: grouped route feasibility or grouped fold/output execution policy.
//! Boundary: exports immutable grouped continuation primitives for runtime consumers.

mod capabilities;
mod context;
mod window;

pub(in crate::db::executor) use capabilities::GroupedContinuationCapabilities;
pub(in crate::db::executor) use context::GroupedContinuationContext;
pub(in crate::db::executor) use window::GroupedPaginationWindow;
