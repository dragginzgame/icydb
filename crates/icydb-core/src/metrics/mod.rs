//! Module: metrics
//!
//! Responsibility: module-local ownership and contracts for metrics.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Observability: runtime telemetry (metrics) and sink abstractions.
//!
//! Storage inspection and execution diagnostics live in `db`.
//! This module is intentionally metrics-focused.

pub(crate) mod sink;
mod state;

// re-exports
pub use sink::{MetricsSink, metrics_report, metrics_reset_all};
pub use state::EventReport;

///
/// TESTS
///

#[cfg(test)]
#[expect(clippy::float_cmp)]
mod tests;
