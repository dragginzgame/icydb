//! Module: metrics
//!
//! Responsibility: runtime telemetry sinks and report state.
//! Does not own: executor diagnostics or storage inspection surfaces under `db`.
//! Boundary: crate-level metrics reporting/reset surface.

pub(crate) mod sink;
mod state;

// re-exports
pub use sink::{MetricsSink, metrics_report, metrics_reset_all};
pub use state::{EntitySummary, EventCounters, EventOps, EventReport};

///
/// TESTS
///

#[cfg(test)]
mod tests;
