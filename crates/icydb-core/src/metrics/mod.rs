//! Module: metrics
//!
//! Responsibility: runtime telemetry sinks and report state.
//! Does not own: executor diagnostics or storage inspection surfaces under `db`.
//! Boundary: crate-level metrics reporting/reset surface.

pub(crate) mod sink;
mod state;

// re-exports
#[cfg(any(test, feature = "metrics-extended"))]
pub use sink::metrics_report;
pub use sink::{
    CacheKind, CacheMissReason, CacheOutcome, ExecKind, ExecOutcome, GroupedPlanExecutionMode,
    MetricsEvent, MetricsSink, MutationCommitClass, PlanChoiceReason, PlanKind, SaveMutationKind,
    SchemaReconcileOutcome, SchemaTransitionOutcome, SqlCompileRejectPhase, SqlWriteKind,
    compact_metrics_report, metrics_reset_all,
};
pub use state::{
    CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport,
    compact_metric_code,
};
#[cfg(any(test, feature = "metrics-extended"))]
pub use state::{EntitySummary, EventCounters, EventOps, EventReport, MetricRatio};

///
/// TESTS
///

#[cfg(test)]
mod tests;
