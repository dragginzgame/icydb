//! Module: metrics
//!
//! Responsibility: runtime telemetry sinks and report state.
//! Does not own: executor diagnostics or storage inspection surfaces under `db`.
//! Boundary: crate-level metrics reporting/reset surface.

pub(crate) mod sink;
mod state;

// re-exports
pub use sink::metrics_report;
#[doc(hidden)]
pub use sink::with_query_metrics_context;
pub use sink::{
    CacheKind, CacheMissReason, CacheOutcome, ExecKind, ExecOutcome, GroupedPlanExecutionMode,
    MetricsEvent, MetricsSink, MutationCommitClass, MutationJobLifecycleEvent, PlanChoiceReason,
    PlanKind, SchemaReconcileOutcome, SchemaTransitionOutcome, SqlCompileRejectPhase, SqlWriteKind,
    compact_metrics_report, metrics_reset_all,
};
pub use state::{
    CompactEntityMetrics, CompactEventCounters, CompactMetric, CompactMetricsReport,
    compact_metric_code,
};
pub use state::{
    EntitySummary, EventCounters, EventOps, EventReport, MetricRatio, MutationJobMetrics,
};

///
/// TESTS
///

#[cfg(test)]
mod tests;
