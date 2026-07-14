//! Module: metrics::sink
//! Responsibility: instrumentation sink traits and the bridge into metrics state.
//! Does not own: stored metrics DTO definitions or executor business logic.
//! Boundary: the only allowed connection between runtime instrumentation and global metrics state.
//!
//! Core DB logic MUST NOT depend on `metrics::state` directly.
//! All instrumentation flows through `MetricsEvent` and `MetricsSink`.
mod counters;
mod dispatch;
mod events;
mod instrumentation;

use crate::metrics::state as metrics;
use dispatch::GLOBAL_METRICS_SINK;
#[cfg(feature = "sql")]
pub(crate) use instrumentation::record_sql_compile_reject_for_path;
pub(crate) use instrumentation::{
    PathSpan, Span, record_accepted_schema_footprint_for_path, record_cache_entries,
    record_cache_event_for_path, record_cache_miss_reason_for_path, record_exec_error_for_path,
    record_prepared_shape_already_finalized_for_path, record_schema_store_footprint_for_path,
};
use std::cell::RefCell;
#[cfg(test)]
use std::rc::Rc;

pub use events::{
    CacheKind, CacheMissReason, CacheOutcome, ExecKind, ExecOutcome, GroupedPlanExecutionMode,
    MetricsEvent, MutationCommitClass, PlanChoiceReason, PlanKind, SaveMutationKind,
    SchemaReconcileOutcome, SchemaTransitionOutcome, SqlCompileRejectPhase, SqlWriteKind,
};

thread_local! {
    static SINK_OVERRIDE: RefCell<Vec<MetricsSinkOverride>> = const { RefCell::new(Vec::new()) };
}

///
/// MetricsSink
///

pub trait MetricsSink {
    fn record(&self, event: MetricsEvent);
}

#[derive(Clone)]
enum MetricsSinkOverride {
    Static(&'static dyn MetricsSink),
    #[cfg(test)]
    Shared(Rc<dyn MetricsSink>),
}

impl MetricsSinkOverride {
    fn record(&self, event: MetricsEvent) {
        match self {
            Self::Static(sink) => sink.record(event),
            #[cfg(test)]
            Self::Shared(sink) => sink.record(event),
        }
    }
}

pub(crate) fn record(event: MetricsEvent) {
    // Clone the scoped override before dispatch so sink implementations can
    // record nested metrics without re-entering this RefCell borrow.
    let override_sink = SINK_OVERRIDE.with(|stack| stack.borrow().last().cloned());
    if let Some(sink) = override_sink {
        sink.record(event);
    } else {
        GLOBAL_METRICS_SINK.record(event);
    }
}

/// Snapshot the current metrics state for endpoint/test plumbing.
///
/// `window_start_ms` filters by window start (`EventState::window_start_ms`),
/// not by per-event timestamps.
#[must_use]
pub fn metrics_report(window_start_ms: Option<u64>) -> metrics::EventReport {
    metrics::report_window_start(window_start_ms)
}

/// Snapshot the current metrics state using the compact endpoint payload.
///
/// `window_start_ms` filters by window start (`EventState::window_start_ms`),
/// not by per-event timestamps.
#[must_use]
pub fn compact_metrics_report(window_start_ms: Option<u64>) -> metrics::CompactMetricsReport {
    metrics::compact_report_window_start(window_start_ms)
}

/// Reset all metrics state (counters + perf).
pub fn metrics_reset_all() {
    metrics::reset_all();
}

/// Run a closure with a temporary metrics sink override.
pub(crate) fn with_metrics_sink<T>(sink: &'static dyn MetricsSink, f: impl FnOnce() -> T) -> T {
    with_metrics_sink_override(MetricsSinkOverride::Static(sink), f)
}

#[cfg(test)]
pub(crate) fn with_shared_metrics_sink<T>(sink: Rc<dyn MetricsSink>, f: impl FnOnce() -> T) -> T {
    with_metrics_sink_override(MetricsSinkOverride::Shared(sink), f)
}

fn with_metrics_sink_override<T>(sink: MetricsSinkOverride, f: impl FnOnce() -> T) -> T {
    struct Guard {
        depth_before_push: usize,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            SINK_OVERRIDE.with(|stack| {
                let mut stack = stack.borrow_mut();
                debug_assert_eq!(stack.len(), self.depth_before_push + 1);
                if stack.len() > self.depth_before_push {
                    stack.truncate(self.depth_before_push);
                }
            });
        }
    }

    let depth_before_push = SINK_OVERRIDE.with(|stack| {
        let mut stack = stack.borrow_mut();
        let depth = stack.len();
        stack.push(sink);
        depth
    });
    let _guard = Guard { depth_before_push };

    f()
}

#[cfg(test)]
mod tests;
