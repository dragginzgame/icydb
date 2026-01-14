//! Metrics sink boundary.
//!
//! Core DB logic MUST NOT depend on obs::metrics directly.
//! All instrumentation flows through MetricsEvent and MetricsSink.
//!
//! This module is the only allowed bridge between execution logic
//! and the global metrics state.
use crate::{obs::metrics, traits::EntityKind};
use canic_cdk::api::performance_counter;
use std::{cell::RefCell, marker::PhantomData};

thread_local! {
    static SINK_OVERRIDE: RefCell<Option<*const dyn MetricsSink>> = RefCell::new(None);
}

///
/// ExecKind
///

#[derive(Clone, Copy, Debug)]
pub enum ExecKind {
    Load,
    Save,
    Delete,
}

///
/// PlanKind
///

#[derive(Clone, Copy, Debug)]
pub enum PlanKind {
    Keys,
    Index,
    Range,
    FullScan,
}

///
/// MetricsEvent
///

#[derive(Clone, Copy, Debug)]
pub enum MetricsEvent {
    ExecStart {
        kind: ExecKind,
        entity_path: &'static str,
    },
    ExecFinish {
        kind: ExecKind,
        entity_path: &'static str,
        rows_touched: u64,
        inst_delta: u64,
    },
    RowsScanned {
        entity_path: &'static str,
        rows_scanned: u64,
    },
    ExistsCall {
        entity_path: &'static str,
    },
    UniqueViolation {
        entity_path: &'static str,
    },
    IndexInsert {
        entity_path: &'static str,
    },
    IndexRemove {
        entity_path: &'static str,
    },
    Plan {
        kind: PlanKind,
    },
}

///
/// MetricsSink
///

pub trait MetricsSink {
    fn record(&self, event: MetricsEvent);
}

///
/// NoopMetricsSink
///

pub struct NoopMetricsSink;

impl MetricsSink for NoopMetricsSink {
    fn record(&self, _: MetricsEvent) {}
}

///
/// GlobalMetricsSink
///

pub struct GlobalMetricsSink;

impl MetricsSink for GlobalMetricsSink {
    #[expect(clippy::too_many_lines)]
    fn record(&self, event: MetricsEvent) {
        match event {
            MetricsEvent::ExecStart { kind, entity_path } => {
                metrics::with_state_mut(|m| {
                    match kind {
                        ExecKind::Load => m.ops.load_calls = m.ops.load_calls.saturating_add(1),
                        ExecKind::Save => m.ops.save_calls = m.ops.save_calls.saturating_add(1),
                        ExecKind::Delete => {
                            m.ops.delete_calls = m.ops.delete_calls.saturating_add(1);
                        }
                    }

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    match kind {
                        ExecKind::Load => {
                            entry.load_calls = entry.load_calls.saturating_add(1);
                        }
                        ExecKind::Save => {
                            entry.save_calls = entry.save_calls.saturating_add(1);
                        }
                        ExecKind::Delete => {
                            entry.delete_calls = entry.delete_calls.saturating_add(1);
                        }
                    }
                });
            }

            MetricsEvent::ExecFinish {
                kind,
                entity_path,
                rows_touched,
                inst_delta,
            } => {
                metrics::with_state_mut(|m| {
                    match kind {
                        ExecKind::Load => {
                            m.ops.rows_loaded = m.ops.rows_loaded.saturating_add(rows_touched);
                            metrics::add_instructions(
                                &mut m.perf.load_inst_total,
                                &mut m.perf.load_inst_max,
                                inst_delta,
                            );
                        }
                        ExecKind::Save => {
                            metrics::add_instructions(
                                &mut m.perf.save_inst_total,
                                &mut m.perf.save_inst_max,
                                inst_delta,
                            );
                        }
                        ExecKind::Delete => {
                            m.ops.rows_deleted = m.ops.rows_deleted.saturating_add(rows_touched);
                            metrics::add_instructions(
                                &mut m.perf.delete_inst_total,
                                &mut m.perf.delete_inst_max,
                                inst_delta,
                            );
                        }
                    }

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    match kind {
                        ExecKind::Load => {
                            entry.rows_loaded = entry.rows_loaded.saturating_add(rows_touched);
                        }
                        ExecKind::Delete => {
                            entry.rows_deleted = entry.rows_deleted.saturating_add(rows_touched);
                        }
                        ExecKind::Save => {}
                    }
                });
            }

            MetricsEvent::RowsScanned {
                entity_path,
                rows_scanned,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.rows_scanned = m.ops.rows_scanned.saturating_add(rows_scanned);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.rows_scanned = entry.rows_scanned.saturating_add(rows_scanned);
                });
            }

            MetricsEvent::ExistsCall { entity_path } => {
                metrics::with_state_mut(|m| {
                    m.ops.exists_calls = m.ops.exists_calls.saturating_add(1);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.exists_calls = entry.exists_calls.saturating_add(1);
                });
            }

            MetricsEvent::UniqueViolation { entity_path } => {
                metrics::with_state_mut(|m| {
                    m.ops.unique_violations = m.ops.unique_violations.saturating_add(1);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.unique_violations = entry.unique_violations.saturating_add(1);
                });
            }

            MetricsEvent::IndexInsert { entity_path } => {
                metrics::with_state_mut(|m| {
                    m.ops.index_inserts = m.ops.index_inserts.saturating_add(1);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.index_inserts = entry.index_inserts.saturating_add(1);
                });
            }

            MetricsEvent::IndexRemove { entity_path } => {
                metrics::with_state_mut(|m| {
                    m.ops.index_removes = m.ops.index_removes.saturating_add(1);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.index_removes = entry.index_removes.saturating_add(1);
                });
            }

            MetricsEvent::Plan { kind } => {
                metrics::with_state_mut(|m| match kind {
                    PlanKind::Keys => m.ops.plan_keys = m.ops.plan_keys.saturating_add(1),
                    PlanKind::Index => m.ops.plan_index = m.ops.plan_index.saturating_add(1),
                    PlanKind::Range => m.ops.plan_range = m.ops.plan_range.saturating_add(1),
                    PlanKind::FullScan => {
                        m.ops.plan_full_scan = m.ops.plan_full_scan.saturating_add(1);
                    }
                });
            }
        }
    }
}

pub const GLOBAL_METRICS_SINK: GlobalMetricsSink = GlobalMetricsSink;

pub fn record(event: MetricsEvent) {
    let override_ptr = SINK_OVERRIDE.with(|cell| *cell.borrow());
    if let Some(ptr) = override_ptr {
        // SAFETY: override is scoped by with_metrics_sink and only used synchronously.
        unsafe { (&*ptr).record(event) };
    } else {
        GLOBAL_METRICS_SINK.record(event);
    }
}

/// Snapshot the current metrics state for endpoint/test plumbing.
#[must_use]
pub fn metrics_report() -> metrics::EventReport {
    metrics::report()
}

/// Reset ephemeral metrics counters.
pub fn metrics_reset() {
    metrics::reset();
}

/// Reset all metrics state (counters + perf).
pub fn metrics_reset_all() {
    metrics::reset_all();
}

/// Run a closure with a temporary metrics sink override.
pub fn with_metrics_sink<T>(sink: &dyn MetricsSink, f: impl FnOnce() -> T) -> T {
    struct Guard(Option<*const dyn MetricsSink>);

    impl Drop for Guard {
        fn drop(&mut self) {
            SINK_OVERRIDE.with(|cell| {
                *cell.borrow_mut() = self.0;
            });
        }
    }

    // SAFETY: we erase the reference lifetime for scoped storage in TLS and
    // restore the previous value on scope exit via Guard.
    let sink_ptr = unsafe { std::mem::transmute::<&dyn MetricsSink, *const dyn MetricsSink>(sink) };
    let prev = SINK_OVERRIDE.with(|cell| {
        let mut slot = cell.borrow_mut();
        slot.replace(sink_ptr)
    });
    let _guard = Guard(prev);

    f()
}

///
/// Span
/// RAII guard to simplify metrics instrumentation
///

pub(crate) struct Span<E: EntityKind> {
    kind: ExecKind,
    start: u64,
    rows: u64,
    finished: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Span<E> {
    #[must_use]
    /// Start a metrics span for a specific entity and executor kind.
    pub(crate) fn new(kind: ExecKind) -> Self {
        record(MetricsEvent::ExecStart {
            kind,
            entity_path: E::PATH,
        });

        Self {
            kind,
            start: performance_counter(1),
            rows: 0,
            finished: false,
            _marker: PhantomData,
        }
    }

    pub(crate) const fn set_rows(&mut self, rows: u64) {
        self.rows = rows;
    }

    #[expect(dead_code)]
    /// Increment the recorded row count.
    pub(crate) const fn add_rows(&mut self, rows: u64) {
        self.rows = self.rows.saturating_add(rows);
    }

    #[expect(dead_code)]
    /// Finish the span early (also happens on Drop).
    pub(crate) fn finish(mut self) {
        if !self.finished {
            self.finish_inner();
            self.finished = true;
        }
    }

    fn finish_inner(&self) {
        let now = performance_counter(1);
        let delta = now.saturating_sub(self.start);

        record(MetricsEvent::ExecFinish {
            kind: self.kind,
            entity_path: E::PATH,
            rows_touched: self.rows,
            inst_delta: delta,
        });
    }
}

impl<E: EntityKind> Drop for Span<E> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_inner();
            self.finished = true;
        }
    }
}
