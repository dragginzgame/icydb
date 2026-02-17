//! Metrics sink boundary.
//!
//! Core DB logic MUST NOT depend on obs::metrics directly.
//! All instrumentation flows through MetricsEvent and MetricsSink.
//!
//! This module is the only allowed bridge between execution logic
//! and the global metrics state.
use crate::{obs::metrics, traits::EntityKind};
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
    UniqueViolation {
        entity_path: &'static str,
    },
    IndexDelta {
        entity_path: &'static str,
        inserts: u64,
        removes: u64,
    },
    ReverseIndexDelta {
        entity_path: &'static str,
        inserts: u64,
        removes: u64,
    },
    RelationValidation {
        entity_path: &'static str,
        reverse_lookups: u64,
        blocked_deletes: u64,
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

/// GlobalMetricsSink
/// Default process-local sink that writes into global metrics state.
/// Acts as the concrete sink when no scoped override is installed.

pub(crate) struct GlobalMetricsSink;

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

            MetricsEvent::UniqueViolation { entity_path } => {
                metrics::with_state_mut(|m| {
                    m.ops.unique_violations = m.ops.unique_violations.saturating_add(1);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.unique_violations = entry.unique_violations.saturating_add(1);
                });
            }

            MetricsEvent::IndexDelta {
                entity_path,
                inserts,
                removes,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.index_inserts = m.ops.index_inserts.saturating_add(inserts);
                    m.ops.index_removes = m.ops.index_removes.saturating_add(removes);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.index_inserts = entry.index_inserts.saturating_add(inserts);
                    entry.index_removes = entry.index_removes.saturating_add(removes);
                });
            }

            MetricsEvent::ReverseIndexDelta {
                entity_path,
                inserts,
                removes,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.reverse_index_inserts =
                        m.ops.reverse_index_inserts.saturating_add(inserts);
                    m.ops.reverse_index_removes =
                        m.ops.reverse_index_removes.saturating_add(removes);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.reverse_index_inserts =
                        entry.reverse_index_inserts.saturating_add(inserts);
                    entry.reverse_index_removes =
                        entry.reverse_index_removes.saturating_add(removes);
                });
            }

            MetricsEvent::RelationValidation {
                entity_path,
                reverse_lookups,
                blocked_deletes,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.relation_reverse_lookups = m
                        .ops
                        .relation_reverse_lookups
                        .saturating_add(reverse_lookups);
                    m.ops.relation_delete_blocks =
                        m.ops.relation_delete_blocks.saturating_add(blocked_deletes);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.relation_reverse_lookups = entry
                        .relation_reverse_lookups
                        .saturating_add(reverse_lookups);
                    entry.relation_delete_blocks =
                        entry.relation_delete_blocks.saturating_add(blocked_deletes);
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

pub(crate) const GLOBAL_METRICS_SINK: GlobalMetricsSink = GlobalMetricsSink;

pub(crate) fn record(event: MetricsEvent) {
    let override_ptr = SINK_OVERRIDE.with(|cell| *cell.borrow());
    if let Some(ptr) = override_ptr {
        // SAFETY:
        // Preconditions:
        // - `ptr` was produced from a valid `&dyn MetricsSink` in `with_metrics_sink`.
        // - `with_metrics_sink` always restores the previous pointer before returning,
        //   including unwind paths via `Guard::drop`.
        // - `record` is synchronous and never stores `ptr` beyond this call.
        //
        // Aliasing:
        // - We materialize only a shared reference (`&dyn MetricsSink`), matching the
        //   original shared borrow used to install the override.
        // - No mutable alias to the same sink is created here.
        //
        // What would break this:
        // - If `with_metrics_sink` failed to restore on all exits (normal + panic),
        //   `ptr` could outlive the borrowed sink and become dangling.
        // - If `record` were changed to store or dispatch asynchronously using `ptr`,
        //   lifetime assumptions would no longer hold.
        unsafe { (&*ptr).record(event) };
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

/// Reset all metrics state (counters + perf).
pub fn metrics_reset_all() {
    metrics::reset_all();
}

/// Run a closure with a temporary metrics sink override.
pub(crate) fn with_metrics_sink<T>(sink: &dyn MetricsSink, f: impl FnOnce() -> T) -> T {
    struct Guard(Option<*const dyn MetricsSink>);

    impl Drop for Guard {
        fn drop(&mut self) {
            SINK_OVERRIDE.with(|cell| {
                *cell.borrow_mut() = self.0;
            });
        }
    }

    // SAFETY:
    // Preconditions:
    // - `sink_ptr` is installed only for this dynamic scope.
    // - `Guard` always restores the previous slot on all exits, including panic.
    // - `record` only dereferences synchronously and never persists `sink_ptr`.
    //
    // Aliasing:
    // - We erase lifetime to a raw pointer, but still only expose shared access.
    // - No mutable alias to the same sink is introduced by this conversion.
    //
    // What would break this:
    // - Any async/deferred use of `sink_ptr` beyond this scope.
    // - Any path that bypasses Guard restoration.
    let sink_ptr = unsafe { std::mem::transmute::<&dyn MetricsSink, *const dyn MetricsSink>(sink) };
    let prev = SINK_OVERRIDE.with(|cell| {
        let mut slot = cell.borrow_mut();
        slot.replace(sink_ptr)
    });
    let _guard = Guard(prev);

    f()
}

/// Span
/// RAII guard that emits start/finish metrics events for one executor call.
/// Ensures finish accounting happens even on unwind.

pub(crate) struct Span<E: EntityKind> {
    kind: ExecKind,
    start: u64,
    rows: u64,
    finished: bool,
    _marker: PhantomData<E>,
}

#[expect(clippy::missing_const_for_fn)]
fn read_perf_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
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
            start: read_perf_counter(),
            rows: 0,
            finished: false,
            _marker: PhantomData,
        }
    }

    pub(crate) const fn set_rows(&mut self, rows: u64) {
        self.rows = rows;
    }

    fn finish_inner(&self) {
        let now = read_perf_counter();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingSink<'a> {
        calls: &'a AtomicUsize,
    }

    impl MetricsSink for CountingSink<'_> {
        fn record(&self, _: MetricsEvent) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn with_metrics_sink_routes_and_restores_nested_overrides() {
        SINK_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = None;
        });

        let outer_calls = AtomicUsize::new(0);
        let inner_calls = AtomicUsize::new(0);
        let outer = CountingSink {
            calls: &outer_calls,
        };
        let inner = CountingSink {
            calls: &inner_calls,
        };

        // No override installed yet.
        record(MetricsEvent::Plan {
            kind: PlanKind::Keys,
        });
        assert_eq!(outer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner_calls.load(Ordering::SeqCst), 0);

        with_metrics_sink(&outer, || {
            record(MetricsEvent::Plan {
                kind: PlanKind::Index,
            });
            assert_eq!(outer_calls.load(Ordering::SeqCst), 1);
            assert_eq!(inner_calls.load(Ordering::SeqCst), 0);

            with_metrics_sink(&inner, || {
                record(MetricsEvent::Plan {
                    kind: PlanKind::Range,
                });
            });

            // Inner override was restored to outer override.
            record(MetricsEvent::Plan {
                kind: PlanKind::FullScan,
            });
        });

        assert_eq!(outer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(inner_calls.load(Ordering::SeqCst), 1);

        // Outer override was restored to previous (none).
        SINK_OVERRIDE.with(|cell| {
            assert!(cell.borrow().is_none());
        });

        record(MetricsEvent::Plan {
            kind: PlanKind::Keys,
        });
        assert_eq!(outer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(inner_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn with_metrics_sink_restores_override_on_panic() {
        SINK_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = None;
        });

        let calls = AtomicUsize::new(0);
        let sink = CountingSink { calls: &calls };

        let panicked = catch_unwind(AssertUnwindSafe(|| {
            with_metrics_sink(&sink, || {
                record(MetricsEvent::Plan {
                    kind: PlanKind::Index,
                });
                panic!("intentional panic for guard test");
            });
        }))
        .is_err();
        assert!(panicked);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Guard restored TLS slot after unwind.
        SINK_OVERRIDE.with(|cell| {
            assert!(cell.borrow().is_none());
        });

        record(MetricsEvent::Plan {
            kind: PlanKind::Range,
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn metrics_report_without_window_start_returns_counters() {
        metrics_reset_all();
        record(MetricsEvent::Plan {
            kind: PlanKind::Index,
        });

        let report = metrics_report(None);
        let counters = report
            .counters
            .expect("metrics report should include counters without since filter");
        assert_eq!(counters.ops.plan_index, 1);
    }

    #[test]
    fn metrics_report_window_start_before_window_returns_counters() {
        metrics_reset_all();
        let window_start = metrics::with_state(|m| m.window_start_ms);
        record(MetricsEvent::Plan {
            kind: PlanKind::Keys,
        });

        let report = metrics_report(Some(window_start.saturating_sub(1)));
        let counters = report
            .counters
            .expect("metrics report should include counters when window_start_ms is before window");
        assert_eq!(counters.ops.plan_keys, 1);
    }

    #[test]
    fn metrics_report_window_start_after_window_returns_empty() {
        metrics_reset_all();
        let window_start = metrics::with_state(|m| m.window_start_ms);
        record(MetricsEvent::Plan {
            kind: PlanKind::FullScan,
        });

        let report = metrics_report(Some(window_start.saturating_add(1)));
        assert!(report.counters.is_none());
        assert!(report.entity_counters.is_empty());
    }

    #[test]
    fn reverse_and_relation_metrics_events_accumulate() {
        metrics_reset_all();

        record(MetricsEvent::ReverseIndexDelta {
            entity_path: "obs::tests::Entity",
            inserts: 3,
            removes: 2,
        });
        record(MetricsEvent::RelationValidation {
            entity_path: "obs::tests::Entity",
            reverse_lookups: 5,
            blocked_deletes: 1,
        });

        let counters = metrics_report(None)
            .counters
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.reverse_index_inserts, 3);
        assert_eq!(counters.ops.reverse_index_removes, 2);
        assert_eq!(counters.ops.relation_reverse_lookups, 5);
        assert_eq!(counters.ops.relation_delete_blocks, 1);

        let entity = counters
            .entities
            .get("obs::tests::Entity")
            .expect("entity counters should be present");
        assert_eq!(entity.reverse_index_inserts, 3);
        assert_eq!(entity.reverse_index_removes, 2);
        assert_eq!(entity.relation_reverse_lookups, 5);
        assert_eq!(entity.relation_delete_blocks, 1);
    }
}
