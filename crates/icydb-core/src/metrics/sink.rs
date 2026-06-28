//! Module: metrics::sink
//! Responsibility: instrumentation sink traits and the bridge into metrics state.
//! Does not own: stored metrics DTO definitions or executor business logic.
//! Boundary: the only allowed connection between runtime instrumentation and global metrics state.
//!
//! Core DB logic MUST NOT depend on `metrics::state` directly.
//! All instrumentation flows through `MetricsEvent` and `MetricsSink`.
mod counters;
mod events;

use crate::{error::InternalError, metrics::state as metrics, traits::EntityKind};
use counters::*;
#[cfg(test)]
use std::rc::Rc;
use std::{cell::RefCell, marker::PhantomData};

pub use events::{
    CacheKind, CacheMissReason, CacheOutcome, ExecKind, ExecOutcome, GroupedPlanExecutionMode,
    MetricsEvent, MutationCommitClass, PlanChoiceReason, PlanKind,
    PreparedShapeFinalizationOutcome, SaveMutationKind, SchemaReconcileOutcome,
    SchemaTransitionOutcome, SqlCompileRejectPhase, SqlWriteKind,
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

/// GlobalMetricsSink
/// Default process-local sink that writes into global metrics state.
/// Acts as the concrete sink when no scoped override is installed.

pub(crate) struct GlobalMetricsSink;

impl MetricsSink for GlobalMetricsSink {
    #[remain::check]
    #[expect(clippy::too_many_lines)]
    fn record(&self, event: MetricsEvent) {
        #[remain::sorted]
        match event {
            MetricsEvent::AcceptedSchemaFootprint {
                entity_path,
                fields,
                nested_leaf_facts,
            } => {
                metrics::with_state_mut(|m| {
                    let (previous_fields, previous_nested_leaf_facts) = {
                        let entry = m.entities.entry(entity_path.to_string()).or_default();
                        let previous = (
                            entry.accepted_schema_fields,
                            entry.accepted_schema_nested_leaf_facts,
                        );
                        entry.accepted_schema_fields = fields;
                        entry.accepted_schema_nested_leaf_facts = nested_leaf_facts;

                        previous
                    };
                    replace_gauge_total(&mut m.ops.accepted_schema_fields, previous_fields, fields);
                    replace_gauge_total(
                        &mut m.ops.accepted_schema_nested_leaf_facts,
                        previous_nested_leaf_facts,
                        nested_leaf_facts,
                    );
                });
            }
            MetricsEvent::Cache {
                entity_path,
                kind,
                outcome,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_cache_outcome(&mut m.ops, kind, outcome);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_cache_outcome(entry, kind, outcome);
                });
            }

            MetricsEvent::CacheEntries { kind, entries } => {
                metrics::with_state_mut(|m| {
                    record_global_cache_entries(&mut m.ops, kind, entries);
                });
            }

            MetricsEvent::CacheMissReason {
                entity_path,
                kind,
                reason,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_cache_miss_reason(&mut m.ops, kind, reason);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_cache_miss_reason(entry, kind, reason);
                });
            }
            MetricsEvent::ExecError {
                kind,
                entity_path,
                outcome,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_exec_start(&mut m.ops, kind);
                    record_global_exec_outcome(&mut m.ops, outcome);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_exec_start(entry, kind);
                    record_entity_exec_outcome(entry, outcome);
                });
            }
            MetricsEvent::ExecFinish {
                kind,
                entity_path,
                rows_touched,
                inst_delta,
                outcome,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_exec_outcome(&mut m.ops, outcome);

                    #[remain::sorted]
                    match kind {
                        ExecKind::Delete => {
                            m.ops.rows_deleted = m.ops.rows_deleted.saturating_add(rows_touched);
                            m.ops.write_rows_touched =
                                m.ops.write_rows_touched.saturating_add(rows_touched);
                            metrics::add_instructions(
                                &mut m.perf.delete_inst_total,
                                &mut m.perf.delete_inst_max,
                                inst_delta,
                            );
                        }
                        ExecKind::Load => {
                            m.ops.rows_loaded = m.ops.rows_loaded.saturating_add(rows_touched);
                            metrics::add_instructions(
                                &mut m.perf.load_inst_total,
                                &mut m.perf.load_inst_max,
                                inst_delta,
                            );
                        }
                        ExecKind::Save => {
                            m.ops.rows_saved = m.ops.rows_saved.saturating_add(rows_touched);
                            m.ops.write_rows_touched =
                                m.ops.write_rows_touched.saturating_add(rows_touched);
                            metrics::add_instructions(
                                &mut m.perf.save_inst_total,
                                &mut m.perf.save_inst_max,
                                inst_delta,
                            );
                        }
                    }

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_exec_outcome(entry, outcome);
                    #[remain::sorted]
                    match kind {
                        ExecKind::Delete => {
                            entry.rows_deleted = entry.rows_deleted.saturating_add(rows_touched);
                            entry.write_rows_touched =
                                entry.write_rows_touched.saturating_add(rows_touched);
                        }
                        ExecKind::Load => {
                            entry.rows_loaded = entry.rows_loaded.saturating_add(rows_touched);
                        }
                        ExecKind::Save => {
                            entry.rows_saved = entry.rows_saved.saturating_add(rows_touched);
                            entry.write_rows_touched =
                                entry.write_rows_touched.saturating_add(rows_touched);
                        }
                    }
                });
            }
            MetricsEvent::ExecStart { kind, entity_path } => {
                metrics::with_state_mut(|m| {
                    record_global_exec_start(&mut m.ops, kind);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_exec_start(entry, kind);
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
                    let changed = inserts.saturating_add(removes);
                    m.ops.write_index_entries_changed =
                        m.ops.write_index_entries_changed.saturating_add(changed);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.index_inserts = entry.index_inserts.saturating_add(inserts);
                    entry.index_removes = entry.index_removes.saturating_add(removes);
                    entry.write_index_entries_changed =
                        entry.write_index_entries_changed.saturating_add(changed);
                });
            }
            MetricsEvent::LoadRowEfficiency {
                entity_path,
                candidate_rows_scanned,
                candidate_rows_filtered,
                result_rows_emitted,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.load_candidate_rows_scanned = m
                        .ops
                        .load_candidate_rows_scanned
                        .saturating_add(candidate_rows_scanned);
                    m.ops.load_candidate_rows_filtered = m
                        .ops
                        .load_candidate_rows_filtered
                        .saturating_add(candidate_rows_filtered);
                    m.ops.load_result_rows_emitted = m
                        .ops
                        .load_result_rows_emitted
                        .saturating_add(result_rows_emitted);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.load_candidate_rows_scanned = entry
                        .load_candidate_rows_scanned
                        .saturating_add(candidate_rows_scanned);
                    entry.load_candidate_rows_filtered = entry
                        .load_candidate_rows_filtered
                        .saturating_add(candidate_rows_filtered);
                    entry.load_result_rows_emitted = entry
                        .load_result_rows_emitted
                        .saturating_add(result_rows_emitted);
                });
            }
            MetricsEvent::MutationCommitPlan { .. } => {}
            MetricsEvent::NonAtomicPartialCommit {
                entity_path,
                committed_rows,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.non_atomic_partial_commits =
                        m.ops.non_atomic_partial_commits.saturating_add(1);
                    m.ops.non_atomic_partial_rows_committed = m
                        .ops
                        .non_atomic_partial_rows_committed
                        .saturating_add(committed_rows);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.non_atomic_partial_commits =
                        entry.non_atomic_partial_commits.saturating_add(1);
                    entry.non_atomic_partial_rows_committed = entry
                        .non_atomic_partial_rows_committed
                        .saturating_add(committed_rows);
                });
            }
            MetricsEvent::Plan {
                entity_path,
                kind,
                grouped_execution_mode,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_plan_kind(&mut m.ops, kind);
                    record_global_grouped_plan_mode(&mut m.ops, grouped_execution_mode);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_plan_kind(entry, kind);
                    record_entity_grouped_plan_mode(entry, grouped_execution_mode);
                });
            }
            MetricsEvent::PlanChoice {
                entity_path,
                reason,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_plan_choice_reason(&mut m.ops, reason);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_plan_choice_reason(entry, reason);
                });
            }
            MetricsEvent::PreparedShapeFinalization {
                entity_path,
                outcome,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_prepared_shape_finalization_outcome(&mut m.ops, outcome);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_prepared_shape_finalization_outcome(entry, outcome);
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
                    m.ops.write_relation_checks =
                        m.ops.write_relation_checks.saturating_add(reverse_lookups);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.relation_reverse_lookups = entry
                        .relation_reverse_lookups
                        .saturating_add(reverse_lookups);
                    entry.relation_delete_blocks =
                        entry.relation_delete_blocks.saturating_add(blocked_deletes);
                    entry.write_relation_checks =
                        entry.write_relation_checks.saturating_add(reverse_lookups);
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
                    let changed = inserts.saturating_add(removes);
                    m.ops.write_reverse_index_entries_changed = m
                        .ops
                        .write_reverse_index_entries_changed
                        .saturating_add(changed);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.reverse_index_inserts =
                        entry.reverse_index_inserts.saturating_add(inserts);
                    entry.reverse_index_removes =
                        entry.reverse_index_removes.saturating_add(removes);
                    entry.write_reverse_index_entries_changed = entry
                        .write_reverse_index_entries_changed
                        .saturating_add(changed);
                });
            }
            MetricsEvent::RowsAggregated {
                entity_path,
                rows_aggregated,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.rows_aggregated = m.ops.rows_aggregated.saturating_add(rows_aggregated);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.rows_aggregated = entry.rows_aggregated.saturating_add(rows_aggregated);
                });
            }
            MetricsEvent::RowsEmitted {
                entity_path,
                rows_emitted,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.rows_emitted = m.ops.rows_emitted.saturating_add(rows_emitted);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.rows_emitted = entry.rows_emitted.saturating_add(rows_emitted);
                });
            }
            MetricsEvent::RowsFiltered {
                entity_path,
                rows_filtered,
            } => {
                metrics::with_state_mut(|m| {
                    m.ops.rows_filtered = m.ops.rows_filtered.saturating_add(rows_filtered);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.rows_filtered = entry.rows_filtered.saturating_add(rows_filtered);
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
            MetricsEvent::SaveMutation {
                entity_path,
                kind,
                rows_touched,
            } => {
                metrics::with_state_mut(|m| {
                    #[remain::sorted]
                    match kind {
                        SaveMutationKind::Insert => {
                            m.ops.save_insert_calls = m.ops.save_insert_calls.saturating_add(1);
                            m.ops.rows_inserted = m.ops.rows_inserted.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Replace => {
                            m.ops.save_replace_calls = m.ops.save_replace_calls.saturating_add(1);
                            m.ops.rows_replaced = m.ops.rows_replaced.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Update => {
                            m.ops.save_update_calls = m.ops.save_update_calls.saturating_add(1);
                            m.ops.rows_updated = m.ops.rows_updated.saturating_add(rows_touched);
                        }
                    }

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    #[remain::sorted]
                    match kind {
                        SaveMutationKind::Insert => {
                            entry.save_insert_calls = entry.save_insert_calls.saturating_add(1);
                            entry.rows_inserted = entry.rows_inserted.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Replace => {
                            entry.save_replace_calls = entry.save_replace_calls.saturating_add(1);
                            entry.rows_replaced = entry.rows_replaced.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Update => {
                            entry.save_update_calls = entry.save_update_calls.saturating_add(1);
                            entry.rows_updated = entry.rows_updated.saturating_add(rows_touched);
                        }
                    }
                });
            }
            MetricsEvent::SchemaReconcile {
                entity_path,
                outcome,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_schema_reconcile_outcome(&mut m.ops, outcome);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_schema_reconcile_outcome(entry, outcome);
                });
            }
            MetricsEvent::SchemaStoreFootprint {
                encoded_bytes,
                entity_path,
                latest_snapshot_bytes,
                snapshots,
            } => {
                metrics::with_state_mut(|m| {
                    let (
                        previous_snapshots,
                        previous_encoded_bytes,
                        previous_latest_snapshot_bytes,
                    ) = {
                        let entry = m.entities.entry(entity_path.to_string()).or_default();
                        let previous = (
                            entry.schema_store_snapshots,
                            entry.schema_store_encoded_bytes,
                            entry.schema_store_latest_snapshot_bytes,
                        );
                        entry.schema_store_snapshots = snapshots;
                        entry.schema_store_encoded_bytes = encoded_bytes;
                        entry.schema_store_latest_snapshot_bytes = latest_snapshot_bytes;

                        previous
                    };
                    replace_gauge_total(
                        &mut m.ops.schema_store_snapshots,
                        previous_snapshots,
                        snapshots,
                    );
                    replace_gauge_total(
                        &mut m.ops.schema_store_encoded_bytes,
                        previous_encoded_bytes,
                        encoded_bytes,
                    );
                    replace_gauge_total(
                        &mut m.ops.schema_store_latest_snapshot_bytes,
                        previous_latest_snapshot_bytes,
                        latest_snapshot_bytes,
                    );
                });
            }
            MetricsEvent::SchemaTransition {
                entity_path,
                outcome,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_schema_transition_outcome(&mut m.ops, outcome);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_schema_transition_outcome(entry, outcome);
                });
            }
            MetricsEvent::SqlCompileReject { entity_path, phase } => {
                metrics::with_state_mut(|m| {
                    record_global_sql_compile_reject_phase(&mut m.ops, phase);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_sql_compile_reject_phase(entry, phase);
                });
            }
            MetricsEvent::SqlWrite {
                entity_path,
                kind,
                staged_rows,
                matched_rows,
                mutated_rows,
                returning_rows,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_sql_write_kind(&mut m.ops, kind);
                    m.ops.sql_write_staged_rows =
                        m.ops.sql_write_staged_rows.saturating_add(staged_rows);
                    m.ops.sql_write_matched_rows =
                        m.ops.sql_write_matched_rows.saturating_add(matched_rows);
                    m.ops.sql_write_mutated_rows =
                        m.ops.sql_write_mutated_rows.saturating_add(mutated_rows);
                    m.ops.sql_write_returning_rows = m
                        .ops
                        .sql_write_returning_rows
                        .saturating_add(returning_rows);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_sql_write_kind(entry, kind);
                    entry.sql_write_staged_rows =
                        entry.sql_write_staged_rows.saturating_add(staged_rows);
                    entry.sql_write_matched_rows =
                        entry.sql_write_matched_rows.saturating_add(matched_rows);
                    entry.sql_write_mutated_rows =
                        entry.sql_write_mutated_rows.saturating_add(mutated_rows);
                    entry.sql_write_returning_rows = entry
                        .sql_write_returning_rows
                        .saturating_add(returning_rows);
                });
            }
            MetricsEvent::SqlWriteError {
                entity_path,
                kind,
                class,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_sql_write_error_kind(&mut m.ops, kind);
                    record_global_sql_write_error_class(&mut m.ops, class);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_sql_write_error_kind(entry, kind);
                    record_entity_sql_write_error_class(entry, class);
                });
            }
            MetricsEvent::UniqueViolation { entity_path } => {
                metrics::with_state_mut(|m| {
                    m.ops.unique_violations = m.ops.unique_violations.saturating_add(1);
                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    entry.unique_violations = entry.unique_violations.saturating_add(1);
                });
            }
        }
    }
}

pub(crate) const GLOBAL_METRICS_SINK: GlobalMetricsSink = GlobalMetricsSink;

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

/// Span
/// RAII guard that emits start/finish metrics events for one executor call.
/// Ensures finish accounting happens even on unwind.

pub(crate) struct Span<E: EntityKind> {
    inner: PathSpan,
    _marker: PhantomData<E>,
}

///
/// PathSpan
///
/// PathSpan is the structural metrics span used when execution observability
/// already resolved the target entity path at a non-generic boundary.
/// It preserves the same start/finish accounting contract as `Span<E>` without
/// requiring an entity-typed caller.
///

pub(crate) struct PathSpan {
    kind: ExecKind,
    entity_path: &'static str,
    start: u64,
    rows: u64,
    outcome: ExecOutcome,
    finished: bool,
}

#[cfg_attr(
    not(target_arch = "wasm32"),
    expect(
        clippy::missing_const_for_fn,
        reason = "host metrics counter stub intentionally mirrors the wasm runtime hook"
    )
)]
fn read_perf_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        crate::runtime::performance_counter(1)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

impl<E: EntityKind> Span<E> {
    /// Start a metrics span for a specific entity and executor kind.
    #[must_use]
    pub(crate) fn new(kind: ExecKind) -> Self {
        Self {
            inner: PathSpan::new(kind, E::PATH),
            _marker: PhantomData,
        }
    }

    pub(crate) const fn set_rows(&mut self, rows: u64) {
        self.inner.set_rows(rows);
    }

    pub(crate) const fn set_error(&mut self, error: &InternalError) {
        self.inner.set_error(error);
    }
}

/// Record one classified executor error for a path that failed before the
/// ordinary success span boundary was reached.
pub(crate) fn record_exec_error_for_path(
    kind: ExecKind,
    entity_path: &'static str,
    error: &InternalError,
) {
    record(MetricsEvent::ExecError {
        kind,
        entity_path,
        outcome: ExecOutcome::from_error(error),
    });
}

/// Record one cache outcome for a cache key already scoped to an entity.
pub(crate) fn record_cache_event_for_path(
    kind: CacheKind,
    outcome: CacheOutcome,
    entity_path: &'static str,
) {
    record(MetricsEvent::Cache {
        entity_path,
        kind,
        outcome,
    });
}

/// Record the low-cardinality reason for one cache miss.
pub(crate) fn record_cache_miss_reason_for_path(
    kind: CacheKind,
    reason: CacheMissReason,
    entity_path: &'static str,
) {
    record(MetricsEvent::CacheMissReason {
        entity_path,
        kind,
        reason,
    });
}

/// Record one SQL compile rejection for a command already scoped to an entity.
#[cfg(feature = "sql")]
pub(crate) fn record_sql_compile_reject_for_path(
    phase: SqlCompileRejectPhase,
    entity_path: &'static str,
) {
    record(MetricsEvent::SqlCompileReject { entity_path, phase });
}

/// Record the latest observed schema-store footprint for one entity.
pub(crate) fn record_schema_store_footprint_for_path(
    entity_path: &'static str,
    snapshots: u64,
    encoded_bytes: u64,
    latest_snapshot_bytes: u64,
) {
    record(MetricsEvent::SchemaStoreFootprint {
        encoded_bytes,
        entity_path,
        latest_snapshot_bytes,
        snapshots,
    });
}

/// Record the latest observed accepted schema fact footprint for one entity.
pub(crate) fn record_accepted_schema_footprint_for_path(
    entity_path: &'static str,
    fields: u64,
    nested_leaf_facts: u64,
) {
    record(MetricsEvent::AcceptedSchemaFootprint {
        entity_path,
        fields,
        nested_leaf_facts,
    });
}

/// Record one executor authority prepared-shape finalization outcome.
pub(crate) fn record_prepared_shape_finalization_for_path(
    entity_path: &'static str,
    outcome: PreparedShapeFinalizationOutcome,
) {
    record(MetricsEvent::PreparedShapeFinalization {
        entity_path,
        outcome,
    });
}

/// Record the latest observed entry count for one cache family.
pub(crate) fn record_cache_entries(kind: CacheKind, entries: usize) {
    let entries = u64::try_from(entries).unwrap_or(u64::MAX);

    record(MetricsEvent::CacheEntries { kind, entries });
}

impl<E: EntityKind> Drop for Span<E> {
    fn drop(&mut self) {
        self.inner.finish();
    }
}

impl PathSpan {
    /// Start a metrics span for one structural entity path and executor kind.
    #[must_use]
    pub(crate) fn new(kind: ExecKind, entity_path: &'static str) -> Self {
        record(MetricsEvent::ExecStart { kind, entity_path });

        Self {
            kind,
            entity_path,
            start: read_perf_counter(),
            rows: 0,
            outcome: ExecOutcome::Aborted,
            finished: false,
        }
    }

    pub(crate) const fn set_rows(&mut self, rows: u64) {
        self.rows = rows;
        self.outcome = ExecOutcome::Success;
    }

    pub(crate) const fn set_error(&mut self, error: &InternalError) {
        self.outcome = ExecOutcome::from_error(error);
    }

    fn finish_inner(&self) {
        let now = read_perf_counter();
        let delta = now.saturating_sub(self.start);

        record(MetricsEvent::ExecFinish {
            kind: self.kind,
            entity_path: self.entity_path,
            rows_touched: self.rows,
            inst_delta: delta,
            outcome: self.outcome,
        });
    }

    fn finish(&mut self) {
        if !self.finished {
            self.finish_inner();
            self.finished = true;
        }
    }
}

impl Drop for PathSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests;
