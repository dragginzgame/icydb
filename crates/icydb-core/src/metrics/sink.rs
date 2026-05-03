//! Module: metrics::sink
//! Responsibility: instrumentation sink traits and the bridge into metrics state.
//! Does not own: stored metrics DTO definitions or executor business logic.
//! Boundary: the only allowed connection between runtime instrumentation and global metrics state.
//!
//! Core DB logic MUST NOT depend on `metrics::state` directly.
//! All instrumentation flows through `MetricsEvent` and `MetricsSink`.
use crate::{
    error::{ErrorClass, InternalError},
    metrics::state as metrics,
    traits::EntityKind,
};
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
/// ExecOutcome
///

#[derive(Clone, Copy, Debug)]
pub enum ExecOutcome {
    Success,
    ErrorCorruption,
    ErrorIncompatiblePersistedFormat,
    ErrorNotFound,
    ErrorInternal,
    ErrorConflict,
    ErrorUnsupported,
    ErrorInvariantViolation,
    Aborted,
}

///
/// CacheKind
///

#[derive(Clone, Copy, Debug)]
pub enum CacheKind {
    SharedQueryPlan,
    SqlCompiledCommand,
}

///
/// CacheOutcome
///

#[derive(Clone, Copy, Debug)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Insert,
}

impl ExecOutcome {
    // Map the crate's typed runtime error taxonomy into stable metrics buckets.
    const fn from_error(error: &InternalError) -> Self {
        match error.class() {
            ErrorClass::Corruption => Self::ErrorCorruption,
            ErrorClass::IncompatiblePersistedFormat => Self::ErrorIncompatiblePersistedFormat,
            ErrorClass::NotFound => Self::ErrorNotFound,
            ErrorClass::Internal => Self::ErrorInternal,
            ErrorClass::Conflict => Self::ErrorConflict,
            ErrorClass::Unsupported => Self::ErrorUnsupported,
            ErrorClass::InvariantViolation => Self::ErrorInvariantViolation,
        }
    }
}

///
/// SaveMutationKind
///

#[derive(Clone, Copy, Debug)]
pub enum SaveMutationKind {
    Insert,
    Update,
    Replace,
}

///
/// SqlWriteKind
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlWriteKind {
    Insert,
    InsertSelect,
    Update,
    Delete,
}

///
/// PlanKind
///

#[derive(Clone, Copy, Debug)]
pub enum PlanKind {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexMultiLookup,
    IndexRange,
    FullScan,
    Union,
    Intersection,
}

///
/// GroupedPlanExecutionMode
///
/// Canonical grouped-plan mode carried by metrics events.
/// This keeps grouped metrics classification structured without routing
/// through string codes that the sink would immediately decode again.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedPlanExecutionMode {
    HashMaterialized,
    OrderedMaterialized,
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
        outcome: ExecOutcome,
    },
    ExecError {
        kind: ExecKind,
        entity_path: &'static str,
        outcome: ExecOutcome,
    },
    Cache {
        entity_path: &'static str,
        kind: CacheKind,
        outcome: CacheOutcome,
    },
    CacheEntries {
        kind: CacheKind,
        entries: u64,
    },
    RowsScanned {
        entity_path: &'static str,
        rows_scanned: u64,
    },
    RowsFiltered {
        entity_path: &'static str,
        rows_filtered: u64,
    },
    RowsAggregated {
        entity_path: &'static str,
        rows_aggregated: u64,
    },
    RowsEmitted {
        entity_path: &'static str,
        rows_emitted: u64,
    },
    LoadRowEfficiency {
        entity_path: &'static str,
        candidate_rows_scanned: u64,
        candidate_rows_filtered: u64,
        result_rows_emitted: u64,
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
    NonAtomicPartialCommit {
        entity_path: &'static str,
        committed_rows: u64,
    },
    SaveMutation {
        entity_path: &'static str,
        kind: SaveMutationKind,
        rows_touched: u64,
    },
    SqlWrite {
        entity_path: &'static str,
        kind: SqlWriteKind,
        matched_rows: u64,
        mutated_rows: u64,
        returning_rows: u64,
    },
    Plan {
        entity_path: &'static str,
        kind: PlanKind,
        grouped_execution_mode: Option<GroupedPlanExecutionMode>,
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
                    record_global_exec_start(&mut m.ops, kind);

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_exec_start(entry, kind);
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
                            m.ops.rows_saved = m.ops.rows_saved.saturating_add(rows_touched);
                            m.ops.write_rows_touched =
                                m.ops.write_rows_touched.saturating_add(rows_touched);
                            metrics::add_instructions(
                                &mut m.perf.save_inst_total,
                                &mut m.perf.save_inst_max,
                                inst_delta,
                            );
                        }
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
                    }

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    record_entity_exec_outcome(entry, outcome);
                    match kind {
                        ExecKind::Load => {
                            entry.rows_loaded = entry.rows_loaded.saturating_add(rows_touched);
                        }
                        ExecKind::Delete => {
                            entry.rows_deleted = entry.rows_deleted.saturating_add(rows_touched);
                            entry.write_rows_touched =
                                entry.write_rows_touched.saturating_add(rows_touched);
                        }
                        ExecKind::Save => {
                            entry.rows_saved = entry.rows_saved.saturating_add(rows_touched);
                            entry.write_rows_touched =
                                entry.write_rows_touched.saturating_add(rows_touched);
                        }
                    }
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

            MetricsEvent::SaveMutation {
                entity_path,
                kind,
                rows_touched,
            } => {
                metrics::with_state_mut(|m| {
                    match kind {
                        SaveMutationKind::Insert => {
                            m.ops.save_insert_calls = m.ops.save_insert_calls.saturating_add(1);
                            m.ops.rows_inserted = m.ops.rows_inserted.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Update => {
                            m.ops.save_update_calls = m.ops.save_update_calls.saturating_add(1);
                            m.ops.rows_updated = m.ops.rows_updated.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Replace => {
                            m.ops.save_replace_calls = m.ops.save_replace_calls.saturating_add(1);
                            m.ops.rows_replaced = m.ops.rows_replaced.saturating_add(rows_touched);
                        }
                    }

                    let entry = m.entities.entry(entity_path.to_string()).or_default();
                    match kind {
                        SaveMutationKind::Insert => {
                            entry.save_insert_calls = entry.save_insert_calls.saturating_add(1);
                            entry.rows_inserted = entry.rows_inserted.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Update => {
                            entry.save_update_calls = entry.save_update_calls.saturating_add(1);
                            entry.rows_updated = entry.rows_updated.saturating_add(rows_touched);
                        }
                        SaveMutationKind::Replace => {
                            entry.save_replace_calls = entry.save_replace_calls.saturating_add(1);
                            entry.rows_replaced = entry.rows_replaced.saturating_add(rows_touched);
                        }
                    }
                });
            }

            MetricsEvent::SqlWrite {
                entity_path,
                kind,
                matched_rows,
                mutated_rows,
                returning_rows,
            } => {
                metrics::with_state_mut(|m| {
                    record_global_sql_write_kind(&mut m.ops, kind);
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
                    entry.sql_write_matched_rows =
                        entry.sql_write_matched_rows.saturating_add(matched_rows);
                    entry.sql_write_mutated_rows =
                        entry.sql_write_mutated_rows.saturating_add(mutated_rows);
                    entry.sql_write_returning_rows = entry
                        .sql_write_returning_rows
                        .saturating_add(returning_rows);
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

// Start counters are used for ordinary spans and for load errors that return
// before the successful load finalizers create their normal span.
const fn record_global_exec_start(ops: &mut metrics::EventOps, kind: ExecKind) {
    match kind {
        ExecKind::Load => ops.load_calls = ops.load_calls.saturating_add(1),
        ExecKind::Save => ops.save_calls = ops.save_calls.saturating_add(1),
        ExecKind::Delete => {
            ops.delete_calls = ops.delete_calls.saturating_add(1);
        }
    }
}

// Mirror executor starts into entity summaries so attempts and outcomes can be
// read from the same per-entity row in the report.
const fn record_entity_exec_start(ops: &mut metrics::EntityCounters, kind: ExecKind) {
    match kind {
        ExecKind::Load => {
            ops.load_calls = ops.load_calls.saturating_add(1);
        }
        ExecKind::Save => {
            ops.save_calls = ops.save_calls.saturating_add(1);
        }
        ExecKind::Delete => {
            ops.delete_calls = ops.delete_calls.saturating_add(1);
        }
    }
}

// Outcome counters are shared by all executor kinds. Per-kind attempts still
// come from load/save/delete call counters, so this layer only tracks finish
// status and error taxonomy.
const fn record_global_exec_outcome(ops: &mut metrics::EventOps, outcome: ExecOutcome) {
    match outcome {
        ExecOutcome::Success => {
            ops.exec_success = ops.exec_success.saturating_add(1);
        }
        ExecOutcome::ErrorCorruption => {
            ops.exec_error_corruption = ops.exec_error_corruption.saturating_add(1);
        }
        ExecOutcome::ErrorIncompatiblePersistedFormat => {
            ops.exec_error_incompatible_persisted_format = ops
                .exec_error_incompatible_persisted_format
                .saturating_add(1);
        }
        ExecOutcome::ErrorNotFound => {
            ops.exec_error_not_found = ops.exec_error_not_found.saturating_add(1);
        }
        ExecOutcome::ErrorInternal => {
            ops.exec_error_internal = ops.exec_error_internal.saturating_add(1);
        }
        ExecOutcome::ErrorConflict => {
            ops.exec_error_conflict = ops.exec_error_conflict.saturating_add(1);
        }
        ExecOutcome::ErrorUnsupported => {
            ops.exec_error_unsupported = ops.exec_error_unsupported.saturating_add(1);
        }
        ExecOutcome::ErrorInvariantViolation => {
            ops.exec_error_invariant_violation =
                ops.exec_error_invariant_violation.saturating_add(1);
        }
        ExecOutcome::Aborted => {
            ops.exec_aborted = ops.exec_aborted.saturating_add(1);
        }
    }
}

// Mirror outcome attribution into entity summaries so failed operations can be
// correlated with the model that owned the executor span.
const fn record_entity_exec_outcome(ops: &mut metrics::EntityCounters, outcome: ExecOutcome) {
    match outcome {
        ExecOutcome::Success => {
            ops.exec_success = ops.exec_success.saturating_add(1);
        }
        ExecOutcome::ErrorCorruption => {
            ops.exec_error_corruption = ops.exec_error_corruption.saturating_add(1);
        }
        ExecOutcome::ErrorIncompatiblePersistedFormat => {
            ops.exec_error_incompatible_persisted_format = ops
                .exec_error_incompatible_persisted_format
                .saturating_add(1);
        }
        ExecOutcome::ErrorNotFound => {
            ops.exec_error_not_found = ops.exec_error_not_found.saturating_add(1);
        }
        ExecOutcome::ErrorInternal => {
            ops.exec_error_internal = ops.exec_error_internal.saturating_add(1);
        }
        ExecOutcome::ErrorConflict => {
            ops.exec_error_conflict = ops.exec_error_conflict.saturating_add(1);
        }
        ExecOutcome::ErrorUnsupported => {
            ops.exec_error_unsupported = ops.exec_error_unsupported.saturating_add(1);
        }
        ExecOutcome::ErrorInvariantViolation => {
            ops.exec_error_invariant_violation =
                ops.exec_error_invariant_violation.saturating_add(1);
        }
        ExecOutcome::Aborted => {
            ops.exec_aborted = ops.exec_aborted.saturating_add(1);
        }
    }
}

const fn record_global_sql_write_kind(ops: &mut metrics::EventOps, kind: SqlWriteKind) {
    match kind {
        SqlWriteKind::Insert => {
            ops.sql_insert_calls = ops.sql_insert_calls.saturating_add(1);
        }
        SqlWriteKind::InsertSelect => {
            ops.sql_insert_select_calls = ops.sql_insert_select_calls.saturating_add(1);
        }
        SqlWriteKind::Update => {
            ops.sql_update_calls = ops.sql_update_calls.saturating_add(1);
        }
        SqlWriteKind::Delete => {
            ops.sql_delete_calls = ops.sql_delete_calls.saturating_add(1);
        }
    }
}

const fn record_entity_sql_write_kind(ops: &mut metrics::EntityCounters, kind: SqlWriteKind) {
    match kind {
        SqlWriteKind::Insert => {
            ops.sql_insert_calls = ops.sql_insert_calls.saturating_add(1);
        }
        SqlWriteKind::InsertSelect => {
            ops.sql_insert_select_calls = ops.sql_insert_select_calls.saturating_add(1);
        }
        SqlWriteKind::Update => {
            ops.sql_update_calls = ops.sql_update_calls.saturating_add(1);
        }
        SqlWriteKind::Delete => {
            ops.sql_delete_calls = ops.sql_delete_calls.saturating_add(1);
        }
    }
}

// Cache counters are intentionally cache-family specific and outcome specific
// so the report can distinguish a cold cache from a warmed cache that inserts
// successfully after misses.
const fn record_global_cache_outcome(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    outcome: CacheOutcome,
) {
    match (kind, outcome) {
        (CacheKind::SharedQueryPlan, CacheOutcome::Hit) => {
            ops.cache_shared_query_plan_hits = ops.cache_shared_query_plan_hits.saturating_add(1);
        }
        (CacheKind::SharedQueryPlan, CacheOutcome::Miss) => {
            ops.cache_shared_query_plan_misses =
                ops.cache_shared_query_plan_misses.saturating_add(1);
        }
        (CacheKind::SharedQueryPlan, CacheOutcome::Insert) => {
            ops.cache_shared_query_plan_inserts =
                ops.cache_shared_query_plan_inserts.saturating_add(1);
        }
        (CacheKind::SqlCompiledCommand, CacheOutcome::Hit) => {
            ops.cache_sql_compiled_command_hits =
                ops.cache_sql_compiled_command_hits.saturating_add(1);
        }
        (CacheKind::SqlCompiledCommand, CacheOutcome::Miss) => {
            ops.cache_sql_compiled_command_misses =
                ops.cache_sql_compiled_command_misses.saturating_add(1);
        }
        (CacheKind::SqlCompiledCommand, CacheOutcome::Insert) => {
            ops.cache_sql_compiled_command_inserts =
                ops.cache_sql_compiled_command_inserts.saturating_add(1);
        }
    }
}

// Cache size is a gauge for the current scope, not an event count. Cache owners
// refresh it after lookups and insertions so the metrics report can show memory
// pressure alongside reuse outcomes.
const fn record_global_cache_entries(ops: &mut metrics::EventOps, kind: CacheKind, entries: u64) {
    match kind {
        CacheKind::SharedQueryPlan => {
            ops.cache_shared_query_plan_entries = entries;
        }
        CacheKind::SqlCompiledCommand => {
            ops.cache_sql_compiled_command_entries = entries;
        }
    }
}

// Mirror cache activity to the owning entity so global cache movement can be
// traced back to the model whose schema/query identity produced it.
const fn record_entity_cache_outcome(
    ops: &mut metrics::EntityCounters,
    kind: CacheKind,
    outcome: CacheOutcome,
) {
    match (kind, outcome) {
        (CacheKind::SharedQueryPlan, CacheOutcome::Hit) => {
            ops.cache_shared_query_plan_hits = ops.cache_shared_query_plan_hits.saturating_add(1);
        }
        (CacheKind::SharedQueryPlan, CacheOutcome::Miss) => {
            ops.cache_shared_query_plan_misses =
                ops.cache_shared_query_plan_misses.saturating_add(1);
        }
        (CacheKind::SharedQueryPlan, CacheOutcome::Insert) => {
            ops.cache_shared_query_plan_inserts =
                ops.cache_shared_query_plan_inserts.saturating_add(1);
        }
        (CacheKind::SqlCompiledCommand, CacheOutcome::Hit) => {
            ops.cache_sql_compiled_command_hits =
                ops.cache_sql_compiled_command_hits.saturating_add(1);
        }
        (CacheKind::SqlCompiledCommand, CacheOutcome::Miss) => {
            ops.cache_sql_compiled_command_misses =
                ops.cache_sql_compiled_command_misses.saturating_add(1);
        }
        (CacheKind::SqlCompiledCommand, CacheOutcome::Insert) => {
            ops.cache_sql_compiled_command_inserts =
                ops.cache_sql_compiled_command_inserts.saturating_add(1);
        }
    }
}

// Keep the legacy coarse global plan groups in lockstep with the detailed
// route counters so existing dashboards and newer diagnostics can agree.
const fn record_global_plan_kind(ops: &mut metrics::EventOps, kind: PlanKind) {
    match kind {
        PlanKind::ByKey => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_key = ops.plan_by_key.saturating_add(1);
        }
        PlanKind::ByKeys => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_keys = ops.plan_by_keys.saturating_add(1);
        }
        PlanKind::KeyRange => {
            ops.plan_range = ops.plan_range.saturating_add(1);
            ops.plan_key_range = ops.plan_key_range.saturating_add(1);
        }
        PlanKind::IndexPrefix => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_prefix = ops.plan_index_prefix.saturating_add(1);
        }
        PlanKind::IndexMultiLookup => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_multi_lookup = ops.plan_index_multi_lookup.saturating_add(1);
        }
        PlanKind::IndexRange => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_range = ops.plan_index_range.saturating_add(1);
        }
        PlanKind::FullScan => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_explicit_full_scan = ops.plan_explicit_full_scan.saturating_add(1);
        }
        PlanKind::Union => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_union = ops.plan_union.saturating_add(1);
        }
        PlanKind::Intersection => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_intersection = ops.plan_intersection.saturating_add(1);
        }
    }
}

// Grouped plan modes are orthogonal to access shape, so count them beside the
// route counters instead of deriving them from a single access kind.
const fn record_global_grouped_plan_mode(
    ops: &mut metrics::EventOps,
    grouped_execution_mode: Option<GroupedPlanExecutionMode>,
) {
    match grouped_execution_mode {
        Some(GroupedPlanExecutionMode::HashMaterialized) => {
            ops.plan_grouped_hash_materialized =
                ops.plan_grouped_hash_materialized.saturating_add(1);
        }
        Some(GroupedPlanExecutionMode::OrderedMaterialized) => {
            ops.plan_grouped_ordered_materialized =
                ops.plan_grouped_ordered_materialized.saturating_add(1);
        }
        None => {}
    }
}

// Mirror global plan attribution into the owning entity summary so operators
// can identify which model is causing full scans, unions, or expensive grouped
// routes without correlating separate counters.
const fn record_entity_plan_kind(ops: &mut metrics::EntityCounters, kind: PlanKind) {
    match kind {
        PlanKind::ByKey => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_key = ops.plan_by_key.saturating_add(1);
        }
        PlanKind::ByKeys => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_keys = ops.plan_by_keys.saturating_add(1);
        }
        PlanKind::KeyRange => {
            ops.plan_range = ops.plan_range.saturating_add(1);
            ops.plan_key_range = ops.plan_key_range.saturating_add(1);
        }
        PlanKind::IndexPrefix => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_prefix = ops.plan_index_prefix.saturating_add(1);
        }
        PlanKind::IndexMultiLookup => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_multi_lookup = ops.plan_index_multi_lookup.saturating_add(1);
        }
        PlanKind::IndexRange => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_range = ops.plan_index_range.saturating_add(1);
        }
        PlanKind::FullScan => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_explicit_full_scan = ops.plan_explicit_full_scan.saturating_add(1);
        }
        PlanKind::Union => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_union = ops.plan_union.saturating_add(1);
        }
        PlanKind::Intersection => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_intersection = ops.plan_intersection.saturating_add(1);
        }
    }
}

// Grouped execution counters stay per entity for the same reason as access
// route counters: global counts show shape drift, but entity counts show owner.
const fn record_entity_grouped_plan_mode(
    ops: &mut metrics::EntityCounters,
    grouped_execution_mode: Option<GroupedPlanExecutionMode>,
) {
    match grouped_execution_mode {
        Some(GroupedPlanExecutionMode::HashMaterialized) => {
            ops.plan_grouped_hash_materialized =
                ops.plan_grouped_hash_materialized.saturating_add(1);
        }
        Some(GroupedPlanExecutionMode::OrderedMaterialized) => {
            ops.plan_grouped_ordered_materialized =
                ops.plan_grouped_ordered_materialized.saturating_add(1);
        }
        None => {}
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
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::ByKey,
            grouped_execution_mode: None,
        });
        assert_eq!(outer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner_calls.load(Ordering::SeqCst), 0);

        with_metrics_sink(&outer, || {
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::Entity",
                kind: PlanKind::IndexPrefix,
                grouped_execution_mode: None,
            });
            assert_eq!(outer_calls.load(Ordering::SeqCst), 1);
            assert_eq!(inner_calls.load(Ordering::SeqCst), 0);

            with_metrics_sink(&inner, || {
                record(MetricsEvent::Plan {
                    entity_path: "metrics::tests::Entity",
                    kind: PlanKind::KeyRange,
                    grouped_execution_mode: None,
                });
            });

            // Inner override was restored to outer override.
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::Entity",
                kind: PlanKind::FullScan,
                grouped_execution_mode: None,
            });
        });

        assert_eq!(outer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(inner_calls.load(Ordering::SeqCst), 1);

        // Outer override was restored to previous (none).
        SINK_OVERRIDE.with(|cell| {
            assert!(cell.borrow().is_none());
        });

        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::ByKey,
            grouped_execution_mode: None,
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
                    entity_path: "metrics::tests::Entity",
                    kind: PlanKind::IndexPrefix,
                    grouped_execution_mode: None,
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
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::KeyRange,
            grouped_execution_mode: None,
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn metrics_report_without_window_start_returns_counters() {
        metrics_reset_all();
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::IndexPrefix,
            grouped_execution_mode: None,
        });

        let report = metrics_report(None);
        assert!(report.window_filter_matched());
        let counters = report
            .counters()
            .expect("metrics report should include counters without since filter");
        assert_eq!(counters.ops.plan_index, 1);
    }

    #[test]
    fn metrics_report_window_start_before_window_returns_counters() {
        metrics_reset_all();
        let window_start = metrics::with_state(|m| m.window_start_ms);
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::ByKey,
            grouped_execution_mode: None,
        });

        let report = metrics_report(Some(window_start.saturating_sub(1)));
        assert!(report.window_filter_matched());
        assert_eq!(
            report.requested_window_start_ms(),
            Some(window_start.saturating_sub(1)),
        );
        assert_eq!(report.active_window_start_ms(), window_start);
        let counters = report
            .counters()
            .expect("metrics report should include counters when window_start_ms is before window");
        assert_eq!(counters.ops.plan_keys, 1);
    }

    #[test]
    fn metrics_report_window_start_after_window_returns_empty() {
        metrics_reset_all();
        let window_start = metrics::with_state(|m| m.window_start_ms);
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::FullScan,
            grouped_execution_mode: None,
        });

        let report = metrics_report(Some(window_start.saturating_add(1)));
        assert!(!report.window_filter_matched());
        assert_eq!(
            report.requested_window_start_ms(),
            Some(window_start.saturating_add(1)),
        );
        assert_eq!(report.active_window_start_ms(), window_start);
        assert!(report.counters().is_none());
        assert!(report.entity_counters().is_empty());
    }

    #[test]
    fn metrics_report_grouped_execution_mode_counters_accumulate() {
        metrics_reset_all();
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::IndexPrefix,
            grouped_execution_mode: Some(GroupedPlanExecutionMode::HashMaterialized),
        });
        record(MetricsEvent::Plan {
            entity_path: "metrics::tests::Entity",
            kind: PlanKind::KeyRange,
            grouped_execution_mode: Some(GroupedPlanExecutionMode::OrderedMaterialized),
        });

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.plan_index, 1);
        assert_eq!(counters.ops.plan_range, 1);
        assert_eq!(counters.ops.plan_grouped_hash_materialized, 1);
        assert_eq!(counters.ops.plan_grouped_ordered_materialized, 1);

        let entity = report
            .entity_counters()
            .first()
            .expect("grouped plan metrics should retain per-entity counters");
        assert_eq!(entity.plan_grouped_hash_materialized(), 1);
        assert_eq!(entity.plan_grouped_ordered_materialized(), 1);
    }

    #[test]
    fn detailed_plan_metrics_accumulate_alongside_coarse_groups() {
        metrics_reset_all();

        for kind in [
            PlanKind::ByKey,
            PlanKind::ByKeys,
            PlanKind::KeyRange,
            PlanKind::IndexPrefix,
            PlanKind::IndexMultiLookup,
            PlanKind::IndexRange,
            PlanKind::FullScan,
            PlanKind::Union,
            PlanKind::Intersection,
        ] {
            record(MetricsEvent::Plan {
                entity_path: "metrics::tests::Entity",
                kind,
                grouped_execution_mode: None,
            });
        }

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.plan_keys, 2);
        assert_eq!(counters.ops.plan_range, 1);
        assert_eq!(counters.ops.plan_index, 3);
        assert_eq!(counters.ops.plan_full_scan, 3);
        assert_eq!(counters.ops.plan_by_key, 1);
        assert_eq!(counters.ops.plan_by_keys, 1);
        assert_eq!(counters.ops.plan_key_range, 1);
        assert_eq!(counters.ops.plan_index_prefix, 1);
        assert_eq!(counters.ops.plan_index_multi_lookup, 1);
        assert_eq!(counters.ops.plan_index_range, 1);
        assert_eq!(counters.ops.plan_explicit_full_scan, 1);
        assert_eq!(counters.ops.plan_union, 1);
        assert_eq!(counters.ops.plan_intersection, 1);

        let entity = report
            .entity_counters()
            .first()
            .expect("plan metrics should retain per-entity counters");
        assert_eq!(entity.path(), "metrics::tests::Entity");
        assert_eq!(entity.plan_keys(), 2);
        assert_eq!(entity.plan_range(), 1);
        assert_eq!(entity.plan_index(), 3);
        assert_eq!(entity.plan_full_scan(), 3);
        assert_eq!(entity.plan_by_key(), 1);
        assert_eq!(entity.plan_by_keys(), 1);
        assert_eq!(entity.plan_key_range(), 1);
        assert_eq!(entity.plan_index_prefix(), 1);
        assert_eq!(entity.plan_index_multi_lookup(), 1);
        assert_eq!(entity.plan_index_range(), 1);
        assert_eq!(entity.plan_explicit_full_scan(), 1);
        assert_eq!(entity.plan_union(), 1);
        assert_eq!(entity.plan_intersection(), 1);
    }

    #[test]
    fn save_finish_metrics_accumulate_saved_rows() {
        metrics_reset_all();

        record(MetricsEvent::ExecFinish {
            kind: ExecKind::Save,
            entity_path: "metrics::tests::Entity",
            rows_touched: 4,
            inst_delta: 11,
            outcome: ExecOutcome::Success,
        });

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.rows_saved, 4);
        assert_eq!(counters.ops.write_rows_touched, 4);
        assert_eq!(counters.perf.save_inst_total, 11);

        let entity = report
            .entity_counters()
            .first()
            .expect("save finish should retain per-entity counters");
        assert_eq!(entity.rows_saved(), 4);
        assert_eq!(entity.write_rows_touched(), 4);
    }

    #[test]
    fn exec_finish_metrics_accumulate_outcomes_by_entity() {
        metrics_reset_all();

        for outcome in [
            ExecOutcome::Success,
            ExecOutcome::ErrorUnsupported,
            ExecOutcome::ErrorCorruption,
            ExecOutcome::Aborted,
        ] {
            record(MetricsEvent::ExecFinish {
                kind: ExecKind::Load,
                entity_path: "metrics::tests::Entity",
                rows_touched: 0,
                inst_delta: 0,
                outcome,
            });
        }

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.exec_success(), 1);
        assert_eq!(counters.ops.exec_error_unsupported(), 1);
        assert_eq!(counters.ops.exec_error_corruption(), 1);
        assert_eq!(counters.ops.exec_aborted(), 1);

        let entity = report
            .entity_counters()
            .first()
            .expect("outcome metrics should retain per-entity counters");
        assert_eq!(entity.exec_success(), 1);
        assert_eq!(entity.exec_error_unsupported(), 1);
        assert_eq!(entity.exec_error_corruption(), 1);
        assert_eq!(entity.exec_aborted(), 1);
    }

    #[test]
    fn exec_error_metrics_count_attempt_and_outcome_without_rows() {
        metrics_reset_all();

        record(MetricsEvent::ExecError {
            kind: ExecKind::Load,
            entity_path: "metrics::tests::Entity",
            outcome: ExecOutcome::ErrorInternal,
        });

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.load_calls(), 1);
        assert_eq!(counters.ops.exec_error_internal(), 1);
        assert_eq!(counters.ops.rows_loaded(), 0);

        let entity = report
            .entity_counters()
            .first()
            .expect("exec error should retain per-entity counters");
        assert_eq!(entity.load_calls(), 1);
        assert_eq!(entity.exec_error_internal(), 1);
        assert_eq!(entity.rows_loaded(), 0);
    }

    #[test]
    fn save_mutation_metrics_accumulate_by_mode() {
        metrics_reset_all();

        for (kind, rows_touched) in [
            (SaveMutationKind::Insert, 2),
            (SaveMutationKind::Update, 3),
            (SaveMutationKind::Replace, 4),
        ] {
            record(MetricsEvent::SaveMutation {
                entity_path: "metrics::tests::Entity",
                kind,
                rows_touched,
            });
        }

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.save_insert_calls, 1);
        assert_eq!(counters.ops.save_update_calls, 1);
        assert_eq!(counters.ops.save_replace_calls, 1);
        assert_eq!(counters.ops.rows_inserted, 2);
        assert_eq!(counters.ops.rows_updated, 3);
        assert_eq!(counters.ops.rows_replaced, 4);

        let entity = report
            .entity_counters()
            .first()
            .expect("save mutation should retain per-entity counters");
        assert_eq!(entity.save_insert_calls(), 1);
        assert_eq!(entity.save_update_calls(), 1);
        assert_eq!(entity.save_replace_calls(), 1);
        assert_eq!(entity.rows_inserted(), 2);
        assert_eq!(entity.rows_updated(), 3);
        assert_eq!(entity.rows_replaced(), 4);
    }

    #[test]
    fn sql_write_metrics_accumulate_by_command_shape() {
        metrics_reset_all();

        for (kind, matched_rows, mutated_rows, returning_rows) in [
            (SqlWriteKind::Insert, 2, 2, 0),
            (SqlWriteKind::InsertSelect, 3, 3, 3),
            (SqlWriteKind::Update, 5, 4, 4),
            (SqlWriteKind::Delete, 2, 2, 1),
        ] {
            record(MetricsEvent::SqlWrite {
                entity_path: "metrics::tests::Entity",
                kind,
                matched_rows,
                mutated_rows,
                returning_rows,
            });
        }

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.sql_insert_calls(), 1);
        assert_eq!(counters.ops.sql_insert_select_calls(), 1);
        assert_eq!(counters.ops.sql_update_calls(), 1);
        assert_eq!(counters.ops.sql_delete_calls(), 1);
        assert_eq!(counters.ops.sql_write_matched_rows(), 12);
        assert_eq!(counters.ops.sql_write_mutated_rows(), 11);
        assert_eq!(counters.ops.sql_write_returning_rows(), 8);

        let entity = report
            .entity_counters()
            .first()
            .expect("sql write metrics should retain per-entity counters");
        assert_eq!(entity.sql_insert_calls(), 1);
        assert_eq!(entity.sql_insert_select_calls(), 1);
        assert_eq!(entity.sql_update_calls(), 1);
        assert_eq!(entity.sql_delete_calls(), 1);
        assert_eq!(entity.sql_write_matched_rows(), 12);
        assert_eq!(entity.sql_write_mutated_rows(), 11);
        assert_eq!(entity.sql_write_returning_rows(), 8);
    }

    #[test]
    fn reverse_and_relation_metrics_events_accumulate() {
        metrics_reset_all();

        record(MetricsEvent::IndexDelta {
            entity_path: "metrics::tests::Entity",
            inserts: 4,
            removes: 1,
        });
        record(MetricsEvent::ReverseIndexDelta {
            entity_path: "metrics::tests::Entity",
            inserts: 3,
            removes: 2,
        });
        record(MetricsEvent::RelationValidation {
            entity_path: "metrics::tests::Entity",
            reverse_lookups: 5,
            blocked_deletes: 1,
        });

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.index_inserts, 4);
        assert_eq!(counters.ops.index_removes, 1);
        assert_eq!(counters.ops.write_index_entries_changed, 5);
        assert_eq!(counters.ops.reverse_index_inserts, 3);
        assert_eq!(counters.ops.reverse_index_removes, 2);
        assert_eq!(counters.ops.write_reverse_index_entries_changed, 5);
        assert_eq!(counters.ops.relation_reverse_lookups, 5);
        assert_eq!(counters.ops.relation_delete_blocks, 1);
        assert_eq!(counters.ops.write_relation_checks, 5);

        let entity = report
            .entity_counters()
            .first()
            .expect("maintenance events should retain per-entity counters");
        assert_eq!(entity.index_inserts(), 4);
        assert_eq!(entity.index_removes(), 1);
        assert_eq!(entity.write_index_entries_changed(), 5);
        assert_eq!(entity.reverse_index_inserts(), 3);
        assert_eq!(entity.reverse_index_removes(), 2);
        assert_eq!(entity.write_reverse_index_entries_changed(), 5);
        assert_eq!(entity.relation_reverse_lookups(), 5);
        assert_eq!(entity.relation_delete_blocks(), 1);
        assert_eq!(entity.write_relation_checks(), 5);
    }

    #[test]
    fn row_flow_metrics_events_accumulate() {
        metrics_reset_all();

        record(MetricsEvent::RowsFiltered {
            entity_path: "metrics::tests::Entity",
            rows_filtered: 9,
        });
        record(MetricsEvent::RowsAggregated {
            entity_path: "metrics::tests::Entity",
            rows_aggregated: 4,
        });
        record(MetricsEvent::RowsEmitted {
            entity_path: "metrics::tests::Entity",
            rows_emitted: 3,
        });
        record(MetricsEvent::LoadRowEfficiency {
            entity_path: "metrics::tests::Entity",
            candidate_rows_scanned: 11,
            candidate_rows_filtered: 8,
            result_rows_emitted: 3,
        });

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("metrics report should include counters");
        assert_eq!(counters.ops.rows_filtered, 9);
        assert_eq!(counters.ops.rows_aggregated, 4);
        assert_eq!(counters.ops.rows_emitted, 3);
        assert_eq!(counters.ops.load_candidate_rows_scanned, 11);
        assert_eq!(counters.ops.load_candidate_rows_filtered, 8);
        assert_eq!(counters.ops.load_result_rows_emitted, 3);
    }
}
