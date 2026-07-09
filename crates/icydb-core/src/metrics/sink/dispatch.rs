//! Module: metrics::sink::dispatch
//! Responsibility: apply stable metrics events to mutable metrics state.
//! Does not own: sink override routing, span lifetimes, or report/reset APIs.
//! Boundary: concrete global sink implementation behind the sink facade.

use crate::metrics::state as metrics;

use super::counters::*;
use super::{ExecKind, MetricsEvent, MetricsSink, SaveMutationKind};

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
