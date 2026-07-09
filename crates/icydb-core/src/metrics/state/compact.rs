//! Module: metrics::state::compact
//! Responsibility: compact metrics DTOs and sparse compact report building.
//! Does not own: rich metrics report DTOs or mutable metrics state updates.
//! Boundary: keeps numeric compact metrics encoding separate from rich reports.

use candid::CandidType;
use serde::Deserialize;

use crate::runtime::now_millis;

use super::{EntityCounters, EventOps, with_state};

/// Numeric codes used by compact metrics reports.
///
/// The default metrics endpoint returns these codes instead of the full
/// `EventOps` field graph so live canisters do not retain the rich report's
/// Candid schema unless the extended metrics endpoint is explicitly enabled.
pub mod compact_metric_code {
    /// Load entrypoint calls.
    pub const LOAD_CALLS: u16 = 1;
    /// Save entrypoint calls.
    pub const SAVE_CALLS: u16 = 2;
    /// Delete entrypoint calls.
    pub const DELETE_CALLS: u16 = 3;
    /// Successful executions.
    pub const EXEC_SUCCESS: u16 = 4;
    /// Execution errors collapsed across error classes.
    pub const EXEC_ERRORS: u16 = 5;
    /// Aborted executions.
    pub const EXEC_ABORTED: u16 = 6;
    /// Rows loaded.
    pub const ROWS_LOADED: u16 = 7;
    /// Rows saved.
    pub const ROWS_SAVED: u16 = 8;
    /// Rows deleted.
    pub const ROWS_DELETED: u16 = 9;
    /// Rows scanned.
    pub const ROWS_SCANNED: u16 = 10;
    /// Rows filtered.
    pub const ROWS_FILTERED: u16 = 11;
    /// Rows emitted.
    pub const ROWS_EMITTED: u16 = 12;
    /// SQL INSERT calls.
    pub const SQL_INSERT_CALLS: u16 = 13;
    /// SQL INSERT SELECT calls.
    pub const SQL_INSERT_SELECT_CALLS: u16 = 14;
    /// SQL UPDATE calls.
    pub const SQL_UPDATE_CALLS: u16 = 15;
    /// SQL DELETE calls.
    pub const SQL_DELETE_CALLS: u16 = 16;
    /// SQL write matched rows.
    pub const SQL_WRITE_MATCHED_ROWS: u16 = 17;
    /// SQL write mutated rows.
    pub const SQL_WRITE_MUTATED_ROWS: u16 = 18;
    /// SQL write RETURNING rows.
    pub const SQL_WRITE_RETURNING_ROWS: u16 = 19;
    /// Shared query-plan cache hits.
    pub const CACHE_SHARED_QUERY_PLAN_HITS: u16 = 20;
    /// Shared query-plan cache misses.
    pub const CACHE_SHARED_QUERY_PLAN_MISSES: u16 = 21;
    /// SQL compiled-command cache hits.
    pub const CACHE_SQL_COMPILED_COMMAND_HITS: u16 = 22;
    /// SQL compiled-command cache misses.
    pub const CACHE_SQL_COMPILED_COMMAND_MISSES: u16 = 23;
    /// SQL write rows staged before mutation.
    pub const SQL_WRITE_STAGED_ROWS: u16 = 24;
}

#[cfg_attr(doc, doc = "CompactMetric\n\nCompact metrics counter.")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct CompactMetric(u16, u64);

impl CompactMetric {
    #[must_use]
    const fn new(code: u16, value: u64) -> Self {
        Self(code, value)
    }

    /// Return the numeric metric code.
    #[must_use]
    pub const fn code(&self) -> u16 {
        self.0
    }

    /// Return the metric value.
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.1
    }

    /// Return the metric as a numeric code/value pair.
    #[must_use]
    pub const fn into_code_and_value(self) -> (u16, u64) {
        (self.0, self.1)
    }
}

#[cfg_attr(doc, doc = "CompactEventCounters\n\nCompact global metrics counters.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct CompactEventCounters(Vec<CompactMetric>, u64, u64);

impl CompactEventCounters {
    #[must_use]
    const fn new(metrics: Vec<CompactMetric>, window_start_ms: u64, window_end_ms: u64) -> Self {
        Self(metrics, window_start_ms, window_end_ms)
    }

    /// Borrow the sparse global metrics vector.
    #[must_use]
    pub fn metrics(&self) -> &[CompactMetric] {
        &self.0
    }

    /// Return the active window start timestamp in milliseconds.
    #[must_use]
    pub const fn window_start_ms(&self) -> u64 {
        self.1
    }

    /// Return the report window end timestamp in milliseconds.
    #[must_use]
    pub const fn window_end_ms(&self) -> u64 {
        self.2
    }

    /// Return the report window duration in milliseconds.
    #[must_use]
    pub const fn window_duration_ms(&self) -> u64 {
        self.2.saturating_sub(self.1)
    }
}

#[cfg_attr(
    doc,
    doc = "CompactEntityMetrics\n\nCompact per-entity metrics counters."
)]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct CompactEntityMetrics(String, Vec<CompactMetric>);

impl CompactEntityMetrics {
    #[must_use]
    const fn new(path: String, metrics: Vec<CompactMetric>) -> Self {
        Self(path, metrics)
    }

    /// Return the entity schema path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.0.as_str()
    }

    /// Borrow the sparse entity metrics vector.
    #[must_use]
    pub fn metrics(&self) -> &[CompactMetric] {
        &self.1
    }
}

#[cfg_attr(doc, doc = "CompactMetricsReport\n\nCompact metrics query payload.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct CompactMetricsReport(
    Option<CompactEventCounters>,
    Vec<CompactEntityMetrics>,
    Option<u64>,
    u64,
);

impl CompactMetricsReport {
    #[must_use]
    const fn new(
        counters: Option<CompactEventCounters>,
        entity_counters: Vec<CompactEntityMetrics>,
        requested_window_start_ms: Option<u64>,
        active_window_start_ms: u64,
    ) -> Self {
        Self(
            counters,
            entity_counters,
            requested_window_start_ms,
            active_window_start_ms,
        )
    }

    /// Borrow the compact global counters when the requested window matched.
    #[must_use]
    pub const fn counters(&self) -> Option<&CompactEventCounters> {
        self.0.as_ref()
    }

    /// Borrow compact per-entity counters.
    #[must_use]
    pub fn entity_counters(&self) -> &[CompactEntityMetrics] {
        &self.1
    }

    /// Return whether the requested window matched the active window.
    #[must_use]
    pub const fn window_filter_matched(&self) -> bool {
        self.0.is_some()
    }

    /// Return the requested window start timestamp, if supplied.
    #[must_use]
    pub const fn requested_window_start_ms(&self) -> Option<u64> {
        self.2
    }

    /// Return the active metrics window start timestamp.
    #[must_use]
    pub const fn active_window_start_ms(&self) -> u64 {
        self.3
    }
}

fn push_compact_metric(metrics: &mut Vec<CompactMetric>, code: u16, value: u64) {
    if value != 0 {
        metrics.push(CompactMetric::new(code, value));
    }
}

const fn event_ops_exec_errors(ops: &EventOps) -> u64 {
    ops.exec_error_corruption
        .saturating_add(ops.exec_error_incompatible_persisted_format)
        .saturating_add(ops.exec_error_not_found)
        .saturating_add(ops.exec_error_internal)
        .saturating_add(ops.exec_error_conflict)
        .saturating_add(ops.exec_error_unsupported)
        .saturating_add(ops.exec_error_invariant_violation)
}

const fn entity_ops_exec_errors(ops: &EntityCounters) -> u64 {
    ops.exec_error_corruption
        .saturating_add(ops.exec_error_incompatible_persisted_format)
        .saturating_add(ops.exec_error_not_found)
        .saturating_add(ops.exec_error_internal)
        .saturating_add(ops.exec_error_conflict)
        .saturating_add(ops.exec_error_unsupported)
        .saturating_add(ops.exec_error_invariant_violation)
}

fn compact_event_metrics(ops: &EventOps) -> Vec<CompactMetric> {
    use compact_metric_code::{
        CACHE_SHARED_QUERY_PLAN_HITS, CACHE_SHARED_QUERY_PLAN_MISSES,
        CACHE_SQL_COMPILED_COMMAND_HITS, CACHE_SQL_COMPILED_COMMAND_MISSES, DELETE_CALLS,
        EXEC_ABORTED, EXEC_ERRORS, EXEC_SUCCESS, LOAD_CALLS, ROWS_DELETED, ROWS_EMITTED,
        ROWS_FILTERED, ROWS_LOADED, ROWS_SAVED, ROWS_SCANNED, SAVE_CALLS, SQL_DELETE_CALLS,
        SQL_INSERT_CALLS, SQL_INSERT_SELECT_CALLS, SQL_UPDATE_CALLS, SQL_WRITE_MATCHED_ROWS,
        SQL_WRITE_MUTATED_ROWS, SQL_WRITE_RETURNING_ROWS, SQL_WRITE_STAGED_ROWS,
    };

    let mut metrics = Vec::new();
    push_compact_metric(&mut metrics, LOAD_CALLS, ops.load_calls);
    push_compact_metric(&mut metrics, SAVE_CALLS, ops.save_calls);
    push_compact_metric(&mut metrics, DELETE_CALLS, ops.delete_calls);
    push_compact_metric(&mut metrics, EXEC_SUCCESS, ops.exec_success);
    push_compact_metric(&mut metrics, EXEC_ERRORS, event_ops_exec_errors(ops));
    push_compact_metric(&mut metrics, EXEC_ABORTED, ops.exec_aborted);
    push_compact_metric(&mut metrics, ROWS_LOADED, ops.rows_loaded);
    push_compact_metric(&mut metrics, ROWS_SAVED, ops.rows_saved);
    push_compact_metric(&mut metrics, ROWS_DELETED, ops.rows_deleted);
    push_compact_metric(&mut metrics, ROWS_SCANNED, ops.rows_scanned);
    push_compact_metric(&mut metrics, ROWS_FILTERED, ops.rows_filtered);
    push_compact_metric(&mut metrics, ROWS_EMITTED, ops.rows_emitted);
    push_compact_metric(&mut metrics, SQL_INSERT_CALLS, ops.sql_insert_calls);
    push_compact_metric(
        &mut metrics,
        SQL_INSERT_SELECT_CALLS,
        ops.sql_insert_select_calls,
    );
    push_compact_metric(&mut metrics, SQL_UPDATE_CALLS, ops.sql_update_calls);
    push_compact_metric(&mut metrics, SQL_DELETE_CALLS, ops.sql_delete_calls);
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_MATCHED_ROWS,
        ops.sql_write_matched_rows,
    );
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_MUTATED_ROWS,
        ops.sql_write_mutated_rows,
    );
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_RETURNING_ROWS,
        ops.sql_write_returning_rows,
    );
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_STAGED_ROWS,
        ops.sql_write_staged_rows,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SHARED_QUERY_PLAN_HITS,
        ops.cache_shared_query_plan_hits,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SHARED_QUERY_PLAN_MISSES,
        ops.cache_shared_query_plan_misses,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SQL_COMPILED_COMMAND_HITS,
        ops.cache_sql_compiled_command_hits,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SQL_COMPILED_COMMAND_MISSES,
        ops.cache_sql_compiled_command_misses,
    );

    metrics
}

fn compact_entity_metrics(ops: &EntityCounters) -> Vec<CompactMetric> {
    use compact_metric_code::{
        CACHE_SHARED_QUERY_PLAN_HITS, CACHE_SHARED_QUERY_PLAN_MISSES,
        CACHE_SQL_COMPILED_COMMAND_HITS, CACHE_SQL_COMPILED_COMMAND_MISSES, DELETE_CALLS,
        EXEC_ABORTED, EXEC_ERRORS, EXEC_SUCCESS, LOAD_CALLS, ROWS_DELETED, ROWS_EMITTED,
        ROWS_FILTERED, ROWS_LOADED, ROWS_SAVED, ROWS_SCANNED, SAVE_CALLS, SQL_DELETE_CALLS,
        SQL_INSERT_CALLS, SQL_INSERT_SELECT_CALLS, SQL_UPDATE_CALLS, SQL_WRITE_MATCHED_ROWS,
        SQL_WRITE_MUTATED_ROWS, SQL_WRITE_RETURNING_ROWS, SQL_WRITE_STAGED_ROWS,
    };

    let mut metrics = Vec::new();
    push_compact_metric(&mut metrics, LOAD_CALLS, ops.load_calls);
    push_compact_metric(&mut metrics, SAVE_CALLS, ops.save_calls);
    push_compact_metric(&mut metrics, DELETE_CALLS, ops.delete_calls);
    push_compact_metric(&mut metrics, EXEC_SUCCESS, ops.exec_success);
    push_compact_metric(&mut metrics, EXEC_ERRORS, entity_ops_exec_errors(ops));
    push_compact_metric(&mut metrics, EXEC_ABORTED, ops.exec_aborted);
    push_compact_metric(&mut metrics, ROWS_LOADED, ops.rows_loaded);
    push_compact_metric(&mut metrics, ROWS_SAVED, ops.rows_saved);
    push_compact_metric(&mut metrics, ROWS_DELETED, ops.rows_deleted);
    push_compact_metric(&mut metrics, ROWS_SCANNED, ops.rows_scanned);
    push_compact_metric(&mut metrics, ROWS_FILTERED, ops.rows_filtered);
    push_compact_metric(&mut metrics, ROWS_EMITTED, ops.rows_emitted);
    push_compact_metric(&mut metrics, SQL_INSERT_CALLS, ops.sql_insert_calls);
    push_compact_metric(
        &mut metrics,
        SQL_INSERT_SELECT_CALLS,
        ops.sql_insert_select_calls,
    );
    push_compact_metric(&mut metrics, SQL_UPDATE_CALLS, ops.sql_update_calls);
    push_compact_metric(&mut metrics, SQL_DELETE_CALLS, ops.sql_delete_calls);
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_MATCHED_ROWS,
        ops.sql_write_matched_rows,
    );
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_MUTATED_ROWS,
        ops.sql_write_mutated_rows,
    );
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_RETURNING_ROWS,
        ops.sql_write_returning_rows,
    );
    push_compact_metric(
        &mut metrics,
        SQL_WRITE_STAGED_ROWS,
        ops.sql_write_staged_rows,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SHARED_QUERY_PLAN_HITS,
        ops.cache_shared_query_plan_hits,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SHARED_QUERY_PLAN_MISSES,
        ops.cache_shared_query_plan_misses,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SQL_COMPILED_COMMAND_HITS,
        ops.cache_sql_compiled_command_hits,
    );
    push_compact_metric(
        &mut metrics,
        CACHE_SQL_COMPILED_COMMAND_MISSES,
        ops.cache_sql_compiled_command_misses,
    );

    metrics
}

/// Build a compact metrics report gated by `window_start_ms`.
#[must_use]
pub(in crate::metrics) fn compact_report_window_start(
    window_start_ms: Option<u64>,
) -> CompactMetricsReport {
    let snap = with_state(Clone::clone);
    if let Some(requested_window_start_ms) = window_start_ms
        && requested_window_start_ms > snap.window_start_ms
    {
        return CompactMetricsReport::new(None, Vec::new(), window_start_ms, snap.window_start_ms);
    }

    let mut entity_counters: Vec<CompactEntityMetrics> = Vec::new();
    for (path, ops) in &snap.entities {
        let metrics = compact_entity_metrics(ops);
        if !metrics.is_empty() {
            entity_counters.push(CompactEntityMetrics::new(path.clone(), metrics));
        }
    }

    entity_counters.sort_by(|a, b| {
        let a_activity = compact_metrics_activity(a.metrics());
        let b_activity = compact_metrics_activity(b.metrics());
        b_activity
            .cmp(&a_activity)
            .then_with(|| a.path().cmp(b.path()))
    });

    CompactMetricsReport::new(
        Some(CompactEventCounters::new(
            compact_event_metrics(&snap.ops),
            snap.window_start_ms,
            now_millis(),
        )),
        entity_counters,
        window_start_ms,
        snap.window_start_ms,
    )
}

fn compact_metrics_activity(metrics: &[CompactMetric]) -> u64 {
    metrics
        .iter()
        .fold(0, |total, metric| total.saturating_add(metric.value()))
}
