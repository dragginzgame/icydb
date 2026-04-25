//! Module: db::executor::profiling
//! Responsibility: lightweight execution-local profiling counters.
//! Does not own: diagnostics response formatting or execution routing policy.
//! Boundary: records optional per-query stats while executor operators run.

use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::db::diagnostics::ExecutionStats;

static EXECUTION_STATS_ACTIVE: AtomicBool = AtomicBool::new(false);

std::thread_local! {
    static EXECUTION_STATS: RefCell<Option<ExecutionProfileStats>> = const {
        RefCell::new(None)
    };
}

///
/// ExecutionProfileStats
///
/// ExecutionProfileStats is the executor-owned lightweight profiling snapshot for
/// one traced query execution.
/// It records operator counters and elapsed microseconds without changing
/// response payloads or execution semantics.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExecutionProfileStats {
    rows_scanned_pre_filter: u64,
    rows_after_predicate: u64,
    rows_after_projection: u64,
    rows_after_distinct: u64,
    rows_sorted: u64,
    keys_streamed: u64,
    key_stream_micros: u64,
    ordering_micros: u64,
    projection_micros: u64,
    aggregation_micros: u64,
}

impl ExecutionProfileStats {
    /// Convert executor-local profiling counters into the diagnostics DTO.
    #[must_use]
    pub(in crate::db::executor) const fn into_execution_stats(self) -> ExecutionStats {
        ExecutionStats::new(
            self.rows_scanned_pre_filter,
            self.rows_after_predicate,
            self.rows_after_projection,
            self.rows_after_distinct,
            self.rows_sorted,
            self.keys_streamed,
            self.key_stream_micros,
            self.ordering_micros,
            self.projection_micros,
            self.aggregation_micros,
        )
    }

    /// Apply final scalar outcome counters that are already produced by the
    /// execution attempt boundary.
    pub(in crate::db::executor) fn apply_scalar_outcome(
        &mut self,
        rows_scanned: usize,
        post_access_rows: usize,
        projected_rows: usize,
        distinct_keys_deduped: u64,
    ) {
        self.rows_scanned_pre_filter = usize_to_u64(rows_scanned);
        if self.rows_after_predicate == 0 {
            self.rows_after_predicate = usize_to_u64(post_access_rows);
        }
        self.rows_after_projection = usize_to_u64(projected_rows);
        if distinct_keys_deduped > 0 {
            self.rows_after_distinct = usize_to_u64(post_access_rows);
        }
    }

    /// Apply grouped output counters after grouped finalization has completed.
    pub(in crate::db::executor) fn apply_grouped_outcome(&mut self, projected_rows: usize) {
        let projected_rows = usize_to_u64(projected_rows);
        self.rows_after_projection = projected_rows;
        if self.rows_after_predicate == 0 {
            self.rows_after_predicate = projected_rows;
        }
    }
}

/// Run one closure while collecting execution stats if the caller requested it.
pub(in crate::db::executor) fn with_execution_stats_capture<T>(
    enabled: bool,
    run: impl FnOnce() -> T,
) -> (T, Option<ExecutionProfileStats>) {
    if !enabled {
        return (run(), None);
    }

    EXECUTION_STATS.with(|stats| {
        debug_assert!(
            stats.borrow().is_none(),
            "execution stats captures should not nest",
        );
        *stats.borrow_mut() = Some(ExecutionProfileStats::default());
    });
    EXECUTION_STATS_ACTIVE.store(true, Ordering::Relaxed);

    let result = run();
    EXECUTION_STATS_ACTIVE.store(false, Ordering::Relaxed);
    let stats = EXECUTION_STATS.with(|stats| stats.borrow_mut().take());

    (result, stats)
}

/// Measure one execution profiling phase in microseconds.
pub(in crate::db::executor) fn measure_execution_stats_phase<T>(
    run: impl FnOnce() -> T,
) -> (T, u64) {
    let started_at = start_operator_timer();
    let result = run();
    let elapsed_micros = elapsed_operator_micros(started_at);

    (result, elapsed_micros)
}

/// Record one yielded physical key.
pub(in crate::db::executor) fn record_key_stream_yield() {
    update_execution_stats(|stats| {
        stats.keys_streamed = stats.keys_streamed.saturating_add(1);
    });
}

/// Record elapsed key-stream polling time.
pub(in crate::db::executor) fn record_key_stream_micros(delta: u64) {
    if delta == 0 {
        return;
    }

    update_execution_stats(|stats| {
        stats.key_stream_micros = stats.key_stream_micros.saturating_add(delta);
    });
}

/// Record the row count after predicate filtering.
pub(in crate::db::executor) fn record_rows_after_predicate(rows: usize) {
    update_execution_stats(|stats| {
        stats.rows_after_predicate = usize_to_u64(rows);
    });
}

/// Record one in-memory ordering pass.
pub(in crate::db::executor) fn record_ordering(rows_sorted: usize, elapsed_micros: u64) {
    update_execution_stats(|stats| {
        stats.rows_sorted = stats.rows_sorted.saturating_add(usize_to_u64(rows_sorted));
        stats.ordering_micros = stats.ordering_micros.saturating_add(elapsed_micros);
    });
}

/// Record one projection/materialization payload finalization pass.
pub(in crate::db::executor) fn record_projection(rows_projected: usize, elapsed_micros: u64) {
    update_execution_stats(|stats| {
        stats.rows_after_projection = usize_to_u64(rows_projected);
        stats.projection_micros = stats.projection_micros.saturating_add(elapsed_micros);
    });
}

/// Record one grouped aggregation fold phase.
pub(in crate::db::executor) fn record_aggregation(elapsed_micros: u64) {
    if elapsed_micros == 0 {
        return;
    }

    update_execution_stats(|stats| {
        stats.aggregation_micros = stats.aggregation_micros.saturating_add(elapsed_micros);
    });
}

fn update_execution_stats(update: impl FnOnce(&mut ExecutionProfileStats)) {
    if !EXECUTION_STATS_ACTIVE.load(Ordering::Relaxed) {
        return;
    }

    EXECUTION_STATS.with(|stats| {
        let mut stats = stats.borrow_mut();
        let Some(stats) = stats.as_mut() else {
            return;
        };

        update(stats);
    });
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(target_arch = "wasm32")]
fn start_operator_timer() -> u64 {
    canic_cdk::utils::time::now_millis()
}

#[cfg(target_arch = "wasm32")]
fn elapsed_operator_micros(started_at_ms: u64) -> u64 {
    canic_cdk::utils::time::now_millis()
        .saturating_sub(started_at_ms)
        .saturating_mul(1_000)
}

#[cfg(not(target_arch = "wasm32"))]
fn start_operator_timer() -> std::time::Instant {
    std::time::Instant::now()
}

#[cfg(not(target_arch = "wasm32"))]
fn elapsed_operator_micros(started_at: std::time::Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX)
}
