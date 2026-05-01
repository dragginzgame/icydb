#[cfg(feature = "diagnostics")]
use std::cell::RefCell;

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta;

///
/// GroupedCountFoldMetrics
///
/// GroupedCountFoldMetrics aggregates one test-scoped view of the dedicated
/// grouped `COUNT(*)` fold path inside executor runtime.
/// It lets perf probes separate fold-path row ingestion, bucket lookup,
/// grouped-key insertion, and page finalization work without changing runtime
/// behavior.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedCountFoldMetrics {
    pub fold_stage_runs: u64,
    pub rows_folded: u64,
    pub borrowed_probe_rows: u64,
    pub borrowed_hash_computations: u64,
    pub owned_group_fallback_rows: u64,
    pub owned_key_materializations: u64,
    pub bucket_candidate_checks: u64,
    pub existing_group_hits: u64,
    pub new_group_inserts: u64,
    pub row_materialization_local_instructions: u64,
    pub group_lookup_local_instructions: u64,
    pub existing_group_update_local_instructions: u64,
    pub new_group_insert_local_instructions: u64,
    pub finalize_stage_runs: u64,
    pub finalized_group_count: u64,
    pub window_rows_considered: u64,
    pub having_rows_rejected: u64,
    pub resume_boundary_rows_rejected: u64,
    pub candidate_rows_qualified: u64,
    pub bounded_selection_candidates_seen: u64,
    pub bounded_selection_heap_replacements: u64,
    pub bounded_selection_rows_sorted: u64,
    pub unbounded_selection_rows_sorted: u64,
    pub page_rows_skipped_for_offset: u64,
    pub projection_rows_input: u64,
    pub page_rows_emitted: u64,
    pub cursor_construction_attempts: u64,
    pub next_cursor_emitted: u64,
}

#[cfg(feature = "diagnostics")]
std::thread_local! {
    static GROUPED_COUNT_FOLD_METRICS: RefCell<Option<GroupedCountFoldMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(feature = "diagnostics")]
fn update(update: impl FnOnce(&mut GroupedCountFoldMetrics)) {
    GROUPED_COUNT_FOLD_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

#[cfg(not(feature = "diagnostics"))]
fn update(_update: impl FnOnce(&mut GroupedCountFoldMetrics)) {}

#[cfg(feature = "diagnostics")]
pub(super) fn measure<T>(f: impl FnOnce() -> T) -> (u64, T) {
    measure_local_instruction_delta(f)
}

#[cfg(not(feature = "diagnostics"))]
pub(super) fn measure<T>(f: impl FnOnce() -> T) -> (u64, T) {
    (0, f())
}

pub(super) fn record_row_materialization(delta: u64) {
    update(|metrics| {
        metrics.row_materialization_local_instructions = metrics
            .row_materialization_local_instructions
            .saturating_add(delta);
    });
}

pub(super) fn record_lookup(delta: u64) {
    update(|metrics| {
        metrics.group_lookup_local_instructions = metrics
            .group_lookup_local_instructions
            .saturating_add(delta);
    });
}

pub(super) fn record_existing_group_hit(delta: u64) {
    update(|metrics| {
        metrics.existing_group_hits = metrics.existing_group_hits.saturating_add(1);
        metrics.existing_group_update_local_instructions = metrics
            .existing_group_update_local_instructions
            .saturating_add(delta);
    });
}

pub(super) fn record_new_group_insert(delta: u64) {
    update(|metrics| {
        metrics.new_group_inserts = metrics.new_group_inserts.saturating_add(1);
        metrics.new_group_insert_local_instructions = metrics
            .new_group_insert_local_instructions
            .saturating_add(delta);
    });
}

pub(super) fn record_rows_folded() {
    update(|metrics| {
        metrics.rows_folded = metrics.rows_folded.saturating_add(1);
    });
}

pub(super) fn record_borrowed_probe_row() {
    update(|metrics| {
        metrics.borrowed_probe_rows = metrics.borrowed_probe_rows.saturating_add(1);
    });
}

pub(super) fn record_borrowed_hash_computation() {
    update(|metrics| {
        metrics.borrowed_hash_computations = metrics.borrowed_hash_computations.saturating_add(1);
    });
}

pub(super) fn record_owned_group_fallback_row() {
    update(|metrics| {
        metrics.owned_group_fallback_rows = metrics.owned_group_fallback_rows.saturating_add(1);
    });
}

pub(super) fn record_owned_key_materialization() {
    update(|metrics| {
        metrics.owned_key_materializations = metrics.owned_key_materializations.saturating_add(1);
    });
}

pub(super) fn record_bucket_candidate_check() {
    update(|metrics| {
        metrics.bucket_candidate_checks = metrics.bucket_candidate_checks.saturating_add(1);
    });
}

pub(super) fn record_fold_stage_run() {
    update(|metrics| {
        metrics.fold_stage_runs = metrics.fold_stage_runs.saturating_add(1);
    });
}

pub(super) fn record_finalize_stage(group_count: usize) {
    update(|metrics| {
        metrics.finalize_stage_runs = metrics.finalize_stage_runs.saturating_add(1);
        metrics.finalized_group_count = u64::try_from(group_count).unwrap_or(u64::MAX);
    });
}

pub(super) fn record_projection_rows_input(row_count: usize) {
    update(|metrics| {
        metrics.projection_rows_input = metrics
            .projection_rows_input
            .saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
    });
}

pub(super) fn record_candidate_row_qualified() {
    update(|metrics| {
        metrics.candidate_rows_qualified = metrics.candidate_rows_qualified.saturating_add(1);
    });
}

pub(super) fn record_page_row_skipped_for_offset() {
    update(|metrics| {
        metrics.page_rows_skipped_for_offset =
            metrics.page_rows_skipped_for_offset.saturating_add(1);
    });
}

pub(super) fn record_page_row_emitted() {
    update(|metrics| {
        metrics.page_rows_emitted = metrics.page_rows_emitted.saturating_add(1);
    });
}

pub(super) fn record_unbounded_selection_rows_sorted(row_count: usize) {
    update(|metrics| {
        metrics.unbounded_selection_rows_sorted = metrics
            .unbounded_selection_rows_sorted
            .saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
    });
}

pub(super) fn record_window_row_considered() {
    update(|metrics| {
        metrics.window_rows_considered = metrics.window_rows_considered.saturating_add(1);
    });
}

pub(super) fn record_having_row_rejected() {
    update(|metrics| {
        metrics.having_rows_rejected = metrics.having_rows_rejected.saturating_add(1);
    });
}

pub(super) fn record_resume_boundary_row_rejected() {
    update(|metrics| {
        metrics.resume_boundary_rows_rejected =
            metrics.resume_boundary_rows_rejected.saturating_add(1);
    });
}

pub(super) fn record_bounded_selection_candidate_seen() {
    update(|metrics| {
        metrics.bounded_selection_candidates_seen =
            metrics.bounded_selection_candidates_seen.saturating_add(1);
    });
}

pub(super) fn record_bounded_selection_heap_replacement() {
    update(|metrics| {
        metrics.bounded_selection_heap_replacements = metrics
            .bounded_selection_heap_replacements
            .saturating_add(1);
    });
}

pub(super) fn record_bounded_selection_rows_sorted(row_count: usize) {
    update(|metrics| {
        metrics.bounded_selection_rows_sorted = metrics
            .bounded_selection_rows_sorted
            .saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
    });
}

pub(super) fn record_cursor_construction_attempt() {
    update(|metrics| {
        metrics.cursor_construction_attempts =
            metrics.cursor_construction_attempts.saturating_add(1);
    });
}

pub(super) fn record_next_cursor_emitted() {
    update(|metrics| {
        metrics.next_cursor_emitted = metrics.next_cursor_emitted.saturating_add(1);
    });
}

/// with_grouped_count_fold_metrics
///
/// Run one closure while collecting dedicated grouped `COUNT(*)` fold metrics
/// on the current thread, then return the closure result plus the aggregated
/// snapshot.
///

#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) fn with_grouped_count_fold_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, GroupedCountFoldMetrics) {
    GROUPED_COUNT_FOLD_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "grouped count fold metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(GroupedCountFoldMetrics::default());
    });

    let result = f();
    let metrics =
        GROUPED_COUNT_FOLD_METRICS.with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}

#[cfg(not(feature = "diagnostics"))]
#[expect(
    dead_code,
    reason = "non-diagnostics builds keep the grouped-count metrics entrypoint aligned with test and diagnostics callers"
)]
pub(in crate::db::executor) fn with_grouped_count_fold_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, GroupedCountFoldMetrics) {
    (f(), GroupedCountFoldMetrics::default())
}
