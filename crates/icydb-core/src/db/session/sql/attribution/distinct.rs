//! SQL projected-DISTINCT diagnostics DTOs.
//! Does not own: DISTINCT execution, budgeting, or strategy selection.

use crate::db::session::sql::projection::SqlProjectionMaterializationMetrics;
use candid::CandidType;
use serde::Deserialize;

/// Query-scoped projected-row DISTINCT work and retained-state evidence.
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlDistinctProjectionAttribution {
    /// Projected candidate rows examined by DISTINCT.
    pub candidate_rows: u64,
    /// Canonically unique projected rows discovered.
    pub unique_rows: u64,
    /// Executions using contiguous-key adjacent DISTINCT.
    pub adjacent_path_hits: u64,
    /// Executions using a complete global DISTINCT build.
    pub global_path_hits: u64,
    /// Executions that proved another output beyond the requested window.
    pub bounded_stop_hits: u64,
    /// Maximum canonical DISTINCT entries retained concurrently.
    pub peak_retained_entries: u64,
    /// Maximum complete owned backing bytes retained concurrently.
    pub peak_retained_backing_bytes: u64,
}

impl SqlDistinctProjectionAttribution {
    pub(in crate::db::session::sql) const fn from_projection_metrics(
        metrics: SqlProjectionMaterializationMetrics,
    ) -> Option<Self> {
        if metrics.distinct_adjacent_path_hits == 0 && metrics.distinct_global_path_hits == 0 {
            return None;
        }

        Some(Self {
            candidate_rows: metrics.distinct_candidate_rows,
            unique_rows: metrics.distinct_unique_rows,
            adjacent_path_hits: metrics.distinct_adjacent_path_hits,
            global_path_hits: metrics.distinct_global_path_hits,
            bounded_stop_hits: metrics.distinct_bounded_stop_hits,
            peak_retained_entries: metrics.distinct_peak_retained_entries,
            peak_retained_backing_bytes: metrics.distinct_peak_retained_backing_bytes,
        })
    }
}
