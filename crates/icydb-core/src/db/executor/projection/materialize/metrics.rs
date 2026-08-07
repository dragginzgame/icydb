//! Module: db::executor::projection::materialize::metrics
//! Responsibility: projection materialization instrumentation callbacks.
//! Does not own: projection shape, row execution, DISTINCT, or page dispatch.
//! Boundary: keeps optional diagnostics plumbing separate from projection metadata.

///
/// ProjectionMaterializationMetricsRecorder
///
/// Executor callback bundle for structural projection materialization counters.
/// This keeps projection row shaping in executor ownership while allowing
/// the SQL diagnostics adapter to own its counter storage and labels.
///

#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct ProjectionMaterializationMetricsRecorder {
    slot_rows_path_hit: fn(),
    data_rows_path_hit: fn(),
    data_rows_scalar_fallback_hit: fn(),
    data_rows_slot_access: fn(bool),
    distinct: DistinctProjectionMetricsRecorder,
}

/// Query-scoped callback bundle for projected-row DISTINCT diagnostics.
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct DistinctProjectionMetricsRecorder {
    candidate_row: fn(),
    bounded_stop: fn(),
    adjacent_path_hit: fn(),
    global_path_hit: fn(),
    unique_rows: fn(u64),
    peak_retained_entries: fn(u64),
    peak_retained_backing_bytes: fn(u64),
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
const fn ignore_projection_event() {}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
const fn ignore_projection_slot_event(_projected_slot: bool) {}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
const fn ignore_projection_count_event(_count: u64) {}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
impl DistinctProjectionMetricsRecorder {
    /// Construct one observer from adapter-owned DISTINCT counters.
    pub(in crate::db) const fn new(
        candidate_row: fn(),
        bounded_stop: fn(),
        adjacent_path_hit: fn(),
        global_path_hit: fn(),
        unique_rows: fn(u64),
        peak_retained_entries: fn(u64),
        peak_retained_backing_bytes: fn(u64),
    ) -> Self {
        Self {
            candidate_row,
            bounded_stop,
            adjacent_path_hit,
            global_path_hit,
            unique_rows,
            peak_retained_entries,
            peak_retained_backing_bytes,
        }
    }

    const fn none() -> Self {
        Self::new(
            ignore_projection_event,
            ignore_projection_event,
            ignore_projection_event,
            ignore_projection_event,
            ignore_projection_count_event,
            ignore_projection_count_event,
            ignore_projection_count_event,
        )
    }
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
impl ProjectionMaterializationMetricsRecorder {
    /// Construct one observer from adapter-owned materialization counters.
    pub(in crate::db) const fn new(
        slot_rows_path_hit: fn(),
        data_rows_path_hit: fn(),
        data_rows_scalar_fallback_hit: fn(),
        data_rows_slot_access: fn(bool),
        distinct: DistinctProjectionMetricsRecorder,
    ) -> Self {
        Self {
            slot_rows_path_hit,
            data_rows_path_hit,
            data_rows_scalar_fallback_hit,
            data_rows_slot_access,
            distinct,
        }
    }

    /// Construct one observer that intentionally records no adapter metrics.
    pub(in crate::db) const fn none() -> Self {
        Self::new(
            ignore_projection_event,
            ignore_projection_event,
            ignore_projection_event,
            ignore_projection_slot_event,
            DistinctProjectionMetricsRecorder::none(),
        )
    }

    pub(super) fn record_slot_rows_path_hit(self) {
        (self.slot_rows_path_hit)();
    }

    pub(super) fn record_data_rows_path_hit(self) {
        (self.data_rows_path_hit)();
    }

    pub(super) fn record_data_rows_scalar_fallback_hit(self) {
        (self.data_rows_scalar_fallback_hit)();
    }

    pub(super) fn record_data_rows_slot_access(self, projected_slot: bool) {
        (self.data_rows_slot_access)(projected_slot);
    }

    pub(super) fn record_distinct_candidate_row(self) {
        (self.distinct.candidate_row)();
    }

    pub(super) fn record_distinct_bounded_stop(self) {
        (self.distinct.bounded_stop)();
    }

    pub(super) fn record_distinct_adjacent_path_hit(self) {
        (self.distinct.adjacent_path_hit)();
    }

    pub(super) fn record_distinct_global_path_hit(self) {
        (self.distinct.global_path_hit)();
    }

    pub(super) fn record_distinct_unique_rows(self, count: u64) {
        (self.distinct.unique_rows)(count);
    }

    pub(super) fn record_distinct_peak_retained_entries(self, count: u64) {
        (self.distinct.peak_retained_entries)(count);
    }

    pub(super) fn record_distinct_peak_retained_backing_bytes(self, bytes: u64) {
        (self.distinct.peak_retained_backing_bytes)(bytes);
    }
}

///
/// ProjectionMaterializationMetricsRecorder
///
/// Zero-sized no-op recorder used when SQL materialization diagnostics are not
/// compiled. Keeping the type available avoids cfg-heavy executor signatures.
///

#[cfg(not(all(feature = "sql", feature = "diagnostics")))]
#[derive(Clone, Copy)]
pub(in crate::db) struct ProjectionMaterializationMetricsRecorder;

#[cfg(not(all(feature = "sql", feature = "diagnostics")))]
impl ProjectionMaterializationMetricsRecorder {
    /// Construct one no-op structural projection materialization observer.
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    pub(in crate::db) const fn none() -> Self {
        Self::new()
    }

    pub(super) const fn record_slot_rows_path_hit(self) {
        let _ = self;
    }

    pub(super) const fn record_data_rows_path_hit(self) {
        let _ = self;
    }

    pub(super) const fn record_data_rows_scalar_fallback_hit(self) {
        let _ = self;
    }

    pub(super) const fn record_data_rows_slot_access(self, projected_slot: bool) {
        let _ = (self, projected_slot);
    }

    pub(super) const fn record_distinct_candidate_row(self) {
        let _ = self;
    }

    pub(super) const fn record_distinct_bounded_stop(self) {
        let _ = self;
    }

    pub(super) const fn record_distinct_adjacent_path_hit(self) {
        let _ = self;
    }

    pub(super) const fn record_distinct_global_path_hit(self) {
        let _ = self;
    }

    pub(super) const fn record_distinct_unique_rows(self, count: u64) {
        let _ = (self, count);
    }

    pub(super) const fn record_distinct_peak_retained_entries(self, count: u64) {
        let _ = (self, count);
    }

    pub(super) const fn record_distinct_peak_retained_backing_bytes(self, bytes: u64) {
        let _ = (self, bytes);
    }
}
