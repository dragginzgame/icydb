//! Module: db::executor::projection::materialize::metrics
//! Responsibility: projection materialization instrumentation callbacks.
//! Does not own: projection shape, row execution, DISTINCT, or page dispatch.
//! Boundary: keeps optional diagnostics plumbing separate from projection metadata.

///
/// ProjectionMaterializationMetricsRecorder
///
/// Executor callback bundle for structural projection materialization counters.
/// This keeps projection row shaping in executor ownership while allowing
/// adapter layers to own their diagnostic counter storage and labels.
///

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct ProjectionMaterializationMetricsRecorder {
    slot_rows_path_hit: fn(),
    data_rows_path_hit: fn(),
    data_rows_scalar_fallback_hit: fn(),
    data_rows_slot_access: fn(bool),
    distinct_candidate_row: fn(),
    distinct_bounded_stop: fn(),
}

#[cfg(any(test, feature = "diagnostics"))]
impl ProjectionMaterializationMetricsRecorder {
    /// Construct one observer from adapter-owned materialization counters.
    pub(in crate::db) const fn new(
        slot_rows_path_hit: fn(),
        data_rows_path_hit: fn(),
        data_rows_scalar_fallback_hit: fn(),
        data_rows_slot_access: fn(bool),
        distinct_candidate_row: fn(),
        distinct_bounded_stop: fn(),
    ) -> Self {
        Self {
            slot_rows_path_hit,
            data_rows_path_hit,
            data_rows_scalar_fallback_hit,
            data_rows_slot_access,
            distinct_candidate_row,
            distinct_bounded_stop,
        }
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
        (self.distinct_candidate_row)();
    }

    pub(super) fn record_distinct_bounded_stop(self) {
        (self.distinct_bounded_stop)();
    }
}

///
/// ProjectionMaterializationMetricsRecorder
///
/// Zero-sized no-op recorder used when materialization diagnostics are not
/// compiled. Keeping the type available avoids cfg-heavy executor signatures.
///

#[cfg(not(any(test, feature = "diagnostics")))]
#[derive(Clone, Copy)]
pub(in crate::db) struct ProjectionMaterializationMetricsRecorder;

#[cfg(not(any(test, feature = "diagnostics")))]
impl ProjectionMaterializationMetricsRecorder {
    /// Construct one no-op structural projection materialization observer.
    pub(in crate::db) const fn new() -> Self {
        Self
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
}
