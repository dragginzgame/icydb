//! Module: metrics::sink::instrumentation
//! Responsibility: typed instrumentation helpers and executor span lifetimes.
//! Does not own: sink override routing, event taxonomy, or metrics state mutation.
//! Boundary: convenience wrappers that emit stable `MetricsEvent` values.

#[cfg(feature = "sql")]
use super::SqlCompileRejectPhase;
use super::{
    CacheKind, CacheMissReason, CacheOutcome, ExecKind, ExecOutcome, MetricsEvent, record,
};

///
/// PathSpan
///
/// PathSpan is the structural metrics span used when execution observability
/// already resolved the target entity path at a non-generic boundary.
/// It preserves start/finish accounting for one accepted entity path.
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

/// Record that executor authority received an already-finalized prepared shape.
pub(crate) fn record_prepared_shape_already_finalized_for_path(entity_path: &'static str) {
    record(MetricsEvent::PreparedShapeAlreadyFinalized { entity_path });
}

/// Record the latest observed entry count for one cache family.
pub(crate) fn record_cache_entries(kind: CacheKind, entries: usize) {
    let entries = u64::try_from(entries).unwrap_or(u64::MAX);

    record(MetricsEvent::CacheEntries { kind, entries });
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
