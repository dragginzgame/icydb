#[cfg(any(test, feature = "diagnostics"))]
use std::cell::RefCell;

///
/// SqlProjectionMaterializationMetrics
///
/// SqlProjectionMaterializationMetrics aggregates one test-scoped view of the
/// row-backed SQL projection path selection and fallback slot access behavior.
/// It lets perf probes distinguish retained projected rows, retained slot
/// rows, and `data_rows` fallback execution without changing runtime policy.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlProjectionMaterializationMetrics {
    pub hybrid_covering_path_hits: u64,
    pub hybrid_covering_index_field_accesses: u64,
    pub hybrid_covering_row_field_accesses: u64,
    pub projected_rows_path_hits: u64,
    pub slot_rows_path_hits: u64,
    pub data_rows_path_hits: u64,
    pub data_rows_scalar_fallback_hits: u64,
    pub data_rows_generic_fallback_hits: u64,
    pub data_rows_projected_slot_accesses: u64,
    pub data_rows_non_projected_slot_accesses: u64,
    pub full_row_decode_materializations: u64,
    pub distinct_candidate_rows: u64,
    pub distinct_bounded_stop_hits: u64,
}

#[cfg(any(test, feature = "diagnostics"))]
std::thread_local! {
    static SQL_PROJECTION_MATERIALIZATION_METRICS: RefCell<Option<SqlProjectionMaterializationMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(any(test, feature = "diagnostics"))]
fn update_sql_projection_materialization_metrics(
    update: impl FnOnce(&mut SqlProjectionMaterializationMetrics),
) {
    SQL_PROJECTION_MATERIALIZATION_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_slot_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.slot_rows_path_hits = metrics.slot_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_data_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_path_hits = metrics.data_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_sql_projection_hybrid_covering_path_hit()
 {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_path_hits = metrics.hybrid_covering_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_sql_projection_hybrid_covering_index_field_access()
 {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_index_field_accesses = metrics
            .hybrid_covering_index_field_accesses
            .saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_sql_projection_hybrid_covering_row_field_access()
 {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_row_field_accesses =
            metrics.hybrid_covering_row_field_accesses.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_data_rows_scalar_fallback_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_scalar_fallback_hits =
            metrics.data_rows_scalar_fallback_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_data_rows_slot_access(projected_slot: bool) {
    update_sql_projection_materialization_metrics(|metrics| {
        if projected_slot {
            metrics.data_rows_projected_slot_accesses =
                metrics.data_rows_projected_slot_accesses.saturating_add(1);
        } else {
            metrics.data_rows_non_projected_slot_accesses = metrics
                .data_rows_non_projected_slot_accesses
                .saturating_add(1);
        }
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_distinct_candidate_row() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.distinct_candidate_rows = metrics.distinct_candidate_rows.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_distinct_bounded_stop() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.distinct_bounded_stop_hits = metrics.distinct_bounded_stop_hits.saturating_add(1);
    });
}

///
/// with_sql_projection_materialization_metrics
///
/// Run one closure while collecting row-backed SQL projection metrics on the
/// current thread, then return the closure result plus the aggregated
/// snapshot.
///

#[cfg(feature = "diagnostics")]
pub fn with_sql_projection_materialization_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, SqlProjectionMaterializationMetrics) {
    SQL_PROJECTION_MATERIALIZATION_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "sql projection metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(SqlProjectionMaterializationMetrics::default());
    });

    let result = f();
    let metrics = SQL_PROJECTION_MATERIALIZATION_METRICS
        .with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) fn with_sql_projection_materialization_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, SqlProjectionMaterializationMetrics) {
    SQL_PROJECTION_MATERIALIZATION_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "sql projection metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(SqlProjectionMaterializationMetrics::default());
    });

    let result = f();
    let metrics = SQL_PROJECTION_MATERIALIZATION_METRICS
        .with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}
