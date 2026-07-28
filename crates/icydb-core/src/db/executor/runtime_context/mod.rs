//! Module: executor::runtime_context
//! Responsibility: executor read-path diagnostic counters.
//! Does not own: store resolution, row decoding, routing, or mutation semantics.
//! Boundary: maintained structural execution -> optional diagnostics.

#[cfg(any(test, feature = "diagnostics"))]
use std::cell::RefCell;

/// Diagnostic counters for the authoritative row-presence checks performed by
/// secondary covering reads.
#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), expect(unreachable_pub))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowCheckMetrics {
    pub index_entries_scanned: u64,
    pub index_key_owned_entries: u64,
    pub index_row_identities_decoded: u64,
    pub row_check_covering_candidates_seen: u64,
    pub row_check_rows_emitted: u64,
    pub row_presence_probe_count: u64,
    pub row_presence_probe_hits: u64,
    pub row_presence_probe_misses: u64,
    pub row_presence_key_to_raw_encodes: u64,
}

#[cfg(any(test, feature = "diagnostics"))]
std::thread_local! {
    static ROW_CHECK_METRICS: RefCell<Option<RowCheckMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(any(test, feature = "diagnostics"))]
fn update_row_check_metrics(update: impl FnOnce(&mut RowCheckMetrics)) {
    ROW_CHECK_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };
        update(metrics);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::executor) fn record_row_check_index_entry_scanned() {
    update_row_check_metrics(|metrics| {
        metrics.index_entries_scanned = metrics.index_entries_scanned.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(in crate::db::executor) const fn record_row_check_index_entry_scanned() {}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::executor) fn record_row_check_index_key_owned_entry() {
    update_row_check_metrics(|metrics| {
        metrics.index_key_owned_entries = metrics.index_key_owned_entries.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(in crate::db::executor) const fn record_row_check_index_key_owned_entry() {}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::executor) fn record_row_check_index_row_identity_decoded() {
    update_row_check_metrics(|metrics| {
        metrics.index_row_identities_decoded =
            metrics.index_row_identities_decoded.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(in crate::db::executor) const fn record_row_check_index_row_identity_decoded() {}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::executor) fn record_row_check_covering_candidate_seen() {
    update_row_check_metrics(|metrics| {
        metrics.row_check_covering_candidates_seen =
            metrics.row_check_covering_candidates_seen.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(in crate::db::executor) const fn record_row_check_covering_candidate_seen() {}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::executor) fn record_row_check_row_emitted() {
    update_row_check_metrics(|metrics| {
        metrics.row_check_rows_emitted = metrics.row_check_rows_emitted.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(in crate::db::executor) const fn record_row_check_row_emitted() {}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::executor) fn record_row_presence_probe(row_exists: bool) {
    update_row_check_metrics(|metrics| {
        metrics.row_presence_key_to_raw_encodes =
            metrics.row_presence_key_to_raw_encodes.saturating_add(1);
        metrics.row_presence_probe_count = metrics.row_presence_probe_count.saturating_add(1);
        if row_exists {
            metrics.row_presence_probe_hits = metrics.row_presence_probe_hits.saturating_add(1);
        } else {
            metrics.row_presence_probe_misses = metrics.row_presence_probe_misses.saturating_add(1);
        }
    });
}

#[cfg(not(any(test, feature = "diagnostics")))]
pub(in crate::db::executor) const fn record_row_presence_probe(_row_exists: bool) {}

/// Run a closure while collecting row-check diagnostics on the current thread.
#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(
    all(test, not(feature = "diagnostics")),
    expect(
        dead_code,
        unreachable_pub,
        reason = "capture helper is consumed only by diagnostics-shaped tests"
    )
)]
pub fn with_row_check_metrics<T>(f: impl FnOnce() -> T) -> (T, RowCheckMetrics) {
    ROW_CHECK_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "row-check metric captures must not nest"
        );
        *metrics.borrow_mut() = Some(RowCheckMetrics::default());
    });

    let result = f();
    let metrics = ROW_CHECK_METRICS.with(|metrics| metrics.borrow_mut().take().unwrap_or_default());
    (result, metrics)
}
