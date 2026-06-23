#[cfg(feature = "diagnostics")]
use std::cell::Cell;
#[cfg(any(test, feature = "diagnostics"))]
use std::cell::RefCell;

#[cfg(any(test, feature = "diagnostics"))]
use super::{RetainedSlotLayout, RetainedSlotValueMode};

#[cfg(feature = "diagnostics")]
pub(super) use crate::db::diagnostics::measure_local_instruction_delta as measure_direct_data_row_phase;
#[cfg(feature = "diagnostics")]
pub(super) use crate::db::diagnostics::measure_local_instruction_delta as measure_kernel_row_phase;

///
/// ScalarMaterializationLaneMetrics
///
/// ScalarMaterializationLaneMetrics aggregates one test-scoped or
/// metrics-scoped view of which shared scalar materialization lane actually
/// executed for one workload.
/// This keeps lane attribution explicit so runtime work can be tied back to
/// the executor contract instead of inferred indirectly from instruction
/// totals alone.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), expect(unreachable_pub))]
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScalarMaterializationLaneMetrics {
    pub direct_data_row_path_hits: u64,
    pub direct_filtered_data_row_path_hits: u64,
    pub kernel_data_row_path_hits: u64,
    pub kernel_full_row_retained_path_hits: u64,
    pub kernel_slots_only_path_hits: u64,
    pub kernel_retained_layout_hits: u64,
    pub kernel_retained_slot_values: u64,
    pub kernel_retained_octet_length_values: u64,
}

///
/// DirectDataRowPhaseAttribution
///
/// DirectDataRowPhaseAttribution isolates the direct raw-row scalar lane into
/// scan-local subphases plus the later order/page windows that still matter
/// for warmed fluent perf work.
/// Non-direct executor lanes leave these counters at zero so the attribution
/// surface stays lane-local instead of pretending to describe every runtime.
///

#[cfg(feature = "diagnostics")]
#[expect(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct DirectDataRowPhaseAttribution {
    pub(in crate::db) scan_local_instructions: u64,
    pub(in crate::db) key_stream_local_instructions: u64,
    pub(in crate::db) row_read_local_instructions: u64,
    pub(in crate::db) key_encode_local_instructions: u64,
    pub(in crate::db) store_get_local_instructions: u64,
    pub(in crate::db) order_window_local_instructions: u64,
    pub(in crate::db) page_window_local_instructions: u64,
}

#[cfg(all(feature = "diagnostics", any(test, feature = "sql")))]
impl DirectDataRowPhaseAttribution {
    pub(in crate::db) const fn has_work(self) -> bool {
        self.scan_local_instructions != 0
            || self.key_stream_local_instructions != 0
            || self.row_read_local_instructions != 0
            || self.key_encode_local_instructions != 0
            || self.store_get_local_instructions != 0
            || self.order_window_local_instructions != 0
            || self.page_window_local_instructions != 0
    }
}

///
/// KernelRowPhaseAttribution
///
/// KernelRowPhaseAttribution isolates the retained/data kernel-row scalar lane
/// into scan-local subphases. Direct raw-row lanes leave these counters at zero
/// so perf tooling can distinguish the two executor families.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct KernelRowPhaseAttribution {
    pub(in crate::db) scan_local_instructions: u64,
    pub(in crate::db) key_stream_local_instructions: u64,
    pub(in crate::db) row_read_local_instructions: u64,
    pub(in crate::db) order_window_local_instructions: u64,
    pub(in crate::db) page_window_local_instructions: u64,
    pub(in crate::db) retained_layout_hits: u64,
    pub(in crate::db) retained_slot_values: u64,
    pub(in crate::db) retained_octet_length_values: u64,
}

#[cfg(feature = "diagnostics")]
impl KernelRowPhaseAttribution {
    pub(in crate::db) const fn has_work(self) -> bool {
        self.scan_local_instructions != 0
            || self.key_stream_local_instructions != 0
            || self.row_read_local_instructions != 0
            || self.order_window_local_instructions != 0
            || self.page_window_local_instructions != 0
            || self.retained_layout_hits != 0
            || self.retained_slot_values != 0
            || self.retained_octet_length_values != 0
    }
}

#[cfg(any(test, feature = "diagnostics"))]
std::thread_local! {
    static SCALAR_MATERIALIZATION_LANE_METRICS: RefCell<Option<ScalarMaterializationLaneMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(feature = "diagnostics")]
std::thread_local! {
    static DIRECT_DATA_ROW_PHASE_ATTRIBUTION: Cell<DirectDataRowPhaseAttribution> = const {
        Cell::new(DirectDataRowPhaseAttribution {
            scan_local_instructions: 0,
            key_stream_local_instructions: 0,
            row_read_local_instructions: 0,
            key_encode_local_instructions: 0,
            store_get_local_instructions: 0,
            order_window_local_instructions: 0,
            page_window_local_instructions: 0,
        })
    };
}

#[cfg(feature = "diagnostics")]
std::thread_local! {
    static KERNEL_ROW_PHASE_ATTRIBUTION: Cell<KernelRowPhaseAttribution> = const {
        Cell::new(KernelRowPhaseAttribution {
            scan_local_instructions: 0,
            key_stream_local_instructions: 0,
            row_read_local_instructions: 0,
            order_window_local_instructions: 0,
            page_window_local_instructions: 0,
            retained_layout_hits: 0,
            retained_slot_values: 0,
            retained_octet_length_values: 0,
        })
    };
}

#[cfg(any(test, feature = "diagnostics"))]
fn update_scalar_materialization_lane_metrics(
    update: impl FnOnce(&mut ScalarMaterializationLaneMetrics),
) {
    SCALAR_MATERIALIZATION_LANE_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_direct_data_row_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.direct_data_row_path_hits = metrics.direct_data_row_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_direct_filtered_data_row_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.direct_filtered_data_row_path_hits =
            metrics.direct_filtered_data_row_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_kernel_data_row_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_data_row_path_hits = metrics.kernel_data_row_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_kernel_full_row_retained_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_full_row_retained_path_hits =
            metrics.kernel_full_row_retained_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_kernel_slots_only_path_hit() {
    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_slots_only_path_hits = metrics.kernel_slots_only_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_kernel_retained_slot_layout(layout: &RetainedSlotLayout) {
    let retained_values = usize_to_u64(layout.retained_value_count());
    let octet_length_values = usize_to_u64(
        layout
            .value_modes()
            .iter()
            .filter(|mode| **mode == RetainedSlotValueMode::ScalarOctetLength)
            .count(),
    );

    update_scalar_materialization_lane_metrics(|metrics| {
        metrics.kernel_retained_layout_hits = metrics.kernel_retained_layout_hits.saturating_add(1);
        metrics.kernel_retained_slot_values = metrics
            .kernel_retained_slot_values
            .saturating_add(retained_values);
        metrics.kernel_retained_octet_length_values = metrics
            .kernel_retained_octet_length_values
            .saturating_add(octet_length_values);
    });

    #[cfg(feature = "diagnostics")]
    update_kernel_row_phase_attribution(1, |current, _| {
        current.retained_layout_hits = current.retained_layout_hits.saturating_add(1);
        current.retained_slot_values = current.retained_slot_values.saturating_add(retained_values);
        current.retained_octet_length_values = current
            .retained_octet_length_values
            .saturating_add(octet_length_values);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

///
/// with_scalar_materialization_lane_metrics
///
/// Run one closure while collecting executor-owned scalar materialization lane
/// metrics on the current thread, then return the closure result plus the
/// aggregated snapshot.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), expect(unreachable_pub))]
pub fn with_scalar_materialization_lane_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, ScalarMaterializationLaneMetrics) {
    SCALAR_MATERIALIZATION_LANE_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "scalar materialization lane metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(ScalarMaterializationLaneMetrics::default());
    });

    let result = f();
    let metrics = SCALAR_MATERIALIZATION_LANE_METRICS
        .with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_scan_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.scan_local_instructions = current.scan_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_key_stream_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.key_stream_local_instructions =
            current.key_stream_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_row_read_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.row_read_local_instructions =
            current.row_read_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_key_encode_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.key_encode_local_instructions =
            current.key_encode_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_store_get_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.store_get_local_instructions =
            current.store_get_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_order_window_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.order_window_local_instructions = current
            .order_window_local_instructions
            .saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_page_window_local_instructions(delta: u64) {
    update_direct_data_row_phase_attribution(delta, |current, delta| {
        current.page_window_local_instructions =
            current.page_window_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_kernel_row_scan_local_instructions(delta: u64) {
    update_kernel_row_phase_attribution(delta, |current, delta| {
        current.scan_local_instructions = current.scan_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_kernel_row_key_stream_local_instructions(delta: u64) {
    update_kernel_row_phase_attribution(delta, |current, delta| {
        current.key_stream_local_instructions =
            current.key_stream_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_kernel_row_row_read_local_instructions(delta: u64) {
    update_kernel_row_phase_attribution(delta, |current, delta| {
        current.row_read_local_instructions =
            current.row_read_local_instructions.saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_kernel_row_order_window_local_instructions(delta: u64) {
    update_kernel_row_phase_attribution(delta, |current, delta| {
        current.order_window_local_instructions = current
            .order_window_local_instructions
            .saturating_add(delta);
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_kernel_row_page_window_local_instructions(delta: u64) {
    update_kernel_row_phase_attribution(delta, |current, delta| {
        current.page_window_local_instructions =
            current.page_window_local_instructions.saturating_add(delta);
    });
}

// Apply one direct-row phase counter update through the shared thread-local
// capture slot so individual bucket recorders only own bucket selection.
#[cfg(feature = "diagnostics")]
fn update_direct_data_row_phase_attribution(
    delta: u64,
    update: impl FnOnce(&mut DirectDataRowPhaseAttribution, u64),
) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let mut current = attribution.get();
        update(&mut current, delta);
        attribution.set(current);
    });
}

// Apply one kernel-row phase counter update through the shared thread-local
// capture slot so individual bucket recorders only own bucket selection.
#[cfg(feature = "diagnostics")]
fn update_kernel_row_phase_attribution(
    delta: u64,
    update: impl FnOnce(&mut KernelRowPhaseAttribution, u64),
) {
    if delta == 0 {
        return;
    }

    KERNEL_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let mut current = attribution.get();
        update(&mut current, delta);
        attribution.set(current);
    });
}

#[cfg(feature = "diagnostics")]
pub(in crate::db) fn with_direct_data_row_phase_attribution<T>(
    f: impl FnOnce() -> T,
) -> (T, DirectDataRowPhaseAttribution) {
    let previous = DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let previous = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution::default());

        previous
    });

    let result = f();
    let captured = DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let captured = attribution.get();
        attribution.set(previous);

        captured
    });

    (result, captured)
}

#[cfg(feature = "diagnostics")]
pub(in crate::db) fn with_kernel_row_phase_attribution<T>(
    f: impl FnOnce() -> T,
) -> (T, KernelRowPhaseAttribution) {
    let previous = KERNEL_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let previous = attribution.get();
        attribution.set(KernelRowPhaseAttribution::default());

        previous
    });

    let result = f();
    let captured = KERNEL_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let captured = attribution.get();
        attribution.set(previous);

        captured
    });

    (result, captured)
}
