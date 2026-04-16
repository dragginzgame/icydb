#[cfg(feature = "diagnostics")]
use std::cell::Cell;
#[cfg(any(test, feature = "diagnostics"))]
use std::cell::RefCell;

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
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScalarMaterializationLaneMetrics {
    pub direct_data_row_path_hits: u64,
    pub direct_filtered_data_row_path_hits: u64,
    pub kernel_data_row_path_hits: u64,
    pub kernel_full_row_retained_path_hits: u64,
    pub kernel_slots_only_path_hits: u64,
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
pub(in crate::db::executor) struct DirectDataRowPhaseAttribution {
    pub(in crate::db::executor) scan_local_instructions: u64,
    pub(in crate::db::executor) key_stream_local_instructions: u64,
    pub(in crate::db::executor) row_read_local_instructions: u64,
    pub(in crate::db::executor) key_encode_local_instructions: u64,
    pub(in crate::db::executor) store_get_local_instructions: u64,
    pub(in crate::db::executor) order_window_local_instructions: u64,
    pub(in crate::db::executor) page_window_local_instructions: u64,
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

///
/// with_scalar_materialization_lane_metrics
///
/// Run one closure while collecting executor-owned scalar materialization lane
/// metrics on the current thread, then return the closure result plus the
/// aggregated snapshot.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
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
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_direct_data_row_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
pub(super) fn measure_direct_data_row_phase<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> (u64, Result<T, E>) {
    let start = read_direct_data_row_local_instruction_counter();
    let result = run();
    let delta = read_direct_data_row_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_scan_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            scan_local_instructions: current.scan_local_instructions.saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_key_stream_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            key_stream_local_instructions: current
                .key_stream_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_row_read_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            row_read_local_instructions: current.row_read_local_instructions.saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_key_encode_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            key_encode_local_instructions: current
                .key_encode_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_store_get_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            store_get_local_instructions: current
                .store_get_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_order_window_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            order_window_local_instructions: current
                .order_window_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_direct_data_row_page_window_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    DIRECT_DATA_ROW_PHASE_ATTRIBUTION.with(|attribution| {
        let current = attribution.get();
        attribution.set(DirectDataRowPhaseAttribution {
            page_window_local_instructions: current
                .page_window_local_instructions
                .saturating_add(delta),
            ..current
        });
    });
}

#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) fn with_direct_data_row_phase_attribution<T>(
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
