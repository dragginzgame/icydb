use crate::{
    db::{
        data::{DataRow, DecodedDataStoreKey},
        executor::{
            BoundedOrderWindow, OrderedKeyStreamBox, PendingOrderRows, ScalarContinuationContext,
            exact_output_key_count_hint, key_stream_budget_is_redundant,
            measure_execution_stats_phase, record_key_stream_micros, record_key_stream_yield,
            route::LoadOrderRouteMode,
            terminal::page::{
                KernelRow, KernelRowOrderWindow, KernelRowScanStrategy, RetainedSlotLayout,
                ScalarRowRuntimeHandle,
            },
        },
        predicate::MissingRowPolicy,
        query::plan::EffectiveRuntimeFilterProgram,
    },
    error::InternalError,
};

#[cfg(feature = "diagnostics")]
use super::metrics::{
    measure_direct_data_row_phase, record_direct_data_row_key_stream_local_instructions,
    record_direct_data_row_row_read_local_instructions,
};
#[cfg(feature = "diagnostics")]
use super::metrics::{
    measure_kernel_row_phase, record_kernel_row_key_stream_local_instructions,
    record_kernel_row_peak_retained_candidates, record_kernel_row_row_read_local_instructions,
    record_kernel_row_scan_local_instructions,
};
#[cfg(any(test, feature = "diagnostics"))]
use super::metrics::{
    record_kernel_data_row_path_hit, record_kernel_full_row_retained_path_hit,
    record_kernel_retained_slot_layout, record_kernel_slots_only_path_hit,
};

///
/// DirectDataRowScanResult
///
/// DirectDataRowScanResult carries the rows retained by the direct raw-row
/// lane plus the accounting needed by the scalar materialization boundary.
/// The retained rows may already have had the cursorless page offset applied,
/// while `rows_matched` preserves the pre-page matched-row count.
///

pub(super) struct DirectDataRowScanResult {
    pub(super) rows: Vec<DataRow>,
    pub(super) rows_scanned: usize,
    pub(super) rows_matched: usize,
}

///
/// RowScanResult
///
/// RowScanResult is the shared scan collector payload for scalar row lanes.
/// The concrete row type is lane-owned; scan accounting and skip/keep
/// retention are identical for kernel rows and direct data rows.
///

struct RowScanResult<T> {
    rows: Vec<T>,
    rows_scanned: usize,
    rows_matched: usize,
}

#[derive(Clone, Copy)]
struct KernelRowScanBounds<'a> {
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    order_window: Option<KernelRowOrderWindow<'a>>,
}

impl<'a> KernelRowScanBounds<'a> {
    const fn new(
        row_keep_cap: Option<usize>,
        row_skip_count: usize,
        order_window: Option<KernelRowOrderWindow<'a>>,
    ) -> Self {
        Self {
            row_keep_cap,
            row_skip_count,
            order_window,
        }
    }
}

// Shared scalar load page-kernel orchestration boundary.
// Typed wrappers provide scan/decode callbacks so this loop can remain
// non-generic while preserving fail-closed continuation invariants.
pub(super) struct ScalarPageKernelRequest<'a, 'r> {
    pub(super) key_stream: &'a mut OrderedKeyStreamBox,
    pub(super) scan_budget_hint: Option<usize>,
    pub(super) row_keep_cap: Option<usize>,
    pub(super) order_window: Option<KernelRowOrderWindow<'a>>,
    pub(super) load_order_route_mode: LoadOrderRouteMode,
    pub(super) consistency: MissingRowPolicy,
    pub(super) scan_strategy: KernelRowScanStrategy<'a>,
    pub(super) continuation: &'a ScalarContinuationContext,
    pub(super) row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

///
/// KernelRowScanRequest
///
/// KernelRowScanRequest is the canonical executor-owned row scan contract for
/// structural key-stream materialization.
/// Both the generic scalar-page path and the row-collector short path
/// select one payload kernel through this boundary instead of duplicating the
/// same payload-mode dispatch locally.
///

pub(in crate::db::executor) struct KernelRowScanRequest<'a, 'r> {
    pub(in crate::db::executor) key_stream: &'a mut OrderedKeyStreamBox,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) scan_strategy: KernelRowScanStrategy<'a>,
    pub(in crate::db::executor) row_keep_cap: Option<usize>,
    pub(in crate::db::executor) row_skip_count: usize,
    pub(in crate::db::executor) order_window: Option<KernelRowOrderWindow<'a>>,
    pub(in crate::db::executor) row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

pub(in crate::db::executor) fn execute_kernel_row_scan(
    request: KernelRowScanRequest<'_, '_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    #[cfg(feature = "diagnostics")]
    {
        let (scan_local_instructions, result) =
            measure_kernel_row_phase(|| execute_kernel_row_scan_inner(request));
        record_kernel_row_scan_local_instructions(scan_local_instructions);
        let result = result?;
        record_kernel_row_peak_retained_candidates(result.0.retained_count());

        Ok(result)
    }

    #[cfg(not(feature = "diagnostics"))]
    execute_kernel_row_scan_inner(request)
}

#[expect(clippy::too_many_lines)]
fn execute_kernel_row_scan_inner(
    request: KernelRowScanRequest<'_, '_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    let KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        scan_strategy,
        row_keep_cap,
        row_skip_count,
        order_window,
        row_runtime,
    } = request;
    let scan_bounds = KernelRowScanBounds::new(row_keep_cap, row_skip_count, order_window);

    // Phase 1: select the concrete row-read kernel once so the inner scan
    // loop does not branch on payload shape or predicate mode per row.
    match scan_strategy {
        KernelRowScanStrategy::DataRows => {
            #[cfg(any(test, feature = "diagnostics"))]
            record_kernel_data_row_path_hit();

            execute_scalar_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_data_rows_only_into_kernel(key_stream, consistency, scan_bounds, row_runtime)
            })
        }
        KernelRowScanStrategy::DataRowsFiltered { filter_program } => {
            #[cfg(any(test, feature = "diagnostics"))]
            record_kernel_data_row_path_hit();

            execute_scalar_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_data_rows_only_into_kernel_with_filter_program(
                    key_stream,
                    consistency,
                    filter_program,
                    scan_bounds,
                    row_runtime,
                )
            })
        }
        KernelRowScanStrategy::RetainedFullRows {
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                record_kernel_full_row_retained_path_hit();
                record_kernel_retained_slot_layout(retained_slot_layout);
            }

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                |key_stream, retained_slot_layout| {
                    scan_full_retained_rows_into_kernel(
                        key_stream,
                        consistency,
                        retained_slot_layout,
                        scan_bounds,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::RetainedFullRowsFiltered {
            filter_program,
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                record_kernel_full_row_retained_path_hit();
                record_kernel_retained_slot_layout(retained_slot_layout);
            }

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                |key_stream, retained_slot_layout| {
                    scan_full_retained_rows_into_kernel_with_filter_program(
                        key_stream,
                        consistency,
                        filter_program,
                        retained_slot_layout,
                        scan_bounds,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::SlotOnlyRows {
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                record_kernel_slots_only_path_hit();
                record_kernel_retained_slot_layout(retained_slot_layout);
            }

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                |key_stream, retained_slot_layout| {
                    scan_slot_rows_into_kernel(
                        key_stream,
                        consistency,
                        retained_slot_layout,
                        scan_bounds,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::SlotOnlyRowsFiltered {
            filter_program,
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                record_kernel_slots_only_path_hit();
                record_kernel_retained_slot_layout(retained_slot_layout);
            }

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                |key_stream, retained_slot_layout| {
                    scan_slot_rows_into_kernel_with_filter_program(
                        key_stream,
                        consistency,
                        filter_program,
                        retained_slot_layout,
                        scan_bounds,
                        row_runtime,
                    )
                },
            )
        }
    }
}

// Require one retained-slot layout and run the shared scalar read loop over
// one retained-row scan closure. Full-row-retained and slot-only kernel lanes
// both use this shell, so retained-layout enforcement lives in one place.
fn execute_retained_kernel_scan(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
    mut scan_rows: impl FnMut(
        &mut OrderedKeyStreamBox,
        &RetainedSlotLayout,
    ) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    let retained_slot_layout =
        retained_slot_layout.ok_or_else(InternalError::query_executor_invariant)?;

    execute_scalar_read_loop(key_stream, scan_budget_hint, |key_stream| {
        scan_rows(key_stream, retained_slot_layout)
    })
}

pub(super) fn execute_scalar_page_kernel_dyn(
    request: ScalarPageKernelRequest<'_, '_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    let ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        row_keep_cap,
        order_window,
        load_order_route_mode,
        consistency,
        scan_strategy,
        continuation,
        row_runtime,
    } = request;

    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_mode)?;

    execute_kernel_row_scan(KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        scan_strategy,
        row_keep_cap,
        row_skip_count: 0,
        order_window,
        row_runtime,
    })
}

// Run one scalar read loop with one optional scan budget without re-checking
// the same budget logic inside each specialized row reader.
fn execute_scalar_read_loop<T>(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    mut scan_rows: impl FnMut(&mut OrderedKeyStreamBox) -> Result<T, InternalError>,
) -> Result<T, InternalError> {
    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let inner = std::mem::replace(key_stream, OrderedKeyStreamBox::empty());
        *key_stream = OrderedKeyStreamBox::budgeted(inner, scan_budget);

        return scan_rows(key_stream);
    }

    scan_rows(key_stream)
}

// Scan one ordered key stream into kernel rows through one caller-selected
// row reader while preserving the shared scanned-key accounting contract.
fn scan_kernel_rows_with(
    key_stream: &mut OrderedKeyStreamBox,
    bounds: KernelRowScanBounds<'_>,
    mut read_row: impl FnMut(DecodedDataStoreKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    if let Some(order_window) = bounds.order_window {
        return scan_kernel_rows_with_bounded_order_window(
            key_stream,
            bounds,
            order_window,
            read_row,
        );
    }

    let result = scan_rows_with(
        key_stream,
        bounds.row_keep_cap,
        bounds.row_skip_count,
        next_kernel_scan_key,
        |key| read_kernel_scan_row(key, &mut read_row),
    )?;

    Ok((PendingOrderRows::plain(result.rows), result.rows_scanned))
}

fn scan_kernel_rows_with_bounded_order_window(
    key_stream: &mut OrderedKeyStreamBox,
    bounds: KernelRowScanBounds<'_>,
    order_window: KernelRowOrderWindow<'_>,
    mut read_row: impl FnMut(DecodedDataStoreKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    if bounds.row_keep_cap.is_some() || bounds.row_skip_count != 0 {
        return Err(InternalError::query_executor_invariant());
    }

    let mut rows_scanned = 0usize;
    let mut window = BoundedOrderWindow::new(order_window.keep_count, order_window.resolved_order);

    while let Some(key) = next_kernel_scan_key(key_stream)? {
        record_key_stream_yield();

        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = read_kernel_scan_row(key, &mut read_row)? else {
            continue;
        };
        if !row.has_materialized_slots() {
            return Err(InternalError::query_executor_invariant());
        }

        window.push(row);
    }

    Ok((window.into_pending_rows(), rows_scanned))
}

// Scan one ordered key stream into caller-owned row payloads while preserving
// the shared matched-row skip/keep accounting contract.
fn scan_rows_with<T>(
    key_stream: &mut OrderedKeyStreamBox,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    mut next_key: impl FnMut(
        &mut OrderedKeyStreamBox,
    ) -> Result<Option<DecodedDataStoreKey>, InternalError>,
    mut read_row: impl FnMut(DecodedDataStoreKey) -> Result<Option<T>, InternalError>,
) -> Result<RowScanResult<T>, InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = staged_row_capacity(key_stream, row_keep_cap, row_skip_count);
    let mut rows = Vec::with_capacity(staged_capacity);
    let mut rows_matched = 0usize;
    let Some(row_keep_cap) = row_keep_cap else {
        while let Some(key) = next_key(key_stream)? {
            record_key_stream_yield();

            rows_scanned = rows_scanned.saturating_add(1);
            let Some(row) = read_row(key)? else {
                continue;
            };
            retain_scanned_row(row, row_skip_count, rows_matched, &mut rows);
            rows_matched = rows_matched.saturating_add(1);
        }

        return Ok(RowScanResult {
            rows,
            rows_scanned,
            rows_matched,
        });
    };

    while let Some(key) = next_key(key_stream)? {
        record_key_stream_yield();

        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = read_row(key)? else {
            continue;
        };
        retain_scanned_row(row, row_skip_count, rows_matched, &mut rows);
        rows_matched = rows_matched.saturating_add(1);
        if rows_matched >= row_keep_cap {
            break;
        }
    }

    Ok(RowScanResult {
        rows,
        rows_scanned,
        rows_matched,
    })
}

fn next_kernel_scan_key(
    key_stream: &mut OrderedKeyStreamBox,
) -> Result<Option<DecodedDataStoreKey>, InternalError> {
    #[cfg(feature = "diagnostics")]
    let ((key_stream_local_instructions, next_key), key_stream_micros) =
        measure_execution_stats_phase(|| measure_kernel_row_phase(|| key_stream.next_key()));
    #[cfg(not(feature = "diagnostics"))]
    let (next_key, key_stream_micros) = measure_execution_stats_phase(|| key_stream.next_key());
    record_key_stream_micros(key_stream_micros);
    #[cfg(feature = "diagnostics")]
    record_kernel_row_key_stream_local_instructions(key_stream_local_instructions);

    next_key
}

fn read_kernel_scan_row(
    key: DecodedDataStoreKey,
    read_row: &mut impl FnMut(DecodedDataStoreKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<Option<KernelRow>, InternalError> {
    #[cfg(feature = "diagnostics")]
    let (row_read_local_instructions, row) = measure_kernel_row_phase(|| read_row(key));
    #[cfg(not(feature = "diagnostics"))]
    let row = read_row(key);
    #[cfg(feature = "diagnostics")]
    record_kernel_row_row_read_local_instructions(row_read_local_instructions);

    row
}

// Compute the staged row capacity after the caller has converted a cursorless
// page offset into a scan-time skip.
fn staged_row_capacity(
    key_stream: &OrderedKeyStreamBox,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
) -> usize {
    row_keep_cap
        .map(|row_keep_cap| row_keep_cap.saturating_sub(row_skip_count))
        .or_else(|| {
            exact_output_key_count_hint(key_stream, row_keep_cap)
                .map(|hint| hint.saturating_sub(row_skip_count))
        })
        .unwrap_or(0)
}

// Retain one matched scan row only after the caller-owned cursorless page
// offset has been satisfied.
fn retain_scanned_row<T>(row: T, row_skip_count: usize, rows_matched: usize, rows: &mut Vec<T>) {
    if rows_matched >= row_skip_count {
        rows.push(row);
    }
}

// Scan one ordered key stream directly into canonical data rows when the
// caller already proved no later phase needs a kernel-row wrapper.
fn scan_data_rows_direct(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<DirectDataRowScanResult, InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, row_skip_count, |key| {
        row_runtime.read_data_row(consistency, key)
    })
}

// Scan one ordered key stream directly into canonical data rows through one
// caller-selected row reader while preserving shared direct-lane key-stream
// and row-read attribution.
fn scan_data_rows_direct_with_reader(
    key_stream: &mut OrderedKeyStreamBox,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    mut read_data_row: impl FnMut(DecodedDataStoreKey) -> Result<Option<DataRow>, InternalError>,
) -> Result<DirectDataRowScanResult, InternalError> {
    let result = scan_rows_with(
        key_stream,
        row_keep_cap,
        row_skip_count,
        next_direct_data_row_scan_key,
        |key| read_direct_data_row_scan_row(key, &mut read_data_row),
    )?;

    Ok(DirectDataRowScanResult {
        rows: result.rows,
        rows_scanned: result.rows_scanned,
        rows_matched: result.rows_matched,
    })
}

fn next_direct_data_row_scan_key(
    key_stream: &mut OrderedKeyStreamBox,
) -> Result<Option<DecodedDataStoreKey>, InternalError> {
    #[cfg(feature = "diagnostics")]
    let ((key_stream_local_instructions, read_result), key_stream_micros) =
        measure_execution_stats_phase(|| measure_direct_data_row_phase(|| key_stream.next_key()));
    #[cfg(not(feature = "diagnostics"))]
    let (read_result, key_stream_micros) = measure_execution_stats_phase(|| key_stream.next_key());
    record_key_stream_micros(key_stream_micros);
    #[cfg(feature = "diagnostics")]
    record_direct_data_row_key_stream_local_instructions(key_stream_local_instructions);

    read_result
}

fn read_direct_data_row_scan_row(
    key: DecodedDataStoreKey,
    read_data_row: &mut impl FnMut(DecodedDataStoreKey) -> Result<Option<DataRow>, InternalError>,
) -> Result<Option<DataRow>, InternalError> {
    #[cfg(feature = "diagnostics")]
    let (row_read_local_instructions, row_read_result) =
        measure_direct_data_row_phase(|| read_data_row(key));
    #[cfg(not(feature = "diagnostics"))]
    let row_read_result = read_data_row(key);
    #[cfg(feature = "diagnostics")]
    record_direct_data_row_row_read_local_instructions(row_read_local_instructions);

    row_read_result
}

// Scan one ordered key stream directly into canonical data rows while
// applying the residual predicate against each opened raw row.
fn scan_data_rows_direct_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    filter_program: &EffectiveRuntimeFilterProgram,
) -> Result<DirectDataRowScanResult, InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, row_skip_count, |key| {
        row_runtime.read_data_row_with_filter_program(consistency, key, filter_program)
    })
}

// Run the materialized-order raw data-row lane through one residual-predicate
// policy helper so perf-attributed and normal scans share the same scan-time
// filtering contract.
pub(super) fn scan_materialized_order_direct_data_rows(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    consistency: MissingRowPolicy,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
) -> Result<DirectDataRowScanResult, InternalError> {
    scan_direct_data_rows_with_residual_policy(
        key_stream,
        scan_budget_hint,
        None,
        0,
        consistency,
        row_runtime,
        residual_filter_program,
    )
}

// Run one direct data-row scan through the shared residual-filter contract so
// plain, filtered, and materialized-order raw lanes all choose the same row
// reader in one place.
pub(super) fn scan_direct_data_rows_with_residual_policy(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    consistency: MissingRowPolicy,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
) -> Result<DirectDataRowScanResult, InternalError> {
    if row_keep_cap == Some(0) {
        return Ok(DirectDataRowScanResult {
            rows: Vec::new(),
            rows_scanned: 0,
            rows_matched: 0,
        });
    }

    execute_scalar_read_loop(key_stream, scan_budget_hint, |key_stream| {
        match residual_filter_program {
            None => scan_data_rows_direct(
                key_stream,
                consistency,
                row_keep_cap,
                row_skip_count,
                row_runtime,
            ),
            Some(filter_program) => scan_data_rows_direct_with_filter_program(
                key_stream,
                consistency,
                row_keep_cap,
                row_skip_count,
                row_runtime,
                filter_program,
            ),
        }
    })
}

fn scan_data_rows_only_into_kernel(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    bounds: KernelRowScanBounds<'_>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, bounds, |key| {
        row_runtime.read_data_row_only(consistency, key)
    })
}

// Scan keys into data-row-only kernel rows while applying the canonical
// residual filter directly against each opened raw row.
fn scan_data_rows_only_into_kernel_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    bounds: KernelRowScanBounds<'_>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, bounds, |key| {
        row_runtime
            .read_data_row_with_filter_program(consistency, key, filter_program)
            .map(|row| row.map(KernelRow::new_data_row_only))
    })
}

// Scan keys into full structural rows while retaining only the caller-declared
// shared slot subset needed by later executor phases.
fn scan_full_retained_rows_into_kernel(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    bounds: KernelRowScanBounds<'_>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_full_retained_rows_into_kernel_with_reader(key_stream, bounds, |key| {
        row_runtime.read_full_row_retained(consistency, key, retained_slot_layout)
    })
}

// Scan keys into full retained structural rows through one caller-selected
// row reader while preserving the shared kernel-row scan envelope.
fn scan_full_retained_rows_into_kernel_with_reader(
    key_stream: &mut OrderedKeyStreamBox,
    bounds: KernelRowScanBounds<'_>,
    read_row: impl FnMut(DecodedDataStoreKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, bounds, read_row)
}

// Scan keys into retained full structural rows while applying the residual
// predicate before rows enter shared post-access processing.
fn scan_full_retained_rows_into_kernel_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    bounds: KernelRowScanBounds<'_>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_full_retained_rows_into_kernel_with_reader(key_stream, bounds, |key| {
        row_runtime.read_full_row_retained_with_filter_program(
            consistency,
            key,
            filter_program,
            retained_slot_layout,
        )
    })
}

// Scan keys into compact slot-only rows when the final lane never needs a
// full `DataRow` payload.
fn scan_slot_rows_into_kernel(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    bounds: KernelRowScanBounds<'_>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_slot_rows_into_kernel_with_reader(key_stream, bounds, |key| {
        row_runtime.read_slot_only(consistency, &key, retained_slot_layout)
    })
}

// Scan keys into compact slot-only rows through one caller-selected row
// reader while preserving the shared kernel-row scan envelope.
fn scan_slot_rows_into_kernel_with_reader(
    key_stream: &mut OrderedKeyStreamBox,
    bounds: KernelRowScanBounds<'_>,
    read_row: impl FnMut(DecodedDataStoreKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, bounds, read_row)
}

// Scan keys into compact slot-only rows while applying the residual predicate
// before rows enter shared post-access processing.
fn scan_slot_rows_into_kernel_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    bounds: KernelRowScanBounds<'_>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(PendingOrderRows<KernelRow>, usize), InternalError> {
    scan_slot_rows_into_kernel_with_reader(key_stream, bounds, |key| {
        row_runtime.read_slot_only_with_filter_program(
            consistency,
            &key,
            filter_program,
            retained_slot_layout,
        )
    })
}
