use crate::{
    db::{
        data::{DataKey, DataRow},
        executor::{
            OrderedKeyStreamBox, ScalarContinuationContext, exact_output_key_count_hint,
            key_stream_budget_is_redundant, measure_execution_stats_phase,
            record_key_stream_micros, record_key_stream_yield,
            route::LoadOrderRouteContract,
            terminal::page::{
                KernelRow, KernelRowScanStrategy, ResidualFilterScanMode, RetainedSlotLayout,
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
#[cfg(any(test, feature = "diagnostics"))]
use super::metrics::{
    record_kernel_data_row_path_hit, record_kernel_full_row_retained_path_hit,
    record_kernel_slots_only_path_hit,
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

// Shared scalar load page-kernel orchestration boundary.
// Typed wrappers provide scan/decode callbacks so this loop can remain
// non-generic while preserving fail-closed continuation invariants.
pub(super) struct ScalarPageKernelRequest<'a, 'r> {
    pub(super) key_stream: &'a mut OrderedKeyStreamBox,
    pub(super) scan_budget_hint: Option<usize>,
    pub(super) load_order_route_contract: LoadOrderRouteContract,
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
    pub(in crate::db::executor) row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

#[expect(clippy::too_many_lines)]
pub(in crate::db::executor) fn execute_kernel_row_scan(
    request: KernelRowScanRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        scan_strategy,
        row_keep_cap,
        row_skip_count,
        row_runtime,
    } = request;

    // Phase 1: select the concrete row-read kernel once so the inner scan
    // loop does not branch on payload shape or predicate mode per row.
    match scan_strategy {
        KernelRowScanStrategy::DataRows => {
            #[cfg(any(test, feature = "diagnostics"))]
            record_kernel_data_row_path_hit();

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_data_rows_only_into_kernel(
                    key_stream,
                    consistency,
                    row_keep_cap,
                    row_skip_count,
                    row_runtime,
                )
            })
        }
        KernelRowScanStrategy::RetainedFullRows {
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "diagnostics"))]
            record_kernel_full_row_retained_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "retained full-row kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_full_retained_rows_into_kernel(
                        key_stream,
                        consistency,
                        retained_slot_layout,
                        row_keep_cap,
                        row_skip_count,
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
            record_kernel_full_row_retained_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "retained full-row kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_full_retained_rows_into_kernel_with_filter_program(
                        key_stream,
                        consistency,
                        filter_program,
                        retained_slot_layout,
                        row_keep_cap,
                        row_skip_count,
                        row_runtime,
                    )
                },
            )
        }
        KernelRowScanStrategy::SlotOnlyRows {
            retained_slot_layout,
        } => {
            #[cfg(any(test, feature = "diagnostics"))]
            record_kernel_slots_only_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "slot-only kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_slot_rows_into_kernel(
                        key_stream,
                        consistency,
                        retained_slot_layout,
                        row_keep_cap,
                        row_skip_count,
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
            record_kernel_slots_only_path_hit();

            execute_retained_kernel_scan(
                key_stream,
                scan_budget_hint,
                Some(retained_slot_layout),
                "slot-only kernel rows require one retained-slot layout",
                |key_stream, retained_slot_layout| {
                    scan_slot_rows_into_kernel_with_filter_program(
                        key_stream,
                        consistency,
                        filter_program,
                        retained_slot_layout,
                        row_keep_cap,
                        row_skip_count,
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
    missing_layout_message: &'static str,
    mut scan_rows: impl FnMut(
        &mut OrderedKeyStreamBox,
        &RetainedSlotLayout,
    ) -> Result<(Vec<KernelRow>, usize), InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let retained_slot_layout = retained_slot_layout
        .ok_or_else(|| InternalError::query_executor_invariant(missing_layout_message))?;

    execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
        scan_rows(key_stream, retained_slot_layout)
    })
}

pub(super) fn execute_scalar_page_kernel_dyn(
    request: ScalarPageKernelRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        consistency,
        scan_strategy,
        continuation,
        row_runtime,
    } = request;

    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

    execute_kernel_row_scan(KernelRowScanRequest {
        key_stream,
        scan_budget_hint,
        consistency,
        scan_strategy,
        row_keep_cap: None,
        row_skip_count: 0,
        row_runtime,
    })
}

// Run one scalar read loop with one optional scan budget without re-checking
// the same budget logic inside each specialized row reader.
fn execute_scalar_page_read_loop(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    mut scan_rows: impl FnMut(
        &mut OrderedKeyStreamBox,
    ) -> Result<(Vec<KernelRow>, usize), InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let inner = std::mem::replace(key_stream, OrderedKeyStreamBox::empty());
        *key_stream = OrderedKeyStreamBox::budgeted(inner, scan_budget);

        return scan_rows(key_stream);
    }

    scan_rows(key_stream)
}

// Run one direct data-row read loop with one optional scan budget without
// paying the structural kernel-row envelope cost.
fn execute_scalar_data_row_read_loop(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    mut scan_rows: impl FnMut(
        &mut OrderedKeyStreamBox,
        Option<usize>,
        usize,
    ) -> Result<DirectDataRowScanResult, InternalError>,
) -> Result<DirectDataRowScanResult, InternalError> {
    if row_keep_cap == Some(0) {
        return Ok(DirectDataRowScanResult {
            rows: Vec::new(),
            rows_scanned: 0,
            rows_matched: 0,
        });
    }

    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let inner = std::mem::replace(key_stream, OrderedKeyStreamBox::empty());
        *key_stream = OrderedKeyStreamBox::budgeted(inner, scan_budget);

        return scan_rows(key_stream, row_keep_cap, row_skip_count);
    }

    scan_rows(key_stream, row_keep_cap, row_skip_count)
}

// Scan one ordered key stream into kernel rows through one caller-selected
// row reader while preserving the shared scanned-key accounting contract.
fn scan_kernel_rows_with(
    key_stream: &mut OrderedKeyStreamBox,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    mut read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = kernel_row_staged_capacity(key_stream, row_keep_cap, row_skip_count);
    let mut rows = Vec::with_capacity(staged_capacity);
    let mut rows_matched = 0usize;
    let Some(row_keep_cap) = row_keep_cap else {
        loop {
            let (next_key, key_stream_micros) =
                measure_execution_stats_phase(|| key_stream.next_key());
            record_key_stream_micros(key_stream_micros);
            let Some(key) = next_key? else {
                break;
            };
            record_key_stream_yield();

            rows_scanned = rows_scanned.saturating_add(1);
            let Some(row) = read_row(key)? else {
                continue;
            };
            retain_kernel_row(row, row_skip_count, rows_matched, &mut rows);
            rows_matched = rows_matched.saturating_add(1);
        }

        return Ok((rows, rows_scanned));
    };

    loop {
        let (next_key, key_stream_micros) = measure_execution_stats_phase(|| key_stream.next_key());
        record_key_stream_micros(key_stream_micros);
        let Some(key) = next_key? else {
            break;
        };
        record_key_stream_yield();

        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = read_row(key)? else {
            continue;
        };
        retain_kernel_row(row, row_skip_count, rows_matched, &mut rows);
        rows_matched = rows_matched.saturating_add(1);
        if rows_matched >= row_keep_cap {
            break;
        }
    }

    Ok((rows, rows_scanned))
}

// Compute the retained kernel-row capacity after the caller has converted a
// cursorless page offset into a scan-time skip.
fn kernel_row_staged_capacity(
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

// Retain one matched kernel row only after the caller-owned cursorless page
// offset has been satisfied.
fn retain_kernel_row(
    row: KernelRow,
    row_skip_count: usize,
    rows_matched: usize,
    rows: &mut Vec<KernelRow>,
) {
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
    mut read_data_row: impl FnMut(DataKey) -> Result<Option<DataRow>, InternalError>,
) -> Result<DirectDataRowScanResult, InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = direct_data_row_staged_capacity(key_stream, row_keep_cap, row_skip_count);
    let mut data_rows = Vec::with_capacity(staged_capacity);
    let mut rows_matched = 0usize;
    let Some(row_keep_cap) = row_keep_cap else {
        loop {
            #[cfg(feature = "diagnostics")]
            let ((key_stream_local_instructions, read_result), key_stream_micros) =
                measure_execution_stats_phase(|| {
                    measure_direct_data_row_phase(|| key_stream.next_key())
                });
            #[cfg(not(feature = "diagnostics"))]
            let (read_result, key_stream_micros) =
                measure_execution_stats_phase(|| key_stream.next_key());
            record_key_stream_micros(key_stream_micros);
            let Some(key) = read_result? else {
                break;
            };
            #[cfg(feature = "diagnostics")]
            record_direct_data_row_key_stream_local_instructions(key_stream_local_instructions);
            record_key_stream_yield();

            rows_scanned = rows_scanned.saturating_add(1);
            #[cfg(feature = "diagnostics")]
            let (row_read_local_instructions, row_read_result) =
                measure_direct_data_row_phase(|| read_data_row(key));
            #[cfg(not(feature = "diagnostics"))]
            let row_read_result = read_data_row(key);
            #[cfg(feature = "diagnostics")]
            record_direct_data_row_row_read_local_instructions(row_read_local_instructions);
            let Some(data_row) = row_read_result? else {
                continue;
            };
            retain_direct_data_row(data_row, row_skip_count, rows_matched, &mut data_rows);
            rows_matched = rows_matched.saturating_add(1);
        }

        return Ok(DirectDataRowScanResult {
            rows: data_rows,
            rows_scanned,
            rows_matched,
        });
    };

    loop {
        #[cfg(feature = "diagnostics")]
        let ((key_stream_local_instructions, read_result), key_stream_micros) =
            measure_execution_stats_phase(|| {
                measure_direct_data_row_phase(|| key_stream.next_key())
            });
        #[cfg(not(feature = "diagnostics"))]
        let (read_result, key_stream_micros) =
            measure_execution_stats_phase(|| key_stream.next_key());
        record_key_stream_micros(key_stream_micros);
        let Some(key) = read_result? else {
            break;
        };
        #[cfg(feature = "diagnostics")]
        record_direct_data_row_key_stream_local_instructions(key_stream_local_instructions);
        record_key_stream_yield();

        rows_scanned = rows_scanned.saturating_add(1);
        #[cfg(feature = "diagnostics")]
        let (row_read_local_instructions, row_read_result) =
            measure_direct_data_row_phase(|| read_data_row(key));
        #[cfg(not(feature = "diagnostics"))]
        let row_read_result = read_data_row(key);
        #[cfg(feature = "diagnostics")]
        record_direct_data_row_row_read_local_instructions(row_read_local_instructions);
        let Some(data_row) = row_read_result? else {
            continue;
        };
        retain_direct_data_row(data_row, row_skip_count, rows_matched, &mut data_rows);
        rows_matched = rows_matched.saturating_add(1);
        if rows_matched >= row_keep_cap {
            break;
        }
    }

    Ok(DirectDataRowScanResult {
        rows: data_rows,
        rows_scanned,
        rows_matched,
    })
}

// Scan one ordered key stream directly into canonical data rows while
// applying the residual predicate during scan-time retained-slot decoding.
fn scan_data_rows_direct_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
) -> Result<DirectDataRowScanResult, InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, row_skip_count, |key| {
        row_runtime.read_data_row_with_filter_program(
            consistency,
            key,
            filter_program,
            retained_slot_layout,
        )
    })
}

// Run the materialized-order raw data-row lane through one residual-predicate
// policy helper so perf-attributed and normal scans share the same scan-time
// filtering contract.
pub(super) fn scan_materialized_order_direct_data_rows(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    consistency: MissingRowPolicy,
    residual_filter_scan_mode: ResidualFilterScanMode,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> Result<DirectDataRowScanResult, InternalError> {
    scan_direct_data_rows_with_residual_policy(
        key_stream,
        scan_budget_hint,
        None,
        0,
        consistency,
        residual_filter_scan_mode,
        row_runtime,
        residual_filter_program,
        retained_slot_layout,
        "materialized-order direct data-row path cannot defer residual filtering",
    )
}

// Run one direct data-row scan through the shared residual-predicate timing
// contract so plain, filtered, and materialized-order raw lanes all choose
// the same row reader in one place.
#[expect(clippy::too_many_arguments)]
pub(super) fn scan_direct_data_rows_with_residual_policy(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    consistency: MissingRowPolicy,
    residual_filter_scan_mode: ResidualFilterScanMode,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
    deferred_filtering_message: &'static str,
) -> Result<DirectDataRowScanResult, InternalError> {
    execute_scalar_data_row_read_loop(
        key_stream,
        scan_budget_hint,
        row_keep_cap,
        row_skip_count,
        |key_stream, row_keep_cap, row_skip_count| match residual_filter_scan_mode {
            ResidualFilterScanMode::Absent => scan_data_rows_direct(
                key_stream,
                consistency,
                row_keep_cap,
                row_skip_count,
                row_runtime,
            ),
            ResidualFilterScanMode::AppliedDuringScan => {
                let filter_program = residual_filter_program.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "scan-time residual filtering requires one compiled residual filter program",
                    )
                })?;
                let retained_slot_layout = retained_slot_layout.ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "scan-time residual filtering requires one retained-slot layout",
                    )
                })?;

                scan_data_rows_direct_with_filter_program(
                    key_stream,
                    consistency,
                    row_keep_cap,
                    row_skip_count,
                    row_runtime,
                    filter_program,
                    retained_slot_layout,
                )
            }
            ResidualFilterScanMode::DeferredPostAccess => Err(
                InternalError::query_executor_invariant(deferred_filtering_message),
            ),
        },
    )
}

// Compute the number of direct raw rows worth reserving after the caller's
// cursorless page offset has already been converted into a scan-time skip.
fn direct_data_row_staged_capacity(
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

// Retain one matched direct raw row only after the cursorless page offset has
// been satisfied. This lets route-satisfied direct scans avoid staging rows
// that the final page window would immediately discard.
fn retain_direct_data_row(
    data_row: DataRow,
    row_skip_count: usize,
    rows_matched: usize,
    data_rows: &mut Vec<DataRow>,
) {
    if rows_matched >= row_skip_count {
        data_rows.push(data_row);
    }
}

fn scan_data_rows_only_into_kernel(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, row_skip_count, |key| {
        row_runtime.read_data_row_only(consistency, key)
    })
}

// Scan keys into full structural rows while retaining only the caller-declared
// shared slot subset needed by later executor phases.
fn scan_full_retained_rows_into_kernel(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_full_retained_rows_into_kernel_with_reader(
        key_stream,
        row_keep_cap,
        row_skip_count,
        |key| row_runtime.read_full_row_retained(consistency, key, retained_slot_layout),
    )
}

// Scan keys into full retained structural rows through one caller-selected
// row reader while preserving the shared kernel-row scan envelope.
fn scan_full_retained_rows_into_kernel_with_reader(
    key_stream: &mut OrderedKeyStreamBox,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, row_skip_count, read_row)
}

// Scan keys into retained full structural rows while applying the residual
// predicate before rows enter shared post-access processing.
fn scan_full_retained_rows_into_kernel_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_full_retained_rows_into_kernel_with_reader(
        key_stream,
        row_keep_cap,
        row_skip_count,
        |key| {
            row_runtime.read_full_row_retained_with_filter_program(
                consistency,
                key,
                filter_program,
                retained_slot_layout,
            )
        },
    )
}

// Scan keys into compact slot-only rows when the final lane never needs a
// full `DataRow` payload.
fn scan_slot_rows_into_kernel(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_slot_rows_into_kernel_with_reader(key_stream, row_keep_cap, row_skip_count, |key| {
        row_runtime.read_slot_only(consistency, &key, retained_slot_layout)
    })
}

// Scan keys into compact slot-only rows through one caller-selected row
// reader while preserving the shared kernel-row scan envelope.
fn scan_slot_rows_into_kernel_with_reader(
    key_stream: &mut OrderedKeyStreamBox,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, row_skip_count, read_row)
}

// Scan keys into compact slot-only rows while applying the residual predicate
// before rows enter shared post-access processing.
fn scan_slot_rows_into_kernel_with_filter_program(
    key_stream: &mut OrderedKeyStreamBox,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_skip_count: usize,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_slot_rows_into_kernel_with_reader(key_stream, row_keep_cap, row_skip_count, |key| {
        row_runtime.read_slot_only_with_filter_program(
            consistency,
            &key,
            filter_program,
            retained_slot_layout,
        )
    })
}
