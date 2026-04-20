use crate::db::executor::terminal::page::{
    KernelRow, KernelRowScanStrategy, ResidualFilterScanMode, ScalarRowRuntimeHandle,
};
use crate::{
    db::{
        data::{DataKey, DataRow},
        executor::{
            BudgetedOrderedKeyStream, OrderedKeyStream, ScalarContinuationContext,
            exact_output_key_count_hint, key_stream_budget_is_redundant,
            projection::eval_effective_runtime_filter_program_with_value_ref_reader,
            route::LoadOrderRouteContract, terminal::page::RetainedSlotLayout,
        },
        predicate::MissingRowPolicy,
        query::plan::EffectiveRuntimeFilterProgram,
    },
    error::InternalError,
    value::Value,
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

// Shared scalar load page-kernel orchestration boundary.
// Typed wrappers provide scan/decode callbacks so this loop can remain
// non-generic while preserving fail-closed continuation invariants.
pub(super) struct ScalarPageKernelRequest<'a, 'r> {
    pub(super) key_stream: &'a mut dyn OrderedKeyStream,
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
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) scan_strategy: KernelRowScanStrategy<'a>,
    pub(in crate::db::executor) row_keep_cap: Option<usize>,
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
        row_runtime,
    } = request;

    // Phase 1: select the concrete row-read kernel once so the inner scan
    // loop does not branch on payload shape or predicate mode per row.
    match scan_strategy {
        KernelRowScanStrategy::DataRows => {
            #[cfg(any(test, feature = "diagnostics"))]
            record_kernel_data_row_path_hit();

            execute_scalar_page_read_loop(key_stream, scan_budget_hint, |key_stream| {
                scan_data_rows_only_into_kernel(key_stream, consistency, row_keep_cap, row_runtime)
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
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
    missing_layout_message: &'static str,
    mut scan_rows: impl FnMut(
        &mut dyn OrderedKeyStream,
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
        row_runtime,
    })
}

// Run one scalar read loop with one optional scan budget without re-checking
// the same budget logic inside each specialized row reader.
fn execute_scalar_page_read_loop(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    mut scan_rows: impl FnMut(
        &mut dyn OrderedKeyStream,
    ) -> Result<(Vec<KernelRow>, usize), InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        return scan_rows(&mut budgeted);
    }

    scan_rows(key_stream)
}

// Run one direct data-row read loop with one optional scan budget without
// paying the structural kernel-row envelope cost.
fn execute_scalar_data_row_read_loop(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    mut scan_rows: impl FnMut(
        &mut dyn OrderedKeyStream,
        Option<usize>,
    ) -> Result<(Vec<DataRow>, usize), InternalError>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    if row_keep_cap == Some(0) {
        return Ok((Vec::new(), 0));
    }

    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        return scan_rows(&mut budgeted, row_keep_cap);
    }

    scan_rows(key_stream, row_keep_cap)
}

// Scan one ordered key stream into kernel rows through one caller-selected
// row reader while preserving the shared scanned-key accounting contract.
fn scan_kernel_rows_with(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    mut read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = exact_output_key_count_hint(key_stream, None).map_or_else(
        || row_keep_cap.unwrap_or(0),
        |hint| row_keep_cap.map_or(hint, |cap| usize::min(hint, cap)),
    );
    let mut rows = Vec::with_capacity(staged_capacity);

    while let Some(key) = key_stream.next_key()? {
        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = read_row(key)? else {
            continue;
        };
        rows.push(row);
        if row_keep_cap.is_some_and(|cap| rows.len() >= cap) {
            break;
        }
    }

    Ok((rows, rows_scanned))
}

// Evaluate one residual filter program against compact retained-slot values
// before the executor commits to a retained-row wrapper for the surviving row.
pub(super) fn filter_matches_retained_values(
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    retained_values: &[Option<Value>],
) -> Result<bool, InternalError> {
    eval_effective_runtime_filter_program_with_value_ref_reader(
        filter_program,
        &mut |slot| {
            let index = retained_slot_layout.value_index_for_slot(slot)?;

            retained_values.get(index).and_then(Option::as_ref)
        },
        "scalar filter expression could not read retained slot",
    )
}

// Scan one ordered key stream directly into canonical data rows when the
// caller already proved no later phase needs a kernel-row wrapper.
fn scan_data_rows_direct(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_data_row(consistency, key)
    })
}

// Scan one ordered key stream directly into canonical data rows through one
// caller-selected row reader while preserving shared direct-lane key-stream
// and row-read attribution.
fn scan_data_rows_direct_with_reader(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    mut read_data_row: impl FnMut(DataKey) -> Result<Option<DataRow>, InternalError>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let staged_capacity = exact_output_key_count_hint(key_stream, None).map_or_else(
        || row_keep_cap.unwrap_or(0),
        |hint| row_keep_cap.map_or(hint, |cap| usize::min(hint, cap)),
    );
    let mut data_rows = Vec::with_capacity(staged_capacity);

    loop {
        #[cfg(feature = "diagnostics")]
        let (key_stream_local_instructions, read_result) =
            measure_direct_data_row_phase(|| key_stream.next_key());
        #[cfg(not(feature = "diagnostics"))]
        let read_result = key_stream.next_key();
        let Some(key) = read_result? else {
            break;
        };
        #[cfg(feature = "diagnostics")]
        record_direct_data_row_key_stream_local_instructions(key_stream_local_instructions);

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
        data_rows.push(data_row);
        if row_keep_cap.is_some_and(|cap| data_rows.len() >= cap) {
            break;
        }
    }

    Ok((data_rows, rows_scanned))
}

// Scan one ordered key stream directly into canonical data rows while
// applying the residual predicate during scan-time retained-slot decoding.
fn scan_data_rows_direct_with_filter_program(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    scan_data_rows_direct_with_reader(key_stream, row_keep_cap, |key| {
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
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    consistency: MissingRowPolicy,
    residual_filter_predicate_scan_mode: ResidualFilterScanMode,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    scan_direct_data_rows_with_residual_policy(
        key_stream,
        scan_budget_hint,
        None,
        consistency,
        residual_filter_predicate_scan_mode,
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
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    row_keep_cap: Option<usize>,
    consistency: MissingRowPolicy,
    residual_filter_predicate_scan_mode: ResidualFilterScanMode,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    residual_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    retained_slot_layout: Option<&RetainedSlotLayout>,
    deferred_filtering_message: &'static str,
) -> Result<(Vec<DataRow>, usize), InternalError> {
    execute_scalar_data_row_read_loop(
        key_stream,
        scan_budget_hint,
        row_keep_cap,
        |key_stream, row_keep_cap| match residual_filter_predicate_scan_mode {
            ResidualFilterScanMode::Absent => {
                scan_data_rows_direct(key_stream, consistency, row_keep_cap, row_runtime)
            }
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

fn scan_data_rows_only_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, |key| {
        row_runtime.read_data_row_only(consistency, key)
    })
}

// Scan keys into full structural rows while retaining only the caller-declared
// shared slot subset needed by later executor phases.
fn scan_full_retained_rows_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_full_retained_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_full_row_retained(consistency, key, retained_slot_layout)
    })
}

// Scan keys into full retained structural rows through one caller-selected
// row reader while preserving the shared kernel-row scan envelope.
fn scan_full_retained_rows_into_kernel_with_reader(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, read_row)
}

// Scan keys into retained full structural rows while applying the residual
// predicate before rows enter shared post-access processing.
fn scan_full_retained_rows_into_kernel_with_filter_program(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_full_retained_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
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
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_slot_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_slot_only(consistency, &key, retained_slot_layout)
    })
}

// Scan keys into compact slot-only rows through one caller-selected row
// reader while preserving the shared kernel-row scan envelope.
fn scan_slot_rows_into_kernel_with_reader(
    key_stream: &mut dyn OrderedKeyStream,
    row_keep_cap: Option<usize>,
    read_row: impl FnMut(DataKey) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_kernel_rows_with(key_stream, row_keep_cap, read_row)
}

// Scan keys into compact slot-only rows while applying the residual predicate
// before rows enter shared post-access processing.
fn scan_slot_rows_into_kernel_with_filter_program(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    filter_program: &EffectiveRuntimeFilterProgram,
    retained_slot_layout: &RetainedSlotLayout,
    row_keep_cap: Option<usize>,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    scan_slot_rows_into_kernel_with_reader(key_stream, row_keep_cap, |key| {
        row_runtime.read_slot_only_with_filter_program(
            consistency,
            &key,
            filter_program,
            retained_slot_layout,
        )
    })
}
