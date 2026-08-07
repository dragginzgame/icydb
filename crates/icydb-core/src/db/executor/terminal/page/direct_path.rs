use crate::{
    db::{
        data::DataRow,
        executor::{
            ExecutionKernel, OrderedKeyStreamBox, ScalarContinuationContext,
            pipeline::contracts::{ScalarPageMaterialization, StructuralCursorPage},
            route::LoadOrderRouteMode,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

use super::{
    plan::DirectDataRowPath,
    post_access::apply_data_row_page_window,
    row_runtime::ScalarRowRuntimeHandle,
    scan::{
        DataRowOrderScanResult, RowScanResult, scan_direct_data_rows_with_residual_policy,
        scan_materialized_order_direct_data_rows,
    },
};

#[cfg(feature = "diagnostics")]
use super::metrics::{
    measure_direct_data_row_phase, record_direct_data_row_order_window_local_instructions,
    record_direct_data_row_page_window_local_instructions,
    record_direct_data_row_scan_local_instructions,
};

// Execute one already-resolved direct `DataRow` strategy through the shared
// direct-lane scan and page-window shell.
#[expect(clippy::too_many_arguments)]
pub(super) fn execute_direct_data_row_path(
    plan: &AccessPlannedQuery,
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    load_order_route_mode: LoadOrderRouteMode,
    consistency: MissingRowPolicy,
    continuation: ScalarContinuationContext,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    direct_data_row_path: DirectDataRowPath<'_>,
) -> Result<ScalarPageMaterialization, InternalError> {
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_mode)?;

    // Phase 1: run the direct scan through the shared residual-policy helper.
    // Incompatible-order lanes select their bounded winner set during this
    // scan instead of retaining every raw candidate until a later sort pass.
    let row_skip_count = direct_data_row_page_skip_count(plan);
    let order_keep_count = ExecutionKernel::bounded_order_keep_count(plan, None, false);
    #[cfg(feature = "diagnostics")]
    let (scan_local_instructions, scan_result) = measure_direct_data_row_phase(|| {
        execute_direct_data_row_scan(
            key_stream,
            scan_budget_hint,
            row_skip_count,
            consistency,
            row_runtime,
            direct_data_row_path,
            order_keep_count,
        )
    });
    #[cfg(not(feature = "diagnostics"))]
    let scan_result = execute_direct_data_row_scan(
        key_stream,
        scan_budget_hint,
        row_skip_count,
        consistency,
        row_runtime,
        direct_data_row_path,
        order_keep_count,
    );
    let scan_result = scan_result?;
    #[cfg(feature = "diagnostics")]
    record_direct_data_row_scan_local_instructions(scan_local_instructions);

    // Phase 2: only the retained winner set reaches final canonical ordering.
    let (mut data_rows, rows_scanned, rows_matched, page_window_already_applied) = match scan_result
    {
        DirectDataRowScanOutcome::PageRows(RowScanResult {
            rows,
            rows_scanned,
            rows_matched,
        }) => (rows, rows_scanned, rows_matched, true),
        DirectDataRowScanOutcome::OrderWindow(DataRowOrderScanResult {
            window,
            rows_scanned,
            rows_matched,
        }) => {
            #[cfg(feature = "diagnostics")]
            let (order_window_local_instructions, rows) =
                measure_direct_data_row_phase(|| window.into_sorted_rows());
            #[cfg(not(feature = "diagnostics"))]
            let rows = window.into_sorted_rows();
            #[cfg(feature = "diagnostics")]
            record_direct_data_row_order_window_local_instructions(order_window_local_instructions);

            (rows?, rows_scanned, rows_matched, false)
        }
    };

    // Phase 3: direct-lane accounting matches the shared kernel path, then
    // the final offset/limit window runs once on canonical data rows.
    let post_access_rows = if page_window_already_applied {
        rows_matched
    } else {
        data_rows.len()
    };
    #[cfg(feature = "diagnostics")]
    let (page_window_local_instructions, page_window_result) =
        measure_direct_data_row_phase(|| {
            if !page_window_already_applied {
                apply_data_row_page_window(plan, &mut data_rows);
            }

            Ok::<(), InternalError>(())
        });
    #[cfg(not(feature = "diagnostics"))]
    if !page_window_already_applied {
        apply_data_row_page_window(plan, &mut data_rows);
    }
    #[cfg(feature = "diagnostics")]
    page_window_result?;
    #[cfg(feature = "diagnostics")]
    record_direct_data_row_page_window_local_instructions(page_window_local_instructions);

    Ok(ScalarPageMaterialization {
        payload: StructuralCursorPage::new(data_rows),
        rows_scanned,
        post_access_rows,
    })
}

enum DirectDataRowScanOutcome<'a> {
    PageRows(RowScanResult<DataRow>),
    OrderWindow(DataRowOrderScanResult<'a>),
}

fn execute_direct_data_row_scan<'a>(
    key_stream: &mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    row_skip_count: usize,
    consistency: MissingRowPolicy,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    direct_data_row_path: DirectDataRowPath<'a>,
    order_keep_count: Option<usize>,
) -> Result<DirectDataRowScanOutcome<'a>, InternalError> {
    match direct_data_row_path {
        DirectDataRowPath::Plain { row_keep_cap } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            row_skip_count,
            consistency,
            row_runtime,
            None,
        )
        .map(DirectDataRowScanOutcome::PageRows),
        DirectDataRowPath::Filtered {
            row_keep_cap,
            filter_program,
        } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            row_skip_count,
            consistency,
            row_runtime,
            Some(filter_program),
        )
        .map(DirectDataRowScanOutcome::PageRows),
        DirectDataRowPath::MaterializedOrder {
            resolved_order,
            filter_program,
        } => scan_materialized_order_direct_data_rows(
            key_stream,
            scan_budget_hint,
            consistency,
            row_runtime,
            filter_program,
            resolved_order,
            order_keep_count,
        )
        .map(DirectDataRowScanOutcome::OrderWindow),
    }
}

// Return the cursorless scalar page offset that route-satisfied direct raw-row
// scans can skip during collection. Materialized-order direct lanes pass
// through the same value but ignore it because ordering must run before paging.
fn direct_data_row_page_skip_count(plan: &AccessPlannedQuery) -> usize {
    plan.scalar_plan()
        .page
        .as_ref()
        .map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX))
}
