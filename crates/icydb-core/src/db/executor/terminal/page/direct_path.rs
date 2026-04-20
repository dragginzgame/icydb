use crate::{
    db::{
        executor::{
            ExecutionKernel, OrderedKeyStream, ScalarContinuationContext,
            apply_structural_order_window_to_data_rows,
            pipeline::contracts::{MaterializedExecutionPayload, StructuralCursorPage},
            route::LoadOrderRouteContract,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

use super::{
    plan::DirectDataRowPath,
    post_access::apply_data_row_page_window,
    row_runtime::{ResidualPredicateScanMode, ScalarRowRuntimeHandle},
    scan::{scan_direct_data_rows_with_residual_policy, scan_materialized_order_direct_data_rows},
};

#[cfg(feature = "diagnostics")]
use super::metrics::{
    measure_direct_data_row_phase, record_direct_data_row_order_window_local_instructions,
    record_direct_data_row_page_window_local_instructions,
    record_direct_data_row_scan_local_instructions,
};
#[cfg(any(test, feature = "diagnostics"))]
use super::metrics::{record_direct_data_row_path_hit, record_direct_filtered_data_row_path_hit};

// Execute one already-resolved direct `DataRow` strategy through the shared
// direct-lane scan and page-window shell.
#[expect(clippy::too_many_arguments)]
#[expect(clippy::too_many_lines)]
pub(super) fn execute_direct_data_row_path(
    plan: &AccessPlannedQuery,
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    consistency: MissingRowPolicy,
    continuation: &ScalarContinuationContext,
    row_runtime: &ScalarRowRuntimeHandle<'_>,
    direct_data_row_path: DirectDataRowPath<'_>,
) -> Result<(MaterializedExecutionPayload, usize, usize), InternalError> {
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

    // Phase 1: record the chosen direct-lane family once before scan.
    #[cfg(any(test, feature = "diagnostics"))]
    match direct_data_row_path {
        DirectDataRowPath::Plain { .. } => record_direct_data_row_path_hit(),
        DirectDataRowPath::Filtered { .. } => record_direct_filtered_data_row_path_hit(),
        DirectDataRowPath::MaterializedOrder {
            residual_predicate_scan_mode,
            ..
        } => match residual_predicate_scan_mode {
            ResidualPredicateScanMode::Absent => record_direct_data_row_path_hit(),
            ResidualPredicateScanMode::AppliedDuringScan => {
                record_direct_filtered_data_row_path_hit();
            }
            ResidualPredicateScanMode::DeferredPostAccess => {
                return Err(InternalError::query_executor_invariant(
                    "materialized-order direct data-row path cannot defer residual filtering",
                ));
            }
        },
    }

    // Phase 2: run the direct scan through the shared residual-policy helper.
    #[cfg(feature = "diagnostics")]
    let (scan_local_instructions, scan_result) =
        measure_direct_data_row_phase(|| match direct_data_row_path {
            DirectDataRowPath::Plain { row_keep_cap } => {
                scan_direct_data_rows_with_residual_policy(
                    key_stream,
                    scan_budget_hint,
                    row_keep_cap,
                    consistency,
                    ResidualPredicateScanMode::Absent,
                    row_runtime,
                    None,
                    None,
                    "direct data-row path cannot defer residual filtering",
                )
            }
            DirectDataRowPath::Filtered {
                row_keep_cap,
                filter_program,
                retained_slot_layout,
            } => scan_direct_data_rows_with_residual_policy(
                key_stream,
                scan_budget_hint,
                row_keep_cap,
                consistency,
                ResidualPredicateScanMode::AppliedDuringScan,
                row_runtime,
                Some(filter_program),
                Some(retained_slot_layout),
                "direct filtered data-row path cannot defer residual filtering",
            ),
            DirectDataRowPath::MaterializedOrder {
                residual_predicate_scan_mode,
                filter_program,
                retained_slot_layout,
                ..
            } => scan_materialized_order_direct_data_rows(
                key_stream,
                scan_budget_hint,
                consistency,
                residual_predicate_scan_mode,
                row_runtime,
                filter_program,
                retained_slot_layout,
            ),
        });
    #[cfg(not(feature = "diagnostics"))]
    let scan_result = match direct_data_row_path {
        DirectDataRowPath::Plain { row_keep_cap } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            consistency,
            ResidualPredicateScanMode::Absent,
            row_runtime,
            None,
            None,
            "direct data-row path cannot defer residual filtering",
        ),
        DirectDataRowPath::Filtered {
            row_keep_cap,
            filter_program,
            retained_slot_layout,
        } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            consistency,
            ResidualPredicateScanMode::AppliedDuringScan,
            row_runtime,
            Some(filter_program),
            Some(retained_slot_layout),
            "direct filtered data-row path cannot defer residual filtering",
        ),
        DirectDataRowPath::MaterializedOrder {
            residual_predicate_scan_mode,
            filter_program,
            retained_slot_layout,
            ..
        } => scan_materialized_order_direct_data_rows(
            key_stream,
            scan_budget_hint,
            consistency,
            residual_predicate_scan_mode,
            row_runtime,
            filter_program,
            retained_slot_layout,
        ),
    };
    let (mut data_rows, rows_scanned) = scan_result?;
    #[cfg(feature = "diagnostics")]
    record_direct_data_row_scan_local_instructions(scan_local_instructions);

    // Phase 3: materialized-order direct lanes still own one in-memory order
    // pass before the final page window.
    if let DirectDataRowPath::MaterializedOrder { resolved_order, .. } = direct_data_row_path
        && data_rows.len() > 1
    {
        #[cfg(feature = "diagnostics")]
        let (order_window_local_instructions, order_window_result) =
            measure_direct_data_row_phase(|| {
                apply_structural_order_window_to_data_rows(
                    &mut data_rows,
                    row_runtime.row_layout(),
                    resolved_order,
                    ExecutionKernel::bounded_order_keep_count(plan, None),
                )
            });
        #[cfg(not(feature = "diagnostics"))]
        apply_structural_order_window_to_data_rows(
            &mut data_rows,
            row_runtime.row_layout(),
            resolved_order,
            ExecutionKernel::bounded_order_keep_count(plan, None),
        )?;
        #[cfg(feature = "diagnostics")]
        order_window_result?;
        #[cfg(feature = "diagnostics")]
        record_direct_data_row_order_window_local_instructions(order_window_local_instructions);
    }

    // Phase 4: direct-lane accounting matches the shared kernel path, then
    // the final offset/limit window runs once on canonical data rows.
    let post_access_rows = data_rows.len();
    #[cfg(feature = "diagnostics")]
    let (page_window_local_instructions, page_window_result) =
        measure_direct_data_row_phase(|| {
            apply_data_row_page_window(plan, &mut data_rows);

            Ok::<(), InternalError>(())
        });
    #[cfg(not(feature = "diagnostics"))]
    apply_data_row_page_window(plan, &mut data_rows);
    #[cfg(feature = "diagnostics")]
    page_window_result?;
    #[cfg(feature = "diagnostics")]
    record_direct_data_row_page_window_local_instructions(page_window_local_instructions);

    Ok((
        StructuralCursorPage::new(data_rows, None),
        rows_scanned,
        post_access_rows,
    ))
}
