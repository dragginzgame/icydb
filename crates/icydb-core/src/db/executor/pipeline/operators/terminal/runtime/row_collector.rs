//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: cursorless row-collector materialization over structural
//! row and retained-slot-row payloads.
//! Does not own: planner route selection or outer-session projection shaping.
//! Boundary: exposes the shared row-collector short path while keeping payload
//! assembly inside executor-owned structural contracts.

use crate::{
    db::{
        cursor::CursorBoundary,
        data::DataRow,
        executor::{
            ExecutionKernel, OrderedKeyStream, ScalarContinuationContext,
            pipeline::contracts::{
                MaterializedExecutionPayload, RowCollectorMaterializationRequest,
            },
            route::{LoadOrderRouteContract, access_order_satisfied_by_route_contract},
            terminal::{
                RetainedSlotRow,
                page::{
                    KernelRow, KernelRowPayloadMode, KernelRowScanRequest,
                    ResidualPredicateScanMode, ScalarRowRuntimeHandle, execute_kernel_row_scan,
                    resolve_kernel_row_scan_strategy,
                },
            },
            traversal::row_read_consistency_for_plan,
        },
        predicate::PredicateProgram,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

impl ExecutionKernel {
    // Return whether load execution can safely use the row-collector short
    // path without changing cursor, pagination, or residual-filter semantics.
    pub(in crate::db::executor::pipeline::operators::terminal) fn load_row_collector_short_path_eligible(
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
        retain_slot_rows: bool,
    ) -> bool {
        let logical = plan.scalar_plan();
        let generic_short_path = logical.mode.is_load()
            && cursor_boundary.is_none()
            && logical.predicate.is_none()
            && logical.order.is_none()
            && logical.page.is_none();
        let retained_slot_short_path = logical.mode.is_load()
            && retain_slot_rows
            && cursor_boundary.is_none()
            && !logical.distinct
            && (logical.order.is_none() || access_order_satisfied_by_route_contract(plan));

        generic_short_path || retained_slot_short_path
    }

    // Run one row-collector stream over the already decorated key stream and
    // stage structural kernel rows only.
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_collector_stream(
        request: RowCollectorStreamRequest<'_, '_>,
    ) -> Result<(Vec<KernelRow>, usize), InternalError> {
        let RowCollectorStreamRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            row_keep_cap,
            payload_mode,
            key_stream,
            row_runtime,
            predicate_slots,
            retained_slot_layout,
        } = request;

        // Phase 1: derive the shared row scan contract from plan-owned
        // consistency and residual-predicate state.
        let consistency = row_read_consistency_for_plan(plan);
        let residual_predicate_scan_mode = ResidualPredicateScanMode::from_plan_and_layout(
            plan.has_residual_predicate(),
            retained_slot_layout,
        );
        let scan_strategy = resolve_kernel_row_scan_strategy(
            payload_mode,
            predicate_slots,
            residual_predicate_scan_mode,
            retained_slot_layout,
        )?;
        let _ = continuation;
        let _ = load_order_route_contract;

        // Phase 2: reuse the canonical structural row scan boundary and only
        // add the retained-slot keep cap needed by cursorless materialization.
        execute_kernel_row_scan(KernelRowScanRequest {
            key_stream,
            scan_budget_hint,
            consistency,
            scan_strategy,
            row_keep_cap,
            row_runtime,
        })
    }

    // Materialize one cursorless short-path load through the structural row
    // runtime under the same continuation and bounded-scan contract as the
    // canonical scalar page kernel.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'a>(
        request: RowCollectorMaterializationRequest<'a>,
        row_runtime: &mut ScalarRowRuntimeHandle<'a>,
    ) -> Result<Option<(MaterializedExecutionPayload, usize, usize)>, InternalError> {
        // Phase 1: destructure the request once so the short path cannot drift
        // from the kernel-owned scan contract.
        let RowCollectorMaterializationRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            cursor_boundary,
            predicate_slots,
            validate_projection,
            retain_slot_rows,
            retained_slot_layout,
            prepared_projection_validation,
            key_stream,
        } = request;

        if !Self::load_row_collector_short_path_eligible(plan, cursor_boundary, retain_slot_rows) {
            return Ok(None);
        }

        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

        let payload_mode = select_cursorless_row_collector_payload_mode(
            retain_slot_rows,
            cursor_boundary,
            retained_slot_layout,
        );
        let row_keep_cap =
            cursorless_row_collector_keep_cap(plan, cursor_boundary, retain_slot_rows);
        let (mut rows, keys_scanned) = Self::run_row_collector_stream(RowCollectorStreamRequest {
            plan,
            scan_budget_hint,
            load_order_route_contract,
            continuation,
            row_keep_cap,
            payload_mode,
            key_stream,
            row_runtime,
            predicate_slots,
            retained_slot_layout,
        })?;

        apply_cursorless_row_collector_post_access(
            plan,
            validate_projection,
            prepared_projection_validation,
            retain_slot_rows,
            &mut rows,
        )?;

        let post_access_rows = rows.len();
        let payload = finalize_cursorless_row_collector_payload(rows, retain_slot_rows)?;

        Ok(Some((payload, keys_scanned, post_access_rows)))
    }
}

///
/// RowCollectorStreamRequest
///
/// RowCollectorStreamRequest keeps the structural row-collector scan contract
/// explicit while avoiding another wide helper signature in the terminal
/// runtime. The slot-only payload mode belongs to the same boundary as the
/// scan budget, continuation contract, and decorated key stream.
///

pub(in crate::db::executor::pipeline::operators::terminal) struct RowCollectorStreamRequest<'a, 'r>
{
    plan: &'a AccessPlannedQuery,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    continuation: &'a ScalarContinuationContext,
    row_keep_cap: Option<usize>,
    payload_mode: KernelRowPayloadMode,
    key_stream: &'a mut dyn OrderedKeyStream,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    predicate_slots: Option<&'a PredicateProgram>,
    retained_slot_layout: Option<&'a crate::db::executor::RetainedSlotLayout>,
}

// Return the number of kept rows the cursorless retained-slot path must
// materialize before later pagination becomes redundant.
fn cursorless_row_collector_keep_cap(
    plan: &AccessPlannedQuery,
    cursor_boundary: Option<&CursorBoundary>,
    retain_slot_rows: bool,
) -> Option<usize> {
    if !retain_slot_rows || cursor_boundary.is_some() {
        return None;
    }

    let page = plan.scalar_plan().page.as_ref()?;
    let limit = page.limit?;
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);

    Some(offset.saturating_add(limit))
}

// Return whether the cursorless retained-slot path already staged the final
// page window.
fn cursorless_page_window_is_redundant(plan: &AccessPlannedQuery, row_count: usize) -> bool {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return true;
    };

    if page.offset != 0 {
        return false;
    }

    page.limit
        .is_none_or(|limit| row_count <= usize::try_from(limit).unwrap_or(usize::MAX))
}

// Select one row payload mode before cursorless row collection so the scan
// loop does not branch on data-vs-slot materialization per row.
const fn select_cursorless_row_collector_payload_mode(
    retain_slot_rows: bool,
    cursor_boundary: Option<&CursorBoundary>,
    retained_slot_layout: Option<&crate::db::executor::RetainedSlotLayout>,
) -> KernelRowPayloadMode {
    if retain_slot_rows && cursor_boundary.is_none() {
        KernelRowPayloadMode::SlotsOnly
    } else if retained_slot_layout.is_some() {
        KernelRowPayloadMode::FullRowRetained
    } else {
        KernelRowPayloadMode::DataRowOnly
    }
}

// Apply the remaining cursorless post-access work after the kernel scan:
// optional page window and optional slot-row projection validation.
fn apply_cursorless_row_collector_post_access(
    plan: &AccessPlannedQuery,
    validate_projection: bool,
    prepared_projection_validation: Option<
        &crate::db::executor::projection::PreparedSlotProjectionValidation,
    >,
    retain_slot_rows: bool,
    rows: &mut Vec<KernelRow>,
) -> Result<(), InternalError> {
    if retain_slot_rows && !cursorless_page_window_is_redundant(plan, rows.len()) {
        apply_cursorless_page_window(plan, rows);
    }

    if validate_projection {
        let prepared_projection_validation =
            required_prepared_projection_validation(prepared_projection_validation)?;
        for row in rows {
            crate::db::executor::projection::validate_prepared_projection_row(
                prepared_projection_validation,
                row,
            )?;
        }
    }

    Ok(())
}

// Finalize one cursorless row-collector payload onto the executor-owned
// structural page boundary.
fn finalize_cursorless_row_collector_payload(
    rows: Vec<KernelRow>,
    retain_slot_rows: bool,
) -> Result<MaterializedExecutionPayload, InternalError> {
    if retain_slot_rows {
        return Ok(
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                collect_cursorless_slot_rows(rows)?,
                None,
            ),
        );
    }

    Ok(
        crate::db::executor::pipeline::contracts::StructuralCursorPage::new(
            collect_cursorless_data_rows(rows)?,
            None,
        ),
    )
}

fn collect_cursorless_slot_rows(
    rows: Vec<KernelRow>,
) -> Result<Vec<RetainedSlotRow>, InternalError> {
    rows.into_iter()
        .map(KernelRow::into_retained_slot_row)
        .collect()
}

fn collect_cursorless_data_rows(rows: Vec<KernelRow>) -> Result<Vec<DataRow>, InternalError> {
    rows.into_iter().map(KernelRow::into_data_row).collect()
}

// Apply the cursorless LIMIT/OFFSET window directly on the collected row set
// when the route already guarantees final order and the outer surface does not
// retain scalar continuation state.
fn apply_cursorless_page_window<T>(plan: &AccessPlannedQuery, rows: &mut Vec<T>) {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return;
    };

    let total = rows.len();
    let start = usize::try_from(page.offset)
        .unwrap_or(usize::MAX)
        .min(total);
    let end = match page.limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };
    if start == 0 {
        rows.truncate(end);
        return;
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);
}

// Require the prepared projection-validation bundle whenever one retained-slot
// path still asks the shared executor validator to run.
fn required_prepared_projection_validation(
    prepared_projection_validation: Option<
        &crate::db::executor::projection::PreparedSlotProjectionValidation,
    >,
) -> Result<&crate::db::executor::projection::PreparedSlotProjectionValidation, InternalError> {
    prepared_projection_validation.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "retained-slot projection validation requires prepared projection state",
        )
    })
}
