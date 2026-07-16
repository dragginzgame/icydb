//! Module: executor::terminal::page
//! Responsibility: materialize ordered key streams into cursor-paged read rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by scalar execution paths.

mod cursor;
mod direct_path;
mod metrics;
mod plan;
mod post_access;
mod post_scan;
mod retained;
mod row_runtime;
mod scan;
#[cfg(test)]
mod tests;

#[cfg(feature = "sql")]
use crate::db::executor::pipeline::contracts::KernelRowsExecutionAttempt;
use crate::{
    db::{
        data::DataRow,
        executor::{
            OrderReadableRow, OrderedKeyStreamBox, ScalarContinuationContext,
            measure_execution_stats_phase,
            pipeline::contracts::{KernelPageMaterializationRequest, StructuralCursorPage},
            projection::ProjectionValidationRow,
            record_projection,
            route::LoadOrderRouteMode,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

use cursor::build_scalar_page_cursor;
use direct_path::execute_direct_data_row_path;
use plan::resolve_scalar_materialization_plan;
use post_access::apply_post_access_to_kernel_rows_dyn;
use scan::execute_scalar_page_kernel_dyn;

#[cfg(feature = "diagnostics")]
pub(in crate::db) use metrics::{
    DirectDataRowPhaseAttribution, KernelRowPhaseAttribution,
    with_direct_data_row_phase_attribution, with_kernel_row_phase_attribution,
};
#[cfg(feature = "diagnostics")]
pub use metrics::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use metrics::{
    ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics,
};
pub(in crate::db::executor) use plan::{
    KernelRowOrderWindow, KernelRowScanStrategy, resolve_cursorless_short_path_plan,
};
pub(in crate::db) use retained::RetainedSlotRow;
pub(in crate::db::executor) use retained::{RetainedSlotLayout, RetainedSlotValueMode};
pub(in crate::db::executor) use row_runtime::{
    KernelRowPayloadMode, ScalarRowRuntimeHandle, ScalarRowRuntimeState,
};
pub(in crate::db::executor) use scan::execute_kernel_row_scan;

///
/// KernelRow
///
/// Non-generic scalar-kernel row envelope used by shared ordering/cursor/page
/// control flow before conversion back to typed `(Id<E>, E)` rows.
///

pub(in crate::db) struct KernelRow {
    data_row: Option<DataRow>,
    slots: KernelRowSlots,
}

enum KernelRowSlots {
    NotMaterialized,
    Dense(Vec<Option<Value>>),
    Retained(RetainedSlotRow),
}

impl KernelRow {
    /// Build one structural kernel row from canonical data-row storage plus
    /// slot-indexed runtime values.
    #[must_use]
    pub(in crate::db) const fn new(data_row: DataRow, slots: Vec<Option<Value>>) -> Self {
        Self {
            data_row: Some(data_row),
            slots: KernelRowSlots::Dense(slots),
        }
    }

    /// Build one structural kernel row that keeps only the canonical data row.
    #[must_use]
    pub(in crate::db::executor) const fn new_data_row_only(data_row: DataRow) -> Self {
        Self {
            data_row: Some(data_row),
            slots: KernelRowSlots::NotMaterialized,
        }
    }

    /// Build one structural kernel row from canonical data-row storage plus
    /// one compact retained-slot row.
    #[must_use]
    pub(in crate::db::executor) const fn new_with_retained_slots(
        data_row: DataRow,
        slots: RetainedSlotRow,
    ) -> Self {
        Self {
            data_row: Some(data_row),
            slots: KernelRowSlots::Retained(slots),
        }
    }

    /// Build one structural kernel row that retains only decoded slot values.
    #[must_use]
    pub(in crate::db::executor) const fn new_slot_only(slots: RetainedSlotRow) -> Self {
        Self {
            data_row: None,
            slots: KernelRowSlots::Retained(slots),
        }
    }

    /// Borrow one decoded slot value without cloning it back out of the
    /// structural row cache.
    #[must_use]
    pub(in crate::db) fn slot_ref(&self, slot: usize) -> Option<&Value> {
        match &self.slots {
            KernelRowSlots::NotMaterialized => None,
            KernelRowSlots::Dense(slots) => slots.get(slot).and_then(Option::as_ref),
            KernelRowSlots::Retained(slots) => slots.slot_ref(slot),
        }
    }

    /// Return whether this row carries decoded slot values for slot-aware
    /// post-access phases such as in-memory ordering and cursor boundaries.
    #[must_use]
    pub(in crate::db::executor) const fn has_materialized_slots(&self) -> bool {
        !matches!(self.slots, KernelRowSlots::NotMaterialized)
    }

    #[cfg(test)]
    pub(in crate::db) fn slot(&self, slot: usize) -> Option<Value> {
        self.slot_ref(slot).cloned()
    }

    pub(in crate::db) fn into_data_row(self) -> Result<DataRow, InternalError> {
        self.data_row
            .ok_or_else(InternalError::query_executor_invariant)
    }

    pub(in crate::db::executor) fn into_retained_slot_row(
        self,
    ) -> Result<RetainedSlotRow, InternalError> {
        match self.slots {
            KernelRowSlots::NotMaterialized => Err(InternalError::query_executor_invariant()),
            KernelRowSlots::Dense(slots) => Ok(RetainedSlotRow::from_dense_slots(slots)),
            KernelRowSlots::Retained(slots) => Ok(slots),
        }
    }
    pub(in crate::db) fn into_data_row_and_slots(
        self,
    ) -> Result<(DataRow, Vec<Option<Value>>), InternalError> {
        let Self { data_row, slots } = self;
        let data_row = data_row.ok_or_else(InternalError::query_executor_invariant)?;

        let slots = match slots {
            KernelRowSlots::NotMaterialized => {
                return Err(InternalError::query_executor_invariant());
            }
            KernelRowSlots::Dense(slots) => slots,
            KernelRowSlots::Retained(slots) => slots.into_dense_slots(),
        };

        Ok((data_row, slots))
    }
}

impl ProjectionValidationRow for KernelRow {
    fn projection_validation_slot_value(&self, slot: usize) -> Option<&Value> {
        self.slot_ref(slot)
    }
}

impl OrderReadableRow for KernelRow {
    fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
        self.slot_ref(slot)
    }

    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_ref(slot).map(Cow::Borrowed)
    }

    fn order_slots_are_borrowed(&self) -> bool {
        self.has_materialized_slots()
    }
}

struct WindowedKernelRowsRequest<'a, 'r> {
    plan: &'a AccessPlannedQuery,
    key_stream: &'a mut OrderedKeyStreamBox,
    scan_budget_hint: Option<usize>,
    load_order_route_mode: LoadOrderRouteMode,
    consistency: MissingRowPolicy,
    continuation: &'a ScalarContinuationContext,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

struct WindowedKernelRows {
    rows: Vec<KernelRow>,
    rows_scanned: usize,
    rows_after_cursor: usize,
    post_access_rows: usize,
}

fn scan_key_stream_into_windowed_kernel_rows<'a>(
    scalar_materialization_plan: &plan::ScalarMaterializationPlan<'a>,
    request: WindowedKernelRowsRequest<'a, '_>,
) -> Result<WindowedKernelRows, InternalError> {
    let WindowedKernelRowsRequest {
        plan,
        key_stream,
        scan_budget_hint,
        load_order_route_mode,
        consistency,
        continuation,
        row_runtime,
    } = request;

    let (mut rows, rows_scanned) =
        execute_scalar_page_kernel_dyn(scalar_materialization_plan.kernel_request(
            plan,
            key_stream,
            scan_budget_hint,
            load_order_route_mode,
            consistency,
            continuation,
            row_runtime,
        )?)?;
    let rows_after_cursor = apply_post_access_to_kernel_rows_dyn(
        plan,
        &mut rows,
        continuation.cursor_boundary(),
        scalar_materialization_plan.defer_retained_slot_distinct_window(),
    )?;
    scalar_materialization_plan.apply_post_scan_tail(plan, &mut rows)?;
    let post_access_rows = rows.len();

    Ok(WindowedKernelRows {
        rows,
        rows_scanned,
        rows_after_cursor,
        post_access_rows,
    })
}

/// Materialize one ordered key stream into one execution payload.
pub(in crate::db::executor) fn materialize_key_stream_into_execution_payload<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<(StructuralCursorPage, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        authority,
        plan,
        key_stream,
        scan_budget_hint,
        load_order_route_mode,
        capabilities,
        consistency,
        continuation,
        direction,
    } = request;
    let scalar_materialization_plan = resolve_scalar_materialization_plan(plan, capabilities)?;
    if let Some(direct_data_row_path) = scalar_materialization_plan.direct_data_row_path() {
        return execute_direct_data_row_path(
            plan,
            key_stream,
            scan_budget_hint,
            load_order_route_mode,
            consistency,
            continuation,
            row_runtime,
            direct_data_row_path,
        );
    }

    // Phase 1: run the shared scalar page kernel and post-access windowing.
    let WindowedKernelRows {
        rows,
        rows_scanned,
        rows_after_cursor,
        post_access_rows,
    } = scan_key_stream_into_windowed_kernel_rows(
        &scalar_materialization_plan,
        WindowedKernelRowsRequest {
            plan,
            key_stream,
            scan_budget_hint,
            load_order_route_mode,
            consistency,
            continuation,
            row_runtime,
        },
    )?;

    // Phase 2: assemble the structural cursor boundary before typed page emission.
    let next_cursor = build_scalar_page_cursor(
        authority,
        plan,
        rows.as_slice(),
        scalar_materialization_plan.cursor_emission(),
        rows_after_cursor,
        continuation,
        direction,
    )?;

    // Phase 3: select the final payload shape once, then build it in one
    // explicit kernel-row shaping pass.
    let (payload, projection_micros) = measure_execution_stats_phase(|| {
        scalar_materialization_plan.finalize_payload(rows, next_cursor)
    });
    let payload = payload?;
    record_projection(payload.row_count(), projection_micros);

    Ok((payload, rows_scanned, post_access_rows))
}

/// Materialize one ordered key stream through scalar post-access phases and
/// return kernel rows before structural page payload shaping.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn materialize_key_stream_into_kernel_rows<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<KernelRowsExecutionAttempt, InternalError> {
    let KernelPageMaterializationRequest {
        authority: _,
        plan,
        key_stream,
        scan_budget_hint,
        load_order_route_mode,
        capabilities,
        consistency,
        continuation,
        direction: _,
    } = request;
    let scalar_materialization_plan = resolve_scalar_materialization_plan(plan, capabilities)?;
    if scalar_materialization_plan.direct_data_row_path().is_some() {
        return Err(InternalError::query_executor_invariant());
    }

    // Scan through the same scalar kernel and windowing helper as structural
    // page materialization, then stop before cursor construction and payload
    // shaping.
    let WindowedKernelRows {
        rows,
        rows_scanned,
        post_access_rows,
        rows_after_cursor: _,
    } = scan_key_stream_into_windowed_kernel_rows(
        &scalar_materialization_plan,
        WindowedKernelRowsRequest {
            plan,
            key_stream,
            scan_budget_hint,
            load_order_route_mode,
            consistency,
            continuation,
            row_runtime,
        },
    )?;

    Ok(KernelRowsExecutionAttempt {
        rows,
        rows_scanned,
        post_access_rows,
        optimization: None,
        index_predicate_applied: false,
        index_predicate_keys_rejected: 0,
        distinct_keys_deduped: 0,
    })
}
