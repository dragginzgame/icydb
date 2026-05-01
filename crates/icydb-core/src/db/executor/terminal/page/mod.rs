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

use crate::{
    db::{
        data::DataRow,
        executor::{
            OrderReadableRow, measure_execution_stats_phase,
            pipeline::contracts::{
                KernelPageMaterializationRequest, KernelRowsExecutionAttempt,
                MaterializedExecutionPayload,
            },
            projection::ProjectionValidationRow,
            record_projection,
        },
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
pub(in crate::db::executor) use metrics::with_direct_data_row_phase_attribution;
#[cfg(feature = "diagnostics")]
pub use metrics::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use metrics::{
    ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics,
};
pub(in crate::db::executor) use plan::{KernelRowScanStrategy, resolve_cursorless_short_path_plan};
pub(in crate::db) use retained::RetainedSlotRow;
pub(in crate::db::executor) use retained::{RetainedSlotLayout, RetainedSlotValueMode};
pub(in crate::db::executor) use row_runtime::{
    KernelRowPayloadMode, ResidualFilterScanMode, ScalarRowRuntimeHandle, ScalarRowRuntimeState,
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

    #[cfg(test)]
    pub(in crate::db) fn slot(&self, slot: usize) -> Option<Value> {
        self.slot_ref(slot).cloned()
    }

    pub(in crate::db) fn into_data_row(self) -> Result<DataRow, InternalError> {
        self.data_row.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "slot-only kernel row reached data-row materialization path",
            )
        })
    }

    pub(in crate::db::executor) fn into_retained_slot_row(
        self,
    ) -> Result<RetainedSlotRow, InternalError> {
        match self.slots {
            KernelRowSlots::NotMaterialized => Err(InternalError::query_executor_invariant(
                "data-row-only kernel row reached retained-slot materialization path",
            )),
            KernelRowSlots::Dense(slots) => Ok(RetainedSlotRow::from_dense_slots(slots)),
            KernelRowSlots::Retained(slots) => Ok(slots),
        }
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn into_parts(self) -> Result<(DataRow, Vec<Option<Value>>), InternalError> {
        let Self { data_row, slots } = self;
        let data_row = data_row.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "slot-only kernel row reached delete row materialization path",
            )
        })?;

        let slots = match slots {
            KernelRowSlots::NotMaterialized => {
                return Err(InternalError::query_executor_invariant(
                    "data-row-only kernel row reached delete row materialization path",
                ));
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
    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_ref(slot).map(Cow::Borrowed)
    }

    fn order_slots_are_borrowed(&self) -> bool {
        !matches!(self.slots, KernelRowSlots::NotMaterialized)
    }
}

/// Materialize one ordered key stream into one execution payload.
pub(in crate::db::executor) fn materialize_key_stream_into_execution_payload<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<(MaterializedExecutionPayload, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        authority,
        plan,
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
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
            load_order_route_contract,
            consistency,
            continuation,
            row_runtime,
            direct_data_row_path,
        );
    }

    // Phase 1: run the shared scalar page kernel against typed boundary callbacks.
    let (mut rows, rows_scanned) =
        execute_scalar_page_kernel_dyn(scalar_materialization_plan.kernel_request(
            key_stream,
            scan_budget_hint,
            load_order_route_contract,
            consistency,
            continuation,
            row_runtime,
        ))?;

    // Phase 2: apply post-access phases and only retain the shared projection
    // validation pass for surfaces that are not about to materialize the same
    // projection immediately afterwards.
    let rows_after_cursor = apply_post_access_to_kernel_rows_dyn(
        plan,
        &mut rows,
        continuation.cursor_boundary(),
        scalar_materialization_plan.post_access_strategy(),
    )?;
    scalar_materialization_plan.apply_post_scan_tail(plan, &mut rows)?;

    // Phase 3: assemble the structural cursor boundary before typed page emission.
    let post_access_rows = rows.len();
    let next_cursor = build_scalar_page_cursor(
        authority,
        plan,
        rows.as_slice(),
        scalar_materialization_plan.cursor_emission(),
        rows_after_cursor,
        continuation,
        direction,
    )?;

    // Phase 4: select the final payload shape once, then build it in one
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
pub(in crate::db::executor) fn materialize_key_stream_into_kernel_rows<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<KernelRowsExecutionAttempt, InternalError> {
    let KernelPageMaterializationRequest {
        authority: _,
        plan,
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        capabilities,
        consistency,
        continuation,
        direction: _,
    } = request;
    let scalar_materialization_plan = resolve_scalar_materialization_plan(plan, capabilities)?;
    if scalar_materialization_plan.direct_data_row_path().is_some() {
        return Err(InternalError::query_executor_invariant(
            "scalar aggregate kernel rows require the retained-slot kernel path",
        ));
    }

    // Phase 1: scan through the same scalar kernel used by structural page
    // materialization so residual filtering and row-read accounting stay shared.
    let (mut rows, rows_scanned) =
        execute_scalar_page_kernel_dyn(scalar_materialization_plan.kernel_request(
            key_stream,
            scan_budget_hint,
            load_order_route_contract,
            consistency,
            continuation,
            row_runtime,
        ))?;

    // Phase 2: apply the same post-access and post-scan windowing as the retained
    // slot page path, then stop before cursor construction and payload shaping.
    apply_post_access_to_kernel_rows_dyn(
        plan,
        &mut rows,
        continuation.cursor_boundary(),
        scalar_materialization_plan.post_access_strategy(),
    )?;
    scalar_materialization_plan.apply_post_scan_tail(plan, &mut rows)?;
    let post_access_rows = rows.len();

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
