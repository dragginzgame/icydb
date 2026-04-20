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

use crate::{
    db::{
        data::DataRow,
        direction::Direction,
        executor::{
            EntityAuthority, OrderReadableRow, OrderedKeyStream, ScalarContinuationContext,
            pipeline::contracts::{CursorEmissionMode, MaterializedExecutionPayload},
            projection::{PreparedSlotProjectionValidation, ProjectionValidationRow},
            route::LoadOrderRouteContract,
        },
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram, ResolvedOrder},
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
#[cfg(test)]
pub(crate) use post_access::{
    apply_load_cursor_and_pagination_window, compact_kernel_rows_in_place,
};
pub(in crate::db::executor) use retained::RetainedSlotLayout;
pub(in crate::db) use retained::RetainedSlotRow;
pub(in crate::db::executor) use row_runtime::{
    KernelRowPayloadMode, ResidualPredicateScanMode, ScalarRowRuntimeHandle, ScalarRowRuntimeState,
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

    pub(in crate::db) fn into_slots(self) -> Result<Vec<Option<Value>>, InternalError> {
        match self.slots {
            KernelRowSlots::NotMaterialized => Err(InternalError::query_executor_invariant(
                "data-row-only kernel row reached slot materialization path",
            )),
            KernelRowSlots::Dense(slots) => Ok(slots),
            KernelRowSlots::Retained(slots) => Ok(slots.into_dense_slots()),
        }
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
}

fn resolved_order_required(plan: &AccessPlannedQuery) -> Result<&ResolvedOrder, InternalError> {
    plan.resolved_order().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "ordered execution must consume one planner-frozen resolved order program",
        )
    })
}

///
/// KernelPageMaterializationRequest
///
/// Structural inputs for one shared scalar page-materialization pass.
/// This keeps the kernel loop monomorphic while boundary adapters supply only
/// store access and outer typed response reconstruction.
///

///
/// ScalarMaterializationCapabilities
///
/// ScalarMaterializationCapabilities carries the raw scalar-page execution
/// capabilities recovered before the terminal runtime runs.
/// It is intentionally capability-only data: the terminal resolver decides
/// policy from this bundle once instead of reinterpreting layout and cursor
/// fields across multiple sibling branches.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct ScalarMaterializationCapabilities<'a> {
    pub(in crate::db::executor) residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
}

pub(in crate::db::executor) struct KernelPageMaterializationRequest<'a> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) capabilities: ScalarMaterializationCapabilities<'a>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: &'a ScalarContinuationContext,
    pub(in crate::db::executor) direction: Direction,
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
        continuation.post_access_cursor_boundary(),
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
    let payload = scalar_materialization_plan.finalize_payload(rows, next_cursor)?;

    Ok((payload, rows_scanned, post_access_rows))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        cursor::{CursorBoundary, CursorBoundarySlot},
        executor::terminal::page::metrics::{
            record_direct_data_row_path_hit, record_direct_filtered_data_row_path_hit,
            record_kernel_data_row_path_hit, record_kernel_full_row_retained_path_hit,
            record_kernel_slots_only_path_hit,
        },
        query::plan::{OrderDirection, ResolvedOrderField, ResolvedOrderValueSource},
    };

    fn kernel_row_u64(value: u64) -> KernelRow {
        KernelRow::new_slot_only(RetainedSlotRow::new(1, vec![(0, Value::Uint(value))]))
    }

    fn direct_field_order(slot: usize) -> ResolvedOrder {
        ResolvedOrder::new(vec![ResolvedOrderField::new(
            ResolvedOrderValueSource::direct_field(slot),
            OrderDirection::Asc,
        )])
    }

    #[test]
    fn retained_slot_row_slot_ref_and_take_slot_use_indexed_lookup() {
        let mut row = RetainedSlotRow::new(
            8,
            vec![
                (1, Value::Text("alpha".to_string())),
                (5, Value::Uint(7)),
                (3, Value::Bool(true)),
            ],
        );

        assert_eq!(row.slot_ref(5), Some(&Value::Uint(7)));
        assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
        assert_eq!(row.slot_ref(1), None);
        assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
        assert_eq!(row.take_slot(5), Some(Value::Uint(7)));
        assert_eq!(row.slot_ref(5), None);
        assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
    }

    #[test]
    fn retained_slot_row_sparse_constructor_preserves_dense_overwrite_semantics() {
        let row = RetainedSlotRow::new(
            4,
            vec![
                (3, Value::Bool(false)),
                (1, Value::Text("first".to_string())),
                (7, Value::Uint(99)),
                (1, Value::Text("last".to_string())),
            ],
        );

        assert_eq!(
            row.into_dense_slots(),
            vec![
                None,
                Some(Value::Text("last".to_string())),
                None,
                Some(Value::Bool(false)),
            ]
        );
    }

    #[test]
    fn retained_slot_row_indexed_layout_uses_shared_slot_lookup() {
        let layout = RetainedSlotLayout::compile(8, vec![1, 3, 5]);
        let mut row = RetainedSlotRow::from_indexed_values(
            &layout,
            vec![
                Some(Value::Text("alpha".to_string())),
                Some(Value::Bool(true)),
                Some(Value::Uint(7)),
            ],
        );

        assert_eq!(row.slot_ref(5), Some(&Value::Uint(7)));
        assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
        assert_eq!(row.slot_ref(1), None);
        assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
        assert_eq!(row.into_dense_slots()[5], Some(Value::Uint(7)));
    }

    #[test]
    fn residual_predicate_scan_mode_fails_closed_by_row_capability() {
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(false, None, None),
            ResidualPredicateScanMode::Absent
        );
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(true, None, None),
            ResidualPredicateScanMode::DeferredPostAccess
        );
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(
                true,
                Some(&RetainedSlotLayout::compile(2, vec![0])),
                None,
            ),
            ResidualPredicateScanMode::AppliedDuringScan
        );
    }

    #[test]
    fn scalar_materialization_lane_metrics_capture_direct_and_kernel_paths() {
        let ((), metrics) = with_scalar_materialization_lane_metrics(|| {
            record_direct_data_row_path_hit();
            record_direct_filtered_data_row_path_hit();
            record_kernel_data_row_path_hit();
            record_kernel_full_row_retained_path_hit();
            record_kernel_slots_only_path_hit();
        });

        assert_eq!(
            metrics.direct_data_row_path_hits, 1,
            "direct data-row lane should increment once",
        );
        assert_eq!(
            metrics.direct_filtered_data_row_path_hits, 1,
            "direct filtered data-row lane should increment once",
        );
        assert_eq!(
            metrics.kernel_data_row_path_hits, 1,
            "kernel data-row lane should increment once",
        );
        assert_eq!(
            metrics.kernel_full_row_retained_path_hits, 1,
            "kernel retained full-row lane should increment once",
        );
        assert_eq!(
            metrics.kernel_slots_only_path_hits, 1,
            "kernel slot-only lane should increment once",
        );
    }

    #[test]
    fn load_cursor_and_pagination_window_compacts_in_one_pass() {
        let resolved_order = direct_field_order(0);
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(2))],
        };
        let mut rows = vec![
            kernel_row_u64(1),
            kernel_row_u64(2),
            kernel_row_u64(3),
            kernel_row_u64(4),
            kernel_row_u64(5),
        ];

        let rows_after_cursor = apply_load_cursor_and_pagination_window(
            &mut rows,
            Some((&resolved_order, &boundary)),
            1,
            Some(2),
        );

        assert_eq!(rows_after_cursor, 3);
        assert_eq!(
            rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
            vec![Some(Value::Uint(4)), Some(Value::Uint(5))]
        );
    }

    #[test]
    fn load_pagination_window_without_cursor_skips_offset_then_limits() {
        let mut rows = vec![
            kernel_row_u64(10),
            kernel_row_u64(20),
            kernel_row_u64(30),
            kernel_row_u64(40),
        ];

        let rows_after_cursor =
            apply_load_cursor_and_pagination_window(&mut rows, None, 2, Some(1));

        assert_eq!(rows_after_cursor, 4);
        assert_eq!(
            rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
            vec![Some(Value::Uint(30))]
        );
    }

    #[test]
    fn compact_kernel_rows_in_place_preserves_kept_order() {
        let mut rows = vec![
            kernel_row_u64(1),
            kernel_row_u64(2),
            kernel_row_u64(3),
            kernel_row_u64(4),
        ];

        let kept = compact_kernel_rows_in_place(
            &mut rows,
            |row| matches!(row.slot(0), Some(Value::Uint(value)) if value % 2 == 0),
        );

        assert_eq!(kept, 2);
        assert_eq!(
            rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
            vec![Some(Value::Uint(2)), Some(Value::Uint(4))]
        );
    }
}
