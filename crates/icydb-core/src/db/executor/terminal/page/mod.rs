//! Module: executor::terminal::page
//! Responsibility: materialize ordered key streams into cursor-paged read rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by scalar execution paths.

mod metrics;
mod plan;
mod post_scan;
mod retained;
mod scan;

use crate::{
    db::{
        cursor::{CursorBoundary, MaterializedCursorRow, next_cursor_for_materialized_rows},
        data::{DataKey, DataRow, RawRow},
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionKernel, ExecutorError, OrderReadableRow, OrderedKeyStream,
            ScalarContinuationContext, apply_structural_order_window,
            apply_structural_order_window_to_data_rows, compare_orderable_row_with_boundary,
            order::cursor_boundary_from_orderable_row,
            pipeline::contracts::{
                CursorEmissionMode, MaterializedExecutionPayload, PageCursor, StructuralCursorPage,
            },
            projection::{PreparedSlotProjectionValidation, ProjectionValidationRow},
            route::{LoadOrderRouteContract, access_order_satisfied_by_route_contract},
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::{AccessPlannedQuery, ResolvedOrder},
        registry::StoreHandle,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

use crate::db::executor::terminal::{RowDecoder, RowLayout};
#[cfg(feature = "perf-attribution")]
use metrics::{
    measure_direct_data_row_phase, record_direct_data_row_key_encode_local_instructions,
    record_direct_data_row_order_window_local_instructions,
    record_direct_data_row_page_window_local_instructions,
    record_direct_data_row_store_get_local_instructions,
};
#[cfg(any(test, feature = "structural-read-metrics"))]
use metrics::{
    record_direct_data_row_path_hit, record_direct_filtered_data_row_path_hit,
    record_kernel_data_row_path_hit, record_kernel_full_row_retained_path_hit,
    record_kernel_slots_only_path_hit,
};
use plan::{
    DirectDataRowPath, PostAccessPredicateStrategy, PostAccessStrategy,
    resolve_scalar_materialization_plan,
};
use scan::{
    execute_scalar_page_kernel_dyn, predicate_matches_retained_values,
    scan_direct_data_rows_with_residual_policy, scan_materialized_order_direct_data_rows,
};

#[cfg(feature = "perf-attribution")]
pub(in crate::db::executor) use metrics::with_direct_data_row_phase_attribution;
#[cfg(feature = "structural-read-metrics")]
pub use metrics::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "structural-read-metrics")))]
pub(crate) use metrics::{
    ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics,
};
pub(in crate::db::executor) use plan::{KernelRowScanStrategy, resolve_cursorless_short_path_plan};
pub(in crate::db::executor) use retained::RetainedSlotLayout;
pub(in crate::db) use retained::RetainedSlotRow;
pub(in crate::db::executor) use scan::{KernelRowScanRequest, execute_kernel_row_scan};

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
/// ScalarRowRuntimeState
///
/// ScalarRowRuntimeState is the concrete scalar row reader shared by the
/// executor's structural load paths.
/// It keeps store access plus precomputed decode metadata together so row
/// loops can call one fixed runtime shape without rebuilding decode state.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor) struct ScalarRowRuntimeState {
    store: StoreHandle,
    row_layout: RowLayout,
}

impl ScalarRowRuntimeState {
    /// Build one structural scalar row-runtime descriptor from resolved
    /// boundary inputs.
    #[must_use]
    pub(in crate::db::executor) const fn new(store: StoreHandle, row_layout: RowLayout) -> Self {
        Self { store, row_layout }
    }

    // Read one raw row through the structural store handle while preserving
    // the scalar missing-row consistency contract.
    fn read_row(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
    ) -> Result<Option<RawRow>, InternalError> {
        #[cfg(feature = "perf-attribution")]
        let (key_encode_local_instructions, raw_key_result) =
            measure_direct_data_row_phase(|| key.to_raw());
        #[cfg(not(feature = "perf-attribution"))]
        let raw_key_result = key.to_raw();
        let raw_key = raw_key_result?;
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_key_encode_local_instructions(key_encode_local_instructions);

        #[cfg(feature = "perf-attribution")]
        let (store_get_local_instructions, row) = measure_direct_data_row_phase(|| {
            Ok::<_, InternalError>(self.store.with_data(|store| store.get(&raw_key)))
        });
        #[cfg(not(feature = "perf-attribution"))]
        let row = self.store.with_data(|store| store.get(&raw_key));
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_store_get_local_instructions(store_get_local_instructions);
        #[cfg(feature = "perf-attribution")]
        let row = row?;

        match consistency {
            MissingRowPolicy::Error => row
                .map(Some)
                .ok_or_else(|| InternalError::from(ExecutorError::missing_row(key))),
            MissingRowPolicy::Ignore => Ok(row),
        }
    }

    // Read one full structural row without decoding any slot values when the
    // caller can prove no later executor phase will consume them.
    fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(data_row) = self.read_data_row(consistency, key)? else {
            return Ok(None);
        };

        Ok(Some(KernelRow::new_data_row_only(data_row)))
    }

    // Read one canonical structural data row without constructing one
    // intermediate kernel-row envelope.
    fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<DataRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };

        Ok(Some((key, row)))
    }

    // Read one canonical structural data row and drop it early when the
    // residual predicate rejects the retained slot values needed by scan-time
    // filtering.
    fn read_data_row_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<DataRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let retained_values = RowDecoder::decode_indexed_slot_values(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        if !predicate_matches_retained_values(
            predicate_program,
            retained_slot_layout,
            retained_values.as_slice(),
        ) {
            return Ok(None);
        }

        Ok(Some((key, row)))
    }

    // Decode one full structural row while retaining only one caller-declared
    // slot subset alongside the canonical data row.
    fn read_full_row_retained(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let retained_slots = RowDecoder::decode_retained_slots(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        let data_row = (key, row);

        Ok(Some(KernelRow::new_with_retained_slots(
            data_row,
            retained_slots,
        )))
    }

    // Decode one retained full structural row and drop it early when the
    // residual predicate rejects the retained slot values.
    fn read_full_row_retained_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, &key)? else {
            return Ok(None);
        };
        let retained_values = RowDecoder::decode_indexed_slot_values(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        if !predicate_matches_retained_values(
            predicate_program,
            retained_slot_layout,
            retained_values.as_slice(),
        ) {
            return Ok(None);
        }

        Ok(Some(KernelRow::new_with_retained_slots(
            (key, row),
            RetainedSlotRow::from_indexed_values(retained_slot_layout, retained_values),
        )))
    }

    // Decode one compact slot-only structural row under the shared retained layout.
    fn read_slot_only(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, key)? else {
            return Ok(None);
        };
        let slots = RowDecoder::decode_retained_slots(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;

        Ok(Some(KernelRow::new_slot_only(slots)))
    }

    // Decode one compact slot-only structural row and drop it early when the
    // residual predicate rejects the materialized slot values.
    fn read_slot_only_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        let Some(row) = self.read_row(consistency, key)? else {
            return Ok(None);
        };
        let retained_values = RowDecoder::decode_indexed_slot_values(
            &self.row_layout,
            key.storage_key(),
            &row,
            retained_slot_layout,
        )?;
        if !predicate_matches_retained_values(
            predicate_program,
            retained_slot_layout,
            retained_values.as_slice(),
        ) {
            return Ok(None);
        }

        Ok(Some(KernelRow::new_slot_only(
            RetainedSlotRow::from_indexed_values(retained_slot_layout, retained_values),
        )))
    }
}

///
/// KernelRowPayloadMode
///
/// KernelRowPayloadMode selects whether shared scalar row production must keep
/// a full `DataRow` payload or only decoded slot values.
/// Slot-only rows are valid for no-cursor retained-slot materialization lanes
/// that never reconstruct entity rows or continuation anchors.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum KernelRowPayloadMode {
    DataRowOnly,
    FullRowRetained,
    SlotsOnly,
}

///
/// ScalarRowRuntimeHandle
///
/// ScalarRowRuntimeHandle is the borrowed structural row reader passed through
/// the shared scalar page kernels.
/// It keeps the hot loop on one concrete runtime shape while the typed
/// boundary still owns store and decode authority.
///

pub(in crate::db::executor) struct ScalarRowRuntimeHandle<'a> {
    state: &'a ScalarRowRuntimeState,
}

impl<'a> ScalarRowRuntimeHandle<'a> {
    /// Borrow one pre-resolved row-runtime state object behind a structural
    /// runtime handle without rebuilding owned runtime state for the same
    /// query execution.
    #[must_use]
    pub(in crate::db::executor) const fn from_borrowed(state: &'a ScalarRowRuntimeState) -> Self {
        Self { state }
    }

    /// Borrow the authority-owned row layout used by raw-row materialization
    /// and direct raw-row order caching.
    #[must_use]
    pub(in crate::db::executor) const fn row_layout(&self) -> RowLayout {
        self.state.row_layout
    }

    /// Read one structural data row without decoding any slot payload.
    pub(in crate::db::executor) fn read_data_row_only(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_data_row_only(consistency, key)
    }

    /// Read one canonical structural data row without constructing one
    /// intermediate kernel-row envelope.
    pub(in crate::db::executor) fn read_data_row(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state.read_data_row(consistency, key)
    }

    /// Read one canonical structural data row and apply the residual
    /// predicate before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_data_row_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<DataRow>, InternalError> {
        self.state.read_data_row_with_predicate(
            consistency,
            key,
            predicate_program,
            retained_slot_layout,
        )
    }

    /// Read one full structural row while retaining only one shared compact
    /// slot subset alongside the canonical data row.
    pub(in crate::db::executor) fn read_full_row_retained(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state
            .read_full_row_retained(consistency, key, retained_slot_layout)
    }

    /// Read one retained full structural row and apply the residual predicate
    /// before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_full_row_retained_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_full_row_retained_with_predicate(
            consistency,
            key,
            predicate_program,
            retained_slot_layout,
        )
    }

    /// Read one compact slot-only structural row from one data key.
    pub(in crate::db::executor) fn read_slot_only(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state
            .read_slot_only(consistency, key, retained_slot_layout)
    }

    /// Read one compact slot-only structural row and apply the residual
    /// predicate before the row enters shared kernel control flow.
    pub(in crate::db::executor) fn read_slot_only_with_predicate(
        &self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        predicate_program: &PredicateProgram,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Option<KernelRow>, InternalError> {
        self.state.read_slot_only_with_predicate(
            consistency,
            key,
            predicate_program,
            retained_slot_layout,
        )
    }
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
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
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

///
/// ResidualPredicateScanMode
///
/// ResidualPredicateScanMode keeps the scan-owned residual predicate contract
/// explicit instead of overloading a boolean with both logical presence and
/// execution timing. The scalar kernel only needs to know whether no residual
/// predicate exists, whether scan must evaluate it while slot reads are
/// available, or whether post-access must evaluate it later.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ResidualPredicateScanMode {
    Absent,
    AppliedDuringScan,
    DeferredPostAccess,
}

impl ResidualPredicateScanMode {
    /// Select the executor scan contract from the logical residual-predicate
    /// presence plus the row payload capabilities already chosen for this lane.
    #[must_use]
    pub(in crate::db::executor) const fn from_plan_and_layout(
        has_residual_predicate: bool,
        retained_slot_layout: Option<&RetainedSlotLayout>,
    ) -> Self {
        if !has_residual_predicate {
            Self::Absent
        } else if retained_slot_layout.is_some() {
            Self::AppliedDuringScan
        } else {
            Self::DeferredPostAccess
        }
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

// Execute one already-resolved direct `DataRow` strategy through the shared
// direct-lane scan and page-window shell.
#[expect(clippy::too_many_arguments)]
#[expect(clippy::too_many_lines)]
fn execute_direct_data_row_path(
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
    #[cfg(any(test, feature = "structural-read-metrics"))]
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
    #[cfg(feature = "perf-attribution")]
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
                predicate_program,
                retained_slot_layout,
            } => scan_direct_data_rows_with_residual_policy(
                key_stream,
                scan_budget_hint,
                row_keep_cap,
                consistency,
                ResidualPredicateScanMode::AppliedDuringScan,
                row_runtime,
                Some(predicate_program),
                Some(retained_slot_layout),
                "direct filtered data-row path cannot defer residual filtering",
            ),
            DirectDataRowPath::MaterializedOrder {
                residual_predicate_scan_mode,
                predicate_program,
                retained_slot_layout,
                ..
            } => scan_materialized_order_direct_data_rows(
                key_stream,
                scan_budget_hint,
                consistency,
                residual_predicate_scan_mode,
                row_runtime,
                predicate_program,
                retained_slot_layout,
            ),
        });
    #[cfg(not(feature = "perf-attribution"))]
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
            predicate_program,
            retained_slot_layout,
        } => scan_direct_data_rows_with_residual_policy(
            key_stream,
            scan_budget_hint,
            row_keep_cap,
            consistency,
            ResidualPredicateScanMode::AppliedDuringScan,
            row_runtime,
            Some(predicate_program),
            Some(retained_slot_layout),
            "direct filtered data-row path cannot defer residual filtering",
        ),
        DirectDataRowPath::MaterializedOrder {
            residual_predicate_scan_mode,
            predicate_program,
            retained_slot_layout,
            ..
        } => scan_materialized_order_direct_data_rows(
            key_stream,
            scan_budget_hint,
            consistency,
            residual_predicate_scan_mode,
            row_runtime,
            predicate_program,
            retained_slot_layout,
        ),
    };
    let (mut data_rows, rows_scanned) = scan_result?;
    #[cfg(feature = "perf-attribution")]
    record_direct_data_row_scan_local_instructions(scan_local_instructions);

    // Phase 3: materialized-order direct lanes still own one in-memory order
    // pass before the final page window.
    if let DirectDataRowPath::MaterializedOrder { resolved_order, .. } = direct_data_row_path
        && data_rows.len() > 1
    {
        #[cfg(feature = "perf-attribution")]
        let (order_window_local_instructions, order_window_result) =
            measure_direct_data_row_phase(|| {
                apply_structural_order_window_to_data_rows(
                    &mut data_rows,
                    row_runtime.row_layout(),
                    resolved_order,
                    ExecutionKernel::bounded_order_keep_count(plan, None),
                )
            });
        #[cfg(not(feature = "perf-attribution"))]
        apply_structural_order_window_to_data_rows(
            &mut data_rows,
            row_runtime.row_layout(),
            resolved_order,
            ExecutionKernel::bounded_order_keep_count(plan, None),
        )?;
        #[cfg(feature = "perf-attribution")]
        order_window_result?;
        #[cfg(feature = "perf-attribution")]
        record_direct_data_row_order_window_local_instructions(order_window_local_instructions);
    }

    // Phase 4: direct-lane accounting matches the shared kernel path, then
    // the final offset/limit window runs once on canonical data rows.
    let post_access_rows = data_rows.len();
    #[cfg(feature = "perf-attribution")]
    let (page_window_local_instructions, page_window_result) =
        measure_direct_data_row_phase(|| {
            apply_data_row_page_window(plan, &mut data_rows);

            Ok::<(), InternalError>(())
        });
    #[cfg(not(feature = "perf-attribution"))]
    apply_data_row_page_window(plan, &mut data_rows);
    #[cfg(feature = "perf-attribution")]
    page_window_result?;
    #[cfg(feature = "perf-attribution")]
    record_direct_data_row_page_window_local_instructions(page_window_local_instructions);

    Ok((
        StructuralCursorPage::new(data_rows, None),
        rows_scanned,
        post_access_rows,
    ))
}

// Resolve the optional scalar page cursor once from the post-access rows.
fn build_scalar_page_cursor(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
    cursor_emission: CursorEmissionMode,
    rows_after_cursor: usize,
    continuation: &ScalarContinuationContext,
    direction: Direction,
) -> Result<Option<PageCursor>, InternalError> {
    if !cursor_emission.enabled() {
        return Ok(None);
    }

    let post_access_rows = rows.len();
    let last_cursor_row = resolve_last_cursor_row(authority, plan, rows)?;

    Ok(next_cursor_for_materialized_rows(
        &plan.access,
        plan.scalar_plan().order.as_ref(),
        plan.scalar_plan().page.as_ref(),
        post_access_rows,
        last_cursor_row,
        rows_after_cursor,
        continuation.post_access_cursor_boundary(),
        continuation.previous_index_range_anchor(),
        direction,
        continuation.continuation_signature(),
    )?
    .map(PageCursor::Scalar))
}

// Resolve the last structural cursor row before typed response decode.
fn resolve_last_cursor_row(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
) -> Result<Option<MaterializedCursorRow>, InternalError> {
    let Some(resolved_order) = plan.resolved_order() else {
        return Ok(None);
    };
    let Some(row) = rows.last() else {
        return Ok(None);
    };

    // Phase 1: derive the structural boundary from already-materialized row slots.
    let boundary = cursor_boundary_from_orderable_row(row, resolved_order);

    // Phase 2: derive the optional raw index-range anchor once for index-range paths.
    let index_anchor = if let Some((index, _, _, _)) = plan.access.as_index_range_path() {
        let data_key = &row
            .data_row
            .as_ref()
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "slot-only kernel row reached cursor anchor derivation path",
                )
            })?
            .0;
        let mut read_slot = |slot| row.slot_ref(slot);
        authority
            .index_key_from_slot_ref_reader(data_key.storage_key(), index, &mut read_slot)?
            .map(|key| key.to_raw())
    } else {
        None
    };

    Ok(Some(MaterializedCursorRow::new(boundary, index_anchor)))
}

// Run canonical post-access phases over kernel rows.
fn apply_post_access_to_kernel_rows_dyn(
    plan: &AccessPlannedQuery,
    rows: &mut Vec<KernelRow>,
    cursor: Option<&CursorBoundary>,
    post_access_strategy: PostAccessStrategy<'_>,
) -> Result<usize, InternalError> {
    let logical = plan.scalar_plan();

    // Phase 1: predicate filtering.
    let filtered = match post_access_strategy.predicate_strategy {
        PostAccessPredicateStrategy::NotPresent => false,
        PostAccessPredicateStrategy::AppliedDuringScan => true,
        PostAccessPredicateStrategy::Deferred { predicate_program } => {
            if rows.is_empty() {
                return Ok(0);
            }

            compact_kernel_rows_in_place(rows, |row| {
                let mut read_slot = |slot| row.slot_ref(slot);
                predicate_program.eval_with_slot_value_ref_reader(&mut read_slot)
            });

            true
        }
    };

    // Phase 2: ordering.
    let mut ordered = false;
    let mut rows_after_order = rows.len();
    if let Some(order) = logical.order.as_ref()
        && !order.fields.is_empty()
    {
        if post_access_strategy
            .predicate_strategy
            .requires_post_access_filtering()
            && !filtered
        {
            return Err(InternalError::scalar_page_ordering_after_filtering_required());
        }

        ordered = true;
        if !access_order_satisfied_by_route_contract(plan) {
            let resolved_order = resolved_order_required(plan)?;
            let ordered_total = rows.len();

            if rows.len() > 1 {
                apply_structural_order_window(
                    rows,
                    resolved_order,
                    ExecutionKernel::bounded_order_keep_count(plan, cursor),
                );
            }
            rows_after_order = ordered_total;
        }
    }

    // Phase 3: continuation boundary.
    let rows_after_cursor = if logical.mode.is_load() {
        if cursor.is_some() {
            if logical.order.is_none() {
                return Err(InternalError::scalar_page_cursor_boundary_order_required());
            }
            if !ordered {
                return Err(InternalError::scalar_page_cursor_boundary_after_ordering_required());
            }
        }
        if logical
            .page
            .as_ref()
            .is_some_and(|_| logical.order.is_some() && !ordered)
        {
            return Err(InternalError::scalar_page_pagination_after_ordering_required());
        }
        if post_access_strategy.defer_retained_slot_distinct_window {
            rows_after_order
        } else {
            let resolved_order = cursor.map(|_| resolved_order_required(plan)).transpose()?;

            apply_load_cursor_and_pagination_window(
                rows,
                cursor
                    .zip(resolved_order)
                    .map(|(boundary, resolved_order)| (resolved_order, boundary)),
                ExecutionKernel::effective_page_offset(plan, cursor),
                logical.page.as_ref().and_then(|page| page.limit),
            )
        }
    } else {
        rows_after_order
    };

    // Phase 5: apply the ordered delete window.
    if logical.mode.is_delete()
        && let Some(delete_window) = logical.delete_limit.as_ref()
    {
        if logical.order.is_some() && !ordered {
            return Err(InternalError::scalar_page_delete_limit_after_ordering_required());
        }
        apply_delete_window(rows, delete_window.offset, delete_window.limit);
    }

    Ok(rows_after_cursor)
}

fn apply_delete_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let offset = usize::min(rows.len(), offset as usize);
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::min(rows.len(), limit as usize);
        rows.truncate(limit);
    }
}

// Apply one simple cursorless load page window directly on canonical data
// rows when route order is already final and no later slot-aware phase exists.
fn apply_data_row_page_window(plan: &AccessPlannedQuery, rows: &mut Vec<DataRow>) {
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

// Compact kernel rows in place under one keep predicate so row filtering stays
// on one straight-line loop instead of `Vec::retain`'s generic callback path.
fn compact_kernel_rows_in_place(
    rows: &mut Vec<KernelRow>,
    mut keep_row: impl FnMut(&KernelRow) -> bool,
) -> usize {
    let mut kept = 0usize;

    for read_index in 0..rows.len() {
        if !keep_row(&rows[read_index]) {
            continue;
        }

        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);

    kept
}

// Apply the ordered-load continuation boundary and page window in one in-place
// compaction pass so rows do not go through separate retain, drain, and
// truncate passes after materialization.
fn apply_load_cursor_and_pagination_window(
    rows: &mut Vec<KernelRow>,
    cursor: Option<(&ResolvedOrder, &CursorBoundary)>,
    offset: u32,
    limit: Option<u32>,
) -> usize {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let mut kept_after_cursor = 0usize;
    let mut kept_after_page = 0usize;
    let mut limit_remaining = limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    for read_index in 0..rows.len() {
        let passes_cursor = match cursor {
            Some((resolved_order, boundary)) => {
                compare_orderable_row_with_boundary(&rows[read_index], resolved_order, boundary)
                    .is_gt()
            }
            None => true,
        };
        if !passes_cursor {
            continue;
        }

        kept_after_cursor = kept_after_cursor.saturating_add(1);
        if kept_after_cursor <= offset {
            continue;
        }
        if limit_remaining.is_some_and(|remaining| remaining == 0) {
            continue;
        }

        if let Some(remaining) = limit_remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }

        if kept_after_page != read_index {
            rows.swap(kept_after_page, read_index);
        }
        kept_after_page = kept_after_page.saturating_add(1);
    }

    rows.truncate(kept_after_page);

    kept_after_cursor
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        cursor::CursorBoundarySlot,
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
            ResidualPredicateScanMode::from_plan_and_layout(false, None),
            ResidualPredicateScanMode::Absent
        );
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(true, None),
            ResidualPredicateScanMode::DeferredPostAccess
        );
        assert_eq!(
            ResidualPredicateScanMode::from_plan_and_layout(
                true,
                Some(&RetainedSlotLayout::compile(2, vec![0]))
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
