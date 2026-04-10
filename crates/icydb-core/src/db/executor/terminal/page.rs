//! Module: executor::terminal::page
//! Responsibility: materialize ordered key streams into cursor-paged read rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by scalar execution paths.

use crate::{
    db::{
        cursor::{CursorBoundary, MaterializedCursorRow, next_cursor_for_materialized_rows},
        data::{DataKey, DataRow},
        executor::{
            BudgetedOrderedKeyStream, EntityAuthority, ExecutionKernel, OrderReadableRow,
            OrderedKeyStream, ScalarContinuationBindings, apply_structural_order_window,
            compare_orderable_row_with_boundary, compute_page_keep_count,
            key_stream_budget_is_redundant,
            order::cursor_boundary_from_orderable_row,
            pipeline::contracts::{CursorEmissionMode, PageCursor, StructuralCursorPage},
            pipeline::operators::PreparedSqlExecutionProjection,
            projection::{PreparedSlotProjectionValidation, validate_prepared_projection_row},
            route::{LoadOrderRouteContract, access_order_satisfied_by_route_contract},
        },
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::{AccessPlannedQuery, ResolvedOrder},
    },
    error::InternalError,
    value::Value,
};
use std::{borrow::Cow, marker::PhantomData, ptr};

///
/// RetainedSlotRow
///
/// RetainedSlotRow keeps only the caller-declared decoded slot values for one
/// SQL-only structural row.
/// The slot-only execution path keeps those retained values in one dense
/// optional slot image so hot slot reads stay on direct indexing instead of
/// per-access scans across sparse `(slot, value)` pairs.
///

pub(in crate::db::executor) struct RetainedSlotRow {
    slots: Vec<Option<Value>>,
}

impl RetainedSlotRow {
    /// Build one retained slot row from sparse decoded `(slot, value)` pairs.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn new(slot_count: usize, entries: Vec<(usize, Value)>) -> Self {
        Self::from_sparse_entries(slot_count, entries)
    }

    /// Build one retained slot row from sparse decoded `(slot, value)` pairs.
    #[must_use]
    pub(in crate::db::executor) fn from_sparse_entries(
        slot_count: usize,
        entries: Vec<(usize, Value)>,
    ) -> Self {
        let mut slots = vec![None; slot_count];
        for (slot, value) in entries {
            if let Some(entry) = slots.get_mut(slot) {
                *entry = Some(value);
            }
        }

        Self { slots }
    }

    /// Build one retained slot row from an already-dense slot image.
    #[must_use]
    pub(in crate::db::executor) const fn from_dense_slots(slots: Vec<Option<Value>>) -> Self {
        Self { slots }
    }

    /// Borrow one retained slot value without cloning it back out of the row.
    #[must_use]
    pub(in crate::db::executor) fn slot_ref(&self, slot: usize) -> Option<&Value> {
        self.slots.get(slot).and_then(Option::as_ref)
    }

    /// Remove one retained slot value by slot index while consuming the row in
    /// direct field-projection paths.
    pub(in crate::db::executor) fn take_slot(&mut self, slot: usize) -> Option<Value> {
        self.slots.get_mut(slot)?.take()
    }

    /// Expand this retained row back into one dense slot vector only when a
    /// caller still requires the legacy full-width slot image.
    #[must_use]
    pub(in crate::db::executor) fn into_dense_slots(self) -> Vec<Option<Value>> {
        self.slots
    }
}

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
            KernelRowSlots::Dense(slots) => slots.get(slot).and_then(Option::as_ref),
            KernelRowSlots::Retained(slots) => slots.slot_ref(slot),
        }
    }

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

    pub(in crate::db) fn into_slots(self) -> Vec<Option<Value>> {
        match self.slots {
            KernelRowSlots::Dense(slots) => slots,
            KernelRowSlots::Retained(slots) => slots.into_dense_slots(),
        }
    }

    pub(in crate::db::executor) fn into_retained_slot_row(self) -> RetainedSlotRow {
        match self.slots {
            KernelRowSlots::Dense(slots) => RetainedSlotRow::from_dense_slots(slots),
            KernelRowSlots::Retained(slots) => slots,
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
            KernelRowSlots::Dense(slots) => slots,
            KernelRowSlots::Retained(slots) => slots.into_dense_slots(),
        };

        Ok((data_row, slots))
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
/// ScalarRowRuntimeVTable
///
/// Structural function-table contract for scalar row production.
/// Typed row decode stays behind this erased handle so the shared scalar loop
/// no longer calls typed closures per row.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct ScalarRowRuntimeVTable {
    pub(in crate::db::executor) read_kernel_row: ScalarRowReadKernelRowFn,
    pub(in crate::db::executor) drop_state: unsafe fn(*mut ()),
}

type ScalarRowReadKernelRowFn = unsafe fn(
    *mut (),
    MissingRowPolicy,
    &DataKey,
    KernelRowPayloadMode,
    bool,
    Option<&PredicateProgram>,
    Option<&[usize]>,
) -> Result<Option<KernelRow>, InternalError>;

///
/// KernelRowPayloadMode
///
/// KernelRowPayloadMode selects whether shared scalar row production must keep
/// a full `DataRow` payload or only decoded slot values.
/// Slot-only rows are valid for no-cursor SQL materialization lanes that never
/// reconstruct entity rows or continuation anchors.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum KernelRowPayloadMode {
    FullRow,
    SlotsOnly,
}

///
/// ScalarRowRuntimeHandle
///
/// Erased scalar row-production/runtime handle used by scalar page
/// materialization.
/// This keeps the hot loop structural while only the typed store-read boundary
/// remains behind one erased state object.
///

pub(in crate::db::executor) struct ScalarRowRuntimeHandle<'a> {
    state: *mut (),
    vtable: ScalarRowRuntimeVTable,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ScalarRowRuntimeHandle<'a> {
    /// Borrow one pre-resolved row-runtime state object behind a structural
    /// runtime handle without rebuilding owned runtime state for the same
    /// query execution.
    #[must_use]
    pub(in crate::db::executor) const fn from_borrowed<T>(
        state: &'a T,
        vtable: ScalarRowRuntimeVTable,
    ) -> Self {
        Self {
            state: ptr::from_ref(state).cast_mut().cast(),
            vtable,
            _marker: PhantomData,
        }
    }

    /// Read one structural kernel row from one data key.
    pub(in crate::db::executor) fn read_kernel_row(
        &mut self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        payload_mode: KernelRowPayloadMode,
        predicate_preapplied: bool,
        predicate_slots: Option<&PredicateProgram>,
        required_slots: Option<&[usize]>,
    ) -> Result<Option<KernelRow>, InternalError> {
        // SAFETY: `state` was allocated by `new`, the vtable matches the
        // erased state type, and the handle has unique mutable access.
        unsafe {
            (self.vtable.read_kernel_row)(
                self.state,
                consistency,
                key,
                payload_mode,
                predicate_preapplied,
                predicate_slots,
                required_slots,
            )
        }
    }
}

impl Drop for ScalarRowRuntimeHandle<'_> {
    fn drop(&mut self) {
        // SAFETY: `state` originates from `Box::into_raw` in `new` and must be
        // reclaimed exactly once when the handle drops.
        unsafe {
            (self.vtable.drop_state)(self.state);
        }
    }
}

///
/// KernelPageMaterializationRequest
///
/// Structural inputs for one shared scalar page-materialization pass.
/// This keeps the kernel loop monomorphic while boundary adapters supply only
/// store access and outer typed response reconstruction.
///

pub(in crate::db::executor) struct KernelPageMaterializationRequest<'a> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) slot_only_required_slots: Option<&'a [usize]>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) prepared_sql_projection: Option<&'a PreparedSqlExecutionProjection>,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

/// Materialize one ordered key stream into one structural scalar cursor page.
pub(in crate::db::executor) fn materialize_key_stream_into_structural_page<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<(StructuralCursorPage, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        authority,
        plan,
        predicate_slots,
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        validate_projection,
        retain_slot_rows,
        slot_only_required_slots,
        prepared_projection_validation,
        #[cfg(feature = "sql")]
            prepared_sql_projection: _prepared_sql_projection,
        cursor_emission,
        consistency,
        continuation,
    } = request;
    let payload_mode = if retain_slot_rows && !cursor_emission.enabled() {
        KernelRowPayloadMode::SlotsOnly
    } else {
        KernelRowPayloadMode::FullRow
    };
    let predicate_preapplied = plan.has_residual_predicate();
    if predicate_preapplied && predicate_slots.is_none() {
        return Err(InternalError::scalar_page_predicate_slots_required());
    }

    // Phase 1: run the shared scalar page kernel against typed boundary callbacks.
    let (mut rows, rows_scanned) = execute_scalar_page_kernel_dyn(ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        consistency,
        payload_mode,
        predicate_slots,
        predicate_preapplied,
        slot_only_required_slots,
        continuation,
        row_runtime,
    })?;

    // Phase 2: apply post-access phases and only retain the shared projection
    // validation pass for surfaces that are not about to materialize the same
    // projection immediately afterwards.
    let rows_after_cursor = apply_post_access_to_kernel_rows_dyn(
        plan,
        &mut rows,
        continuation.post_access_cursor_boundary(),
        predicate_slots,
        predicate_preapplied,
    )?;
    if validate_projection {
        validate_prepared_projection_rows(prepared_projection_validation, rows.as_slice())?;
    }

    // Phase 3: assemble the structural cursor boundary before typed page emission.
    let post_access_rows = rows.len();
    let next_cursor = if cursor_emission.enabled() {
        let last_cursor_row = resolve_last_cursor_row(authority, plan, rows.as_slice())?;

        next_cursor_for_materialized_rows(
            &plan.access,
            plan.scalar_plan().order.as_ref(),
            plan.scalar_plan().page.as_ref(),
            post_access_rows,
            last_cursor_row,
            rows_after_cursor,
            continuation.post_access_cursor_boundary(),
            continuation.previous_index_range_anchor(),
            continuation.direction(),
            continuation.continuation_signature(),
        )?
        .map(PageCursor::Scalar)
    } else {
        None
    };

    // Phase 4: finalize one structural page payload for outer typed decode.
    #[cfg(feature = "sql")]
    let page = if retain_slot_rows {
        let row_count = rows.len();
        let slot_rows = rows
            .into_iter()
            .map(KernelRow::into_retained_slot_row)
            .collect();
        StructuralCursorPage::new_with_slot_rows(slot_rows, row_count, next_cursor)
    } else {
        let data_rows = rows
            .into_iter()
            .map(KernelRow::into_data_row)
            .collect::<Result<Vec<_>, _>>()?;
        StructuralCursorPage::new(data_rows, next_cursor)
    };

    #[cfg(not(feature = "sql"))]
    let page = {
        let _ = retain_slot_rows;
        let data_rows = rows
            .into_iter()
            .map(KernelRow::into_data_row)
            .collect::<Result<Vec<_>, _>>()?;
        StructuralCursorPage::new(data_rows, next_cursor)
    };

    Ok((page, rows_scanned, post_access_rows))
}

// Run the shared slot-row projection validator from already-prepared
// projection state and fail closed if that prepared state was not supplied.
fn validate_prepared_projection_rows(
    prepared_projection_validation: Option<&PreparedSlotProjectionValidation>,
    rows: &[KernelRow],
) -> Result<(), InternalError> {
    let prepared_projection_validation = prepared_projection_validation.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "projection validation requires prepared slot-row projection state",
        )
    })?;
    for row in rows {
        validate_prepared_projection_row(prepared_projection_validation, &mut |slot| {
            row.slot_ref(slot)
        })?;
    }

    Ok(())
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
        let mut read_slot = |slot| row.slot(slot);
        authority
            .index_key_from_slot_reader(data_key.storage_key(), index, &mut read_slot)?
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
    predicate_slots: Option<&PredicateProgram>,
    predicate_preapplied: bool,
) -> Result<usize, InternalError> {
    let logical = plan.scalar_plan();
    let has_residual_predicate = plan.has_residual_predicate();

    // Phase 1: predicate filtering.
    let filtered = if has_residual_predicate {
        if !predicate_preapplied {
            let Some(predicate_program) = predicate_slots else {
                return Err(InternalError::scalar_page_predicate_slots_required());
            };

            rows.retain(|row| {
                let mut read_slot = |slot| row.slot_ref(slot);
                predicate_program.eval_with_slot_value_ref_reader(&mut read_slot)
            });
        }

        true
    } else {
        false
    };

    // Phase 2: ordering.
    let mut ordered = false;
    let mut rows_after_order = rows.len();
    if let Some(order) = logical.order.as_ref()
        && !order.fields.is_empty()
    {
        if has_residual_predicate && !filtered {
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
        if let Some(boundary) = cursor {
            if logical.order.is_none() {
                return Err(InternalError::scalar_page_cursor_boundary_order_required());
            }
            if !ordered {
                return Err(InternalError::scalar_page_cursor_boundary_after_ordering_required());
            }
            let resolved_order = resolved_order_required(plan)?;
            rows.retain(|row| {
                compare_orderable_row_with_boundary(row, resolved_order, boundary).is_gt()
            });
            rows.len()
        } else {
            rows_after_order
        }
    } else {
        rows_after_order
    };

    // Phase 4: load pagination.
    if logical.mode.is_load()
        && let Some(page) = logical.page.as_ref()
    {
        if logical.order.is_some() && !ordered {
            return Err(InternalError::scalar_page_pagination_after_ordering_required());
        }
        apply_pagination_window(
            rows,
            ExecutionKernel::effective_page_offset(plan, cursor),
            page.limit,
        );
    }

    // Phase 5: delete limiting.
    if logical.mode.is_delete()
        && let Some(delete_limit) = logical.delete_limit.as_ref()
    {
        if logical.order.is_some() && !ordered {
            return Err(InternalError::scalar_page_delete_limit_after_ordering_required());
        }
        apply_delete_limit_window(rows, delete_limit.max_rows);
    }

    Ok(rows_after_cursor)
}

// Shared scalar load page-kernel orchestration boundary.
// Typed wrappers provide scan/decode callbacks so this loop can remain
// non-generic while preserving fail-closed continuation invariants.
struct ScalarPageKernelRequest<'a, 'r> {
    key_stream: &'a mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    load_order_route_contract: LoadOrderRouteContract,
    consistency: MissingRowPolicy,
    payload_mode: KernelRowPayloadMode,
    predicate_slots: Option<&'a PredicateProgram>,
    predicate_preapplied: bool,
    slot_only_required_slots: Option<&'a [usize]>,
    continuation: ScalarContinuationBindings<'a>,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

fn execute_scalar_page_kernel_dyn(
    request: ScalarPageKernelRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        load_order_route_contract,
        consistency,
        payload_mode,
        predicate_slots,
        predicate_preapplied,
        slot_only_required_slots,
        continuation,
        row_runtime,
    } = request;

    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

    // Phase 2: run the scalar row loop (scan -> read -> decode/filter/push).
    if let Some(scan_budget) = scan_budget_hint
        && !key_stream_budget_is_redundant(key_stream, scan_budget)
    {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        scan_rows_into_kernel(
            &mut budgeted,
            consistency,
            payload_mode,
            predicate_slots,
            predicate_preapplied,
            slot_only_required_slots,
            row_runtime,
        )
    } else {
        scan_rows_into_kernel(
            key_stream,
            consistency,
            payload_mode,
            predicate_slots,
            predicate_preapplied,
            slot_only_required_slots,
            row_runtime,
        )
    }
}

fn scan_rows_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    payload_mode: KernelRowPayloadMode,
    predicate_slots: Option<&PredicateProgram>,
    predicate_preapplied: bool,
    slot_only_required_slots: Option<&[usize]>,
    row_runtime: &mut ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let mut rows = Vec::with_capacity(key_stream.exact_key_count_hint().unwrap_or(0));

    while let Some(key) = key_stream.next_key()? {
        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = row_runtime.read_kernel_row(
            consistency,
            &key,
            payload_mode,
            predicate_preapplied,
            predicate_slots,
            slot_only_required_slots,
        )?
        else {
            continue;
        };
        rows.push(row);
    }

    Ok((rows, rows_scanned))
}

#[expect(clippy::cast_possible_truncation)]
fn apply_pagination_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let total: u32 = rows.len() as u32;
    if offset >= total {
        rows.clear();
        return;
    }

    let start_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let total_usize = usize::try_from(total).unwrap_or(usize::MAX);
    let end_usize = match limit {
        Some(limit) => compute_page_keep_count(offset, limit).min(total_usize),
        None => total_usize,
    };

    rows.drain(..start_usize);
    rows.truncate(end_usize.saturating_sub(start_usize));
}

fn apply_delete_limit_window<T>(rows: &mut Vec<T>, max_rows: u32) {
    let limit = usize::min(rows.len(), max_rows as usize);
    rows.truncate(limit);
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

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
}
