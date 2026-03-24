//! Module: executor::terminal::page
//! Responsibility: materialize ordered key streams into cursor-paged read rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by scalar execution paths.

use crate::{
    db::{
        cursor::{CursorBoundary, MaterializedCursorRow, next_cursor_for_materialized_rows},
        data::{DataKey, DataRow},
        executor::{
            BudgetedOrderedKeyStream, ExecutionKernel, OrderReadableRow, OrderedKeyStream,
            ScalarContinuationBindings, apply_structural_order, apply_structural_order_bounded,
            compare_orderable_row_with_boundary, compute_page_keep_count,
            pipeline::contracts::{PageCursor, StructuralCursorPage},
            projection::validate_projection_over_slot_rows,
            resolve_structural_order,
            route::access_order_satisfied_by_route_contract_for_model,
        },
        index::IndexKey,
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};
use std::marker::PhantomData;

///
/// KernelRow
///
/// Non-generic scalar-kernel row envelope used by shared ordering/cursor/page
/// control flow before conversion back to typed `(Id<E>, E)` rows.
///

pub(in crate::db) struct KernelRow {
    data_row: DataRow,
    slots: Vec<Option<Value>>,
}

impl KernelRow {
    /// Build one structural kernel row from canonical data-row storage plus
    /// slot-indexed runtime values.
    #[must_use]
    pub(in crate::db) const fn new(data_row: DataRow, slots: Vec<Option<Value>>) -> Self {
        Self { data_row, slots }
    }

    pub(in crate::db) fn slot(&self, slot: usize) -> Option<Value> {
        self.slots.get(slot).cloned().flatten()
    }

    pub(in crate::db) fn into_data_row(self) -> DataRow {
        self.data_row
    }

    pub(in crate::db) fn into_slots(self) -> Vec<Option<Value>> {
        self.slots
    }

    pub(in crate::db) fn into_parts(self) -> (DataRow, Vec<Option<Value>>) {
        (self.data_row, self.slots)
    }
}

impl OrderReadableRow for KernelRow {
    fn read_order_slot(&self, slot: usize) -> Option<Value> {
        self.slot(slot)
    }
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
    bool,
    Option<&PredicateProgram>,
) -> Result<Option<KernelRow>, InternalError>;

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
    _marker: PhantomData<&'a mut ()>,
}

impl<'a> ScalarRowRuntimeHandle<'a> {
    /// Erase one boundary-resolved row-runtime state object behind a structural runtime handle.
    #[must_use]
    pub(in crate::db::executor) fn new<T>(state: T, vtable: ScalarRowRuntimeVTable) -> Self
    where
        T: 'a,
    {
        Self {
            state: Box::into_raw(Box::new(state)).cast(),
            vtable,
            _marker: PhantomData,
        }
    }

    /// Read one structural kernel row from one data key.
    pub(in crate::db::executor) fn read_kernel_row(
        &mut self,
        consistency: MissingRowPolicy,
        key: &DataKey,
        predicate_preapplied: bool,
        predicate_slots: Option<&PredicateProgram>,
    ) -> Result<Option<KernelRow>, InternalError> {
        // SAFETY: `state` was allocated by `new`, the vtable matches the
        // erased state type, and the handle has unique mutable access.
        unsafe {
            (self.vtable.read_kernel_row)(
                self.state,
                consistency,
                key,
                predicate_preapplied,
                predicate_slots,
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
    pub(in crate::db::executor) model: &'static EntityModel,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) stream_order_contract_safe: bool,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

/// Materialize one ordered key stream into one structural scalar cursor page.
pub(in crate::db::executor) fn materialize_key_stream_into_structural_page<'a>(
    request: KernelPageMaterializationRequest<'a>,
    row_runtime: &mut ScalarRowRuntimeHandle<'a>,
) -> Result<(StructuralCursorPage, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        model,
        plan,
        predicate_slots,
        key_stream,
        scan_budget_hint,
        stream_order_contract_safe,
        consistency,
        continuation,
    } = request;

    let predicate_preapplied = plan.scalar_plan().predicate.is_some();
    if predicate_preapplied && predicate_slots.is_none() {
        return Err(InternalError::scalar_page_predicate_slots_required());
    }

    // Phase 1: run the shared scalar page kernel against typed boundary callbacks.
    let (mut rows, rows_scanned) = execute_scalar_page_kernel_dyn(ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        stream_order_contract_safe,
        consistency,
        predicate_slots,
        predicate_preapplied,
        continuation,
        row_runtime,
    })?;

    // Phase 2: apply post-access phases and validate projection semantics.
    let rows_after_cursor = apply_post_access_to_kernel_rows_dyn(
        model,
        plan,
        &mut rows,
        continuation.post_access_cursor_boundary(),
        predicate_slots,
        predicate_preapplied,
    )?;
    validate_projection_over_slot_rows(
        model,
        &plan.projection_spec(model),
        rows.len(),
        &mut |row_index, slot| rows[row_index].slot(slot),
    )?;

    // Phase 3: assemble the structural cursor boundary before typed page emission.
    let last_cursor_row = resolve_last_cursor_row(model, plan, rows.as_slice())?;
    let post_access_rows = rows.len();
    let next_cursor = next_cursor_for_materialized_rows(
        &plan.access,
        plan.scalar_plan().order.as_ref(),
        plan.scalar_plan().page.as_ref(),
        post_access_rows,
        last_cursor_row.as_ref(),
        rows_after_cursor,
        continuation.post_access_cursor_boundary(),
        continuation.previous_index_range_anchor(),
        continuation.direction(),
        continuation.continuation_signature(),
    )?
    .map(PageCursor::Scalar);

    // Phase 4: finalize one structural page payload for outer typed decode.
    let data_rows = rows.into_iter().map(KernelRow::into_data_row).collect();
    let page = StructuralCursorPage::new(data_rows, next_cursor);

    Ok((page, rows_scanned, post_access_rows))
}

// Resolve the last structural cursor row before typed response decode.
fn resolve_last_cursor_row(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
) -> Result<Option<MaterializedCursorRow>, InternalError> {
    let Some(order) = plan.scalar_plan().order.as_ref() else {
        return Ok(None);
    };
    let Some(row) = rows.last() else {
        return Ok(None);
    };

    // Phase 1: derive the structural boundary from already-materialized row slots.
    let mut read_slot = |slot| row.slot(slot);
    let boundary = CursorBoundary::from_slot_reader(model, order, &mut read_slot);

    // Phase 2: derive the optional raw index-range anchor once for index-range paths.
    let index_anchor = if let Some((index, _, _, _)) = plan.access.as_index_range_path() {
        let data_key = &row.data_row.0;
        let mut read_slot = |slot| row.slot(slot);
        IndexKey::new_from_slot_reader(
            data_key.entity_tag(),
            data_key.storage_key(),
            model,
            index,
            &mut read_slot,
        )?
        .map(|key| key.to_raw())
    } else {
        None
    };

    Ok(Some(MaterializedCursorRow::new(boundary, index_anchor)))
}

// Run canonical post-access phases over kernel rows.
fn apply_post_access_to_kernel_rows_dyn(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    rows: &mut Vec<KernelRow>,
    cursor: Option<&CursorBoundary>,
    predicate_slots: Option<&PredicateProgram>,
    predicate_preapplied: bool,
) -> Result<usize, InternalError> {
    let logical = plan.scalar_plan();

    // Phase 1: predicate filtering.
    let filtered = if logical.predicate.is_some() {
        if !predicate_preapplied {
            let Some(predicate_program) = predicate_slots else {
                return Err(InternalError::scalar_page_predicate_slots_required());
            };

            rows.retain(|row| {
                let mut read_slot = |slot| row.slot(slot);
                predicate_program.eval_with_slot_reader(&mut read_slot)
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
        if logical.predicate.is_some() && !filtered {
            return Err(InternalError::scalar_page_ordering_after_filtering_required());
        }

        ordered = true;
        if !access_order_satisfied_by_route_contract_for_model(model, plan) {
            let resolved_order = resolve_structural_order(model, order);
            let ordered_total = rows.len();

            if rows.len() > 1 {
                if let Some(keep_count) = ExecutionKernel::bounded_order_keep_count(plan, cursor) {
                    apply_structural_order_bounded(rows, &resolved_order, keep_count);
                } else {
                    apply_structural_order(rows, &resolved_order);
                }
            }
            rows_after_order = ordered_total;
        }
    }

    // Phase 3: continuation boundary.
    let rows_after_cursor = if logical.mode.is_load() {
        if let Some(boundary) = cursor {
            let Some(order) = logical.order.as_ref() else {
                return Err(InternalError::scalar_page_cursor_boundary_order_required());
            };
            if !ordered {
                return Err(InternalError::scalar_page_cursor_boundary_after_ordering_required());
            }
            let resolved_order = resolve_structural_order(model, order);
            rows.retain(|row| {
                compare_orderable_row_with_boundary(row, &resolved_order, boundary).is_gt()
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
    stream_order_contract_safe: bool,
    consistency: MissingRowPolicy,
    predicate_slots: Option<&'a PredicateProgram>,
    predicate_preapplied: bool,
    continuation: ScalarContinuationBindings<'a>,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
}

fn execute_scalar_page_kernel_dyn(
    request: ScalarPageKernelRequest<'_, '_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let ScalarPageKernelRequest {
        key_stream,
        scan_budget_hint,
        stream_order_contract_safe,
        consistency,
        predicate_slots,
        predicate_preapplied,
        continuation,
        row_runtime,
    } = request;

    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, stream_order_contract_safe)?;

    // Phase 2: run the scalar row loop (scan -> read -> decode/filter/push).
    if let Some(scan_budget) = scan_budget_hint {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        scan_rows_into_kernel(
            &mut budgeted,
            consistency,
            predicate_slots,
            predicate_preapplied,
            row_runtime,
        )
    } else {
        scan_rows_into_kernel(
            key_stream,
            consistency,
            predicate_slots,
            predicate_preapplied,
            row_runtime,
        )
    }
}

fn scan_rows_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    predicate_slots: Option<&PredicateProgram>,
    predicate_preapplied: bool,
    row_runtime: &mut ScalarRowRuntimeHandle<'_>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let mut rows = Vec::new();

    while let Some(key) = key_stream.next_key()? {
        rows_scanned = rows_scanned.saturating_add(1);
        let Some(row) = row_runtime.read_kernel_row(
            consistency,
            &key,
            predicate_preapplied,
            predicate_slots,
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
