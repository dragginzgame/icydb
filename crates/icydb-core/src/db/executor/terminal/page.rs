//! Module: executor::terminal::page
//! Responsibility: materialize ordered key streams into cursor-paged read rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by scalar execution paths.

use crate::{
    db::{
        cursor::{
            CursorBoundary, CursorBoundarySlot, MaterializedCursorRow, apply_order_direction,
            compare_boundary_slots, next_cursor_for_materialized_rows,
        },
        data::{DataKey, DataRow, RawRow},
        executor::{
            BudgetedOrderedKeyStream, ExecutionKernel, OrderedKeyStream,
            ScalarContinuationBindings, compute_page_keep_count,
            pipeline::contracts::{ErasedCursorPage, PageCursor},
            projection::validate_projection_over_slot_rows,
            route::access_order_satisfied_by_route_contract_for_model,
        },
        index::IndexKey,
        predicate::PredicateProgram,
        query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec},
    },
    error::InternalError,
    model::entity::{EntityModel, resolve_field_slot},
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::cmp::Ordering;

///
/// KernelRow
///
/// Non-generic scalar-kernel row envelope used by shared ordering/cursor/page
/// control flow before conversion back to typed `(Id<E>, E)` rows.
///

pub(in crate::db::executor) struct KernelRow {
    data_row: DataRow,
    slots: Vec<Option<Value>>,
}

impl KernelRow {
    fn from_data_row<E>(data_row: DataRow, entity: &E) -> Self
    where
        E: EntityKind + EntityValue,
    {
        let slots = (0..E::MODEL.fields.len())
            .map(|slot| entity.get_value_by_index(slot))
            .collect::<Vec<_>>();

        Self { data_row, slots }
    }

    fn slot(&self, slot: usize) -> Option<Value> {
        self.slots.get(slot).cloned().flatten()
    }
}

// Materialize one decoded entity into the shared non-generic kernel row shape.
pub(in crate::db::executor) fn kernel_row_from_entity<E>(data_row: DataRow, entity: &E) -> KernelRow
where
    E: EntityKind + EntityValue,
{
    KernelRow::from_data_row::<E>(data_row, entity)
}

///
/// KernelPageMaterializationRequest
///
/// Structural inputs for one shared scalar page-materialization pass.
/// This keeps the kernel loop monomorphic while the typed boundary supplies
/// row decode and final page construction callbacks.
///

pub(in crate::db::executor) struct KernelPageMaterializationRequest<'a> {
    pub(in crate::db::executor) model: &'static EntityModel,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) predicate_slots: Option<&'a PredicateProgram>,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) stream_order_contract_safe: bool,
    pub(in crate::db::executor) continuation: ScalarContinuationBindings<'a>,
}

/// Materialize one ordered key stream into one erased typed cursor page.
pub(in crate::db::executor) fn materialize_key_stream_into_erased_page(
    request: KernelPageMaterializationRequest<'_>,
    read_row_for_key: &mut dyn FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
    on_row: &mut dyn FnMut(DataRow) -> Result<Option<KernelRow>, InternalError>,
    build_page: &mut dyn FnMut(
        Vec<DataRow>,
        Option<PageCursor>,
    ) -> Result<ErasedCursorPage, InternalError>,
) -> Result<(ErasedCursorPage, usize, usize), InternalError> {
    let KernelPageMaterializationRequest {
        model,
        plan,
        predicate_slots,
        key_stream,
        scan_budget_hint,
        stream_order_contract_safe,
        continuation,
    } = request;

    let predicate_preapplied = plan.scalar_plan().predicate.is_some();
    if predicate_preapplied && predicate_slots.is_none() {
        return Err(crate::db::error::query_executor_invariant(
            "post-access filtering requires precompiled predicate slots",
        ));
    }

    // Phase 1: run the shared scalar page kernel against typed boundary callbacks.
    let (mut rows, rows_scanned) = execute_scalar_page_kernel_dyn(
        key_stream,
        scan_budget_hint,
        stream_order_contract_safe,
        continuation,
        read_row_for_key,
        on_row,
    )?;

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

    // Phase 4: hand the structural rows back to the typed boundary for final decode.
    let data_rows = rows.into_iter().map(|row| row.data_row).collect::<Vec<_>>();
    let page = build_page(data_rows, next_cursor)?;

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
                return Err(crate::db::error::query_executor_invariant(
                    "post-access filtering requires precompiled predicate slots",
                ));
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
            return Err(crate::db::error::query_executor_invariant(
                "ordering must run after filtering",
            ));
        }

        ordered = true;
        if !access_order_satisfied_by_route_contract_for_model(model, plan) {
            let resolved_order = resolve_order_fields(model, order);
            let ordered_total = rows.len();

            if rows.len() > 1 {
                if let Some(keep_count) = ExecutionKernel::bounded_order_keep_count(plan, cursor) {
                    apply_kernel_order_bounded(rows, resolved_order.as_slice(), keep_count);
                } else {
                    apply_kernel_order(rows, resolved_order.as_slice());
                }
            }
            rows_after_order = ordered_total;
        }
    }

    // Phase 3: continuation boundary.
    let rows_after_cursor = if logical.mode.is_load() {
        if let Some(boundary) = cursor {
            let Some(order) = logical.order.as_ref() else {
                return Err(crate::db::error::query_executor_invariant(
                    "cursor boundary requires ordering",
                ));
            };
            if !ordered {
                return Err(crate::db::error::query_executor_invariant(
                    "cursor boundary must run after ordering",
                ));
            }
            let resolved_order = resolve_order_fields(model, order);
            rows.retain(|row| {
                compare_kernel_row_with_boundary(row, resolved_order.as_slice(), boundary).is_gt()
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
            return Err(crate::db::error::query_executor_invariant(
                "pagination must run after ordering",
            ));
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
            return Err(crate::db::error::query_executor_invariant(
                "delete limit must run after ordering",
            ));
        }
        apply_delete_limit_window(rows, delete_limit.max_rows);
    }

    Ok(rows_after_cursor)
}

// Shared scalar load page-kernel orchestration boundary.
// Typed wrappers provide scan/decode callbacks so this loop can remain
// non-generic while preserving fail-closed continuation invariants.
fn execute_scalar_page_kernel_dyn(
    key_stream: &mut dyn OrderedKeyStream,
    scan_budget_hint: Option<usize>,
    stream_order_contract_safe: bool,
    continuation: ScalarContinuationBindings<'_>,
    read_row_for_key: &mut dyn FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
    on_row: &mut dyn FnMut(DataRow) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    // Phase 1: continuation-owned budget hints remain validated centrally.
    continuation.validate_load_scan_budget_hint(scan_budget_hint, stream_order_contract_safe)?;

    // Phase 2: run the scalar row loop (scan -> read -> decode/filter/push).
    if let Some(scan_budget) = scan_budget_hint {
        let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);

        scan_rows_into_kernel(&mut budgeted, read_row_for_key, on_row)
    } else {
        scan_rows_into_kernel(key_stream, read_row_for_key, on_row)
    }
}

fn scan_rows_into_kernel(
    key_stream: &mut dyn OrderedKeyStream,
    read_row_for_key: &mut dyn FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
    on_row: &mut dyn FnMut(DataRow) -> Result<Option<KernelRow>, InternalError>,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let mut rows_scanned = 0usize;
    let mut rows = Vec::new();

    while let Some(key) = key_stream.next_key()? {
        let Some(row) = read_row_for_key(&key)? else {
            continue;
        };
        rows_scanned = rows_scanned.saturating_add(1);
        if let Some(row) = on_row((key, row))? {
            rows.push(row);
        }
    }

    Ok((rows, rows_scanned))
}

fn resolve_order_fields(
    model: &EntityModel,
    order: &OrderSpec,
) -> Vec<(Option<usize>, OrderDirection)> {
    order
        .fields
        .iter()
        .map(|(field, direction)| (resolve_field_slot(model, field), *direction))
        .collect()
}

fn boundary_slot_for_kernel_row(row: &KernelRow, field_index: Option<usize>) -> CursorBoundarySlot {
    let value = field_index.and_then(|field_index| row.slot(field_index));

    match value {
        Some(value) => CursorBoundarySlot::Present(value),
        None => CursorBoundarySlot::Missing,
    }
}

fn compare_kernel_rows(
    left: &KernelRow,
    right: &KernelRow,
    resolved_order: &[(Option<usize>, OrderDirection)],
) -> Ordering {
    for (field_index, direction) in resolved_order {
        let left_slot = boundary_slot_for_kernel_row(left, *field_index);
        let right_slot = boundary_slot_for_kernel_row(right, *field_index);
        let ordering =
            apply_order_direction(compare_boundary_slots(&left_slot, &right_slot), *direction);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

fn compare_kernel_row_with_boundary(
    row: &KernelRow,
    resolved_order: &[(Option<usize>, OrderDirection)],
    boundary: &CursorBoundary,
) -> Ordering {
    for ((field_index, direction), boundary_slot) in
        resolved_order.iter().zip(boundary.slots.iter())
    {
        let row_slot = boundary_slot_for_kernel_row(row, *field_index);
        let ordering =
            apply_order_direction(compare_boundary_slots(&row_slot, boundary_slot), *direction);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

fn apply_kernel_order(rows: &mut [KernelRow], resolved_order: &[(Option<usize>, OrderDirection)]) {
    rows.sort_by(|left, right| compare_kernel_rows(left, right, resolved_order));
}

fn apply_kernel_order_bounded(
    rows: &mut Vec<KernelRow>,
    resolved_order: &[(Option<usize>, OrderDirection)],
    keep_count: usize,
) {
    if keep_count == 0 {
        rows.clear();
        return;
    }

    if rows.len() > keep_count {
        rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_kernel_rows(left, right, resolved_order)
        });
        rows.truncate(keep_count);
    }

    rows.sort_by(|left, right| compare_kernel_rows(left, right, resolved_order));
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
