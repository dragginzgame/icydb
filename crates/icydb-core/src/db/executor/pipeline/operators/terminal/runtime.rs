//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::terminal::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::value::Value;

#[cfg(feature = "sql")]
type CoveringSlotRows = (Vec<Vec<Option<Value>>>, usize);
use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            BudgetedOrderedKeyStream, ExecutionKernel, OrderedKeyStream,
            ScalarContinuationBindings, exact_output_key_count_hint,
            key_stream_budget_is_redundant,
            pipeline::contracts::RowCollectorMaterializationRequest,
            route::access_order_satisfied_by_route_contract_for_model,
            terminal::page::{KernelRow, ScalarRowRuntimeHandle},
            traversal::row_read_consistency_for_plan,
        },
        predicate::PredicateProgram,
        query::plan::{
            AccessPlannedQuery, constant_covering_projection_value_from_access,
            expr::projection_references_only_fields,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
};

impl ExecutionKernel {
    // Return whether load execution can safely use the row-collector short path
    // without changing cursor/pagination/filter semantics.
    pub(in crate::db::executor::pipeline::operators::terminal) fn load_row_collector_short_path_eligible(
        plan: &AccessPlannedQuery,
        model: &'static EntityModel,
        cursor_boundary: Option<&CursorBoundary>,
        retain_slot_rows: bool,
    ) -> bool {
        let logical = plan.scalar_plan();
        let generic_short_path = logical.mode.is_load()
            && cursor_boundary.is_none()
            && logical.predicate.is_none()
            && logical.order.is_none()
            && logical.page.is_none();

        let sql_projection_short_path = logical.mode.is_load()
            && retain_slot_rows
            && cursor_boundary.is_none()
            && !logical.distinct
            && (logical.order.is_none()
                || access_order_satisfied_by_route_contract_for_model(model, plan));

        generic_short_path || sql_projection_short_path
    }

    // Run one row-collector stream over the already decorated
    // key stream and stage structural kernel rows only.
    pub(in crate::db::executor::pipeline::operators::terminal) fn run_row_collector_stream(
        plan: &AccessPlannedQuery,
        scan_budget_hint: Option<usize>,
        stream_order_contract_safe: bool,
        continuation: ScalarContinuationBindings<'_>,
        key_stream: &mut dyn OrderedKeyStream,
        row_runtime: &mut ScalarRowRuntimeHandle<'_>,
        predicate_slots: Option<&PredicateProgram>,
    ) -> Result<(Vec<KernelRow>, usize), InternalError> {
        // Phase 1: initialize row staging and read-consistency policy.
        let mut rows = Vec::with_capacity(
            exact_output_key_count_hint(key_stream, scan_budget_hint).unwrap_or(0),
        );
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);
        let predicate_preapplied = plan.scalar_plan().predicate.is_some();
        let _ = continuation;
        let _ = stream_order_contract_safe;

        // Phase 2: materialize rows from keys and append staged structural outputs.
        if let Some(scan_budget) = scan_budget_hint
            && !key_stream_budget_is_redundant(key_stream, scan_budget)
        {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            while let Some(key) = budgeted.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
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
        } else {
            while let Some(key) = key_stream.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
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
        }

        Ok((rows, keys_scanned))
    }

    // Materialize one cursorless short-path load through the structural row
    // runtime under the same continuation and bounded-scan contract as the
    // canonical scalar page kernel.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector(
        request: RowCollectorMaterializationRequest<'_>,
        model: &'static EntityModel,
        row_runtime: &mut ScalarRowRuntimeHandle<'_>,
    ) -> Result<
        Option<(
            crate::db::executor::pipeline::contracts::StructuralCursorPage,
            usize,
            usize,
        )>,
        InternalError,
    > {
        // Phase 1: destructure the one short-path request envelope once so the
        // terminal helper cannot drift from the kernel-owned scan contract.
        let RowCollectorMaterializationRequest {
            plan,
            scan_budget_hint,
            stream_order_contract_safe,
            continuation,
            cursor_boundary,
            predicate_slots,
            validate_projection,
            retain_slot_rows,
            key_stream,
        } = request;

        if !Self::load_row_collector_short_path_eligible(
            plan,
            model,
            cursor_boundary,
            retain_slot_rows,
        ) {
            return Ok(None);
        }

        continuation
            .validate_load_scan_budget_hint(scan_budget_hint, stream_order_contract_safe)?;

        #[cfg(feature = "sql")]
        if retain_slot_rows
            && let Some((mut slot_rows, keys_scanned)) = try_materialize_sql_covering_slot_rows(
                plan,
                model,
                scan_budget_hint,
                key_stream,
                predicate_slots,
            )?
        {
            apply_cursorless_sql_page_window(plan, &mut slot_rows);
            if validate_projection {
                crate::db::executor::projection::validate_projection_over_slot_rows(
                    model,
                    &plan.projection_spec(model),
                    slot_rows.len(),
                    &mut |row_index, slot| slot_rows[row_index].get(slot).cloned().flatten(),
                )?;
            }
            let post_access_rows = slot_rows.len();
            let page =
                crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                    slot_rows,
                    post_access_rows,
                    None,
                );

            return Ok(Some((page, keys_scanned, post_access_rows)));
        }

        let (mut rows, keys_scanned) = Self::run_row_collector_stream(
            plan,
            scan_budget_hint,
            stream_order_contract_safe,
            continuation,
            key_stream,
            row_runtime,
            predicate_slots,
        )?;
        if retain_slot_rows {
            apply_cursorless_sql_page_window(plan, &mut rows);
        }
        if validate_projection {
            crate::db::executor::projection::validate_projection_over_slot_rows(
                model,
                &plan.projection_spec(model),
                rows.len(),
                &mut |row_index, slot| rows[row_index].slot(slot),
            )?;
        }
        let post_access_rows = rows.len();
        #[cfg(feature = "sql")]
        let page = if retain_slot_rows {
            let row_count = rows.len();
            let slot_rows = rows.into_iter().map(KernelRow::into_slots).collect();
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                slot_rows, row_count, None,
            )
        } else {
            let data_rows = rows.into_iter().map(KernelRow::into_data_row).collect();
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new(data_rows, None)
        };

        #[cfg(not(feature = "sql"))]
        let page = {
            let _ = retain_slot_rows;
            let data_rows = rows.into_iter().map(KernelRow::into_data_row).collect();
            crate::db::executor::pipeline::contracts::StructuralCursorPage::new(data_rows, None)
        };

        Ok(Some((page, keys_scanned, post_access_rows)))
    }
}

// Apply the SQL-only cursorless LIMIT/OFFSET window directly on the collected
// row set when the route already guarantees final order and the outer surface
// does not retain scalar continuation state.
fn apply_cursorless_sql_page_window<T>(plan: &AccessPlannedQuery, rows: &mut Vec<T>) {
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

    rows.drain(..start);
    rows.truncate(end.saturating_sub(start));
}

#[cfg(feature = "sql")]
// Attempt one SQL-only index-covered slot-row materialization path that can
// derive every referenced value from bound index-prefix constants plus the
// authoritative primary key carried by each resolved data key.
fn try_materialize_sql_covering_slot_rows(
    plan: &AccessPlannedQuery,
    model: &'static EntityModel,
    scan_budget_hint: Option<usize>,
    key_stream: &mut dyn OrderedKeyStream,
    predicate_slots: Option<&PredicateProgram>,
) -> Result<Option<CoveringSlotRows>, InternalError> {
    // Phase 1: first try the constant-covering path that rebuilds rows from
    // already-resolved keys without re-entering index storage.
    if let Some((slot_template, primary_key_slot)) =
        sql_constant_covering_slot_row_template(plan, model, predicate_slots)
    {
        let mut rows = Vec::with_capacity(
            exact_output_key_count_hint(key_stream, scan_budget_hint).unwrap_or(0),
        );
        let mut keys_scanned = 0usize;

        if let Some(scan_budget) = scan_budget_hint
            && !key_stream_budget_is_redundant(key_stream, scan_budget)
        {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            while let Some(key) = budgeted.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                let mut row = slot_template.clone();
                row[primary_key_slot] = Some(key.storage_key().as_primary_key_value());

                if let Some(predicate_program) = predicate_slots
                    && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                        row.get(slot).and_then(Option::as_ref)
                    })
                {
                    continue;
                }

                rows.push(row);
            }
        } else {
            while let Some(key) = key_stream.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                let mut row = slot_template.clone();
                row[primary_key_slot] = Some(key.storage_key().as_primary_key_value());

                if let Some(predicate_program) = predicate_slots
                    && !predicate_program.eval_with_slot_value_ref_reader(&mut |slot| {
                        row.get(slot).and_then(Option::as_ref)
                    })
                {
                    continue;
                }

                rows.push(row);
            }
        }

        return Ok(Some((rows, keys_scanned)));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
// Build one slot-row template when projection and predicate semantics stay
// fully within bound access-prefix fields plus the primary key.
fn sql_constant_covering_slot_row_template(
    plan: &AccessPlannedQuery,
    model: &'static EntityModel,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<(Vec<Option<Value>>, usize)> {
    let projection = plan.projection_spec(model);
    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key.name)?;
    let mut slot_template = vec![None; model.fields.len()];
    let mut covered_slots = vec![false; model.fields.len()];
    let mut covered_fields = vec![model.primary_key.name];
    covered_slots[primary_key_slot] = true;

    // Phase 1: recover every equality-bound index-prefix component once.
    for (slot, field) in model.fields.iter().enumerate() {
        if slot == primary_key_slot {
            continue;
        }

        if let Some(value) =
            constant_covering_projection_value_from_access(&plan.access, field.name)
        {
            slot_template[slot] = Some(value);
            covered_slots[slot] = true;
            covered_fields.push(field.name);
        }
    }

    // Phase 2: require both projection and residual predicate to stay within
    // the covered slot set before we stop reading persisted rows.
    if !projection_references_only_fields(&projection, covered_fields.as_slice()) {
        return None;
    }
    if plan.scalar_plan().predicate.is_some()
        && !predicate_slots.is_some_and(|predicate| predicate.references_only_slots(&covered_slots))
    {
        return None;
    }

    Some((slot_template, primary_key_slot))
}
