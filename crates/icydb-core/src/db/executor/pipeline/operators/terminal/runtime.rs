//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::operators::terminal::runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::db::data::DataKey;
#[cfg(feature = "sql")]
use crate::value::Value;

#[cfg(feature = "sql")]
type CoveringSlotRows = (Vec<Vec<Option<Value>>>, usize);
#[cfg(feature = "sql")]
type CoveringComponentSlotGroup = (usize, Vec<usize>);
#[cfg(feature = "sql")]
type DecodedCoveringComponentRows = Vec<(DataKey, Vec<Value>)>;
#[cfg(feature = "sql")]
type CoveringSlotRowPairs = Vec<(DataKey, Vec<Option<Value>>)>;

///
/// SqlCoveringMaterializationContext
///
/// Shared immutable inputs for SQL-only covering slot-row materialization.
/// Keeps the outer short-path helper below the argument-count lint while
/// preserving one explicit runtime-owned contract surface.
///

#[cfg(feature = "sql")]
struct SqlCoveringMaterializationContext<'a> {
    plan: &'a AccessPlannedQuery,
    model: &'static EntityModel,
    store: StoreHandle,
    covering_component_scan: Option<CoveringComponentScanState<'a>>,
    load_terminal_fast_path: Option<&'a LoadTerminalFastPathContract>,
    scan_budget_hint: Option<usize>,
    predicate_slots: Option<&'a PredicateProgram>,
}

///
/// SqlRouteCoveringSlotLayout
///
/// Planner-derived slot layout and component-slot grouping for the
/// route-owned SQL covering-read materializer.
///

#[cfg(feature = "sql")]
struct SqlRouteCoveringSlotLayout {
    primary_key_slot: usize,
    slot_count: usize,
    constant_slots: Vec<(usize, Value)>,
    component_slots: Vec<CoveringComponentSlotGroup>,
}

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            BudgetedOrderedKeyStream, CoveringProjectionComponentRows, ExecutionKernel,
            OrderedKeyStream, ScalarContinuationBindings, covering_projection_scan_direction,
            decode_covering_projection_pairs, exact_output_key_count_hint,
            key_stream_budget_is_redundant,
            pipeline::contracts::{CoveringComponentScanState, RowCollectorMaterializationRequest},
            read_row_with_consistency_from_store, reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
            route::{
                LoadOrderRouteContract, LoadTerminalFastPathContract,
                access_order_satisfied_by_route_contract_for_model,
            },
            terminal::page::{KernelRow, KernelRowPayloadMode, ScalarRowRuntimeHandle},
            traversal::row_read_consistency_for_plan,
        },
        predicate::PredicateProgram,
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
            CoveringReadExecutionPlan, CoveringReadFieldSource,
            constant_covering_projection_value_from_access,
            expr::projection_references_only_fields,
        },
        registry::StoreHandle,
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
        } = request;

        // Phase 1: initialize row staging and read-consistency policy.
        let staged_capacity = exact_output_key_count_hint(key_stream, scan_budget_hint)
            .map_or_else(
                || row_keep_cap.unwrap_or(0),
                |hint| row_keep_cap.map_or(hint, |cap| usize::min(hint, cap)),
            );
        let mut rows = Vec::with_capacity(staged_capacity);
        let mut keys_scanned = 0usize;
        let consistency = row_read_consistency_for_plan(plan);
        let predicate_preapplied = plan.has_residual_predicate();
        let _ = continuation;
        let _ = load_order_route_contract;

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
                    payload_mode,
                    predicate_preapplied,
                    predicate_slots,
                )?
                else {
                    continue;
                };
                rows.push(row);
                if row_keep_cap.is_some_and(|cap| rows.len() >= cap) {
                    break;
                }
            }
        } else {
            while let Some(key) = key_stream.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                let Some(row) = row_runtime.read_kernel_row(
                    consistency,
                    &key,
                    payload_mode,
                    predicate_preapplied,
                    predicate_slots,
                )?
                else {
                    continue;
                };
                rows.push(row);
                if row_keep_cap.is_some_and(|cap| rows.len() >= cap) {
                    break;
                }
            }
        }

        Ok((rows, keys_scanned))
    }

    // Materialize one cursorless short-path load through the structural row
    // runtime under the same continuation and bounded-scan contract as the
    // canonical scalar page kernel.
    pub(in crate::db::executor) fn try_materialize_load_via_row_collector<'a>(
        request: RowCollectorMaterializationRequest<'a>,
        model: &'static EntityModel,
        row_runtime: &mut ScalarRowRuntimeHandle<'a>,
        store: StoreHandle,
        covering_component_scan: Option<CoveringComponentScanState<'a>>,
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
            load_order_route_contract,
            continuation,
            cursor_boundary,
            load_terminal_fast_path,
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

        continuation.validate_load_scan_budget_hint(scan_budget_hint, load_order_route_contract)?;

        #[cfg(feature = "sql")]
        let sql_covering_context = SqlCoveringMaterializationContext {
            plan,
            model,
            store,
            covering_component_scan,
            load_terminal_fast_path,
            scan_budget_hint,
            predicate_slots,
        };

        #[cfg(feature = "sql")]
        if retain_slot_rows
            && let Some((mut slot_rows, keys_scanned)) =
                try_materialize_sql_covering_slot_rows(sql_covering_context, key_stream)?
        {
            if !cursorless_sql_page_window_is_redundant(plan, slot_rows.len()) {
                apply_cursorless_sql_page_window(plan, &mut slot_rows);
            }
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

        let payload_mode = if retain_slot_rows && cursor_boundary.is_none() {
            KernelRowPayloadMode::SlotsOnly
        } else {
            KernelRowPayloadMode::FullRow
        };
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
        })?;
        if retain_slot_rows && !cursorless_sql_page_window_is_redundant(plan, rows.len()) {
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
        let page = finalize_cursorless_row_collector_page(rows, retain_slot_rows)?;

        Ok(Some((page, keys_scanned, post_access_rows)))
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
    continuation: ScalarContinuationBindings<'a>,
    row_keep_cap: Option<usize>,
    payload_mode: KernelRowPayloadMode,
    key_stream: &'a mut dyn OrderedKeyStream,
    row_runtime: &'r mut ScalarRowRuntimeHandle<'a>,
    predicate_slots: Option<&'a PredicateProgram>,
}

// Return the number of kept rows the cursorless structural SQL short path
// must materialize before later pagination becomes redundant.
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

// Return whether the cursorless SQL short path already staged the final page.
fn cursorless_sql_page_window_is_redundant(plan: &AccessPlannedQuery, row_count: usize) -> bool {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return true;
    };

    if page.offset != 0 {
        return false;
    }

    page.limit
        .is_none_or(|limit| row_count <= usize::try_from(limit).unwrap_or(usize::MAX))
}

// Finalize one cursorless structural page after short-path row collection.
fn finalize_cursorless_row_collector_page(
    rows: Vec<KernelRow>,
    retain_slot_rows: bool,
) -> Result<crate::db::executor::pipeline::contracts::StructuralCursorPage, InternalError> {
    #[cfg(feature = "sql")]
    {
        if retain_slot_rows {
            let row_count = rows.len();
            let slot_rows = rows.into_iter().map(KernelRow::into_slots).collect();

            return Ok(
                crate::db::executor::pipeline::contracts::StructuralCursorPage::new_with_slot_rows(
                    slot_rows, row_count, None,
                ),
            );
        }
    }

    let _ = retain_slot_rows;
    let data_rows = rows
        .into_iter()
        .map(KernelRow::into_data_row)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(crate::db::executor::pipeline::contracts::StructuralCursorPage::new(data_rows, None))
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
// derive every referenced value from one decoded covering component, bound
// index-prefix constants, and the authoritative primary key on each data key.
fn try_materialize_sql_covering_slot_rows(
    context: SqlCoveringMaterializationContext<'_>,
    key_stream: &mut dyn OrderedKeyStream,
) -> Result<Option<CoveringSlotRows>, InternalError> {
    // Phase 1: first try the explicit route-owned covering-read contract when
    // it can be satisfied by one index component plus any PK/constant fields.
    if let Some(rows) = try_materialize_sql_route_covering_slot_rows(&context)? {
        return Ok(Some(rows));
    }

    // Phase 2: then try the constant-covering path that rebuilds rows from
    // already-resolved keys without re-entering index storage.
    if let Some((slot_template, primary_key_slot)) =
        sql_constant_covering_slot_row_template_from_route_contract(
            context.model,
            context.load_terminal_fast_path,
            context.predicate_slots,
        )
        .or_else(|| {
            sql_constant_covering_slot_row_template(
                context.plan,
                context.model,
                context.predicate_slots,
            )
        })
    {
        let consistency = row_read_consistency_for_plan(context.plan);
        let row_check_required =
            sql_route_covering_row_check_required(context.load_terminal_fast_path);
        let mut rows = Vec::with_capacity(
            exact_output_key_count_hint(key_stream, context.scan_budget_hint).unwrap_or(0),
        );
        let mut keys_scanned = 0usize;

        if let Some(scan_budget) = context.scan_budget_hint
            && !key_stream_budget_is_redundant(key_stream, scan_budget)
        {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            while let Some(key) = budgeted.next_key()? {
                keys_scanned = keys_scanned.saturating_add(1);
                if row_check_required
                    && read_row_with_consistency_from_store(context.store, &key, consistency)?
                        .is_none()
                {
                    continue;
                }

                let mut row = slot_template.clone();
                row[primary_key_slot] = Some(key.storage_key().as_primary_key_value());

                if let Some(predicate_program) = context.predicate_slots
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
                if row_check_required
                    && read_row_with_consistency_from_store(context.store, &key, consistency)?
                        .is_none()
                {
                    continue;
                }

                let mut row = slot_template.clone();
                row[primary_key_slot] = Some(key.storage_key().as_primary_key_value());

                if let Some(predicate_program) = context.predicate_slots
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
// Return whether one route-owned covering contract still requires an explicit
// row-presence check before SQL slot-row emission.
const fn sql_route_covering_row_check_required(
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> bool {
    matches!(
        load_terminal_fast_path,
        Some(LoadTerminalFastPathContract::CoveringRead(
            CoveringReadExecutionPlan {
                existing_row_mode: CoveringExistingRowMode::RequiresRowPresenceCheck,
                ..
            }
        ))
    )
}

#[cfg(feature = "sql")]
// Attempt one route-owned covering-read slot-row materialization path when
// the explicit route contract can satisfy every projected field from index
// components, bound constants, and the primary key alone.
fn try_materialize_sql_route_covering_slot_rows(
    context: &SqlCoveringMaterializationContext<'_>,
) -> Result<Option<CoveringSlotRows>, InternalError> {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) =
        context.load_terminal_fast_path
    else {
        return Ok(None);
    };
    let Some(scan_state) = context.covering_component_scan else {
        return Ok(None);
    };
    let Some(layout) =
        sql_route_covering_slot_layout(context.model, covering, context.predicate_slots)?
    else {
        return Ok(None);
    };
    let Some((decoded_rows, keys_scanned)) = sql_route_covering_component_rows(
        context.plan,
        context.store,
        scan_state,
        covering,
        context.scan_budget_hint,
        layout.component_slots.as_slice(),
    )?
    else {
        return Ok(None);
    };

    // Phase 1: materialize slot rows from decoded covering components.
    let mut rows =
        sql_route_covering_slot_rows_from_decoded(&layout, context.predicate_slots, decoded_rows);

    // Phase 2: restore the required output order when the covering contract is
    // primary-key ordered rather than traversal ordered.
    reorder_covering_projection_pairs(covering.order_contract, rows.as_mut_slice());

    Ok(Some((
        rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
        keys_scanned,
    )))
}

#[cfg(feature = "sql")]
// Derive one route-owned slot layout plus component-slot grouping for the
// SQL covering-read fast path and reject any residual predicate that reaches
// beyond the fail-closed covered slot set.
fn sql_route_covering_slot_layout(
    model: &'static EntityModel,
    covering: &CoveringReadExecutionPlan,
    predicate_slots: Option<&PredicateProgram>,
) -> Result<Option<SqlRouteCoveringSlotLayout>, InternalError> {
    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key.name)
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "covering-read SQL short path requires a primary-key slot",
            )
        })?;
    let mut constant_slots = Vec::new();
    let mut covered_slots = vec![false; model.fields.len()];
    let mut component_slots = Vec::<CoveringComponentSlotGroup>::new();

    // Phase 1: project one sparse slot layout from the planner-owned contract and
    // group decoded component fields by their index component position.
    covered_slots[primary_key_slot] = true;
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::Constant(value) => {
                constant_slots.push((field.field_slot.index, value.clone()));
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                if let Some((_, slots)) = component_slots
                    .iter_mut()
                    .find(|(group_index, _)| group_index == component_index)
                {
                    slots.push(field.field_slot.index);
                } else {
                    component_slots.push((*component_index, vec![field.field_slot.index]));
                }
                covered_slots[field.field_slot.index] = true;
            }
        }
    }

    // Phase 2: reject empty component plans and predicates that would still
    // require row materialization outside the covered slot set.
    if component_slots.is_empty() {
        return Ok(None);
    }
    if predicate_slots.is_some_and(|predicate| !predicate.references_only_slots(&covered_slots)) {
        return Ok(None);
    }

    Ok(Some(SqlRouteCoveringSlotLayout {
        primary_key_slot,
        slot_count: model.fields.len(),
        constant_slots,
        component_slots,
    }))
}

#[cfg(feature = "sql")]
// Scan and decode one route-owned covering component stream under the
// existing-row contract while preserving the planner-owned traversal order.
fn sql_route_covering_component_rows(
    plan: &AccessPlannedQuery,
    store: StoreHandle,
    scan_state: CoveringComponentScanState<'_>,
    covering: &CoveringReadExecutionPlan,
    scan_budget_hint: Option<usize>,
    component_slots: &[CoveringComponentSlotGroup],
) -> Result<Option<(DecodedCoveringComponentRows, usize)>, InternalError> {
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let effective_scan_budget_hint =
        covering_component_scan_budget_hint(covering.order_contract, scan_budget_hint);
    let component_indices = component_slots
        .iter()
        .map(|(component_index, _)| *component_index)
        .collect::<Vec<_>>();

    let raw_pairs: CoveringProjectionComponentRows =
        resolve_covering_projection_components_from_lowered_specs(
            scan_state.entity_tag,
            scan_state.index_prefix_specs,
            scan_state.index_range_specs,
            scan_direction,
            effective_scan_budget_hint.unwrap_or(usize::MAX),
            component_indices.as_slice(),
            |_| Ok(store),
        )?;
    let keys_scanned = raw_pairs.len();
    let consistency = row_read_consistency_for_plan(plan);
    let decoded_rows = decode_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        |decoded| {
            if decoded.len() != component_slots.len() {
                return Err(InternalError::query_executor_invariant(
                    "covering-read SQL short path component scan returned mismatched component count",
                ));
            }

            Ok(decoded)
        },
    )?;

    Ok(decoded_rows.map(|rows| (rows, keys_scanned)))
}

#[cfg(feature = "sql")]
// Materialize final slot rows from one decoded covering component stream while
// preserving residual predicate evaluation over the projected slot layout.
fn sql_route_covering_slot_rows_from_decoded(
    layout: &SqlRouteCoveringSlotLayout,
    predicate_slots: Option<&PredicateProgram>,
    decoded_rows: DecodedCoveringComponentRows,
) -> CoveringSlotRowPairs {
    let mut rows = Vec::with_capacity(decoded_rows.len());

    for (data_key, component_values) in decoded_rows {
        // Phase 1: build one fresh sparse slot row from the immutable covering
        // layout instead of cloning a full slot template per decoded key.
        let mut row = vec![None; layout.slot_count];
        row[layout.primary_key_slot] = Some(data_key.storage_key().as_primary_key_value());
        for (slot, value) in &layout.constant_slots {
            row[*slot] = Some(value.clone());
        }

        // Phase 2: fill decoded component values, cloning only when one
        // component fans out to more than one projected slot.
        for ((_, slots), component_value) in layout
            .component_slots
            .iter()
            .zip(component_values.into_iter())
        {
            let Some((last_slot, prefix_slots)) = slots.split_last() else {
                continue;
            };

            for slot in prefix_slots {
                row[*slot] = Some(component_value.clone());
            }
            row[*last_slot] = Some(component_value);
        }

        // Phase 3: preserve the residual predicate contract before staging the
        // row into the SQL projection output.
        if let Some(predicate_program) = predicate_slots
            && !predicate_program
                .eval_with_slot_value_ref_reader(&mut |slot| row.get(slot).and_then(Option::as_ref))
        {
            continue;
        }

        rows.push((data_key, row));
    }

    rows
}

#[cfg(feature = "sql")]
// Resolve one safe bounded component-scan hint for the SQL covering-read path.
// Index-order projections may stop early at the bounded fetch limit, but any
// route that still owes primary-key reordering must consume the full component
// stream before it can safely apply the logical page window.
const fn covering_component_scan_budget_hint(
    order_contract: CoveringProjectionOrder,
    scan_budget_hint: Option<usize>,
) -> Option<usize> {
    match order_contract {
        CoveringProjectionOrder::IndexOrder(_) => scan_budget_hint,
        CoveringProjectionOrder::PrimaryKeyOrder(_) => None,
    }
}

#[cfg(feature = "sql")]
// Build one slot-row template directly from the route-owned scalar
// covering-read contract when every projected value comes from a bound
// constant or the primary key.
fn sql_constant_covering_slot_row_template_from_route_contract(
    model: &'static EntityModel,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
    predicate_slots: Option<&PredicateProgram>,
) -> Option<(Vec<Option<Value>>, usize)> {
    let LoadTerminalFastPathContract::CoveringRead(covering) = load_terminal_fast_path?;
    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key.name)?;
    let mut slot_template = vec![None; model.fields.len()];
    let mut covered_slots = vec![false; model.fields.len()];
    covered_slots[primary_key_slot] = true;

    // Phase 1: project one canonical slot template directly from the route
    // contract. Any index-component field still falls back to the existing
    // local helper because this short path only reconstructs constants plus
    // primary-key values today.
    for field in &covering.fields {
        match &field.source {
            CoveringReadFieldSource::PrimaryKey => {
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::Constant(value) => {
                slot_template[field.field_slot.index] = Some(value.clone());
                covered_slots[field.field_slot.index] = true;
            }
            CoveringReadFieldSource::IndexComponent { .. } => return None,
        }
    }

    // Phase 2: keep the existing predicate-slot safety rule before the row
    // collector stops reading persisted rows.
    if predicate_slots.is_some_and(|predicate| !predicate.references_only_slots(&covered_slots)) {
        return None;
    }

    Some((slot_template, primary_key_slot))
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
    if plan.has_residual_predicate()
        && !predicate_slots.is_some_and(|predicate| predicate.references_only_slots(&covered_slots))
    {
        return None;
    }

    Some((slot_template, primary_key_slot))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{direction::Direction, query::plan::CoveringProjectionOrder};

    #[test]
    fn covering_component_scan_budget_hint_disables_bounded_fetch_for_pk_reorder() {
        assert_eq!(
            super::covering_component_scan_budget_hint(
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc),
                Some(3),
            ),
            None,
            "component scans that still owe primary-key reordering must ignore bounded fetch hints",
        );
        assert_eq!(
            super::covering_component_scan_budget_hint(
                CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc),
                Some(4),
            ),
            None,
            "descending primary-key reorder must also consume the full component stream",
        );
        assert_eq!(
            super::covering_component_scan_budget_hint(
                CoveringProjectionOrder::IndexOrder(Direction::Asc),
                Some(5),
            ),
            Some(5),
            "index-order covering scans may preserve their bounded fetch hint",
        );
    }
}
