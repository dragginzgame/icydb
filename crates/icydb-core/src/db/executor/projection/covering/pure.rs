#[cfg(all(feature = "sql", feature = "diagnostics"))]
use crate::db::{
    diagnostics::measure_local_instruction_delta as measure_structural_result,
    executor::projection::covering::{
        record_pure_covering_decode_local_instructions,
        record_pure_covering_row_assembly_local_instructions,
    },
};
use crate::{
    db::{
        Db,
        access::lower_access,
        data::DataKey,
        executor::projection::covering::shared::{
            covering_projection_component_indices, project_covering_row_from_decoded_values,
            project_covering_row_from_owned_decoded_values,
            project_covering_row_from_single_decoded_value,
        },
        executor::{
            CoveringProjectionComponentRows, EntityAuthority, OrderedKeyStreamBox,
            PrimaryRangeKeyStream, apply_offset_limit_window, covering_projection_scan_direction,
            decode_covering_projection_pairs, decode_single_covering_projection_pairs,
            map_covering_projection_pairs, reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
        },
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
            CoveringReadExecutionPlan, CoveringReadFieldSource, PageSpec,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
pub(super) fn try_execute_covering_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    if plan.has_residual_filter_expr() || plan.has_residual_filter_predicate() {
        return Ok(None);
    }

    // Phase 1: admit only planner-proven pure covering routes that need no
    // row-backed fields in projection materialization.
    let Some(covering) = authority.covering_read_execution_plan(plan, true) else {
        return Ok(None);
    };
    if covering
        .fields
        .iter()
        .any(|field| matches!(field.source, CoveringReadFieldSource::RowField))
    {
        return Ok(None);
    }

    if let Some(projected_rows) = try_execute_primary_store_covering_projection_rows_for_canister(
        db,
        authority.clone(),
        plan,
        &covering,
    )? {
        return Ok(Some(projected_rows));
    }

    // Phase 2: the remaining pure covering shortcut owns index-backed scans.
    if plan.access.as_index_prefix_contract_path().is_none()
        && plan.access.as_index_range_path().is_none()
    {
        return Ok(None);
    }

    let component_indices = covering_projection_component_indices(covering.fields.as_slice());
    let store = db.recovered_store(authority.store_path())?;
    let lowered_access = lower_access(authority.entity_tag(), &plan.access)
        .map_err(crate::db::access::LoweredAccessError::into_internal_error)?;
    let index_prefix_specs = lowered_access.index_prefix_specs();
    let index_range_specs = lowered_access.index_range_specs();

    // Phase 2: scan the covering component payloads under the same planner
    // order contract the executor already exposes in EXPLAIN EXECUTION.
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let scan_limit = pure_covering_scan_limit(
        covering.order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let raw_pairs = resolve_covering_projection_components_from_lowered_specs(
        authority.entity_tag(),
        index_prefix_specs,
        index_range_specs,
        scan_direction,
        scan_limit,
        component_indices.as_slice(),
        |store_path| db.recovered_store(store_path),
    )?;
    let page = plan.scalar_plan().page.as_ref();
    let order_contract = covering.order_contract;
    let index_order = matches!(order_contract, CoveringProjectionOrder::IndexOrder(_));
    let scan_time_page_skip_count = pure_covering_scan_time_page_skip_count(
        order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        page,
    );
    let scan_time_page_window_applied = pure_covering_scan_time_page_window_applied(
        order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        page,
    );

    if component_indices.is_empty() {
        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        let (decode_local_instructions, projected_keys) = measure_structural_result(|| {
            map_covering_projection_pairs(
                raw_pairs,
                store,
                plan.scalar_consistency(),
                covering.existing_row_mode,
                |_components| Ok::<Option<()>, InternalError>(Some(())),
            )
        });
        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        record_pure_covering_decode_local_instructions(decode_local_instructions);
        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        let Some(projected_keys): Option<Vec<(DataKey, ())>> = projected_keys? else {
            return Ok(None);
        };

        #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
        let Some(projected_keys) = map_covering_projection_pairs(
            raw_pairs,
            store,
            plan.scalar_consistency(),
            covering.existing_row_mode,
            |_components| Ok::<Option<()>, InternalError>(Some(())),
        )?
        else {
            return Ok(None);
        };

        if index_order {
            let mut projected_rows = assemble_covering_rows_in_index_order(
                projected_keys,
                scan_time_page_skip_count,
                |(data_key, ())| {
                    project_covering_row_from_decoded_values(
                        &data_key,
                        covering.fields.as_slice(),
                        &[],
                        &[],
                    )
                },
            )?;
            apply_pure_covering_page_window(
                plan.scalar_plan().distinct,
                page,
                scan_time_page_window_applied,
                &mut projected_rows,
            );

            return Ok(Some(projected_rows));
        }

        let mut projected_rows = assemble_covering_rows_with_reorder(
            projected_keys,
            order_contract,
            |(data_key, ())| {
                let projected_row = project_covering_row_from_decoded_values(
                    &data_key,
                    covering.fields.as_slice(),
                    &[],
                    &[],
                )?;

                Ok::<(DataKey, Vec<Value>), InternalError>((data_key, projected_row))
            },
        )?;
        apply_pure_covering_page_window(
            plan.scalar_plan().distinct,
            page,
            false,
            &mut projected_rows,
        );

        return Ok(Some(projected_rows));
    }

    // Phase 3b: one-component pure covering rows can skip the generic
    // decoded-vector contract and carry one runtime `Value` directly through
    // the covering assembly path.
    if component_indices.len() == 1 {
        let component_index = component_indices[0];

        let decoded_scan_time_skip_count = if index_order {
            scan_time_page_skip_count
        } else {
            0
        };
        let raw_pairs = drop_scan_time_covering_offset(raw_pairs, decoded_scan_time_skip_count);

        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        let (decode_local_instructions, decoded_rows) = measure_structural_result(|| {
            decode_single_covering_projection_pairs(
                raw_pairs,
                store,
                plan.scalar_consistency(),
                covering.existing_row_mode,
                "pure covering projection expected one decodable covering component payload",
                Ok::<Value, InternalError>,
            )
        });
        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        record_pure_covering_decode_local_instructions(decode_local_instructions);
        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        let Some(decoded_rows): Option<Vec<(DataKey, Value)>> = decoded_rows? else {
            return Ok(None);
        };

        #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
        let Some(decoded_rows) = decode_single_covering_projection_pairs(
            raw_pairs,
            store,
            plan.scalar_consistency(),
            covering.existing_row_mode,
            "pure covering projection expected one decodable covering component payload",
            Ok::<Value, InternalError>,
        )?
        else {
            return Ok(None);
        };

        if index_order {
            let mut projected_rows = assemble_covering_rows_in_index_order(
                decoded_rows,
                0,
                |(data_key, decoded_value)| {
                    project_covering_row_from_single_decoded_value(
                        &data_key,
                        covering.fields.as_slice(),
                        component_index,
                        decoded_value,
                    )
                },
            )?;
            apply_pure_covering_page_window(
                plan.scalar_plan().distinct,
                page,
                scan_time_page_window_applied,
                &mut projected_rows,
            );

            return Ok(Some(projected_rows));
        }

        let mut projected_rows = assemble_covering_rows_with_reorder(
            decoded_rows,
            order_contract,
            |(data_key, decoded_value)| {
                let projected_row = project_covering_row_from_single_decoded_value(
                    &data_key,
                    covering.fields.as_slice(),
                    component_index,
                    decoded_value,
                )?;

                Ok::<(DataKey, Vec<Value>), InternalError>((data_key, projected_row))
            },
        )?;
        apply_pure_covering_page_window(
            plan.scalar_plan().distinct,
            page,
            false,
            &mut projected_rows,
        );

        return Ok(Some(projected_rows));
    }

    // Phase 3: reuse the executor-owned covering decode contract so planner-
    // proven routes avoid row-store reads entirely while row-check-required
    // routes still preserve missing-row consistency rules.
    let decoded_scan_time_skip_count = if index_order {
        scan_time_page_skip_count
    } else {
        0
    };
    let raw_pairs = drop_scan_time_covering_offset(raw_pairs, decoded_scan_time_skip_count);

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let (decode_local_instructions, decoded_rows) = measure_structural_result(|| {
        decode_covering_projection_pairs(
            raw_pairs,
            store,
            plan.scalar_consistency(),
            covering.existing_row_mode,
            Ok::<Vec<Value>, InternalError>,
        )
    });
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    record_pure_covering_decode_local_instructions(decode_local_instructions);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let Some(decoded_rows) = decoded_rows? else {
        return Ok(None);
    };

    #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
    let Some(decoded_rows) = decode_covering_projection_pairs(
        raw_pairs,
        store,
        plan.scalar_consistency(),
        covering.existing_row_mode,
        Ok::<Vec<Value>, InternalError>,
    )?
    else {
        return Ok(None);
    };

    if index_order {
        let mut projected_rows = assemble_covering_rows_in_index_order(
            decoded_rows,
            0,
            |(data_key, decoded_values)| {
                project_covering_row_from_owned_decoded_values(
                    &data_key,
                    covering.fields.as_slice(),
                    component_indices.as_slice(),
                    decoded_values,
                )
            },
        )?;
        apply_pure_covering_page_window(
            plan.scalar_plan().distinct,
            page,
            scan_time_page_window_applied,
            &mut projected_rows,
        );

        return Ok(Some(projected_rows));
    }

    let mut projected_rows = assemble_covering_rows_with_reorder(
        decoded_rows,
        order_contract,
        |(data_key, decoded_values)| {
            let projected_row = project_covering_row_from_owned_decoded_values(
                &data_key,
                covering.fields.as_slice(),
                component_indices.as_slice(),
                decoded_values,
            )?;

            Ok::<(DataKey, Vec<Value>), InternalError>((data_key, projected_row))
        },
    )?;
    apply_pure_covering_page_window(
        plan.scalar_plan().distinct,
        page,
        false,
        &mut projected_rows,
    );

    Ok(Some(projected_rows))
}

#[cfg(feature = "sql")]
fn try_execute_primary_store_covering_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    // Primary-store covering is safe only when traversal itself proves row
    // existence. Exact primary-key lookups still need the row-presence checking
    // lane, so they intentionally fall through to retained-slot execution.
    if covering.existing_row_mode != CoveringExistingRowMode::ProvenByPlanner {
        return Ok(None);
    }
    if !covering.fields.iter().all(|field| {
        matches!(
            field.source,
            CoveringReadFieldSource::PrimaryKey | CoveringReadFieldSource::Constant(_)
        )
    }) {
        return Ok(None);
    }

    let Some(stream) = primary_store_covering_key_stream(db, authority, plan, covering)? else {
        return Ok(None);
    };

    let page = plan.scalar_plan().page.as_ref();
    let scan_time_page_skip_count = pure_covering_scan_time_page_skip_count(
        covering.order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        page,
    );
    let scan_time_page_window_applied = pure_covering_scan_time_page_window_applied(
        covering.order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        page,
    );
    let mut projected_rows = assemble_primary_store_covering_rows_in_stream_order(
        stream,
        scan_time_page_skip_count,
        pure_covering_output_capacity_hint(page, scan_time_page_window_applied),
        covering,
    )?;
    apply_pure_covering_page_window(
        plan.scalar_plan().distinct,
        page,
        scan_time_page_window_applied,
        &mut projected_rows,
    );

    Ok(Some(projected_rows))
}

// Assemble primary-store covering projection rows directly from the ordered key
// stream. This route is admitted only for planner-proven row-present scans over
// primary-key and constant fields, so there is no row-presence check or reorder
// step that would require retaining a temporary key vector.
#[cfg(feature = "sql")]
fn assemble_primary_store_covering_rows_in_stream_order(
    stream: OrderedKeyStreamBox,
    skip_count: usize,
    output_capacity_hint: usize,
    covering: &CoveringReadExecutionPlan,
) -> Result<Vec<Vec<Value>>, InternalError> {
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let (row_assembly_local_instructions, projected_rows) = measure_structural_result(|| {
        collect_primary_store_covering_rows_in_stream_order(
            stream,
            skip_count,
            output_capacity_hint,
            covering,
        )
    });
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    record_pure_covering_row_assembly_local_instructions(row_assembly_local_instructions);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let projected_rows = projected_rows?;

    #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
    let projected_rows = collect_primary_store_covering_rows_in_stream_order(
        stream,
        skip_count,
        output_capacity_hint,
        covering,
    )?;

    Ok(projected_rows)
}

// Walk the primary-store key stream once and materialize only the final output
// rows that survive the scan-time offset. The helper keeps the fallible stream
// traversal outside the diagnostics wrapper call site while preserving the same
// projection assembly accounting as other pure covering lanes.
#[cfg(feature = "sql")]
fn collect_primary_store_covering_rows_in_stream_order(
    mut stream: OrderedKeyStreamBox,
    skip_count: usize,
    output_capacity_hint: usize,
    covering: &CoveringReadExecutionPlan,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(output_capacity_hint);
    let mut matched_keys = 0usize;
    while let Some(data_key) = stream.next_key()? {
        if matched_keys >= skip_count {
            projected_rows.push(project_covering_row_from_decoded_values(
                &data_key,
                covering.fields.as_slice(),
                &[],
                &[],
            )?);
        }
        matched_keys = matched_keys.saturating_add(1);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn pure_covering_output_capacity_hint(
    page: Option<&PageSpec>,
    page_window_already_applied: bool,
) -> usize {
    if !page_window_already_applied {
        return 0;
    }

    page.and_then(|page| page.limit)
        .map_or(0, |limit| usize::try_from(limit).unwrap_or(usize::MAX))
}

#[cfg(feature = "sql")]
fn primary_store_covering_key_stream<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> Result<Option<OrderedKeyStreamBox>, InternalError>
where
    C: CanisterKind,
{
    let CoveringProjectionOrder::PrimaryKeyOrder(direction) = covering.order_contract else {
        return Ok(None);
    };

    let store = db.recovered_store(authority.store_path())?;
    let scan_limit = pure_covering_scan_limit(
        covering.order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let scan_limit = (scan_limit != usize::MAX).then_some(scan_limit);

    if let Some((start, end)) = plan.access.as_primary_key_range_path() {
        let start = DataKey::try_from_structural_key(authority.entity_tag(), start)?;
        let end = DataKey::try_from_structural_key(authority.entity_tag(), end)?;

        return Ok(Some(OrderedKeyStreamBox::primary_range(
            PrimaryRangeKeyStream::new(store, start, end, direction, scan_limit)?,
        )));
    }
    if plan.access.is_single_full_scan() {
        let start = DataKey::lower_bound_for(authority.entity_tag());
        let end = DataKey::upper_bound_for(authority.entity_tag());

        return Ok(Some(OrderedKeyStreamBox::primary_range(
            PrimaryRangeKeyStream::new(store, start, end, direction, scan_limit)?,
        )));
    }

    Ok(None)
}

#[cfg(feature = "sql")]
fn pure_covering_scan_limit(
    order_contract: CoveringProjectionOrder,
    existing_row_mode: CoveringExistingRowMode,
    distinct: bool,
    page: Option<&PageSpec>,
) -> usize {
    if distinct {
        // DISTINCT windows apply after projected-row deduplication, so the
        // covering fast path must not pre-truncate the ordered input stream.
        return usize::MAX;
    }
    if existing_row_mode != CoveringExistingRowMode::ProvenByPlanner {
        return usize::MAX;
    }

    let Some(page) = page else {
        return usize::MAX;
    };
    if !matches!(
        order_contract,
        CoveringProjectionOrder::IndexOrder(_) | CoveringProjectionOrder::PrimaryKeyOrder(_)
    ) {
        return usize::MAX;
    }
    let Some(limit) = page.limit else {
        return usize::MAX;
    };

    page.offset
        .saturating_add(limit)
        .max(1)
        .try_into()
        .unwrap_or(usize::MAX)
}

#[cfg(feature = "sql")]
fn pure_covering_scan_time_page_skip_count(
    order_contract: CoveringProjectionOrder,
    existing_row_mode: CoveringExistingRowMode,
    distinct: bool,
    page: Option<&PageSpec>,
) -> usize {
    if !pure_covering_route_can_apply_page_during_scan(order_contract, existing_row_mode, distinct)
    {
        return 0;
    }

    page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX))
}

#[cfg(feature = "sql")]
fn pure_covering_scan_time_page_window_applied(
    order_contract: CoveringProjectionOrder,
    existing_row_mode: CoveringExistingRowMode,
    distinct: bool,
    page: Option<&PageSpec>,
) -> bool {
    if !pure_covering_route_can_apply_page_during_scan(order_contract, existing_row_mode, distinct)
    {
        return false;
    }

    page.is_some_and(|page| page.offset != 0 || page.limit.is_some())
}

#[cfg(feature = "sql")]
fn pure_covering_route_can_apply_page_during_scan(
    order_contract: CoveringProjectionOrder,
    existing_row_mode: CoveringExistingRowMode,
    distinct: bool,
) -> bool {
    !distinct
        && existing_row_mode == CoveringExistingRowMode::ProvenByPlanner
        && matches!(
            order_contract,
            CoveringProjectionOrder::IndexOrder(_) | CoveringProjectionOrder::PrimaryKeyOrder(_)
        )
}

#[cfg(feature = "sql")]
fn apply_pure_covering_page_window<T>(
    distinct: bool,
    page: Option<&PageSpec>,
    page_window_already_applied: bool,
    rows: &mut Vec<T>,
) {
    if distinct {
        // DISTINCT paging is deferred to the projection materializer after
        // projected-row deduplication over the ordered stream.
        return;
    }
    if page_window_already_applied {
        return;
    }

    let Some(page) = page else {
        return;
    };

    apply_offset_limit_window(rows, page.offset, page.limit);
}

#[cfg(feature = "sql")]
fn drop_scan_time_covering_offset(
    mut raw_pairs: CoveringProjectionComponentRows,
    skip_count: usize,
) -> CoveringProjectionComponentRows {
    let skip_count = skip_count.min(raw_pairs.len());
    if skip_count != 0 {
        raw_pairs.drain(..skip_count);
    }

    raw_pairs
}

#[cfg(feature = "sql")]
fn assemble_covering_rows_in_index_order<I>(
    items: Vec<I>,
    skip_count: usize,
    build_row: impl FnMut(I) -> Result<Vec<Value>, InternalError>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let mut build_row = build_row;
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let (row_assembly_local_instructions, projected_rows) = measure_structural_result(|| {
        collect_covering_rows_in_index_order(items, skip_count, &mut build_row)
    });
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    record_pure_covering_row_assembly_local_instructions(row_assembly_local_instructions);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let projected_rows = projected_rows?;

    #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
    let projected_rows = collect_covering_rows_in_index_order(items, skip_count, build_row)?;

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn assemble_covering_rows_with_reorder<I>(
    items: Vec<I>,
    order_contract: CoveringProjectionOrder,
    build_row: impl FnMut(I) -> Result<(DataKey, Vec<Value>), InternalError>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let mut build_row = build_row;
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let (row_assembly_local_instructions, projected_rows) =
        measure_structural_result(|| collect_covering_row_pairs_for_reorder(items, &mut build_row));
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    record_pure_covering_row_assembly_local_instructions(row_assembly_local_instructions);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let mut projected_rows = projected_rows?;

    #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
    let mut projected_rows = collect_covering_row_pairs_for_reorder(items, build_row)?;

    reorder_covering_projection_pairs(order_contract, projected_rows.as_mut_slice());

    Ok(strip_covering_projection_keys(projected_rows))
}

#[cfg(feature = "sql")]
fn collect_covering_rows_in_index_order<I>(
    items: Vec<I>,
    skip_count: usize,
    mut build_row: impl FnMut(I) -> Result<Vec<Value>, InternalError>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let capacity = items.len().saturating_sub(skip_count);
    let mut projected_rows = Vec::with_capacity(capacity);

    // Phase 1: preserve the existing index-order skip point while reserving
    // exactly the surviving row count known at the covering assembly boundary.
    for item in items.into_iter().skip(skip_count) {
        projected_rows.push(build_row(item)?);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn collect_covering_row_pairs_for_reorder<I>(
    items: Vec<I>,
    mut build_row: impl FnMut(I) -> Result<(DataKey, Vec<Value>), InternalError>,
) -> Result<Vec<(DataKey, Vec<Value>)>, InternalError> {
    let mut projected_rows = Vec::with_capacity(items.len());

    // Phase 1: reordered covering projections must retain keys until the
    // planner-owned order contract has been applied.
    for item in items {
        projected_rows.push(build_row(item)?);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn strip_covering_projection_keys(projected_rows: Vec<(DataKey, Vec<Value>)>) -> Vec<Vec<Value>> {
    let mut rows = Vec::with_capacity(projected_rows.len());

    // Phase 1: after reordering, keys are no longer part of the public
    // projection payload, so move each row into the final matrix directly.
    for (_data_key, row) in projected_rows {
        rows.push(row);
    }

    rows
}
