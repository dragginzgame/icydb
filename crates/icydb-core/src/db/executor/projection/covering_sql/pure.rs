#[cfg(all(feature = "sql", feature = "diagnostics"))]
use crate::db::executor::projection::covering_sql::{
    measure_structural_result, record_pure_covering_decode_local_instructions,
    record_pure_covering_row_assembly_local_instructions,
};
use crate::{
    db::{
        Db,
        access::lower_access,
        data::DataKey,
        executor::projection::covering_sql::{
            apply_sql_projection_page_window,
            shared::{
                covering_projection_component_indices, project_covering_row_from_decoded_values,
                project_covering_row_from_single_decoded_value,
            },
        },
        executor::{
            EntityAuthority, OrderedKeyStreamBox, PrimaryRangeKeyStream,
            covering_projection_scan_direction, decode_covering_projection_pairs,
            decode_single_covering_projection_pairs, map_covering_projection_pairs,
            reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
        },
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
            CoveringReadExecutionPlan, CoveringReadFieldSource, PageSpec,
            covering_read_execution_plan_from_fields,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
pub(super) fn try_execute_covering_sql_projection_rows_for_canister<C>(
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
    // row-backed fields in SQL projection materialization.
    let Some(covering) = covering_read_execution_plan_from_fields(
        authority.model().fields(),
        plan,
        authority.primary_key_name(),
        true,
    ) else {
        return Ok(None);
    };
    if covering
        .fields
        .iter()
        .any(|field| matches!(field.source, CoveringReadFieldSource::RowField))
    {
        return Ok(None);
    }

    if let Some(projected_rows) =
        try_execute_primary_store_covering_sql_projection_rows_for_canister(
            db, authority, plan, &covering,
        )?
    {
        return Ok(Some(projected_rows));
    }

    // Phase 2: the remaining pure SQL covering shortcut owns index-backed scans.
    if plan.access.as_index_prefix_path().is_none() && plan.access.as_index_range_path().is_none() {
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
        |index| db.recovered_store(index.store()),
    )?;
    let page = plan.scalar_plan().page.as_ref();
    let order_contract = covering.order_contract;
    let index_order = matches!(order_contract, CoveringProjectionOrder::IndexOrder(_));

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
            let mut projected_rows =
                assemble_covering_rows_in_index_order(projected_keys, |(data_key, ())| {
                    project_covering_row_from_decoded_values(
                        &data_key,
                        covering.fields.as_slice(),
                        &[],
                        &[],
                    )
                })?;
            apply_pure_covering_page_window(plan.scalar_plan().distinct, page, &mut projected_rows);

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
        apply_pure_covering_page_window(plan.scalar_plan().distinct, page, &mut projected_rows);

        return Ok(Some(projected_rows));
    }

    // Phase 3b: one-component pure covering rows can skip the generic
    // decoded-vector contract and carry one runtime `Value` directly through
    // the SQL assembly path.
    if component_indices.len() == 1 {
        let component_index = component_indices[0];

        #[cfg(all(feature = "sql", feature = "diagnostics"))]
        let (decode_local_instructions, decoded_rows) = measure_structural_result(|| {
            decode_single_covering_projection_pairs(
                raw_pairs,
                store,
                plan.scalar_consistency(),
                covering.existing_row_mode,
                "pure covering SQL projection expected one decodable covering component payload",
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
            "pure covering SQL projection expected one decodable covering component payload",
            Ok::<Value, InternalError>,
        )?
        else {
            return Ok(None);
        };

        if index_order {
            let mut projected_rows = assemble_covering_rows_in_index_order(
                decoded_rows,
                |(data_key, decoded_value)| {
                    project_covering_row_from_single_decoded_value(
                        &data_key,
                        covering.fields.as_slice(),
                        component_index,
                        &decoded_value,
                    )
                },
            )?;
            apply_pure_covering_page_window(plan.scalar_plan().distinct, page, &mut projected_rows);

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
                    &decoded_value,
                )?;

                Ok::<(DataKey, Vec<Value>), InternalError>((data_key, projected_row))
            },
        )?;
        apply_pure_covering_page_window(plan.scalar_plan().distinct, page, &mut projected_rows);

        return Ok(Some(projected_rows));
    }

    // Phase 3: reuse the executor-owned covering decode contract so planner-
    // proven routes avoid row-store reads entirely while row-check-required
    // routes still preserve missing-row consistency rules.
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
        let mut projected_rows =
            assemble_covering_rows_in_index_order(decoded_rows, |(data_key, decoded_values)| {
                project_covering_row_from_decoded_values(
                    &data_key,
                    covering.fields.as_slice(),
                    component_indices.as_slice(),
                    decoded_values.as_slice(),
                )
            })?;
        apply_pure_covering_page_window(plan.scalar_plan().distinct, page, &mut projected_rows);

        return Ok(Some(projected_rows));
    }

    let mut projected_rows = assemble_covering_rows_with_reorder(
        decoded_rows,
        order_contract,
        |(data_key, decoded_values)| {
            let projected_row = project_covering_row_from_decoded_values(
                &data_key,
                covering.fields.as_slice(),
                component_indices.as_slice(),
                decoded_values.as_slice(),
            )?;

            Ok::<(DataKey, Vec<Value>), InternalError>((data_key, projected_row))
        },
    )?;
    apply_pure_covering_page_window(plan.scalar_plan().distinct, page, &mut projected_rows);

    Ok(Some(projected_rows))
}

#[cfg(feature = "sql")]
fn try_execute_primary_store_covering_sql_projection_rows_for_canister<C>(
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

    let Some(mut stream) = primary_store_covering_key_stream(db, authority, plan, covering)? else {
        return Ok(None);
    };

    let mut projected_keys = Vec::new();
    while let Some(data_key) = stream.next_key()? {
        projected_keys.push(data_key);
    }

    let mut projected_rows = assemble_covering_rows_in_index_order(projected_keys, |data_key| {
        project_covering_row_from_decoded_values(&data_key, covering.fields.as_slice(), &[], &[])
    })?;
    apply_pure_covering_page_window(
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
        &mut projected_rows,
    );

    Ok(Some(projected_rows))
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
        // SQL DISTINCT windows apply after projected-row deduplication, so the
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
fn apply_pure_covering_page_window<T>(distinct: bool, page: Option<&PageSpec>, rows: &mut Vec<T>) {
    if distinct {
        // DISTINCT paging is deferred to the SQL projection materializer after
        // projected-row deduplication over the ordered stream.
        return;
    }

    let Some(page) = page else {
        return;
    };

    apply_sql_projection_page_window(rows, page.offset, page.limit);
}

#[cfg(feature = "sql")]
fn assemble_covering_rows_in_index_order<I>(
    items: Vec<I>,
    build_row: impl FnMut(I) -> Result<Vec<Value>, InternalError>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let mut build_row = build_row;
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let (row_assembly_local_instructions, projected_rows) = measure_structural_result(|| {
        items
            .into_iter()
            .map(&mut build_row)
            .collect::<Result<Vec<_>, _>>()
    });
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    record_pure_covering_row_assembly_local_instructions(row_assembly_local_instructions);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let projected_rows = projected_rows?;

    #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
    let projected_rows = items
        .into_iter()
        .map(build_row)
        .collect::<Result<Vec<_>, _>>()?;

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
    let (row_assembly_local_instructions, projected_rows) = measure_structural_result(|| {
        items
            .into_iter()
            .map(&mut build_row)
            .collect::<Result<Vec<_>, _>>()
    });
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    record_pure_covering_row_assembly_local_instructions(row_assembly_local_instructions);
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    let mut projected_rows = projected_rows?;

    #[cfg(not(all(feature = "sql", feature = "diagnostics")))]
    let mut projected_rows = items
        .into_iter()
        .map(build_row)
        .collect::<Result<Vec<_>, _>>()?;

    reorder_covering_projection_pairs(order_contract, projected_rows.as_mut_slice());

    Ok(projected_rows
        .into_iter()
        .map(|(_data_key, row)| row)
        .collect())
}
