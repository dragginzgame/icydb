#[cfg(all(feature = "sql", feature = "diagnostics"))]
use crate::db::session::sql::projection::runtime::{
    measure_structural_result, record_pure_covering_decode_local_instructions,
    record_pure_covering_row_assembly_local_instructions,
};
use crate::{
    db::{
        Db,
        access::{lower_index_prefix_specs, lower_index_range_specs},
        data::DataKey,
        executor::{
            EntityAuthority, covering_projection_scan_direction, decode_covering_projection_pairs,
            decode_single_covering_projection_pairs, map_covering_projection_pairs,
            reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
        },
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder, PageSpec,
            covering_read_execution_plan_from_fields,
        },
        session::sql::projection::runtime::{
            covering::shared::{
                covering_projection_component_indices, project_covering_row_from_decoded_values,
                project_covering_row_from_single_decoded_value,
            },
            materialize::apply_sql_projection_page_window,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

#[cfg(feature = "sql")]
#[expect(clippy::too_many_lines)]
pub(in crate::db::session::sql::projection::runtime) fn try_execute_covering_sql_projection_rows_for_canister<
    C,
>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 0: this SQL-side shortcut only owns index-backed covering scans.
    // Planner-proven full-scan / primary-key covering routes still flow
    // through the structural executor path, which already knows how to shape
    // those rows without pretending there are covering index components.
    if plan.access.as_index_prefix_path().is_none() && plan.access.as_index_range_path().is_none() {
        return Ok(None);
    }
    if plan.has_residual_predicate() {
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
    if covering.fields.iter().any(|field| {
        matches!(
            field.source,
            crate::db::query::plan::CoveringReadFieldSource::RowField
        )
    }) {
        return Ok(None);
    }

    let component_indices = covering_projection_component_indices(covering.fields.as_slice());
    let store = db.recovered_store(authority.store_path())?;
    let index_prefix_specs = lower_index_prefix_specs(authority.entity_tag(), &plan.access)?;
    let index_range_specs = lower_index_range_specs(authority.entity_tag(), &plan.access)?;

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
        index_prefix_specs.as_slice(),
        index_range_specs.as_slice(),
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
                plan.scalar_plan().consistency,
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
            plan.scalar_plan().consistency,
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
                plan.scalar_plan().consistency,
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
            plan.scalar_plan().consistency,
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
            plan.scalar_plan().consistency,
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
        plan.scalar_plan().consistency,
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
    if !matches!(order_contract, CoveringProjectionOrder::IndexOrder(_)) {
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
    #[allow(unused_mut)]
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
    #[allow(unused_mut)]
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
