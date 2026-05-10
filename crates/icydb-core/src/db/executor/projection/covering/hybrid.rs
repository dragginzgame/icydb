use crate::{
    db::{
        Db,
        access::lower_access,
        data::{DataKey, DataStore},
        executor::projection::covering::{
            CoveringProjectionMetricsRecorder,
            shared::{covering_projection_component_indices, decode_hybrid_covering_components},
        },
        executor::{
            EntityAuthority, apply_offset_limit_window, covering_projection_scan_direction,
            resolve_covering_projection_components_from_lowered_specs, terminal::RowLayout,
        },
        query::plan::{
            AccessPlannedQuery, CoveringProjectionOrder, CoveringReadField,
            CoveringReadFieldSource, PageSpec,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    value::{Value, storage_key_as_runtime_value},
};
use std::collections::BTreeMap;

#[cfg(feature = "sql")]
pub(super) fn try_execute_hybrid_covering_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    metrics: CoveringProjectionMetricsRecorder,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 0: hybrid projection mixes index-backed covering components
    // with sparse row-backed field reads, so it only applies to genuine
    // secondary-index access paths.
    if plan.access.as_index_prefix_contract_path().is_none()
        && plan.access.as_index_range_path().is_none()
    {
        return Ok(None);
    }

    // Phase 1: admit only the planner-owned direct projection shapes that mix
    // covering-backed fields with row-backed sparse reads over one index path.
    let Some(hybrid) = authority.covering_hybrid_projection_plan(plan) else {
        return Ok(None);
    };

    let component_indices = covering_projection_component_indices(hybrid.fields.as_slice());
    let row_field_slots = hybrid_projection_row_field_slots(hybrid.fields.as_slice());
    let store = db.recovered_store(authority.store_path())?;
    let lowered_access = lower_access(authority.entity_tag(), &plan.access)
        .map_err(crate::db::access::LoweredAccessError::into_internal_error)?;
    let index_prefix_specs = lowered_access.index_prefix_specs();
    let index_range_specs = lowered_access.index_range_specs();

    // Phase 2: read the covering-backed component payloads in the order
    // implied by the planner-owned covering order contract.
    let scan_direction = covering_projection_scan_direction(hybrid.order_contract);
    let scan_limit = hybrid_covering_scan_limit(
        hybrid.order_contract,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let scan_time_page_skip_count = hybrid_covering_scan_time_page_skip_count(
        hybrid.order_contract,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let scan_time_page_window_applied = hybrid_covering_scan_time_page_window_applied(
        hybrid.order_contract,
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

    // Phase 3: assemble final projected rows by mixing decoded covering
    // values with sparse row-backed field reads for uncovered slots.
    metrics.record_hybrid_path_hit();
    let mut projected_rows = store.with_data(|data_store| {
        let mut projected_rows =
            Vec::with_capacity(raw_pairs.len().saturating_sub(scan_time_page_skip_count));
        let mut projected_row_count = 0usize;

        for (data_key, _existence_witness, components) in raw_pairs {
            let sparse_row_fields = read_hybrid_projection_row_fields_from_store(
                authority.row_layout(),
                data_store,
                &data_key,
                row_field_slots.as_slice(),
            )?;
            let Some(sparse_row_fields) = sparse_row_fields else {
                continue;
            };
            if projected_row_count < scan_time_page_skip_count {
                projected_row_count = projected_row_count.saturating_add(1);
                continue;
            }

            let decoded_components =
                decode_hybrid_covering_components(component_indices.as_slice(), components)?;
            let projected_row = project_hybrid_covering_row(
                &data_key,
                hybrid.fields.as_slice(),
                decoded_components,
                sparse_row_fields,
                metrics,
            )?;

            projected_rows.push((data_key, projected_row));
            projected_row_count = projected_row_count.saturating_add(1);
        }

        Ok::<Vec<(DataKey, Vec<Value>)>, InternalError>(projected_rows)
    })?;
    crate::db::executor::reorder_covering_projection_pairs(
        hybrid.order_contract,
        projected_rows.as_mut_slice(),
    );
    if !plan.scalar_plan().distinct
        && !scan_time_page_window_applied
        && let Some(page) = plan.scalar_plan().page.as_ref()
    {
        apply_offset_limit_window(&mut projected_rows, page.offset, page.limit);
    }

    Ok(Some(
        projected_rows
            .into_iter()
            .map(|(_data_key, row)| row)
            .collect(),
    ))
}

#[cfg(feature = "sql")]
fn hybrid_covering_scan_limit(
    order_contract: CoveringProjectionOrder,
    distinct: bool,
    page: Option<&PageSpec>,
) -> usize {
    if distinct {
        // DISTINCT windows apply after projected-row deduplication, so the
        // hybrid covering fast path must keep the full ordered input stream.
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
fn hybrid_covering_scan_time_page_skip_count(
    order_contract: CoveringProjectionOrder,
    distinct: bool,
    page: Option<&PageSpec>,
) -> usize {
    if !hybrid_covering_route_can_apply_page_during_scan(order_contract, distinct) {
        return 0;
    }

    page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX))
}

#[cfg(feature = "sql")]
fn hybrid_covering_scan_time_page_window_applied(
    order_contract: CoveringProjectionOrder,
    distinct: bool,
    page: Option<&PageSpec>,
) -> bool {
    if !hybrid_covering_route_can_apply_page_during_scan(order_contract, distinct) {
        return false;
    }

    page.is_some_and(|page| page.offset != 0 || page.limit.is_some())
}

#[cfg(feature = "sql")]
const fn hybrid_covering_route_can_apply_page_during_scan(
    order_contract: CoveringProjectionOrder,
    distinct: bool,
) -> bool {
    !distinct && matches!(order_contract, CoveringProjectionOrder::IndexOrder(_))
}

#[cfg(feature = "sql")]
fn hybrid_projection_row_field_slots(fields: &[CoveringReadField]) -> Vec<usize> {
    let mut row_field_slots = Vec::with_capacity(fields.len());

    for field in fields {
        if !matches!(field.source, CoveringReadFieldSource::RowField) {
            continue;
        }
        if row_field_slots.contains(&field.field_slot.index()) {
            continue;
        }

        row_field_slots.push(field.field_slot.index());
    }

    row_field_slots
}

#[cfg(feature = "sql")]
fn read_hybrid_projection_row_fields_from_store(
    row_layout: RowLayout,
    data_store: &DataStore,
    data_key: &DataKey,
    row_field_slots: &[usize],
) -> Result<Option<BTreeMap<usize, Value>>, InternalError> {
    // Phase 1: empty row-backed hybrids stay on the covering-only path.
    if row_field_slots.is_empty() {
        return Ok(Some(BTreeMap::new()));
    }

    // Phase 2: fetch the persisted row once. The store boundary still returns
    // one owned `RawRow`, so hybrid selective reads reduce decode work here
    // but do not yet avoid the full row fetch itself.
    let raw_key = data_key.to_raw()?;

    // Phase 3: fetch the raw row from storage and keep sparse slot decode in
    // executor ownership. The one-slot and indexed decode paths stay explicit so
    // storage never decides an execution decode strategy.
    let Some(raw_row) = data_store.get(&raw_key) else {
        return Ok(None);
    };
    if let [required_slot] = row_field_slots {
        let Some(value) =
            row_layout.decode_required_value(&raw_row, data_key.storage_key(), *required_slot)?
        else {
            return Err(InternalError::query_executor_invariant(
                "hybrid projection sparse row decode expected declared direct field value",
            ));
        };
        let mut row_fields = BTreeMap::new();
        row_fields.insert(*required_slot, value);

        return Ok(Some(row_fields));
    }

    let decoded =
        row_layout.decode_indexed_values(&raw_row, data_key.storage_key(), row_field_slots)?;

    // Phase 4: rebuild the field-slot map expected by the hybrid projection
    // row shaper from the compact executor-owned selective decode result.
    let mut row_fields = BTreeMap::new();

    for (slot, value) in row_field_slots.iter().copied().zip(decoded) {
        let Some(value) = value else {
            return Err(InternalError::query_executor_invariant(
                "hybrid projection sparse row decode expected declared direct field value",
            ));
        };
        row_fields.insert(slot, value);
    }

    Ok(Some(row_fields))
}

#[cfg(feature = "sql")]
fn project_hybrid_covering_row(
    data_key: &DataKey,
    fields: &[CoveringReadField],
    mut decoded_components: BTreeMap<usize, Value>,
    mut row_fields: BTreeMap<usize, Value>,
    metrics: CoveringProjectionMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    let mut projected = Vec::with_capacity(fields.len());
    let mut remaining_index_component_uses = covering_index_component_use_counts(fields);
    let mut remaining_row_field_uses = covering_row_field_use_counts(fields);

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index } => {
                metrics.record_hybrid_index_field_access();

                take_or_clone_last_covering_value(
                    &mut decoded_components,
                    &mut remaining_index_component_uses,
                    *component_index,
                    "hybrid projection missing decoded covering component",
                )?
            }
            CoveringReadFieldSource::PrimaryKey => {
                storage_key_as_runtime_value(&data_key.storage_key())
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                metrics.record_hybrid_row_field_access();

                take_or_clone_last_covering_value(
                    &mut row_fields,
                    &mut remaining_row_field_uses,
                    field.field_slot.index(),
                    "hybrid projection missing sparse row-backed field value",
                )?
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

#[cfg(feature = "sql")]
fn covering_index_component_use_counts(fields: &[CoveringReadField]) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for field in fields {
        let CoveringReadFieldSource::IndexComponent { component_index } = &field.source else {
            continue;
        };
        *counts.entry(*component_index).or_insert(0) += 1;
    }

    counts
}

#[cfg(feature = "sql")]
fn covering_row_field_use_counts(fields: &[CoveringReadField]) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for field in fields {
        if !matches!(field.source, CoveringReadFieldSource::RowField) {
            continue;
        }
        *counts.entry(field.field_slot.index()).or_insert(0) += 1;
    }

    counts
}

#[cfg(feature = "sql")]
fn take_or_clone_last_covering_value(
    values: &mut BTreeMap<usize, Value>,
    remaining_uses: &mut BTreeMap<usize, usize>,
    slot: usize,
    missing_message: &'static str,
) -> Result<Value, InternalError> {
    let Some(remaining) = remaining_uses.get_mut(&slot) else {
        return Err(InternalError::query_executor_invariant(missing_message));
    };

    // Projected columns are independently owned. Duplicate references clone
    // from the per-row sparse map until the final projected use can consume it.
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        return values
            .remove(&slot)
            .ok_or_else(|| InternalError::query_executor_invariant(missing_message));
    }

    values
        .get(&slot)
        .cloned()
        .ok_or_else(|| InternalError::query_executor_invariant(missing_message))
}
