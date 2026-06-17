use crate::{
    db::{
        Db,
        access::lower_access,
        data::{DataStore, DecodedDataStoreKey},
        executor::projection::covering::{
            CoveringProjectionMetricsRecorder,
            contracts::{
                AccessPlannedQuery, CoveringReadField, CoveringReadFieldSource, CoveringReadPlan,
            },
            shared::{
                apply_covering_page_window, covering_projection_component_indices,
                covering_scan_window, decode_hybrid_covering_components,
            },
        },
        executor::{
            EntityAuthority, ExecutorPlanError,
            resolve_covering_projection_components_from_lowered_specs, terminal::RowLayout,
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
use std::collections::BTreeMap;

pub(super) fn try_execute_hybrid_covering_projection_rows_with_plan_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    metrics: CoveringProjectionMetricsRecorder,
    hybrid: &CoveringReadPlan,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    if plan.has_residual_filter_expr() {
        return Ok(None);
    }
    if plan.has_residual_filter_predicate() && index_predicate_execution.is_none() {
        return Ok(None);
    }

    if plan.access.as_index_prefix_contract_path().is_none()
        && plan.access.as_index_branch_set_spec_path().is_none()
        && plan.access.as_index_range_path().is_none()
    {
        return Ok(None);
    }

    let component_indices = covering_projection_component_indices(hybrid.fields.as_slice());
    let row_field_slots = hybrid_projection_row_field_slots(hybrid.fields.as_slice());
    let store = db.recovered_store(authority.store_path())?;
    let lowered_access =
        lower_access(authority.entity_tag(), &plan.access).map_err(|err| match err {
            crate::db::access::LoweredAccessError::IndexPrefix => {
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            }
            crate::db::access::LoweredAccessError::IndexRange => {
                ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error()
            }
        })?;
    let index_prefix_specs = lowered_access.index_prefix_specs();
    let index_range_specs = lowered_access.index_range_specs();

    let scan_window = covering_scan_window(
        hybrid.order_contract,
        plan.access.as_index_branch_set_spec_path().is_some()
            || plan.access.as_index_prefix_contract_path().is_some(),
        true,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let raw_pairs = resolve_covering_projection_components_from_lowered_specs(
        authority.entity_tag(),
        index_prefix_specs,
        index_range_specs,
        scan_window.direction,
        scan_window.limit,
        component_indices.as_slice(),
        index_predicate_execution,
        |store_path| db.recovered_store(store_path),
    )?;

    // Phase 3: assemble final projected rows by mixing decoded covering
    // values with sparse row-backed field reads for uncovered slots.
    metrics.record_hybrid_path_hit();
    let mut projected_rows = store.with_data(|data_store| {
        let mut projected_rows =
            Vec::with_capacity(raw_pairs.len().saturating_sub(scan_window.page_skip_count));
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
            if projected_row_count < scan_window.page_skip_count {
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

        Ok::<Vec<(DecodedDataStoreKey, Vec<Value>)>, InternalError>(projected_rows)
    })?;
    crate::db::executor::reorder_covering_projection_pairs(
        hybrid.order_contract,
        projected_rows.as_mut_slice(),
    );
    apply_covering_page_window(
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
        scan_window.page_window_applied,
        &mut projected_rows,
    );

    Ok(Some(
        projected_rows
            .into_iter()
            .map(|(_data_key, row)| row)
            .collect(),
    ))
}

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

fn read_hybrid_projection_row_fields_from_store(
    row_layout: RowLayout,
    data_store: &DataStore,
    data_key: &DecodedDataStoreKey,
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
            row_layout.decode_required_value_from_data_key(&raw_row, data_key, *required_slot)?
        else {
            return Err(InternalError::query_executor_invariant());
        };
        let mut row_fields = BTreeMap::new();
        row_fields.insert(*required_slot, value);

        return Ok(Some(row_fields));
    }

    let decoded =
        row_layout.decode_indexed_values_from_data_key(&raw_row, data_key, row_field_slots)?;

    // Phase 4: rebuild the field-slot map expected by the hybrid projection
    // row shaper from the compact executor-owned selective decode result.
    let mut row_fields = BTreeMap::new();

    for (slot, value) in row_field_slots.iter().copied().zip(decoded) {
        let Some(value) = value else {
            return Err(InternalError::query_executor_invariant());
        };
        row_fields.insert(slot, value);
    }

    Ok(Some(row_fields))
}

fn project_hybrid_covering_row(
    data_key: &DecodedDataStoreKey,
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
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                metrics.record_hybrid_index_field_access();

                take_or_clone_last_covering_value(
                    &mut decoded_components,
                    &mut remaining_index_component_uses,
                    *component_index,
                )?
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                metrics.record_hybrid_row_field_access();

                take_or_clone_last_covering_value(
                    &mut row_fields,
                    &mut remaining_row_field_uses,
                    field.field_slot.index(),
                )?
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

fn covering_index_component_use_counts(fields: &[CoveringReadField]) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for field in fields {
        let component_index = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                component_index
            }
            CoveringReadFieldSource::PrimaryKey { .. }
            | CoveringReadFieldSource::Constant(_)
            | CoveringReadFieldSource::RowField => continue,
        };
        *counts.entry(*component_index).or_insert(0) += 1;
    }

    counts
}

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

fn take_or_clone_last_covering_value(
    values: &mut BTreeMap<usize, Value>,
    remaining_uses: &mut BTreeMap<usize, usize>,
    slot: usize,
) -> Result<Value, InternalError> {
    let Some(remaining) = remaining_uses.get_mut(&slot) else {
        return Err(InternalError::query_executor_invariant());
    };

    // Projected columns are independently owned. Duplicate references clone
    // from the per-row sparse map until the final projected use can consume it.
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        return values
            .remove(&slot)
            .ok_or_else(InternalError::query_executor_invariant);
    }

    values
        .get(&slot)
        .cloned()
        .ok_or_else(InternalError::query_executor_invariant)
}
