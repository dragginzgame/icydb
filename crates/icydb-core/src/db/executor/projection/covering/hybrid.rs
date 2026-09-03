use crate::{
    db::{
        Db,
        data::{DataStore, DecodedDataStoreKey},
        executor::projection::covering::{
            PreparedCoveringProjectionRuntime,
            contracts::{
                AccessPlannedQuery, CoveringExistingRowMode, CoveringHybridReadExecutionPlan,
                CoveringReadField, CoveringReadFieldSource,
            },
            shared::{
                CoveringIndexScanRequest, PreparedCoveringIndexScan, apply_covering_page_window,
                covering_residual_filter_supported, decode_hybrid_covering_components,
                resolve_index_backed_covering_scan,
            },
        },
        executor::{
            EntityAuthority, IndexComponentRows, budget::charge_current_execution_budget,
            terminal::RowLayout,
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;

pub(super) fn try_execute_hybrid_covering_projection_rows_with_plan_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    runtime: PreparedCoveringProjectionRuntime<'_>,
    hybrid: &CoveringHybridReadExecutionPlan,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    if !covering_residual_filter_supported(
        runtime.plan,
        hybrid.strict_predicate_compatible,
        runtime.index_predicate_execution.is_some(),
    ) {
        return Ok(None);
    }

    let row_field_slots = hybrid_projection_row_field_slots(hybrid.fields.as_slice());
    let Some(PreparedCoveringIndexScan {
        component_indices,
        raw_pairs,
        scan_window,
        store,
        existing_row_mode,
        ..
    }) = resolve_index_backed_covering_scan(
        db,
        &authority,
        CoveringIndexScanRequest {
            plan: runtime.plan,
            index_prefix_specs: runtime.index_prefix_specs,
            index_range_specs: runtime.index_range_specs,
            fields: hybrid.fields.as_slice(),
            order_contract: hybrid.order_contract,
            existing_row_mode: hybrid.existing_row_mode,
            index_predicate_execution: runtime.index_predicate_execution,
        },
    )?
    else {
        return Ok(None);
    };
    let row_presence_proven = existing_row_mode == CoveringExistingRowMode::ProvenByPlanner;

    let row_layout = authority.row_layout()?;
    let ownership = HybridProjectionOwnership::compile(hybrid.fields.as_slice());

    store.with_data(|data_store| {
        let projected_rows = if row_presence_proven {
            execute_hybrid_covering_projection_with_proven_rows(
                &row_layout,
                data_store,
                runtime.plan,
                hybrid,
                component_indices.as_slice(),
                row_field_slots.as_slice(),
                scan_window.page_skip_count,
                scan_window.page_window_applied,
                raw_pairs,
                &ownership,
            )?
        } else {
            execute_hybrid_covering_projection_with_checked_rows(
                &row_layout,
                data_store,
                runtime.plan,
                hybrid,
                component_indices.as_slice(),
                row_field_slots.as_slice(),
                scan_window.page_skip_count,
                scan_window.page_window_applied,
                raw_pairs,
                &ownership,
            )?
        };

        Ok(Some(projected_rows))
    })
}

#[expect(clippy::too_many_arguments)]
fn execute_hybrid_covering_projection_with_proven_rows(
    row_layout: &RowLayout,
    data_store: &DataStore,
    plan: &AccessPlannedQuery,
    hybrid: &CoveringHybridReadExecutionPlan,
    component_indices: &[usize],
    row_field_slots: &[usize],
    page_skip_count: usize,
    page_window_applied: bool,
    raw_pairs: IndexComponentRows,
    ownership: &HybridProjectionOwnership,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut keyed_components = Vec::with_capacity(raw_pairs.len().saturating_sub(page_skip_count));

    for (data_key, _existence_witness, components) in raw_pairs.into_iter().skip(page_skip_count) {
        keyed_components.push((data_key, components));
    }

    crate::db::executor::reorder_covering_projection_pairs(
        hybrid.order_contract,
        keyed_components.as_mut_slice(),
    );
    apply_covering_page_window(
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
        page_window_applied,
        &mut keyed_components,
    );

    let mut projected_rows = Vec::with_capacity(keyed_components.len());
    for (data_key, components) in keyed_components {
        let sparse_row_fields = read_hybrid_projection_row_fields_from_store(
            row_layout,
            data_store,
            &data_key,
            row_field_slots,
            false,
        )?
        .ok_or_else(InternalError::query_executor_invariant)?;
        let decoded_components = decode_hybrid_covering_components(component_indices, components)?;
        let projected_row = project_hybrid_covering_row(
            &data_key,
            hybrid.fields.as_slice(),
            decoded_components,
            sparse_row_fields,
            ownership,
        )?;

        projected_rows.push(projected_row);
    }

    Ok(projected_rows)
}

#[expect(clippy::too_many_arguments)]
fn execute_hybrid_covering_projection_with_checked_rows(
    row_layout: &RowLayout,
    data_store: &DataStore,
    plan: &AccessPlannedQuery,
    hybrid: &CoveringHybridReadExecutionPlan,
    component_indices: &[usize],
    row_field_slots: &[usize],
    page_skip_count: usize,
    page_window_applied: bool,
    raw_pairs: IndexComponentRows,
    ownership: &HybridProjectionOwnership,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(raw_pairs.len().saturating_sub(page_skip_count));
    let mut projected_row_count = 0usize;

    for (data_key, _existence_witness, components) in raw_pairs {
        let sparse_row_fields = read_hybrid_projection_row_fields_from_store(
            row_layout,
            data_store,
            &data_key,
            row_field_slots,
            true,
        )?;
        let Some(sparse_row_fields) = sparse_row_fields else {
            if matches!(plan.scalar_consistency(), MissingRowPolicy::Error) {
                return Err(crate::db::executor::ExecutorError::missing_row(&data_key).into());
            }
            continue;
        };
        if projected_row_count < page_skip_count {
            projected_row_count = projected_row_count.saturating_add(1);
            continue;
        }

        let decoded_components = decode_hybrid_covering_components(component_indices, components)?;
        let projected_row = project_hybrid_covering_row(
            &data_key,
            hybrid.fields.as_slice(),
            decoded_components,
            sparse_row_fields,
            ownership,
        )?;

        projected_rows.push((data_key, projected_row));
        projected_row_count = projected_row_count.saturating_add(1);
    }

    crate::db::executor::reorder_covering_projection_pairs(
        hybrid.order_contract,
        projected_rows.as_mut_slice(),
    );
    apply_covering_page_window(
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
        page_window_applied,
        &mut projected_rows,
    );

    Ok(projected_rows
        .into_iter()
        .map(|(_data_key, row)| row)
        .collect())
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
    row_layout: &RowLayout,
    data_store: &DataStore,
    data_key: &DecodedDataStoreKey,
    row_field_slots: &[usize],
    check_presence_without_row_fields: bool,
) -> Result<Option<Vec<(usize, Value)>>, InternalError> {
    // Phase 1: a checked covering-only hybrid still owes an authoritative
    // existence probe even though it has no row-backed projection slot.
    if row_field_slots.is_empty() {
        if check_presence_without_row_fields {
            charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
            let raw_key = data_key.to_raw()?;
            if !data_store.contains(&raw_key) {
                return Ok(None);
            }
        }
        return Ok(Some(Vec::new()));
    }

    // Phase 2: fetch the persisted row once. The store boundary still returns
    // one owned `RawRow`, so hybrid selective reads reduce decode work here
    // but do not yet avoid the full row fetch itself.
    let raw_key = data_key.to_raw()?;

    // Phase 3: fetch the raw row from storage and keep sparse slot decode in
    // executor ownership. The one-slot and indexed decode paths stay explicit so
    // storage never decides an execution decode strategy.
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
    let Some(raw_row) = data_store.get(&raw_key) else {
        return Ok(None);
    };
    let raw_bytes = u64::try_from(raw_row.len()).unwrap_or(u64::MAX);
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::StoredBytesRead,
        raw_bytes,
    )?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::DecodedBytes, raw_bytes)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::MaterializedBytes,
        raw_bytes,
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        u64::try_from(row_field_slots.len()).unwrap_or(u64::MAX),
    )?;
    if let [required_slot] = row_field_slots {
        let Some(value) =
            row_layout.decode_required_value_from_data_key(&raw_row, data_key, *required_slot)?
        else {
            return Err(InternalError::query_executor_invariant());
        };
        let row_fields = vec![(*required_slot, value)];

        return Ok(Some(row_fields));
    }

    let decoded =
        row_layout.decode_indexed_values_from_data_key(&raw_row, data_key, row_field_slots)?;

    // Phase 4: rebuild the field-slot map expected by the hybrid projection
    // row shaper from the compact executor-owned selective decode result.
    let mut row_fields = Vec::with_capacity(row_field_slots.len());

    for (slot, value) in row_field_slots.iter().copied().zip(decoded) {
        let Some(value) = value else {
            return Err(InternalError::query_executor_invariant());
        };
        row_fields.push((slot, value));
    }

    Ok(Some(row_fields))
}

fn project_hybrid_covering_row(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    mut decoded_components: Vec<(usize, Value)>,
    mut row_fields: Vec<(usize, Value)>,
    ownership: &HybridProjectionOwnership,
) -> Result<Vec<Value>, InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        u64::try_from(fields.len()).unwrap_or(u64::MAX),
    )?;
    let mut projected = Vec::with_capacity(fields.len());

    for (field_index, field) in fields.iter().enumerate() {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                take_or_clone_compact_value(
                    &mut decoded_components,
                    *component_index,
                    ownership.move_on_use(field_index)?,
                )?
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => take_or_clone_compact_value(
                &mut row_fields,
                field.field_slot.index(),
                ownership.move_on_use(field_index)?,
            )?,
        };
        projected.push(value);
    }

    Ok(projected)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HybridOwnedSource {
    IndexComponent(usize),
    RowField(usize),
}

struct HybridProjectionOwnership {
    move_on_use: Box<[bool]>,
}

impl HybridProjectionOwnership {
    fn compile(fields: &[CoveringReadField]) -> Self {
        let mut move_on_use = Vec::with_capacity(fields.len());
        for (field_index, field) in fields.iter().enumerate() {
            let Some(source) = hybrid_owned_source(field) else {
                move_on_use.push(false);
                continue;
            };
            let source_is_used_later = fields[field_index.saturating_add(1)..]
                .iter()
                .any(|later| hybrid_owned_source(later) == Some(source));
            move_on_use.push(!source_is_used_later);
        }

        Self {
            move_on_use: move_on_use.into_boxed_slice(),
        }
    }

    fn move_on_use(&self, field_index: usize) -> Result<bool, InternalError> {
        self.move_on_use
            .get(field_index)
            .copied()
            .ok_or_else(InternalError::query_executor_invariant)
    }
}

const fn hybrid_owned_source(field: &CoveringReadField) -> Option<HybridOwnedSource> {
    match &field.source {
        CoveringReadFieldSource::IndexComponent { component_index }
        | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
            Some(HybridOwnedSource::IndexComponent(*component_index))
        }
        CoveringReadFieldSource::RowField => {
            Some(HybridOwnedSource::RowField(field.field_slot.index()))
        }
        CoveringReadFieldSource::PrimaryKey { .. } | CoveringReadFieldSource::Constant(_) => None,
    }
}

fn take_or_clone_compact_value(
    values: &mut Vec<(usize, Value)>,
    slot: usize,
    move_value: bool,
) -> Result<Value, InternalError> {
    let position = values
        .iter()
        .position(|(value_slot, _)| *value_slot == slot)
        .ok_or_else(InternalError::query_executor_invariant)?;
    if move_value {
        return Ok(values.swap_remove(position).1);
    }

    values
        .get(position)
        .map(|(_, value)| value.clone())
        .ok_or_else(InternalError::query_executor_invariant)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_hybrid_use_moves_large_value_without_retaining_or_cloning_backing() {
        let payload = vec![7_u8; 300 * 1_024];
        let payload_ptr = payload.as_ptr();
        let mut values = vec![(4, Value::Blob(payload))];

        let moved = take_or_clone_compact_value(&mut values, 4, true)
            .expect("final use should move the value");
        let Value::Blob(moved) = moved else {
            panic!("fixture should remain a blob");
        };

        assert_eq!(moved.as_ptr(), payload_ptr);
        assert!(values.is_empty());
    }

    #[test]
    fn repeated_hybrid_use_clones_until_the_final_owned_use() {
        let payload = vec![9_u8; 40 * 1_024];
        let payload_ptr = payload.as_ptr();
        let mut values = vec![(2, Value::Blob(payload))];

        let cloned = take_or_clone_compact_value(&mut values, 2, false)
            .expect("non-final use should clone the value");
        let Value::Blob(cloned) = cloned else {
            panic!("fixture should remain a blob");
        };
        let retained_ptr = match &values[0].1 {
            Value::Blob(retained) => retained.as_ptr(),
            _ => panic!("fixture should remain a blob"),
        };

        assert_ne!(cloned.as_ptr(), payload_ptr);
        assert_eq!(retained_ptr, payload_ptr);
    }
}
