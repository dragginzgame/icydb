//! Module: db::executor::projection::materialize::execute
//! Responsibility: row-level projection execution into value rows.
//! Does not own: DISTINCT key storage or structural cursor page dispatch.
//! Boundary: converts retained-slot and data-row inputs into local row views.

use super::contracts::CompiledExpr;
use crate::{
    db::{
        data::DataRow,
        executor::{
            budget::charge_current_execution_budget,
            projection::{
                eval::{
                    ProjectionEvalError, eval_compiled_expr_with_required_slot_reader_cow,
                    eval_compiled_expr_with_value_ref_reader,
                },
                materialize::{
                    metrics::ProjectionMaterializationMetricsRecorder,
                    plan::{
                        PreparedDirectProjectionSlot, PreparedDirectProjectionSlots,
                        PreparedProjectionContract,
                    },
                    row_view::RowView,
                },
            },
            terminal::{RetainedSlotRow, RowLayout},
        },
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
pub(super) fn project_slot_row(
    prepared_projection: &PreparedProjectionContract,
    row: RetainedSlotRow,
) -> Result<RowView, InternalError> {
    if let Some(slots) = prepared_projection.retained_slot_direct_projection_slots() {
        return project_slot_row_from_direct_slots(row, slots).map(RowView::owned);
    }

    project_slot_row_dense(prepared_projection, &row).map(RowView::owned)
}

pub(super) fn project_data_row(
    row_layout: &RowLayout,
    prepared_projection: &PreparedProjectionContract,
    row: &DataRow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<RowView, InternalError> {
    if let Some(slots) = prepared_projection.data_row_direct_projection_slots() {
        return project_data_row_from_direct_slots(row_layout, row, slots, metrics)
            .map(RowView::owned);
    }

    let compiled_fields = prepared_projection.compiled_exprs();
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "diagnostics")))]
    let projected_slot_mask = &[];

    metrics.record_data_rows_scalar_fallback_hit();
    project_scalar_data_row(
        compiled_fields,
        row,
        row_layout,
        projected_slot_mask,
        metrics,
    )
    .map(RowView::owned)
}

// Decode one identity row directly into its final owned response row. The
// previous reusable borrowed buffer required cloning every projected value at
// the structural boundary and could duplicate large nested payloads.
pub(super) fn project_identity_data_row(
    row_layout: &RowLayout,
    row: &DataRow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    let (data_key, raw_row) = row;
    let mut values = Vec::new();
    row_layout.decode_full_value_row_from_data_key_into(data_key, raw_row, &mut values)?;
    for _ in 0..values.len() {
        metrics.record_data_rows_slot_access(true);
    }

    Ok(values)
}

fn project_slot_row_dense(
    prepared_projection: &PreparedProjectionContract,
    row: &RetainedSlotRow,
) -> Result<Vec<Value>, InternalError> {
    let projection = prepared_projection.projection();
    let mut shaped = Vec::with_capacity(projection.len());
    project_slot_row_dense_into(prepared_projection, row, &mut shaped)?;

    Ok(shaped)
}

fn project_slot_row_dense_into(
    prepared_projection: &PreparedProjectionContract,
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    charge_projection_steps(prepared_projection.compiled_exprs().len())?;
    shaped.clear();

    if project_slot_row_direct_octet_lengths_into(prepared_projection, row, shaped)? {
        return Ok(());
    }

    let mut read_slot = |slot: usize| row.slot_ref(slot);
    visit_prepared_projection_values_with_required_value_reader_cow(
        prepared_projection.compiled_exprs(),
        &mut read_slot,
        &mut |value| shaped.push(value),
    )?;

    Ok(())
}

fn project_slot_row_direct_octet_lengths_into(
    prepared_projection: &PreparedProjectionContract,
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<bool, InternalError> {
    let octet_length_slots =
        prepared_projection.retained_slot_direct_octet_length_projection_slots();
    if octet_length_slots.is_empty() {
        return Ok(false);
    }

    let compiled_fields = prepared_projection.compiled_exprs();
    if octet_length_slots.len() != compiled_fields.len() {
        return Ok(false);
    }

    for (compiled, octet_length_slot) in compiled_fields.iter().zip(octet_length_slots) {
        let Some(slot) = octet_length_slot else {
            let mut read_slot = |slot: usize| row.slot_ref(slot);
            let value = eval_compiled_expr_with_value_ref_reader(compiled, &mut read_slot)
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
            shaped.push(value);
            continue;
        };

        let (_slot, _field) = compiled.direct_octet_length_slot().ok_or_else(|| {
            ProjectionEvalError::missing_unknown_value().into_invalid_logical_plan_internal_error()
        })?;
        let value = row
            .slot_ref(*slot)
            .ok_or_else(|| ProjectionEvalError::missing_slot_value(*slot))
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
        shaped.push(retained_slot_octet_length_value(value)?);
    }

    Ok(true)
}

fn retained_slot_octet_length_value(value: &Value) -> Result<Value, InternalError> {
    let value = match value {
        Value::Null => Value::Null,
        Value::Blob(bytes) => Value::Nat64(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
        Value::Text(text) => Value::Nat64(u64::try_from(text.len()).unwrap_or(u64::MAX)),
        Value::Nat64(length) => Value::Nat64(*length),
        _ => {
            return Err(InternalError::query_executor_invariant());
        }
    };

    Ok(value)
}

fn project_slot_row_from_direct_slots(
    row: RetainedSlotRow,
    direct_slots: &PreparedDirectProjectionSlots,
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(direct_slots.len());
    project_slot_row_from_direct_slots_into(row, direct_slots, &mut shaped)?;

    Ok(shaped)
}

fn project_slot_row_from_direct_slots_into(
    mut row: RetainedSlotRow,
    direct_slots: &PreparedDirectProjectionSlots,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    let projections = direct_slots.projections();
    charge_projection_steps(projections.len())?;
    shaped.clear();

    if direct_slots.has_repeated_source() {
        return project_repeated_slot_row_from_direct_slots_into(&mut row, projections, shaped);
    }

    for projection in projections {
        let slot = projection.source_slot();
        let value = row
            .take_slot(slot)
            .ok_or_else(|| ProjectionEvalError::missing_slot_value(slot))
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
        shaped.push(value);
    }

    Ok(())
}

fn project_repeated_slot_row_from_direct_slots_into(
    row: &mut RetainedSlotRow,
    projections: &[PreparedDirectProjectionSlot],
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    for projection in projections {
        let value = if let Some(previous_projection_index) = projection.previous_projection_index()
        {
            shaped
                .get(previous_projection_index)
                .cloned()
                .ok_or_else(InternalError::query_executor_invariant)?
        } else {
            let slot = projection.source_slot();
            row.take_slot(slot)
                .ok_or_else(|| ProjectionEvalError::missing_slot_value(slot))
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?
        };
        shaped.push(value);
    }

    Ok(())
}

fn project_data_row_from_direct_slots(
    row_layout: &RowLayout,
    row: &DataRow,
    direct_slots: &PreparedDirectProjectionSlots,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    let projections = direct_slots.projections();
    let mut shaped = Vec::with_capacity(projections.len());
    if direct_slots.has_repeated_source() {
        project_repeated_data_row_from_direct_slots_into(
            row_layout,
            row,
            projections,
            metrics,
            &mut shaped,
        )?;
    } else {
        project_data_row_from_direct_slots_into(
            row_layout,
            row,
            projections,
            metrics,
            &mut shaped,
        )?;
    }

    Ok(shaped)
}

fn project_data_row_from_direct_slots_into(
    row_layout: &RowLayout,
    row: &DataRow,
    projections: &[PreparedDirectProjectionSlot],
    metrics: ProjectionMaterializationMetricsRecorder,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    charge_projection_steps(projections.len())?;
    shaped.clear();
    let (data_key, raw_row) = row;
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_primary_key(data_key)?;

    for projection in projections {
        let slot = projection.source_slot();
        metrics.record_data_rows_slot_access(true);
        let value = row_fields.required_direct_projection_value(slot)?;
        shaped.push(value);
    }

    Ok(())
}

fn project_repeated_data_row_from_direct_slots_into(
    row_layout: &RowLayout,
    row: &DataRow,
    projections: &[PreparedDirectProjectionSlot],
    metrics: ProjectionMaterializationMetricsRecorder,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    charge_projection_steps(projections.len())?;
    shaped.clear();
    let (data_key, raw_row) = row;
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_primary_key(data_key)?;

    for projection in projections {
        let value = if let Some(previous_projection_index) = projection.previous_projection_index()
        {
            shaped
                .get(previous_projection_index)
                .cloned()
                .ok_or_else(InternalError::query_executor_invariant)?
        } else {
            let slot = projection.source_slot();
            metrics.record_data_rows_slot_access(true);
            row_fields.required_direct_projection_value(slot)?
        };

        shaped.push(value);
    }

    Ok(())
}

fn project_scalar_data_row(
    compiled_fields: &[CompiledExpr],
    row: &DataRow,
    row_layout: &RowLayout,
    projected_slot_mask: &[bool],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(compiled_fields.len());
    project_scalar_data_row_into(
        compiled_fields,
        row,
        row_layout,
        projected_slot_mask,
        metrics,
        &mut shaped,
    )?;

    Ok(shaped)
}

fn project_scalar_data_row_into(
    compiled_fields: &[CompiledExpr],
    (data_key, raw_row): &DataRow,
    row_layout: &RowLayout,
    projected_slot_mask: &[bool],
    metrics: ProjectionMaterializationMetricsRecorder,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = projected_slot_mask;

    charge_projection_steps(compiled_fields.len())?;
    shaped.clear();
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_primary_key(data_key)?;

    for compiled in compiled_fields {
        let mut record_slot = |slot| {
            metrics.record_data_rows_slot_access(
                projected_slot_mask.get(slot).copied().unwrap_or(false),
            );
        };
        let value = eval_compiled_expr_with_required_slot_reader_cow(
            compiled,
            &row_fields,
            &mut record_slot,
        )?;
        shaped.push(value.into_owned());
    }

    Ok(())
}

fn charge_projection_steps(count: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        u64::try_from(count).unwrap_or(u64::MAX),
    )
}

// Walk one prepared projection plan through one reader that can borrow slot
// values from retained structural rows until an expression needs ownership.
fn visit_prepared_projection_values_with_required_value_reader_cow<'a>(
    compiled_exprs: &'a [CompiledExpr],
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), InternalError> {
    for compiled in compiled_exprs {
        on_value(
            crate::db::executor::projection::eval::eval_compiled_expr_with_value_ref_reader(
                compiled, read_slot,
            )
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?,
        );
    }

    Ok(())
}
