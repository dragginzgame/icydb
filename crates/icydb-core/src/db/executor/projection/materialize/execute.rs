//! Module: db::executor::projection::materialize::execute
//! Responsibility: row-level projection execution into value rows.
//! Does not own: DISTINCT key storage or structural cursor page dispatch.
//! Boundary: converts retained-slot and data-row inputs into local row views.

#[cfg(test)]
mod tests;

use super::contracts::CompiledExpr;
#[cfg(test)]
use crate::db::data::DataRow;
use crate::{
    db::{
        data::StructuralSlotReader,
        executor::{
            ProjectionDataRow,
            order::ProjectionOrderValues,
            projection::{
                eval::{
                    CanonicalSlotExprReader, ProjectionEvalError, ValueRefSlotReader,
                    eval_compiled_expr_with_value_ref_reader,
                },
                materialize::{
                    plan::{
                        PreparedDirectProjectionSlot, PreparedDirectProjectionSlots,
                        PreparedProjectionContract,
                    },
                    row_view::RowView,
                },
            },
            terminal::{RetainedSlotRow, RowLayout},
        },
        resource::budget::charge_current_execution_budget,
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
    row: ProjectionDataRow,
) -> Result<RowView, InternalError> {
    let (row, mut order_values) = row.into_parts();
    row.with_reader(row_layout.contract(), |data_key, mut fields| {
        if let Some(slots) = prepared_projection.data_row_direct_projection_slots() {
            return project_direct_reader(data_key, &mut fields, slots, order_values.as_mut())
                .map(RowView::owned);
        }
        let compiled_fields = prepared_projection.compiled_exprs();
        let mut shaped = Vec::with_capacity(compiled_fields.len());
        project_scalar_reader_into(
            compiled_fields,
            data_key,
            &fields,
            &mut shaped,
            order_values.as_ref(),
        )?;
        Ok(RowView::owned(shaped))
    })
}

// Decode one identity row directly into its final owned response row. The
// previous reusable borrowed buffer required cloning every projected value at
// the structural boundary and could duplicate large nested payloads.
pub(super) fn project_identity_data_row(
    row_layout: &RowLayout,
    row: ProjectionDataRow,
) -> Result<Vec<Value>, InternalError> {
    let (row, order_values) = row.into_parts();
    row.with_reader(row_layout.contract(), |data_key, mut fields| {
        let mut values = Vec::new();
        if let Some(mut order_values) = order_values {
            values.reserve(row_layout.field_count());
            // Preserve full-field admission before primary-key validation.
            for slot in 0..row_layout.field_count() {
                values.push(match order_values.take_slot(slot) {
                    Some(value) => value,
                    None => fields.take_required_value(slot)?,
                });
            }
            fields.validate_primary_key(data_key)?;
        } else {
            fields.materialize_all_values()?;
            fields.validate_primary_key(data_key)?;
            fields.decode_all_values_into(&mut values)?;
        }
        Ok(values)
    })
}

fn project_slot_row_dense(
    prepared_projection: &PreparedProjectionContract,
    row: &RetainedSlotRow,
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(prepared_projection.compiled_exprs().len());
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

    project_retained_expressions_into(prepared_projection.compiled_exprs(), row, shaped)?;

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
                .map_err(ProjectionEvalError::into_internal_error)?;
            shaped.push(value);
            continue;
        };

        compiled
            .direct_octet_length_slot()
            .ok_or_else(|| ProjectionEvalError::missing_unknown_value().into_internal_error())?;
        let value = row
            .slot_ref(*slot)
            .ok_or_else(|| ProjectionEvalError::missing_slot_value(*slot))
            .map_err(ProjectionEvalError::into_internal_error)?;
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
            .map_err(ProjectionEvalError::into_internal_error)?;
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
                .map_err(ProjectionEvalError::into_internal_error)?
        };
        shaped.push(value);
    }

    Ok(())
}

fn project_direct_reader(
    data_key: &crate::db::data::DecodedDataStoreKey,
    row_fields: &mut StructuralSlotReader<'_>,
    direct_slots: &PreparedDirectProjectionSlots,
    order_values: Option<&mut ProjectionOrderValues>,
) -> Result<Vec<Value>, InternalError> {
    let projections = direct_slots.projections();
    let mut shaped = Vec::with_capacity(projections.len());
    charge_projection_steps(projections.len())?;
    row_fields.validate_primary_key(data_key)?;

    if direct_slots.has_repeated_source() {
        project_repeated_data_row_from_direct_slots_into(
            row_fields,
            projections,
            &mut shaped,
            order_values,
        )?;
    } else {
        project_data_row_from_direct_slots_into(
            row_fields,
            projections,
            &mut shaped,
            order_values,
        )?;
    }

    Ok(shaped)
}

fn project_data_row_from_direct_slots_into(
    row_fields: &mut StructuralSlotReader<'_>,
    projections: &[PreparedDirectProjectionSlot],
    shaped: &mut Vec<Value>,
    mut order_values: Option<&mut ProjectionOrderValues>,
) -> Result<(), InternalError> {
    for projection in projections {
        let slot = projection.source_slot();
        let value = match order_values
            .as_deref_mut()
            .and_then(|values| values.take_slot(slot))
        {
            Some(value) => value,
            None => row_fields.take_direct_projection_value(slot)?,
        };
        shaped.push(value);
    }

    Ok(())
}

fn project_repeated_data_row_from_direct_slots_into(
    row_fields: &mut StructuralSlotReader<'_>,
    projections: &[PreparedDirectProjectionSlot],
    shaped: &mut Vec<Value>,
    mut order_values: Option<&mut ProjectionOrderValues>,
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
            match order_values
                .as_deref_mut()
                .and_then(|values| values.take_slot(slot))
            {
                Some(value) => value,
                None => row_fields.take_direct_projection_value(slot)?,
            }
        };

        shaped.push(value);
    }

    Ok(())
}

fn project_scalar_reader_into(
    compiled_fields: &[CompiledExpr],
    data_key: &crate::db::data::DecodedDataStoreKey,
    row_fields: &StructuralSlotReader<'_>,
    shaped: &mut Vec<Value>,
    order_values: Option<&ProjectionOrderValues>,
) -> Result<(), InternalError> {
    charge_projection_steps(compiled_fields.len())?;
    shaped.clear();
    row_fields.validate_primary_key(data_key)?;

    let mut record_slot = |_| {};
    let reader = CanonicalSlotExprReader::new(compiled_fields, row_fields, &mut record_slot, true);
    for compiled in compiled_fields {
        // Retained ordering values still win before any raw path is touched.
        if let Some(value) = order_values.and_then(|values| values.projection_value(compiled)) {
            shaped.push(value.clone());
            continue;
        }
        shaped.push(reader.evaluate_projection(compiled)?);
    }

    Ok(())
}

fn charge_projection_steps(count: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        u64::try_from(count).unwrap_or(u64::MAX),
    )
}

// Keep one reader for the row; only an executed path initializes sibling selection.
fn project_retained_expressions_into(
    expressions: &[CompiledExpr],
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    let mut read_slot = |slot| row.slot_ref(slot);
    let reader = ValueRefSlotReader::new(expressions, &mut read_slot);
    for expression in expressions {
        shaped.push(
            reader
                .evaluate(expression)
                .map_err(ProjectionEvalError::into_internal_error)?,
        );
    }
    Ok(())
}

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
use helper_tests::project_data_row_from_direct_slots;
#[cfg(test)]
use helper_tests::project_scalar_data_row;
#[cfg(test)]
use helper_tests::project_scalar_data_row_into;
