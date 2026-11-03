//! Module: db::executor::projection::materialize::execute
//! Responsibility: row-level projection execution into value rows.
//! Does not own: DISTINCT key storage or structural cursor page dispatch.
//! Boundary: converts retained-slot and data-row inputs into local row views.

#[cfg(test)]
mod tests;

use super::contracts::CompiledExpr;
use crate::{
    db::{
        data::{DataRow, StructuralSlotReader},
        executor::{
            ProjectionDataRow,
            order::ProjectionOrderValues,
            projection::{
                eval::{
                    ProjectionEvalError, eval_compiled_expr_with_required_slot_reader_cow,
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
    if let Some(slots) = prepared_projection.data_row_direct_projection_slots() {
        return project_data_row_from_direct_slots(row_layout, &row, slots, order_values.as_mut())
            .map(RowView::owned);
    }

    let compiled_fields = prepared_projection.compiled_exprs();
    project_scalar_data_row(compiled_fields, &row, row_layout, order_values.as_ref())
        .map(RowView::owned)
}

// Decode one identity row directly into its final owned response row. The
// previous reusable borrowed buffer required cloning every projected value at
// the structural boundary and could duplicate large nested payloads.
pub(super) fn project_identity_data_row(
    row_layout: &RowLayout,
    row: ProjectionDataRow,
) -> Result<Vec<Value>, InternalError> {
    let ((data_key, raw_row), order_values) = row.into_parts();
    let mut values = Vec::new();
    if let Some(mut order_values) = order_values {
        let mut fields = row_layout.open_raw_row_with_contract(&raw_row)?;
        values.reserve(row_layout.field_count());
        // Preserve full-field admission before primary-key validation. Cached
        // values passed this frozen row contract during ordering.
        for slot in 0..row_layout.field_count() {
            values.push(match order_values.take_slot(slot) {
                Some(value) => value,
                None => fields.take_required_value(slot)?,
            });
        }
        fields.validate_primary_key(&data_key)?;
    } else {
        row_layout.decode_full_value_row_from_data_key_into(&data_key, &raw_row, &mut values)?;
    }
    Ok(values)
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

fn project_data_row_from_direct_slots(
    row_layout: &RowLayout,
    row: &DataRow,
    direct_slots: &PreparedDirectProjectionSlots,
    order_values: Option<&mut ProjectionOrderValues>,
) -> Result<Vec<Value>, InternalError> {
    let projections = direct_slots.projections();
    let mut shaped = Vec::with_capacity(projections.len());
    charge_projection_steps(projections.len())?;
    let (data_key, raw_row) = row;
    let mut row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_primary_key(data_key)?;

    if direct_slots.has_repeated_source() {
        project_repeated_data_row_from_direct_slots_into(
            &mut row_fields,
            projections,
            &mut shaped,
            order_values,
        )?;
    } else {
        project_data_row_from_direct_slots_into(
            &mut row_fields,
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

fn project_scalar_data_row(
    compiled_fields: &[CompiledExpr],
    row: &DataRow,
    row_layout: &RowLayout,
    order_values: Option<&ProjectionOrderValues>,
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(compiled_fields.len());
    project_scalar_data_row_into(compiled_fields, row, row_layout, &mut shaped, order_values)?;

    Ok(shaped)
}

fn project_scalar_data_row_into(
    compiled_fields: &[CompiledExpr],
    (data_key, raw_row): &DataRow,
    row_layout: &RowLayout,
    shaped: &mut Vec<Value>,
    order_values: Option<&ProjectionOrderValues>,
) -> Result<(), InternalError> {
    charge_projection_steps(compiled_fields.len())?;
    shaped.clear();
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_primary_key(data_key)?;

    let mut remaining = compiled_fields;
    while let Some((compiled, tail)) = remaining.split_first() {
        // Cached evaluations remain authoritative. Split sibling runs at cached
        // leaves instead of decoding them again or allocating while probing reuse.
        if let Some(value) = order_values.and_then(|values| values.projection_value(compiled)) {
            shaped.push(value.clone());
            remaining = tail;
            continue;
        }
        let count = sibling_path_prefix_len(remaining, |expr| {
            order_values.is_none_or(|values| values.projection_value(expr).is_none())
        });
        if project_sibling_paths(
            &remaining[..count],
            shaped,
            |root, prefix, members, shaped| {
                crate::db::executor::projection::path::resolve_slot_sibling_paths(
                    &row_fields,
                    root,
                    prefix,
                    members,
                    shaped,
                )
                .map_err(|error| InternalError::classified(error.class(), error.origin()))
            },
        )? {
            remaining = &remaining[count..];
            continue;
        }
        let mut record_slot = |_| {};
        let value = eval_compiled_expr_with_required_slot_reader_cow(
            compiled,
            &row_fields,
            &mut record_slot,
        )?;
        shaped.push(value.into_owned());
        remaining = tail;
    }

    Ok(())
}

// Retained rows borrow their parent map; projection owns only selected leaves.
fn project_retained_sibling_paths(
    compiled_fields: &[CompiledExpr],
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<bool, InternalError> {
    project_sibling_paths(
        compiled_fields,
        shaped,
        |root_slot, prefix, members, shaped| {
            let root = row.slot_ref(root_slot).ok_or_else(|| {
                ProjectionEvalError::missing_field_path_value(root_slot).into_internal_error()
            })?;
            crate::db::executor::projection::path::resolve_value_sibling_paths(
                root, prefix, members, shaped,
            )
            .map_err(|error| InternalError::classified(error.class(), error.origin()))
        },
    )
}

// Return the leading sibling run, or one ordinary expression. This inspects
// shape only; value/error evaluation stays at the original projection position.
fn sibling_path_prefix_len(
    compiled_fields: &[CompiledExpr],
    mut include: impl FnMut(&CompiledExpr) -> bool,
) -> usize {
    let Some(CompiledExpr::FieldPath {
        root_slot,
        segment_bytes,
        ..
    }) = compiled_fields.first()
    else {
        return usize::from(!compiled_fields.is_empty());
    };
    let Some((_, prefix)) = segment_bytes.split_last() else {
        return 1;
    };
    compiled_fields
        .iter()
        .take_while(|compiled| {
            matches!(compiled, CompiledExpr::FieldPath { root_slot: slot, segment_bytes: path, .. }
            if slot == root_slot && path.split_last().is_some_and(|(_, parent)| parent == prefix))
                && include(compiled)
        })
        .count()
}

// Only one unconditional sibling run shares a traversal; other expressions
// remain outside this batch and keep their authored evaluation order.
fn project_sibling_paths(
    compiled_fields: &[CompiledExpr],
    shaped: &mut Vec<Value>,
    resolve: impl FnOnce(usize, &[Box<[u8]>], &[&[u8]], &mut Vec<Value>) -> Result<(), InternalError>,
) -> Result<bool, InternalError> {
    if compiled_fields.len() < 2 {
        return Ok(false);
    }
    let Some(CompiledExpr::FieldPath {
        root_slot,
        segment_bytes,
        ..
    }) = compiled_fields.first()
    else {
        return Ok(false);
    };
    let Some((_, prefix)) = segment_bytes.split_last() else {
        return Ok(false);
    };
    if sibling_path_prefix_len(compiled_fields, |_| true) != compiled_fields.len() {
        return Ok(false);
    }
    // Admit both borrowed member names and selected slices before allocation.
    // These are row-local scratch; leaf materialization retains its existing owner.
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        (compiled_fields.len() as u64)
            .saturating_mul((size_of::<&[u8]>() + size_of::<Option<&[u8]>>()) as u64),
    )?;
    let mut members = Vec::with_capacity(compiled_fields.len());
    members.extend(compiled_fields.iter().filter_map(|compiled| {
        let CompiledExpr::FieldPath { segment_bytes, .. } = compiled else {
            return None;
        };
        segment_bytes.last().map(AsRef::as_ref)
    }));
    resolve(*root_slot, prefix, &members, shaped)?;
    Ok(true)
}

fn charge_projection_steps(count: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        u64::try_from(count).unwrap_or(u64::MAX),
    )
}

// Evaluate authored expressions in order, sharing only consecutive unconditional
// sibling paths. Never cross another expression or inspect its lazy branches.
fn project_retained_expressions_into(
    mut remaining: &[CompiledExpr],
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    while let Some((compiled, tail)) = remaining.split_first() {
        let count = sibling_path_prefix_len(remaining, |_| true);
        if project_retained_sibling_paths(&remaining[..count], row, shaped)? {
            remaining = &remaining[count..];
            continue;
        }
        shaped.push(
            crate::db::executor::projection::eval::eval_compiled_expr_with_value_ref_reader(
                compiled,
                &mut |slot| row.slot_ref(slot),
            )
            .map_err(ProjectionEvalError::into_internal_error)?,
        );
        remaining = tail;
    }
    Ok(())
}
