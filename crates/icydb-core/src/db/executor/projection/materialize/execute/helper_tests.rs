//! Test-only helpers for the parent module.

use super::*;

pub(super) fn project_data_row_from_direct_slots(
    row_layout: &RowLayout,
    row: &DataRow,
    direct_slots: &PreparedDirectProjectionSlots,
    order_values: Option<&mut ProjectionOrderValues>,
) -> Result<Vec<Value>, InternalError> {
    let (data_key, raw_row) = row;
    let mut fields = row_layout.open_raw_row_with_contract(raw_row)?;
    project_direct_reader(data_key, &mut fields, direct_slots, order_values)
}

pub(super) fn project_scalar_data_row(
    compiled_fields: &[CompiledExpr],
    row: &DataRow,
    row_layout: &RowLayout,
    order_values: Option<&ProjectionOrderValues>,
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(compiled_fields.len());
    project_scalar_data_row_into(compiled_fields, row, row_layout, &mut shaped, order_values)?;

    Ok(shaped)
}

pub(super) fn project_scalar_data_row_into(
    compiled_fields: &[CompiledExpr],
    (data_key, raw_row): &DataRow,
    row_layout: &RowLayout,
    shaped: &mut Vec<Value>,
    order_values: Option<&ProjectionOrderValues>,
) -> Result<(), InternalError> {
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    project_scalar_reader_into(compiled_fields, data_key, &row_fields, shaped, order_values)
}
