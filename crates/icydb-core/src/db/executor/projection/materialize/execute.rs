//! Module: db::executor::projection::materialize::execute
//! Responsibility: row-level projection execution into value rows.
//! Does not own: DISTINCT key storage or structural cursor page dispatch.
//! Boundary: converts retained-slot and data-row inputs into local row views.

#[cfg(test)]
use crate::db::executor::projection::eval::eval_compiled_expr_with_value_reader;
use crate::db::query::plan::expr::CompiledExpr;
#[cfg(test)]
use crate::db::query::plan::expr::ProjectionSpec;
#[cfg(test)]
use crate::db::query::plan::expr::compile_scalar_projection_expr;
#[cfg(test)]
use crate::{
    db::response::ProjectedRow,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use crate::{
    db::{
        data::DataRow,
        executor::{
            projection::{
                eval::{
                    ProjectionEvalError, eval_compiled_expr_with_required_slot_reader_cow,
                    eval_compiled_expr_with_value_ref_reader,
                },
                materialize::{
                    metrics::ProjectionMaterializationMetricsRecorder,
                    plan::{PreparedProjectionPlan, PreparedProjectionShape},
                    row_view::RowView,
                },
            },
            terminal::{RetainedSlotRow, RowLayout},
        },
    },
    error::InternalError,
    value::Value,
};

#[cfg(feature = "sql")]
pub(super) fn project_slot_row(
    prepared_projection: &PreparedProjectionShape,
    row: RetainedSlotRow,
) -> Result<RowView<'static>, InternalError> {
    if let Some(field_slots) = prepared_projection.retained_slot_direct_projection_field_slots() {
        return project_slot_row_from_direct_field_slots(row, field_slots).map(RowView::Owned);
    }

    project_slot_row_dense(prepared_projection, &row).map(RowView::Owned)
}

// Project retained-slot rows through the non-DISTINCT structural path while
// borrowing each completed projected row from a reusable output buffer.
// DISTINCT keeps using `project_slot_row` because accepted rows can outlive one
// projection callback.
#[cfg(feature = "sql")]
pub(super) fn visit_slot_row_views(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    if let Some(field_slots) = prepared_projection.retained_slot_direct_projection_field_slots() {
        return visit_direct_slot_row_views(rows, field_slots, visit);
    }

    visit_scalar_slot_row_views(prepared_projection, rows, visit)
}

#[cfg(feature = "sql")]
pub(super) fn project_data_row(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    row: &DataRow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<RowView<'static>, InternalError> {
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        return project_data_row_from_direct_field_slots(row_layout, row, field_slots, metrics)
            .map(RowView::Owned);
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
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
    .map(RowView::Owned)
}

// Project already-windowed raw data rows through the non-identity data-row
// paths while borrowing each completed projected row from a reusable buffer.
// DISTINCT still uses the single-row owned projector because it may retain
// accepted rows after the candidate callback returns.
#[cfg(feature = "sql")]
pub(super) fn visit_data_row_views(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
    metrics: ProjectionMaterializationMetricsRecorder,
    visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        return visit_direct_data_row_views(row_layout, field_slots, rows, metrics, visit);
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "diagnostics")))]
    let projected_slot_mask = &[];

    metrics.record_data_rows_scalar_fallback_hit();
    visit_scalar_data_row_views(
        row_layout,
        compiled_fields,
        rows,
        projected_slot_mask,
        metrics,
        visit,
    )
}

// Decode already-windowed raw data rows into canonical model-field order for
// identity projections. The reusable decode buffer backs a borrowed `RowView`
// for exactly one callback, keeping the final owned row allocation at the
// structural materialization boundary.
#[cfg(feature = "sql")]
pub(super) fn visit_identity_data_row_views(
    row_layout: RowLayout,
    rows: &[DataRow],
    metrics: ProjectionMaterializationMetricsRecorder,
    mut visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let mut values = Vec::new();

    // Phase 1: decode each raw row through the dense full-row contract once.
    for (data_key, raw_row) in rows {
        row_layout.decode_full_value_row_into(data_key.storage_key(), raw_row, &mut values)?;
        for _ in 0..values.len() {
            metrics.record_data_rows_slot_access(true);
        }
        visit(RowView::Borrowed(values.as_slice()))?;
    }

    Ok(())
}

#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) fn count_borrowed_identity_data_row_views_for_test(
    row_layout: RowLayout,
    rows: &[DataRow],
) -> Result<usize, InternalError> {
    const fn noop() {}
    const fn noop_slot_access(_projected_slot: bool) {}

    let metrics = ProjectionMaterializationMetricsRecorder::new(
        noop,
        noop,
        noop,
        noop_slot_access,
        noop,
        noop,
    );
    let mut borrowed_rows = 0;

    // Phase 1: run the production identity visitor and count only row views
    // that borrow from the reusable row buffer.
    visit_identity_data_row_views(row_layout, rows, metrics, |row_view| {
        if matches!(row_view, RowView::Borrowed(_)) {
            borrowed_rows += 1;
        }

        Ok(())
    })?;

    Ok(borrowed_rows)
}

#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) fn count_borrowed_data_row_views_for_test(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
) -> Result<usize, InternalError> {
    const fn noop() {}
    const fn noop_slot_access(_projected_slot: bool) {}

    let metrics = ProjectionMaterializationMetricsRecorder::new(
        noop,
        noop,
        noop,
        noop_slot_access,
        noop,
        noop,
    );
    let mut borrowed_rows = 0;

    // Phase 1: run the production non-identity visitor and count only row views
    // that borrow from the reusable projection buffer.
    visit_data_row_views(row_layout, prepared_projection, rows, metrics, |row_view| {
        if matches!(row_view, RowView::Borrowed(_)) {
            borrowed_rows += 1;
        }

        Ok(())
    })?;

    Ok(borrowed_rows)
}

#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) fn count_borrowed_slot_row_views_for_test(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<usize, InternalError> {
    let mut borrowed_rows = 0;

    // Phase 1: run the production retained-slot visitor and count only row
    // views that borrow from the reusable projection buffer.
    visit_slot_row_views(prepared_projection, rows, |row_view| {
        if matches!(row_view, RowView::Borrowed(_)) {
            borrowed_rows += 1;
        }

        Ok(())
    })?;

    Ok(borrowed_rows)
}

#[cfg(feature = "sql")]
// Visit one retained slot-row page through the prepared compiled structural
// projection evaluator while borrowing from a reusable output buffer.
fn visit_scalar_slot_row_views(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    mut visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let projection = prepared_projection.projection();
    let mut shaped = Vec::with_capacity(projection.len());

    // Phase 1: evaluate each retained row once and emit final row elements
    // through the reusable output buffer.
    for row in &rows {
        project_slot_row_dense_into(prepared_projection, row, &mut shaped)?;
        debug_assert_eq!(shaped.len(), projection.len());
        visit(RowView::Borrowed(shaped.as_slice()))?;
    }

    Ok(())
}

#[cfg(feature = "sql")]
fn project_slot_row_dense(
    prepared_projection: &PreparedProjectionShape,
    row: &RetainedSlotRow,
) -> Result<Vec<Value>, InternalError> {
    let projection = prepared_projection.projection();
    let mut shaped = Vec::with_capacity(projection.len());
    project_slot_row_dense_into(prepared_projection, row, &mut shaped)?;

    Ok(shaped)
}

#[cfg(feature = "sql")]
fn project_slot_row_dense_into(
    prepared_projection: &PreparedProjectionShape,
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    let projection = prepared_projection.projection();
    shaped.clear();
    shaped.reserve(projection.len());

    if project_slot_row_direct_octet_lengths_into(prepared_projection, row, shaped)? {
        return Ok(());
    }

    let mut read_slot = |slot: usize| row.slot_ref(slot);
    visit_prepared_projection_values_with_required_value_reader_cow(
        prepared_projection.prepared(),
        &mut read_slot,
        &mut |value| shaped.push(value),
    )?;

    Ok(())
}

#[cfg(feature = "sql")]
fn project_slot_row_direct_octet_lengths_into(
    prepared_projection: &PreparedProjectionShape,
    row: &RetainedSlotRow,
    shaped: &mut Vec<Value>,
) -> Result<bool, InternalError> {
    let octet_length_slots =
        prepared_projection.retained_slot_direct_octet_length_projection_slots();
    if octet_length_slots.is_empty() {
        return Ok(false);
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
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

        let (_slot, field) = compiled.direct_octet_length_slot().ok_or_else(|| {
            ProjectionEvalError::MissingFieldValue {
                field: String::new(),
                index: *slot,
            }
            .into_invalid_logical_plan_internal_error()
        })?;
        let value = row
            .slot_ref(*slot)
            .ok_or_else(|| ProjectionEvalError::MissingFieldValue {
                field: field.to_string(),
                index: *slot,
            })
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
        shaped.push(retained_slot_octet_length_value(value)?);
    }

    Ok(true)
}

#[cfg(feature = "sql")]
fn retained_slot_octet_length_value(value: &Value) -> Result<Value, InternalError> {
    let value = match value {
        Value::Null => Value::Null,
        Value::Blob(bytes) => Value::Uint(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
        Value::Text(text) => Value::Uint(u64::try_from(text.len()).unwrap_or(u64::MAX)),
        Value::Uint(length) => Value::Uint(*length),
        _ => {
            return Err(InternalError::query_executor_invariant(
                "retained-slot OCTET_LENGTH optimization requires text, blob, or precomputed length values",
            ));
        }
    };

    Ok(value)
}

#[cfg(feature = "sql")]
// Visit one retained dense slot-row page through direct field-slot copies only.
// The row is still consumed so duplicate projected slots preserve the existing
// `take_slot` missing-field behavior.
fn visit_direct_slot_row_views(
    rows: Vec<RetainedSlotRow>,
    field_slots: &[(String, usize)],
    mut visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let mut shaped = Vec::with_capacity(field_slots.len());

    // Phase 1: move only requested retained slots into the reusable projection
    // buffer. Retained rows do not expose an ordered contiguous projection
    // slice, so the structural boundary still owns the final row copy.
    for row in rows {
        project_slot_row_from_direct_field_slots_into(row, field_slots, &mut shaped)?;
        visit(RowView::Borrowed(shaped.as_slice()))?;
    }

    Ok(())
}

#[cfg(feature = "sql")]
fn project_slot_row_from_direct_field_slots(
    row: RetainedSlotRow,
    field_slots: &[(String, usize)],
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(field_slots.len());
    project_slot_row_from_direct_field_slots_into(row, field_slots, &mut shaped)?;

    Ok(shaped)
}

#[cfg(feature = "sql")]
fn project_slot_row_from_direct_field_slots_into(
    mut row: RetainedSlotRow,
    field_slots: &[(String, usize)],
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    shaped.clear();
    shaped.reserve(field_slots.len());

    for (field_name, slot) in field_slots {
        let value = row
            .take_slot(*slot)
            .ok_or_else(|| ProjectionEvalError::MissingFieldValue {
                field: field_name.clone(),
                index: *slot,
            })
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
        shaped.push(value);
    }

    Ok(())
}

#[cfg(feature = "sql")]
// Visit one raw data-row page through direct field-slot copies only. The
// reusable projection buffer is consumed before the next row clears it.
fn visit_direct_data_row_views(
    row_layout: RowLayout,
    field_slots: &[(String, usize)],
    rows: &[DataRow],
    metrics: ProjectionMaterializationMetricsRecorder,
    mut visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let mut shaped = Vec::with_capacity(field_slots.len());

    // Phase 1: open each structural row once, then decode only the declared
    // direct field slots into the reusable output buffer.
    for row in rows {
        project_data_row_from_direct_field_slots_into(
            row_layout,
            row,
            field_slots,
            metrics,
            &mut shaped,
        )?;
        visit(RowView::Borrowed(shaped.as_slice()))?;
    }

    Ok(())
}

#[cfg(feature = "sql")]
fn project_data_row_from_direct_field_slots(
    row_layout: RowLayout,
    row: &DataRow,
    field_slots: &[(String, usize)],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    let mut shaped = Vec::with_capacity(field_slots.len());
    project_data_row_from_direct_field_slots_into(
        row_layout,
        row,
        field_slots,
        metrics,
        &mut shaped,
    )?;

    Ok(shaped)
}

#[cfg(feature = "sql")]
fn project_data_row_from_direct_field_slots_into(
    row_layout: RowLayout,
    row: &DataRow,
    field_slots: &[(String, usize)],
    metrics: ProjectionMaterializationMetricsRecorder,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    shaped.clear();
    let (data_key, raw_row) = row;
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_storage_key(data_key)?;
    shaped.reserve(field_slots.len());

    for (_field_name, slot) in field_slots {
        metrics.record_data_rows_slot_access(true);

        shaped.push(row_fields.required_direct_projection_value(*slot)?);
    }

    Ok(())
}

#[cfg(feature = "sql")]
fn visit_scalar_data_row_views(
    row_layout: RowLayout,
    compiled_fields: &[CompiledExpr],
    rows: &[DataRow],
    projected_slot_mask: &[bool],
    metrics: ProjectionMaterializationMetricsRecorder,
    mut visit: impl FnMut(RowView<'_>) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let mut shaped = Vec::with_capacity(compiled_fields.len());

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path once and borrow each completed row from the reusable
    // output buffer.
    for row in rows {
        project_scalar_data_row_into(
            compiled_fields,
            row,
            row_layout,
            projected_slot_mask,
            metrics,
            &mut shaped,
        )?;
        visit(RowView::Borrowed(shaped.as_slice()))?;
    }

    Ok(())
}

#[cfg(feature = "sql")]
fn project_scalar_data_row(
    compiled_fields: &[CompiledExpr],
    row: &DataRow,
    row_layout: RowLayout,
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

#[cfg(feature = "sql")]
fn project_scalar_data_row_into(
    compiled_fields: &[CompiledExpr],
    (data_key, raw_row): &DataRow,
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
    metrics: ProjectionMaterializationMetricsRecorder,
    shaped: &mut Vec<Value>,
) -> Result<(), InternalError> {
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = projected_slot_mask;

    shaped.clear();
    let row_fields = row_layout.open_raw_row_with_contract(raw_row)?;
    row_fields.validate_storage_key(data_key)?;
    shaped.reserve(compiled_fields.len());

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

#[cfg(test)]
pub(in crate::db::executor::projection) fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    let mut compiled_fields = Vec::with_capacity(projection.len());
    for field in projection.fields() {
        let compiled = compile_scalar_projection_expr(E::MODEL, field.expr()).expect(
            "test projection materialization helpers require scalar-compilable expressions",
        );
        compiled_fields.push(CompiledExpr::compile(&compiled));
    }
    let prepared = PreparedProjectionPlan::Scalar(compiled_fields);
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| entity.get_value_by_index(slot);
        visit_prepared_projection_values_with_value_reader(
            &prepared,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;
        projected_rows.push(ProjectedRow::from_runtime_values(*id, values));
    }

    Ok(projected_rows)
}

#[cfg(test)]
pub(super) fn visit_prepared_projection_values_with_value_reader(
    prepared: &PreparedProjectionPlan,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), ProjectionEvalError> {
    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared;
    for compiled in compiled_fields {
        on_value(eval_compiled_expr_with_value_reader(compiled, read_slot)?);
    }

    Ok(())
}

// Walk one prepared projection plan through one reader that can borrow slot
// values from retained structural rows until an expression needs ownership.
fn visit_prepared_projection_values_with_required_value_reader_cow<'a>(
    prepared: &'a PreparedProjectionPlan,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), InternalError> {
    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared;
    for compiled in compiled_fields {
        on_value(
            crate::db::executor::projection::eval::eval_compiled_expr_with_value_ref_reader(
                compiled, read_slot,
            )
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?,
        );
    }

    Ok(())
}
