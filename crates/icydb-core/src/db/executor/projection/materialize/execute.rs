//! Module: db::executor::projection::materialize::execute
//! Responsibility: row-level projection execution into value rows.
//! Does not own: DISTINCT key storage or structural cursor page dispatch.
//! Boundary: converts retained-slot and data-row inputs into local row views.

#[cfg(test)]
use crate::{
    db::response::ProjectedRow,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use crate::{
    db::{
        data::{CanonicalSlotReader, DataRow},
        executor::{
            projection::{
                eval::{
                    ProjectionEvalError, ScalarProjectionExpr,
                    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
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
use std::borrow::Cow;

#[cfg(test)]
use crate::db::executor::projection::eval::eval_scalar_projection_expr_with_value_reader;
#[cfg(test)]
use crate::db::query::plan::expr::ProjectionSpec;
#[cfg(test)]
use crate::db::query::plan::expr::compile_scalar_projection_expr;

#[cfg(feature = "sql")]
pub(super) fn project_slot_rows(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<RowView<'static>>, InternalError> {
    shape_slot_rows(prepared_projection, rows)
}

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

#[cfg(feature = "sql")]
pub(super) fn project_data_rows(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<RowView<'static>>, InternalError> {
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        return shape_data_rows_from_direct_field_slots(rows, row_layout, field_slots, metrics);
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "diagnostics")))]
    let projected_slot_mask = &[];

    metrics.record_data_rows_scalar_fallback_hit();
    shape_scalar_data_rows(
        compiled_fields,
        rows,
        row_layout,
        projected_slot_mask,
        metrics,
    )
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

// Decode already-windowed raw data rows into canonical model-field order for
// identity projections. This bypasses scalar projection evaluation and direct
// field-slot projection loops while preserving final `Value` rows.
#[cfg(feature = "sql")]
pub(super) fn project_identity_data_rows(
    row_layout: RowLayout,
    rows: &[DataRow],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<RowView<'static>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: decode each raw row through the dense full-row contract once.
    for (data_key, raw_row) in rows {
        let values = row_layout.decode_full_value_row(data_key.storage_key(), raw_row)?;
        for _ in 0..values.len() {
            metrics.record_data_rows_slot_access(true);
        }
        shaped_rows.push(RowView::Owned(values));
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
// Shape one retained slot-row page through either direct field-slot copies or
// the compiled projection evaluator while keeping one row loop.
fn shape_slot_rows(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<RowView<'static>>, InternalError> {
    if let Some(field_slots) = prepared_projection.retained_slot_direct_projection_field_slots() {
        return shape_slot_rows_from_direct_field_slots(rows, field_slots);
    }

    shape_slot_rows_dense(prepared_projection, rows)
}

#[cfg(feature = "sql")]
// Shape one dense retained slot-row page through the prepared compiled
// structural projection evaluator without staging another row representation.
fn shape_slot_rows_dense(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<RowView<'static>>, InternalError> {
    let projection = prepared_projection.projection();
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: evaluate each retained row once and emit final row elements
    // directly into the selected output representation.
    for row in &rows {
        let projected = project_slot_row_dense(prepared_projection, row)?;
        debug_assert_eq!(projected.len(), projection.len());
        shaped_rows.push(RowView::Owned(projected));
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
fn project_slot_row_dense(
    prepared_projection: &PreparedProjectionShape,
    row: &RetainedSlotRow,
) -> Result<Vec<Value>, InternalError> {
    let projection = prepared_projection.projection();
    let mut shaped = Vec::with_capacity(projection.len());
    let mut read_slot = |slot: usize| {
        row.slot_ref(slot).map(Cow::Borrowed).ok_or_else(|| {
            ProjectionEvalError::MissingFieldValue {
                field: format!("slot[{slot}]"),
                index: slot,
            }
            .into_invalid_logical_plan_internal_error()
        })
    };
    visit_prepared_projection_values_with_required_value_reader_cow(
        prepared_projection.prepared(),
        &mut read_slot,
        &mut |value| shaped.push(value),
    )?;

    Ok(shaped)
}

#[cfg(feature = "sql")]
// Shape one retained dense slot-row page through direct field-slot copies only.
fn shape_slot_rows_from_direct_field_slots(
    rows: Vec<RetainedSlotRow>,
    field_slots: &[(String, usize)],
) -> Result<Vec<RowView<'static>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: move only requested retained slots into one owned row view per
    // output row. Retained rows do not expose an ordered contiguous projection
    // slice, so the final row still owns the selected values.
    for mut row in rows {
        let mut shaped = Vec::with_capacity(field_slots.len());
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

        shaped_rows.push(RowView::Owned(shaped));
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
fn project_slot_row_from_direct_field_slots(
    mut row: RetainedSlotRow,
    field_slots: &[(String, usize)],
) -> Result<Vec<Value>, InternalError> {
    if let [field_slot] = field_slots {
        return project_slot_row_from_single_direct_field(row, field_slot).map(|value| vec![value]);
    }

    let mut shaped = Vec::with_capacity(field_slots.len());
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

    Ok(shaped)
}

#[cfg(feature = "sql")]
fn project_slot_row_from_single_direct_field(
    mut row: RetainedSlotRow,
    (field_name, slot): &(String, usize),
) -> Result<Value, InternalError> {
    row.take_slot(*slot)
        .ok_or_else(|| ProjectionEvalError::MissingFieldValue {
            field: field_name.clone(),
            index: *slot,
        })
        .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
}

#[cfg(feature = "sql")]
// Shape one raw data-row page through direct field-slot copies only.
fn shape_data_rows_from_direct_field_slots(
    rows: &[DataRow],
    row_layout: RowLayout,
    field_slots: &[(String, usize)],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<RowView<'static>>, InternalError> {
    if let [field_slot] = field_slots {
        return shape_data_rows_from_single_direct_field(rows, row_layout, field_slot, metrics);
    }

    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: open each structural row once, then decode only the declared
    // direct field slots into the final output representation.
    for (data_key, raw_row) in rows {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut shaped = Vec::with_capacity(field_slots.len());
        for (_field_name, slot) in field_slots {
            metrics.record_data_rows_slot_access(true);

            let value = row_fields.required_value_by_contract(*slot)?;
            shaped.push(value);
        }
        shaped_rows.push(RowView::Owned(shaped));
    }

    Ok(shaped_rows)
}

// Shape one raw data-row direct slot without the generic multi-field projection
// loop. The single requested value is still materialized at the output boundary.
#[cfg(feature = "sql")]
fn shape_data_rows_from_single_direct_field(
    rows: &[DataRow],
    row_layout: RowLayout,
    field_slot: &(String, usize),
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<RowView<'static>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: validate each row as before, then decode only the one projected
    // slot and emit it directly.
    for row in rows {
        let value =
            project_data_row_from_single_direct_field(row_layout, row, field_slot, metrics)?;
        shaped_rows.push(RowView::Owned(vec![value]));
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
fn project_data_row_from_direct_field_slots(
    row_layout: RowLayout,
    row: &DataRow,
    field_slots: &[(String, usize)],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    if let [field_slot] = field_slots {
        return project_data_row_from_single_direct_field(row_layout, row, field_slot, metrics)
            .map(|value| vec![value]);
    }

    let (data_key, raw_row) = row;
    let row_fields = row_layout.open_raw_row(raw_row)?;
    row_fields.validate_storage_key(data_key)?;

    let mut shaped = Vec::with_capacity(field_slots.len());
    for (_field_name, slot) in field_slots {
        metrics.record_data_rows_slot_access(true);

        shaped.push(row_fields.required_value_by_contract(*slot)?);
    }

    Ok(shaped)
}

#[cfg(feature = "sql")]
fn project_data_row_from_single_direct_field(
    row_layout: RowLayout,
    (data_key, raw_row): &DataRow,
    (_field_name, slot): &(String, usize),
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Value, InternalError> {
    let row_fields = row_layout.open_raw_row(raw_row)?;
    row_fields.validate_storage_key(data_key)?;

    metrics.record_data_rows_slot_access(true);

    row_fields.required_value_by_contract(*slot)
}

#[cfg(feature = "sql")]
fn shape_scalar_data_rows(
    compiled_fields: &[ScalarProjectionExpr],
    rows: &[DataRow],
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<RowView<'static>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = projected_slot_mask;

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path once and emit final row elements immediately.
    for (data_key, raw_row) in rows {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut shaped = Vec::with_capacity(compiled_fields.len());
        for compiled in compiled_fields {
            let value = eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
                compiled,
                &mut |slot| {
                    metrics.record_data_rows_slot_access(
                        projected_slot_mask.get(slot).copied().unwrap_or(false),
                    );

                    row_fields.required_value_by_contract_cow(slot)
                },
            )?;
            shaped.push(value.into_owned());
        }
        shaped_rows.push(RowView::Owned(shaped));
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
fn project_scalar_data_row(
    compiled_fields: &[ScalarProjectionExpr],
    (data_key, raw_row): &DataRow,
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Value>, InternalError> {
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = projected_slot_mask;

    let row_fields = row_layout.open_raw_row(raw_row)?;
    row_fields.validate_storage_key(data_key)?;

    let mut shaped = Vec::with_capacity(compiled_fields.len());
    for compiled in compiled_fields {
        let value = eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
            compiled,
            &mut |slot| {
                metrics.record_data_rows_slot_access(
                    projected_slot_mask.get(slot).copied().unwrap_or(false),
                );

                row_fields.required_value_by_contract_cow(slot)
            },
        )?;
        shaped.push(value.into_owned());
    }

    Ok(shaped)
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
        compiled_fields.push(compiled);
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
        on_value(eval_scalar_projection_expr_with_value_reader(
            compiled, read_slot,
        )?);
    }

    Ok(())
}

// Walk one prepared projection plan through one reader that can borrow slot
// values from retained structural rows until an expression needs ownership.
pub(in crate::db) fn visit_prepared_projection_values_with_required_value_reader_cow<'a>(
    prepared: &'a PreparedProjectionPlan,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), InternalError> {
    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared;
    for compiled in compiled_fields {
        on_value(
            eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
                compiled, read_slot,
            )?
            .into_owned(),
        );
    }

    Ok(())
}
