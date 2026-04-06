//! Module: db::executor::projection::materialize::structural
//! Responsibility: structural SQL projection row materialization over persisted slot rows.
//! Does not own: grouped projection rendering, generic projection validation, or projection expression semantics.
//! Boundary: the materialize root delegates here for the structural SQL row loop once projection shape has been fixed.

use crate::{
    db::{
        Db,
        data::{CanonicalSlotReader, DataRow, StructuralSlotReader},
        executor::{
            EntityAuthority,
            pipeline::entrypoints::execute_initial_scalar_rows_for_canister,
            projection::{
                ProjectionEvalError, direct_projection_field_slots,
                eval::eval_canonical_scalar_projection_expr,
                materialize::{
                    prepare_projection_plan, visit_prepared_projection_values_with_value_reader,
                    visit_projection_values_with_required_value_reader,
                },
            },
        },
        query::plan::{AccessPlannedQuery, expr::ProjectionSpec},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::CanisterKind,
    value::Value,
};

///
/// SqlProjectionRows
///
/// Generic-free SQL projection row payload emitted by executor-owned structural
/// projection execution helpers.
/// Keeps SQL row materialization out of typed `ProjectionResponse<E>` so SQL
/// dispatch can render value rows without reintroducing entity-specific ids.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct SqlProjectionRows {
    rows: Vec<Vec<Value>>,
    row_count: u32,
}

#[cfg(feature = "sql")]
impl SqlProjectionRows {
    #[must_use]
    pub(in crate::db) const fn new(rows: Vec<Vec<Value>>, row_count: u32) -> Self {
        Self { rows, row_count }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<Vec<Value>>, u32) {
        (self.rows, self.row_count)
    }
}

/// Execute one scalar load plan through the shared structural SQL projection
/// path and return only projected SQL values.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    model: &'static EntityModel,
    projection: ProjectionSpec,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<SqlProjectionRows, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: execute the scalar rows path once for the whole canister while
    // reusing the already-derived projection contract from the caller.
    let page = execute_initial_scalar_rows_for_canister(db, debug, authority, plan)?;
    let (slot_rows, projected_rows, data_rows) = page.into_sql_parts();

    // Phase 2: prefer already-decoded slot rows when the scalar kernel kept
    // them for immediate SQL projection materialization. Fall back to the
    // canonical structural-row reader on all other paths.
    let projected = if let Some(projected_rows) = projected_rows {
        projected_rows
    } else if let Some(slot_rows) = slot_rows {
        project_slot_rows_from_projection_structural(model, &projection, slot_rows)?
    } else {
        project_data_rows_from_projection_structural(model, &projection, data_rows.as_slice())?
    };
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}

fn project_slot_rows_from_projection_structural(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    rows: Vec<Vec<Option<Value>>>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = direct_projection_field_slots(model, projection) {
        return project_slot_rows_from_direct_field_slots(rows, field_slots.as_slice());
    }

    let prepared = prepare_projection_plan(model, projection);
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: reuse the executor-decoded slot rows directly instead of
    // rebuilding structural readers from persisted row bytes.
    for row in &rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot: usize| row.get(slot).cloned().flatten();
        visit_prepared_projection_values_with_value_reader(
            &prepared,
            projection,
            model,
            &mut read_slot,
            &mut |value| values.push(value),
        )
        .map_err(
            crate::db::executor::projection::ProjectionEvalError::into_invalid_logical_plan_internal_error,
        )?;
        projected_rows.push(values);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
// Project one retained slot-row page through direct field-slot copies only.
fn project_slot_rows_from_direct_field_slots(
    rows: Vec<Vec<Option<Value>>>,
    field_slots: &[(String, usize)],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let mut values = Vec::with_capacity(field_slots.len());
        for (field_name, slot) in field_slots {
            let value = row
                .get_mut(*slot)
                .and_then(Option::take)
                .ok_or_else(|| ProjectionEvalError::MissingFieldValue {
                    field: field_name.clone(),
                    index: *slot,
                })
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
            values.push(value);
        }

        projected_rows.push(values);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn project_data_rows_from_projection_structural(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    rows: &[DataRow],
) -> Result<Vec<Vec<Value>>, InternalError> {
    match prepare_projection_plan(model, projection) {
        super::PreparedProjectionPlan::Generic => {
            project_generic_data_rows_from_projection_structural(model, projection, rows)
        }
        super::PreparedProjectionPlan::Scalar(compiled_fields) => {
            project_scalar_data_rows_from_projection_structural(
                compiled_fields.as_slice(),
                rows,
                model,
            )
        }
    }
}

#[cfg(feature = "sql")]
fn project_scalar_data_rows_from_projection_structural(
    compiled_fields: &[crate::db::executor::projection::ScalarProjectionExpr],
    rows: &[DataRow],
    model: &'static EntityModel,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path only.
    for (data_key, raw_row) in rows {
        let row_fields = StructuralSlotReader::from_raw_row(raw_row, model)?;
        row_fields.validate_storage_key(data_key)?;

        let mut values = Vec::with_capacity(compiled_fields.len());
        for compiled in compiled_fields {
            let value = eval_canonical_scalar_projection_expr(compiled, &row_fields)?;
            values.push(value);
        }
        projected_rows.push(values);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn project_generic_data_rows_from_projection_structural(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    rows: &[DataRow],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: keep the generic evaluator isolated to projection shapes that
    // genuinely leave the scalar seam.
    for (data_key, raw_row) in rows {
        let row_fields = StructuralSlotReader::from_raw_row(raw_row, model)?;
        row_fields.validate_storage_key(data_key)?;

        // Phase 2: decode declared slots lazily but fail closed when a
        // canonical structural row omits one.
        let mut values = Vec::with_capacity(projection.len());
        let mut slot_cache: Vec<Option<Value>> = vec![None; model.fields().len()];
        let mut read_slot = |slot: usize| {
            if slot_cache[slot].is_none() {
                slot_cache[slot] = Some(row_fields.required_value_by_contract(slot)?);
            }

            slot_cache[slot].clone().ok_or_else(|| {
                InternalError::executor_internal(format!(
                    "structural projection slot cache missing decoded value: slot={slot}",
                ))
            })
        };
        visit_projection_values_with_required_value_reader(
            projection,
            model,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;

        projected_rows.push(values);
    }

    Ok(projected_rows)
}
