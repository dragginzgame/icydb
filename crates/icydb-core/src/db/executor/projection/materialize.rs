//! Module: db::executor::projection::materialize
//! Responsibility: module-local ownership and contracts for db::executor::projection::materialize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "sql")]
use crate::db::{
    Db,
    data::{DataKey, DataRow, StructuralSlotReader, decode_slot_value_by_contract},
    executor::pipeline::entrypoints::execute_prepared_scalar_rows_for_canister,
    executor::{EntityAuthority, PreparedLoadPlan},
};
#[cfg(all(feature = "sql", test))]
use crate::{
    db::response::ProjectedRow,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use crate::{
    db::{
        query::plan::AccessPlannedQuery,
        query::plan::expr::{Expr, ProjectionField, ProjectionSpec},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::CanisterKind,
    value::Value,
};

use crate::db::executor::projection::{
    eval::{
        ProjectionEvalError, ScalarProjectionEvalError, ScalarProjectionExpr,
        compile_scalar_projection_expr, eval_expr_grouped, eval_expr_with_slot_reader,
        eval_scalar_projection_expr,
    },
    grouped::GroupedRowView,
};

///
/// SqlStructuralProjectionRows
///
/// Generic-free SQL projection row payload emitted by executor-owned structural
/// projection execution helpers.
/// Keeps SQL row materialization out of typed `ProjectionResponse<E>` so SQL
/// dispatch can render value rows without reintroducing entity-specific ids.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct SqlStructuralProjectionRows {
    rows: Vec<Vec<Value>>,
    row_count: u32,
}

#[cfg(feature = "sql")]
impl SqlStructuralProjectionRows {
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
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<SqlStructuralProjectionRows, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: derive projection semantics before moving the plan into the
    // shared scalar execution path.
    let projection = plan.projection_spec(authority.model());
    let prepared = PreparedLoadPlan::from_plan(authority, plan);

    // Phase 2: execute the scalar rows path once for the whole canister.
    let page = execute_prepared_scalar_rows_for_canister(db, debug, prepared)?;

    // Phase 3: decode rows structurally and discard ids because SQL projection
    // rendering only needs ordered cell values.
    let projected = project_data_rows_from_projection_structural(
        authority.model(),
        &projection,
        page.data_rows(),
    )?;
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);
    let rows = projected
        .into_iter()
        .map(|(_, values)| values)
        .collect::<Vec<_>>();

    Ok(SqlStructuralProjectionRows::new(rows, row_count))
}

#[cfg(feature = "sql")]
// Compile the projection once so row materialization can dispatch into one
// scalar-only or generic-only loop.
enum StructuralProjectionPlan {
    Scalar(Vec<ScalarProjectionExpr>),
    Generic,
}

/// Validate projection expressions over one row-domain that can expose values
/// by `(row_index, field_slot)` without forcing typed projection materialization.
pub(in crate::db::executor) fn validate_projection_over_slot_rows(
    model: &EntityModel,
    projection: &ProjectionSpec,
    row_count: usize,
    read_slot_for_row: &mut dyn FnMut(usize, usize) -> Option<Value>,
) -> Result<(), InternalError> {
    if projection_is_model_identity_for_model(model, projection) {
        return Ok(());
    }

    // Phase 1: evaluate every projection expression against each row.
    for row_index in 0..row_count {
        let mut read_slot = |slot| read_slot_for_row(row_index, slot);
        visit_projection_values_with_slot_reader(projection, model, &mut read_slot, &mut |_| {})
            .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;
    }

    Ok(())
}

/// Evaluate one grouped projection spec into ordered projected values.
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    projection: &ProjectionSpec,
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ProjectionEvalError> {
    let mut projected_values = Vec::with_capacity(projection.len());
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                projected_values.push(eval_expr_grouped(expr, grouped_row)?);
            }
        }
    }

    Ok(projected_values)
}

#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) fn project_rows_from_projection<E>(
    projection: &ProjectionSpec,
    rows: &[(Id<E>, E)],
) -> Result<Vec<ProjectedRow<E>>, ProjectionEvalError>
where
    E: EntityKind + EntityValue,
{
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (id, entity) in rows {
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot| entity.get_value_by_index(slot);
        visit_projection_values_with_slot_reader(
            projection,
            E::MODEL,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;
        projected_rows.push(ProjectedRow::new(*id, values));
    }

    Ok(projected_rows)
}

// Walk one projection spec through one slot-reader boundary so validation and
// row materialization share the same expression-evaluation spine.
fn visit_projection_values_with_slot_reader(
    projection: &ProjectionSpec,
    model: &EntityModel,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    on_value: &mut dyn FnMut(Value),
) -> Result<(), ProjectionEvalError> {
    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                on_value(eval_expr_with_slot_reader(expr, model, read_slot)?);
            }
        }
    }

    Ok(())
}

fn projection_is_model_identity_for_model(
    model: &EntityModel,
    projection: &ProjectionSpec,
) -> bool {
    if projection.len() != model.fields.len() {
        return false;
    }

    for (field_model, projected_field) in model.fields.iter().zip(projection.fields()) {
        match projected_field {
            ProjectionField::Scalar {
                expr: Expr::Field(field_id),
                alias: None,
            } if field_id.as_str() == field_model.name => {}
            ProjectionField::Scalar { .. } => return false,
        }
    }

    true
}

#[cfg(feature = "sql")]
fn project_data_rows_from_projection_structural(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    rows: &[DataRow],
) -> Result<Vec<(DataKey, Vec<Value>)>, InternalError> {
    match compile_structural_projection_plan(model, projection) {
        StructuralProjectionPlan::Scalar(compiled_fields) => {
            project_scalar_data_rows_from_projection_structural(
                compiled_fields.as_slice(),
                rows,
                model,
            )
        }
        StructuralProjectionPlan::Generic => {
            project_generic_data_rows_from_projection_structural(model, projection, rows)
        }
    }
}

#[cfg(feature = "sql")]
fn compile_structural_projection_plan(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
) -> StructuralProjectionPlan {
    let mut compiled_fields = Vec::with_capacity(projection.len());

    for field in projection.fields() {
        match field {
            ProjectionField::Scalar { expr, .. } => {
                let Some(compiled) = compile_scalar_projection_expr(model, expr) else {
                    return StructuralProjectionPlan::Generic;
                };
                compiled_fields.push(compiled);
            }
        }
    }

    StructuralProjectionPlan::Scalar(compiled_fields)
}

#[cfg(feature = "sql")]
fn project_scalar_data_rows_from_projection_structural(
    compiled_fields: &[ScalarProjectionExpr],
    rows: &[DataRow],
    model: &'static EntityModel,
) -> Result<Vec<(DataKey, Vec<Value>)>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path only.
    for (data_key, raw_row) in rows {
        let row_fields = StructuralSlotReader::from_raw_row(raw_row, model)?;
        row_fields.validate_storage_key(data_key)?;

        let mut values = Vec::with_capacity(compiled_fields.len());
        for compiled in compiled_fields {
            let value =
                eval_scalar_projection_expr(compiled, &row_fields).map_err(|err| match err {
                    ScalarProjectionEvalError::Eval(err) => {
                        crate::db::error::query_invalid_logical_plan(err.to_string())
                    }
                    ScalarProjectionEvalError::Internal(err) => err,
                })?;
            values.push(value);
        }
        projected_rows.push((data_key.clone(), values));
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
fn project_generic_data_rows_from_projection_structural(
    model: &'static EntityModel,
    projection: &ProjectionSpec,
    rows: &[DataRow],
) -> Result<Vec<(DataKey, Vec<Value>)>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: keep the generic evaluator isolated to projection shapes that
    // genuinely leave the scalar seam.
    for (data_key, raw_row) in rows {
        let row_fields = StructuralSlotReader::from_raw_row(raw_row, model)?;
        row_fields.validate_storage_key(data_key)?;

        let mut values = Vec::with_capacity(projection.len());
        let mut slot_cache: Vec<Option<Value>> = vec![None; model.fields().len()];
        let mut slot_decoded = vec![false; model.fields().len()];
        let mut slot_error: Option<InternalError> = None;
        let mut read_slot = |slot: usize| {
            if !slot_decoded[slot] {
                match decode_slot_value_by_contract(&row_fields, slot) {
                    Ok(value) => slot_cache[slot] = value,
                    Err(err) => slot_error = Some(err),
                }
                slot_decoded[slot] = true;
            }

            slot_cache[slot].clone()
        };
        visit_projection_values_with_slot_reader(projection, model, &mut read_slot, &mut |value| {
            values.push(value);
        })
        .map_err(|err| crate::db::error::query_invalid_logical_plan(err.to_string()))?;
        if let Some(err) = slot_error {
            return Err(err);
        }
        projected_rows.push((data_key.clone(), values));
    }

    Ok(projected_rows)
}
