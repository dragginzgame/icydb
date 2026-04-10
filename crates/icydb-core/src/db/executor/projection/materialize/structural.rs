//! Module: db::executor::projection::materialize::structural
//! Responsibility: structural SQL projection row materialization over persisted slot rows.
//! Does not own: grouped projection rendering, generic projection validation, or projection expression semantics.
//! Boundary: the materialize root delegates here for the structural SQL row loop once projection shape has been fixed.

use crate::{
    db::{
        Db,
        data::{CanonicalSlotReader, DataRow, StructuralSlotReader},
        executor::pipeline::contracts::StructuralCursorPage,
        executor::{
            EntityAuthority,
            pipeline::entrypoints::{
                execute_initial_scalar_rows_for_canister,
                execute_initial_scalar_text_rows_for_canister,
            },
            projection::{
                PreparedProjectionShape, ProjectionEvalError,
                eval::eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
                materialize::{
                    prepare_projection_shape,
                    visit_prepared_projection_values_with_required_value_reader_cow,
                    visit_projection_values_with_required_value_reader_cow,
                },
            },
            terminal::RetainedSlotRow,
        },
        query::plan::{AccessPlannedQuery, expr::ProjectionSpec},
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::CanisterKind,
    value::{Value, ValueEnum},
};
#[cfg(feature = "sql")]
use std::borrow::Cow;
#[cfg(any(test, feature = "structural-read-metrics"))]
use std::cell::RefCell;

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

///
/// SqlProjectionTextRows
///
/// Generic-free SQL projection row payload emitted directly as rendered text.
/// This keeps the SQL dispatch fast path narrow: executor-owned direct
/// covering reads can skip `Value` row materialization while generic callers
/// continue using the existing `SqlProjectionRows` contract.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct SqlProjectionTextRows {
    rows: Vec<Vec<String>>,
    row_count: u32,
}

#[cfg(feature = "sql")]
impl SqlProjectionTextRows {
    #[must_use]
    pub(in crate::db) const fn new(rows: Vec<Vec<String>>, row_count: u32) -> Self {
        Self { rows, row_count }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<Vec<String>>, u32) {
        (self.rows, self.row_count)
    }
}

#[cfg(feature = "sql")]
enum StructuralSqlProjectionMaterialization {
    Rendered(Vec<Vec<String>>),
    Values(Vec<Vec<Value>>),
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
    let projected = match materialize_structural_sql_projection_page(model, projection, page)? {
        StructuralSqlProjectionMaterialization::Values(rows) => rows,
        StructuralSqlProjectionMaterialization::Rendered(_) => {
            return Err(InternalError::query_executor_invariant(
                "value SQL projection path must not receive rendered-only projected rows",
            ));
        }
    };
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}

/// Execute one scalar load plan through the shared structural SQL projection
/// path and return rendered projection text rows.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_sql_projection_text_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    model: &'static EntityModel,
    projection: ProjectionSpec,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<SqlProjectionTextRows, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: execute the scalar rows path once for the whole canister while
    // allowing the terminal short path to emit already-rendered SQL rows.
    let page = execute_initial_scalar_text_rows_for_canister(db, debug, authority, plan)?;
    let rendered_rows = match materialize_structural_sql_projection_page(model, projection, page)? {
        StructuralSqlProjectionMaterialization::Rendered(rows) => rows,
        StructuralSqlProjectionMaterialization::Values(rows) => {
            render_sql_projection_rows_from_values(rows)
        }
    };
    let row_count = u32::try_from(rendered_rows.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionTextRows::new(rendered_rows, row_count))
}

#[cfg(feature = "sql")]
fn materialize_structural_sql_projection_page(
    model: &'static EntityModel,
    projection: ProjectionSpec,
    page: StructuralCursorPage,
) -> Result<StructuralSqlProjectionMaterialization, InternalError> {
    let (slot_rows, projected_rows, rendered_projected_rows, data_rows) = page.into_sql_parts();

    // Phase 1: prefer already-materialized projected rows from the scalar
    // kernel before reopening slot-row or raw-row structural fallbacks.
    if let Some(rendered_projected_rows) = rendered_projected_rows {
        return Ok(StructuralSqlProjectionMaterialization::Rendered(
            rendered_projected_rows,
        ));
    }
    if let Some(projected_rows) = projected_rows {
        #[cfg(any(test, feature = "structural-read-metrics"))]
        record_sql_projection_projected_rows_path_hit();

        return Ok(StructuralSqlProjectionMaterialization::Values(
            projected_rows,
        ));
    }

    // Phase 2: project from retained slot rows when they are already
    // available, and fall back to persisted structural row reads last.
    let prepared_projection = prepare_projection_shape(model, projection);
    let projected = if let Some(slot_rows) = slot_rows {
        #[cfg(any(test, feature = "structural-read-metrics"))]
        record_sql_projection_slot_rows_path_hit();
        project_slot_rows_from_projection_structural(&prepared_projection, slot_rows)?
    } else {
        #[cfg(any(test, feature = "structural-read-metrics"))]
        record_sql_projection_data_rows_path_hit();
        project_data_rows_from_projection_structural(
            model,
            &prepared_projection,
            data_rows.as_slice(),
        )?
    };

    Ok(StructuralSqlProjectionMaterialization::Values(projected))
}

#[cfg(feature = "sql")]
fn render_sql_projection_rows_from_values(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
    let mut rendered_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let rendered_row = row
            .iter()
            .map(render_sql_projection_value_text)
            .collect::<Vec<_>>();
        rendered_rows.push(rendered_row);
    }

    rendered_rows
}

#[cfg(feature = "sql")]
fn render_sql_projection_value_text(value: &Value) -> String {
    match value {
        Value::Account(v) => v.to_string(),
        Value::Blob(v) => render_sql_projection_blob(v.as_slice()),
        Value::Bool(v) => v.to_string(),
        Value::Date(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),
        Value::Duration(v) => render_sql_projection_duration(v.as_millis()),
        Value::Enum(v) => render_sql_projection_enum(v),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::Int128(v) => v.to_string(),
        Value::IntBig(v) => v.to_string(),
        Value::List(items) => render_sql_projection_list(items.as_slice()),
        Value::Map(entries) => render_sql_projection_map(entries.as_slice()),
        Value::Null => "null".to_string(),
        Value::Principal(v) => v.to_string(),
        Value::Subaccount(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Timestamp(v) => v.as_millis().to_string(),
        Value::Uint(v) => v.to_string(),
        Value::Uint128(v) => v.to_string(),
        Value::UintBig(v) => v.to_string(),
        Value::Ulid(v) => v.to_string(),
        Value::Unit => "()".to_string(),
    }
}

#[cfg(feature = "sql")]
fn render_sql_projection_blob(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(sql_projection_hex_encode(bytes).as_str());

    rendered
}

#[cfg(feature = "sql")]
fn render_sql_projection_duration(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");

    rendered
}

#[cfg(feature = "sql")]
fn render_sql_projection_list(items: &[Value]) -> String {
    let mut rendered = String::from("[");

    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_sql_projection_value_text(item).as_str());
    }

    rendered.push(']');

    rendered
}

#[cfg(feature = "sql")]
fn render_sql_projection_map(entries: &[(Value, Value)]) -> String {
    let mut rendered = String::from("{");

    for (index, (key, value)) in entries.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_sql_projection_value_text(key).as_str());
        rendered.push_str(": ");
        rendered.push_str(render_sql_projection_value_text(value).as_str());
    }

    rendered.push('}');

    rendered
}

#[cfg(feature = "sql")]
fn sql_projection_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
}

#[cfg(feature = "sql")]
fn render_sql_projection_enum(value: &ValueEnum) -> String {
    let mut rendered = String::new();
    if let Some(path) = value.path() {
        rendered.push_str(path);
        rendered.push_str("::");
    }
    rendered.push_str(value.variant());
    if let Some(payload) = value.payload() {
        rendered.push('(');
        rendered.push_str(render_sql_projection_value_text(payload).as_str());
        rendered.push(')');
    }

    rendered
}

fn project_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = prepared_projection.direct_projection_field_slots() {
        return project_slot_rows_from_direct_field_slots(rows, field_slots);
    }

    project_dense_slot_rows_from_projection_structural(prepared_projection, rows)
}

#[cfg(feature = "sql")]
// Project one dense retained slot-row page through the generic structural
// projection evaluator without reopening persisted rows.
fn project_dense_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let model = prepared_projection.model();
    let projection = prepared_projection.projection();
    let mut projected_rows = Vec::with_capacity(rows.len());

    for row in &rows {
        let mut values = Vec::with_capacity(projection.len());
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
            projection,
            model,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;
        projected_rows.push(values);
    }

    Ok(projected_rows)
}

#[cfg(feature = "sql")]
// Project one retained dense slot-row page through direct field-slot copies only.
fn project_slot_rows_from_direct_field_slots(
    rows: Vec<RetainedSlotRow>,
    field_slots: &[(String, usize)],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let mut values = Vec::with_capacity(field_slots.len());
        for (field_name, slot) in field_slots {
            let value = row
                .take_slot(*slot)
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
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
) -> Result<Vec<Vec<Value>>, InternalError> {
    match prepared_projection.prepared() {
        super::PreparedProjectionPlan::Generic => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_generic_fallback_hit();
            project_generic_data_rows_from_projection_structural(
                model,
                prepared_projection.projection(),
                rows,
                prepared_projection.projected_slot_mask(),
            )
        }
        super::PreparedProjectionPlan::Scalar(compiled_fields) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_scalar_fallback_hit();
            project_scalar_data_rows_from_projection_structural(
                compiled_fields.as_slice(),
                rows,
                model,
                prepared_projection.projected_slot_mask(),
            )
        }
    }
}

#[cfg(feature = "sql")]
fn project_scalar_data_rows_from_projection_structural(
    compiled_fields: &[crate::db::executor::projection::ScalarProjectionExpr],
    rows: &[DataRow],
    model: &'static EntityModel,
    projected_slot_mask: &[bool],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    #[cfg(not(any(test, feature = "structural-read-metrics")))]
    let _ = projected_slot_mask;

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path only.
    for (data_key, raw_row) in rows {
        let row_fields = StructuralSlotReader::from_raw_row(raw_row, model)?;
        row_fields.validate_storage_key(data_key)?;

        let mut values = Vec::with_capacity(compiled_fields.len());
        for compiled in compiled_fields {
            let value = eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
                compiled,
                &mut |slot| {
                    #[cfg(any(test, feature = "structural-read-metrics"))]
                    record_sql_projection_data_rows_slot_access(
                        projected_slot_mask.get(slot).copied().unwrap_or(false),
                    );

                    row_fields.required_value_by_contract_cow(slot)
                },
            )?;
            values.push(value.into_owned());
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
    projected_slot_mask: &[bool],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    #[cfg(not(any(test, feature = "structural-read-metrics")))]
    let _ = projected_slot_mask;

    // Phase 1: keep the generic evaluator isolated to projection shapes that
    // genuinely leave the scalar seam.
    for (data_key, raw_row) in rows {
        let row_fields = StructuralSlotReader::from_raw_row(raw_row, model)?;
        row_fields.validate_storage_key(data_key)?;

        // Phase 2: borrow decoded slot values directly from the structural
        // row cache and materialize only when the projection output needs an
        // owned `Value`.
        let mut values = Vec::with_capacity(projection.len());
        let mut read_slot = |slot: usize| {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_slot_access(
                projected_slot_mask.get(slot).copied().unwrap_or(false),
            );

            row_fields.required_value_by_contract_cow(slot)
        };
        visit_projection_values_with_required_value_reader_cow(
            projection,
            model,
            &mut read_slot,
            &mut |value| values.push(value),
        )?;

        projected_rows.push(values);
    }

    Ok(projected_rows)
}

///
/// SqlProjectionMaterializationMetrics
///
/// SqlProjectionMaterializationMetrics aggregates one test-scoped view of the
/// row-backed SQL projection path selection and fallback slot access behavior.
/// It lets perf probes distinguish retained projected rows, retained slot
/// rows, and `data_rows` fallback execution without changing runtime policy.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlProjectionMaterializationMetrics {
    pub projected_rows_path_hits: u64,
    pub slot_rows_path_hits: u64,
    pub data_rows_path_hits: u64,
    pub data_rows_scalar_fallback_hits: u64,
    pub data_rows_generic_fallback_hits: u64,
    pub data_rows_projected_slot_accesses: u64,
    pub data_rows_non_projected_slot_accesses: u64,
    pub full_row_decode_materializations: u64,
}

#[cfg(any(test, feature = "structural-read-metrics"))]
std::thread_local! {
    static SQL_PROJECTION_MATERIALIZATION_METRICS: RefCell<Option<SqlProjectionMaterializationMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn update_sql_projection_materialization_metrics(
    update: impl FnOnce(&mut SqlProjectionMaterializationMetrics),
) {
    SQL_PROJECTION_MATERIALIZATION_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_projected_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.projected_rows_path_hits = metrics.projected_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_slot_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.slot_rows_path_hits = metrics.slot_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_data_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_path_hits = metrics.data_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_data_rows_scalar_fallback_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_scalar_fallback_hits =
            metrics.data_rows_scalar_fallback_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_data_rows_generic_fallback_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_generic_fallback_hits =
            metrics.data_rows_generic_fallback_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_data_rows_slot_access(projected_slot: bool) {
    update_sql_projection_materialization_metrics(|metrics| {
        if projected_slot {
            metrics.data_rows_projected_slot_accesses =
                metrics.data_rows_projected_slot_accesses.saturating_add(1);
        } else {
            metrics.data_rows_non_projected_slot_accesses = metrics
                .data_rows_non_projected_slot_accesses
                .saturating_add(1);
        }
    });
}

///
/// record_sql_projection_full_row_decode_materialization
///
/// Record one eager full-row slot materialization event under the current
/// SQL projection metrics capture.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db::executor) fn record_sql_projection_full_row_decode_materialization() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.full_row_decode_materializations =
            metrics.full_row_decode_materializations.saturating_add(1);
    });
}

///
/// with_sql_projection_materialization_metrics
///
/// Run one closure while collecting row-backed SQL projection metrics on the
/// current thread, then return the closure result plus the aggregated
/// snapshot.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(dead_code, unreachable_pub)
)]
pub fn with_sql_projection_materialization_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, SqlProjectionMaterializationMetrics) {
    SQL_PROJECTION_MATERIALIZATION_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "sql projection metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(SqlProjectionMaterializationMetrics::default());
    });

    let result = f();
    let metrics = SQL_PROJECTION_MATERIALIZATION_METRICS
        .with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}
