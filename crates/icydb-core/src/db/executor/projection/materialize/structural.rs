//! Module: db::executor::projection::materialize::structural
//! Responsibility: structural SQL projection row materialization over persisted slot rows.
//! Does not own: grouped projection rendering, projection validation, or projection expression semantics.
//! Boundary: the materialize root delegates here for the structural SQL row loop once projection shape has been fixed.

#[cfg(any(test, feature = "perf-attribution"))]
use crate::db::executor::pipeline::contracts::StructuralCursorPagePayload;
#[cfg(any(test, feature = "perf-attribution"))]
use crate::db::{
    data::{CanonicalSlotReader, DataRow},
    executor::{
        pipeline::contracts::StructuralCursorPage,
        projection::eval::eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
        terminal::RowLayout,
    },
};
#[cfg(any(test, feature = "perf-attribution"))]
use crate::{
    db::executor::pipeline::entrypoints::execute_initial_scalar_sql_projection_page_for_canister,
    db::executor::projection::materialize::prepare_projection_shape_from_plan,
};
use crate::{
    db::{
        Db,
        executor::{
            EntityAuthority,
            pipeline::entrypoints::{
                execute_initial_scalar_sql_projection_rows_for_canister,
                execute_initial_scalar_sql_projection_text_rows_for_canister,
            },
            projection::{
                PreparedProjectionShape, ProjectionEvalError,
                materialize::visit_prepared_projection_values_with_required_value_reader_cow,
            },
            terminal::RetainedSlotRow,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
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
/// covering reads can skip `Value` row materialization while structural callers
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

///
/// SqlProjectionTextExecutorAttribution
///
/// SqlProjectionTextExecutorAttribution breaks the rendered SQL projection
/// executor path into structural prepare, scalar runtime, projection
/// materialization, and final row-payload packaging.
/// This lets perf harnesses separate fixed executor setup from the terminal
/// fast path without reopening the session or dispatch layers above it.
///

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlProjectionTextExecutorAttribution {
    pub prepare_projection_local_instructions: u64,
    pub scalar_runtime_local_instructions: u64,
    pub materialize_projection_local_instructions: u64,
    pub result_rows_local_instructions: u64,
    pub total_local_instructions: u64,
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
fn read_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
fn measure_structural_result<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
/// Execute one scalar load plan through the shared rendered SQL projection
/// path and return both the rendered rows and one executor-only phase split.
pub(in crate::db) fn attribute_sql_projection_text_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<(SqlProjectionTextExecutorAttribution, SqlProjectionTextRows), InternalError>
where
    C: CanisterKind,
{
    let row_layout = authority.row_layout();

    // Phase 1: freeze the executor-owned structural projection contract.
    let (prepare_projection_local_instructions, prepared_projection) =
        measure_structural_result(|| {
            Ok::<PreparedProjectionShape, InternalError>(prepare_projection_shape_from_plan(
                row_layout.field_count(),
                &plan,
            ))
        });
    let prepared_projection = prepared_projection?;

    // Phase 2: execute the scalar runtime while allowing rendered terminal
    // short paths to return directly.
    let (scalar_runtime_local_instructions, page) = measure_structural_result(|| {
        execute_initial_scalar_sql_projection_page_for_canister(
            db,
            debug,
            authority,
            plan,
            crate::db::executor::pipeline::contracts::ProjectionMaterializationMode::SqlImmediateRenderedDispatch,
        )
    });
    let page = page?;

    // Phase 3: project or preserve the structural page into rendered SQL rows.
    let (materialize_projection_local_instructions, rendered_rows) =
        measure_structural_result(|| {
            render_structural_sql_projection_page(row_layout, &prepared_projection, page)
        });
    let rendered_rows = rendered_rows?;

    // Phase 4: package the rendered rows onto the stable SQL projection text
    // payload boundary.
    let (result_rows_local_instructions, projected) = measure_structural_result(|| {
        let row_count = u32::try_from(rendered_rows.len()).unwrap_or(u32::MAX);

        Ok::<SqlProjectionTextRows, InternalError>(SqlProjectionTextRows::new(
            rendered_rows,
            row_count,
        ))
    });
    let projected = projected?;

    let total_local_instructions = prepare_projection_local_instructions
        .saturating_add(scalar_runtime_local_instructions)
        .saturating_add(materialize_projection_local_instructions)
        .saturating_add(result_rows_local_instructions);

    Ok((
        SqlProjectionTextExecutorAttribution {
            prepare_projection_local_instructions,
            scalar_runtime_local_instructions,
            materialize_projection_local_instructions,
            result_rows_local_instructions,
            total_local_instructions,
        },
        projected,
    ))
}

#[cfg(feature = "sql")]
/// Execute one scalar load plan through the shared structural SQL projection
/// path and return only projected SQL values.
pub(in crate::db) fn execute_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<SqlProjectionRows, InternalError>
where
    C: CanisterKind,
{
    let row_layout = authority.row_layout();
    let _ = row_layout;

    // Execute the canonical scalar runtime and then shape the resulting
    // immediate SQL terminal directly into projected SQL values.
    let projected =
        execute_initial_scalar_sql_projection_rows_for_canister(db, debug, authority, plan)?;
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}

/// Execute one scalar load plan through the shared structural SQL projection
/// path and return rendered projection text rows.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_sql_projection_text_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<SqlProjectionTextRows, InternalError>
where
    C: CanisterKind,
{
    let row_layout = authority.row_layout();
    let _ = row_layout;

    // Execute the canonical scalar runtime and render the resulting terminal
    // rows at the SQL text boundary without staging another payload adapter.
    let rendered_rows =
        execute_initial_scalar_sql_projection_text_rows_for_canister(db, debug, authority, plan)?;
    let row_count = u32::try_from(rendered_rows.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionTextRows::new(rendered_rows, row_count))
}

#[cfg(any(test, feature = "perf-attribution"))]
fn project_structural_sql_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let payload = page.into_payload();

    // Phase 1: project from retained slot rows when they are already
    // available, and fall back to persisted structural row reads last.
    match payload {
        StructuralCursorPagePayload::SlotRows(slot_rows) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_slot_rows_path_hit();

            project_slot_rows_from_projection_structural(prepared_projection, slot_rows)
        }
        StructuralCursorPagePayload::DataRows(data_rows) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_path_hit();

            project_data_rows_from_projection_structural(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
            )
        }
    }
}

#[cfg(any(test, feature = "perf-attribution"))]
fn render_structural_sql_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
) -> Result<Vec<Vec<String>>, InternalError> {
    let payload = page.into_payload();

    // Phase 1: render from retained slot rows when they are already
    // available, and fall back to persisted structural row reads last.
    match payload {
        StructuralCursorPagePayload::SlotRows(slot_rows) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_slot_rows_path_hit();

            render_slot_rows_from_projection_structural(prepared_projection, slot_rows)
        }
        StructuralCursorPagePayload::DataRows(data_rows) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_path_hit();

            render_data_rows_from_projection_structural(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
            )
        }
    }
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
/// Project retained slot rows directly into final SQL value rows.
pub(in crate::db::executor) fn project_sql_projection_slot_rows_for_dispatch(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    project_slot_rows_from_projection_structural(prepared_projection, rows)
}

#[cfg(feature = "sql")]
fn render_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<String>>, InternalError> {
    if let Some(field_slots) = prepared_projection.direct_projection_field_slots() {
        return render_slot_rows_from_direct_field_slots(rows, field_slots);
    }

    render_dense_slot_rows_from_projection_structural(prepared_projection, rows)
}

#[cfg(feature = "sql")]
/// Render retained slot rows directly into final SQL text rows.
pub(in crate::db::executor) fn render_sql_projection_slot_rows_for_dispatch(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<String>>, InternalError> {
    render_slot_rows_from_projection_structural(prepared_projection, rows)
}

#[cfg(feature = "sql")]
// Render one dense retained slot-row page through the prepared compiled
// structural projection evaluator without staging intermediate `Value` rows.
fn render_dense_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<String>>, InternalError> {
    let projection = prepared_projection.projection();
    let mut rendered_rows = Vec::with_capacity(rows.len());

    for row in &rows {
        let mut rendered = Vec::with_capacity(projection.len());
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
            &mut read_slot,
            &mut |value| rendered.push(render_sql_projection_value_text(&value)),
        )?;
        rendered_rows.push(rendered);
    }

    Ok(rendered_rows)
}

#[cfg(feature = "sql")]
// Project one dense retained slot-row page through the prepared compiled
// structural projection evaluator without reopening persisted rows.
fn project_dense_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<Value>>, InternalError> {
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
// Render one retained dense slot-row page through direct field-slot copies only.
fn render_slot_rows_from_direct_field_slots(
    rows: Vec<RetainedSlotRow>,
    field_slots: &[(String, usize)],
) -> Result<Vec<Vec<String>>, InternalError> {
    let mut rendered_rows = Vec::with_capacity(rows.len());

    for mut row in rows {
        let mut rendered = Vec::with_capacity(field_slots.len());
        for (field_name, slot) in field_slots {
            let value = row
                .take_slot(*slot)
                .ok_or_else(|| ProjectionEvalError::MissingFieldValue {
                    field: field_name.clone(),
                    index: *slot,
                })
                .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
            rendered.push(render_sql_projection_value_text(&value));
        }

        rendered_rows.push(rendered);
    }

    Ok(rendered_rows)
}

#[cfg(any(test, feature = "perf-attribution"))]
fn project_data_rows_from_projection_structural(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let super::PreparedProjectionPlan::Scalar(compiled_fields) = prepared_projection.prepared();

    #[cfg(any(test, feature = "structural-read-metrics"))]
    record_sql_projection_data_rows_scalar_fallback_hit();
    project_scalar_data_rows_from_projection_structural(
        compiled_fields.as_slice(),
        rows,
        row_layout,
        prepared_projection.projected_slot_mask(),
    )
}

#[cfg(any(test, feature = "perf-attribution"))]
fn render_data_rows_from_projection_structural(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
) -> Result<Vec<Vec<String>>, InternalError> {
    let super::PreparedProjectionPlan::Scalar(compiled_fields) = prepared_projection.prepared();

    #[cfg(any(test, feature = "structural-read-metrics"))]
    record_sql_projection_data_rows_scalar_fallback_hit();
    render_scalar_data_rows_from_projection_structural(
        compiled_fields.as_slice(),
        rows,
        row_layout,
        prepared_projection.projected_slot_mask(),
    )
}

#[cfg(any(test, feature = "perf-attribution"))]
fn project_scalar_data_rows_from_projection_structural(
    compiled_fields: &[crate::db::executor::projection::ScalarProjectionExpr],
    rows: &[DataRow],
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());

    #[cfg(not(any(test, feature = "structural-read-metrics")))]
    let _ = projected_slot_mask;

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path only.
    for (data_key, raw_row) in rows {
        let row_fields = row_layout.open_raw_row(raw_row)?;
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

#[cfg(any(test, feature = "perf-attribution"))]
fn render_scalar_data_rows_from_projection_structural(
    compiled_fields: &[crate::db::executor::projection::ScalarProjectionExpr],
    rows: &[DataRow],
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
) -> Result<Vec<Vec<String>>, InternalError> {
    let mut rendered_rows = Vec::with_capacity(rows.len());

    #[cfg(not(any(test, feature = "structural-read-metrics")))]
    let _ = projected_slot_mask;

    // Phase 1: evaluate fully scalar projections through the compiled scalar
    // expression path and render each emitted value immediately.
    for (data_key, raw_row) in rows {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut rendered = Vec::with_capacity(compiled_fields.len());
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
            rendered.push(render_sql_projection_value_text(&value));
        }
        rendered_rows.push(rendered);
    }

    Ok(rendered_rows)
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

///
/// TESTS
///

#[cfg(all(feature = "sql", test))]
mod tests {
    use super::*;
    use crate::{
        db::{
            executor::{
                pipeline::contracts::StructuralCursorPage,
                projection::tests::projection_eval_row_layout_for_materialize_tests,
            },
            query::plan::expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
        },
        value::Value,
    };

    fn direct_rank_projection_shape() -> PreparedProjectionShape {
        PreparedProjectionShape {
            projection: ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("rank")),
                alias: None,
            }]),
            prepared: super::super::PreparedProjectionPlan::Scalar(Vec::new()),
            projection_is_model_identity: false,
            direct_projection_field_slots: Some(vec![("rank".to_string(), 1)]),
            projected_slot_mask: vec![false, true, false, false],
        }
    }

    #[test]
    fn sql_projection_materialization_prefers_retained_slot_rows() {
        let row_layout = projection_eval_row_layout_for_materialize_tests();
        let page = StructuralCursorPage::new_with_slot_rows(
            vec![RetainedSlotRow::new(4, vec![(1, Value::Int(19))])],
            None,
        );
        let prepared_projection = direct_rank_projection_shape();

        let (payload, metrics) = with_sql_projection_materialization_metrics(|| {
            project_structural_sql_projection_page(row_layout, &prepared_projection, page)
        });
        let payload = payload.expect("slot-row SQL projection materialization should succeed");

        assert_eq!(payload, vec![vec![Value::Int(19)]]);

        assert_eq!(
            metrics.slot_rows_path_hits, 1,
            "slot-row projection should stay on the retained-slot path",
        );
        assert_eq!(
            metrics.data_rows_path_hits, 0,
            "slot-row projection should not reopen raw data rows",
        );
        assert_eq!(
            metrics.data_rows_scalar_fallback_hits, 0,
            "slot-row projection should avoid the scalar data-row fallback",
        );
        assert_eq!(
            metrics.full_row_decode_materializations, 0,
            "slot-row projection should not trigger eager full-row decode",
        );
    }
}
