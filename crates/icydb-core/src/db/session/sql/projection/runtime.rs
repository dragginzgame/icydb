//! Module: db::session::sql::projection::runtime
//! Responsibility: session-owned SQL projection row shaping over structural
//! executor pages.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: consumes structural pages from the executor and performs the
//! SQL-specific value/text shaping above that boundary.

use crate::{
    db::{Db, query::plan::AccessPlannedQuery},
    error::InternalError,
    traits::CanisterKind,
    value::ValueEnum,
};
use crate::{
    db::{
        data::{CanonicalSlotReader, DataRow},
        executor::{
            EntityAuthority, StructuralCursorPage, StructuralCursorPagePayload,
            pipeline::execute_initial_scalar_retained_slot_page_for_canister,
            projection::{
                PreparedProjectionShape, ProjectionEvalError, ScalarProjectionExpr,
                eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
                prepare_projection_shape_from_plan,
                visit_prepared_projection_values_with_required_value_reader_cow,
            },
            terminal::{RetainedSlotRow, RowLayout},
        },
    },
    value::Value,
};
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
    pub prepare_projection: u64,
    pub scalar_runtime: u64,
    pub materialize_projection: u64,
    pub result_rows: u64,
    pub total: u64,
}

#[cfg(all(feature = "sql", feature = "perf-attribution", target_arch = "wasm32"))]
fn read_local_instruction_counter() -> u64 {
    canic_cdk::api::performance_counter(1)
}

#[cfg(all(
    feature = "sql",
    feature = "perf-attribution",
    not(target_arch = "wasm32")
))]
const fn read_local_instruction_counter() -> u64 {
    0
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

    // Phase 2: execute the scalar runtime and preserve one structural slot-row
    // page for later SQL-specific shaping.
    let (scalar_runtime_local_instructions, page) = measure_structural_result(|| {
        execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, plan)
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
            prepare_projection: prepare_projection_local_instructions,
            scalar_runtime: scalar_runtime_local_instructions,
            materialize_projection: materialize_projection_local_instructions,
            result_rows: result_rows_local_instructions,
            total: total_local_instructions,
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
    let prepared_projection = prepare_projection_shape_from_plan(row_layout.field_count(), &plan);

    // Execute the canonical scalar runtime and then shape the resulting
    // structural page into projected SQL values.
    let page = execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, plan)?;
    let projected = project_structural_sql_projection_page(row_layout, &prepared_projection, page)?;
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
    let prepared_projection = prepare_projection_shape_from_plan(row_layout.field_count(), &plan);

    // Execute the canonical scalar runtime and render the resulting structural
    // page at the SQL text boundary without staging another payload adapter.
    let page = execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, plan)?;
    let rendered_rows =
        render_structural_sql_projection_page(row_layout, &prepared_projection, page)?;
    let row_count = u32::try_from(rendered_rows.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionTextRows::new(rendered_rows, row_count))
}

#[cfg(feature = "sql")]
fn project_structural_sql_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
) -> Result<Vec<Vec<Value>>, InternalError> {
    shape_structural_sql_projection_page(
        row_layout,
        prepared_projection,
        page,
        project_slot_rows_from_projection_structural,
        project_data_rows_from_projection_structural,
    )
}

#[cfg(feature = "sql")]
fn render_structural_sql_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
) -> Result<Vec<Vec<String>>, InternalError> {
    shape_structural_sql_projection_page(
        row_layout,
        prepared_projection,
        page,
        render_slot_rows_from_projection_structural,
        render_data_rows_from_projection_structural,
    )
}

#[cfg(feature = "sql")]
fn shape_structural_sql_projection_page<T>(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
    shape_slot_rows: impl FnOnce(
        &PreparedProjectionShape,
        Vec<RetainedSlotRow>,
    ) -> Result<Vec<Vec<T>>, InternalError>,
    shape_data_rows: impl FnOnce(
        RowLayout,
        &PreparedProjectionShape,
        &[DataRow],
    ) -> Result<Vec<Vec<T>>, InternalError>,
) -> Result<Vec<Vec<T>>, InternalError> {
    let payload = page.into_payload();

    // Phase 1: choose the structural payload once, then keep the row loop
    // inside the selected shaping path.
    match payload {
        StructuralCursorPagePayload::SlotRows(slot_rows) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_slot_rows_path_hit();

            shape_slot_rows(prepared_projection, slot_rows)
        }
        StructuralCursorPagePayload::DataRows(data_rows) => {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_path_hit();

            shape_data_rows(row_layout, prepared_projection, data_rows.as_slice())
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
    let mut emit_value = std::convert::identity;
    shape_slot_rows_from_projection_structural(prepared_projection, rows, &mut emit_value)
}

#[cfg(feature = "sql")]
fn render_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
) -> Result<Vec<Vec<String>>, InternalError> {
    let mut render_value = |value| render_sql_projection_value_text(&value);
    shape_slot_rows_from_projection_structural(prepared_projection, rows, &mut render_value)
}

#[cfg(feature = "sql")]
// Shape one retained slot-row page through either direct field-slot copies or
// the compiled projection evaluator while keeping one row loop.
fn shape_slot_rows_from_projection_structural<T>(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
    if let Some(field_slots) = prepared_projection.direct_projection_field_slots() {
        return shape_slot_rows_from_direct_field_slots(rows, field_slots, emit_value);
    }

    shape_dense_slot_rows_from_projection_structural(prepared_projection, rows, emit_value)
}

#[cfg(feature = "sql")]
// Shape one dense retained slot-row page through the prepared compiled
// structural projection evaluator without staging another row representation.
fn shape_dense_slot_rows_from_projection_structural<T>(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
    let projection = prepared_projection.projection();
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: evaluate each retained row once and emit final row elements
    // directly into the selected output representation.
    for row in &rows {
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
            projection,
            &mut read_slot,
            &mut |value| shaped.push(emit_value(value)),
        )?;
        shaped_rows.push(shaped);
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
// Shape one retained dense slot-row page through direct field-slot copies only.
fn shape_slot_rows_from_direct_field_slots<T>(
    rows: Vec<RetainedSlotRow>,
    field_slots: &[(String, usize)],
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: move direct slots into their final output representation
    // without staging intermediate row values.
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
            shaped.push(emit_value(value));
        }

        shaped_rows.push(shaped);
    }

    Ok(shaped_rows)
}

#[cfg(feature = "sql")]
fn project_data_rows_from_projection_structural(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
) -> Result<Vec<Vec<Value>>, InternalError> {
    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "perf-attribution"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "perf-attribution")))]
    let projected_slot_mask = &[];

    #[cfg(any(test, feature = "structural-read-metrics"))]
    record_sql_projection_data_rows_scalar_fallback_hit();
    let mut emit_value = std::convert::identity;
    shape_scalar_data_rows_from_projection_structural(
        compiled_fields,
        rows,
        row_layout,
        projected_slot_mask,
        &mut emit_value,
    )
}

#[cfg(feature = "sql")]
fn render_data_rows_from_projection_structural(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
) -> Result<Vec<Vec<String>>, InternalError> {
    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "perf-attribution"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "perf-attribution")))]
    let projected_slot_mask = &[];

    #[cfg(any(test, feature = "structural-read-metrics"))]
    record_sql_projection_data_rows_scalar_fallback_hit();
    let mut render_value = |value| render_sql_projection_value_text(&value);
    shape_scalar_data_rows_from_projection_structural(
        compiled_fields,
        rows,
        row_layout,
        projected_slot_mask,
        &mut render_value,
    )
}

#[cfg(feature = "sql")]
fn shape_scalar_data_rows_from_projection_structural<T>(
    compiled_fields: &[ScalarProjectionExpr],
    rows: &[DataRow],
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    #[cfg(not(any(test, feature = "structural-read-metrics")))]
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
                    #[cfg(any(test, feature = "structural-read-metrics"))]
                    record_sql_projection_data_rows_slot_access(
                        projected_slot_mask.get(slot).copied().unwrap_or(false),
                    );

                    row_fields.required_value_by_contract_cow(slot)
                },
            )?;
            shaped.push(emit_value(value.into_owned()));
        }
        shaped_rows.push(shaped);
    }

    Ok(shaped_rows)
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
/// with_sql_projection_materialization_metrics
///
/// Run one closure while collecting row-backed SQL projection metrics on the
/// current thread, then return the closure result plus the aggregated
/// snapshot.
///

#[cfg(feature = "structural-read-metrics")]
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

#[cfg(all(test, not(feature = "structural-read-metrics")))]
pub(in crate::db) fn with_sql_projection_materialization_metrics<T>(
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
                PreparedProjectionPlan, StructuralCursorPage,
                projection_eval_row_layout_for_materialize_tests,
            },
            query::plan::expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
        },
        value::Value,
    };

    fn direct_rank_projection_shape() -> PreparedProjectionShape {
        PreparedProjectionShape::from_test_parts(
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("rank")),
                alias: None,
            }]),
            PreparedProjectionPlan::Scalar(Vec::new()),
            false,
            Some(vec![("rank".to_string(), 1)]),
            vec![false, true, false, false],
        )
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
