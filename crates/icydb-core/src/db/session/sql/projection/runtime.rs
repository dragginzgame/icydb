//! Module: db::session::sql::projection::runtime
//! Responsibility: session-owned SQL projection row shaping over structural
//! executor pages.
//! Does not own: shared projection validation or scalar execution mechanics.
//! Boundary: consumes structural pages from the executor and performs the
//! SQL-specific value/text shaping above that boundary.

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
use crate::value::ValueEnum;
use crate::{
    db::{Db, query::plan::AccessPlannedQuery},
    error::InternalError,
    traits::CanisterKind,
};
use crate::{
    db::{
        access::{lower_index_prefix_specs, lower_index_range_specs},
        data::{CanonicalSlotReader, DataKey, DataRow, DataStore},
        executor::{
            EntityAuthority, StructuralCursorPage, StructuralCursorPagePayload,
            covering_projection_scan_direction, decode_covering_projection_component,
            decode_covering_projection_pairs,
            pipeline::execute_initial_scalar_retained_slot_page_for_canister,
            projection::{
                PreparedProjectionShape, ProjectionEvalError, ScalarProjectionExpr,
                eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
                prepare_projection_shape_from_plan,
                visit_prepared_projection_values_with_required_value_reader_cow,
            },
            reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
            terminal::{RetainedSlotRow, RowLayout},
        },
        query::plan::{
            CoveringExistingRowMode, CoveringProjectionOrder, CoveringReadField,
            CoveringReadFieldSource, PageSpec, covering_hybrid_projection_plan_from_fields,
            covering_read_execution_plan_from_fields,
        },
    },
    value::Value,
};
#[cfg(any(test, feature = "structural-read-metrics"))]
use std::cell::RefCell;
use std::{borrow::Cow, collections::BTreeMap};

///
/// SqlProjectionRows
///
/// Generic-free SQL projection row payload emitted by executor-owned structural
/// projection execution helpers.
/// Keeps SQL row materialization out of typed `ProjectionResponse<E>` so SQL
/// SQL execution can render value rows without reintroducing entity-specific ids.
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
/// SqlProjectionTextExecutorAttribution
///
/// SqlProjectionTextExecutorAttribution breaks the rendered SQL projection
/// executor path into structural prepare, scalar runtime, projection
/// materialization, and final row-payload packaging.
/// This lets perf harnesses separate fixed executor setup from the terminal
/// fast path without reopening the session or SQL layers above it.
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
) -> Result<SqlProjectionTextExecutorAttribution, InternalError>
where
    C: CanisterKind,
{
    let row_layout = authority.row_layout();

    // Phase 1: freeze the executor-owned structural projection contract.
    let (prepare_projection_local_instructions, prepared_projection) =
        measure_structural_result(|| {
            Ok::<PreparedProjectionShape, InternalError>(prepare_projection_shape_from_plan(
                authority.model(),
                &plan,
            ))
        });
    let prepared_projection = prepared_projection?;

    // Phase 2: execute the scalar runtime and preserve one structural slot-row
    // page for later SQL-specific shaping.
    let runtime_plan = plan.clone();
    let (scalar_runtime_local_instructions, page) = measure_structural_result(|| {
        execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, runtime_plan)
    });
    let page = page?;

    // Phase 3: project or preserve the structural page into rendered SQL rows.
    let (materialize_projection_local_instructions, rendered_rows) =
        measure_structural_result(|| {
            let projected =
                project_structural_sql_projection_page(row_layout, &prepared_projection, page)?;
            let projected = finalize_sql_projection_rows(&plan, projected)?;

            Ok::<Vec<Vec<String>>, InternalError>(render_projected_sql_rows_text(projected))
        });
    let rendered_rows = rendered_rows?;

    // Phase 4: package the rendered rows onto the stable SQL projection text
    // payload boundary.
    let (result_rows_local_instructions, row_count) = measure_structural_result(|| {
        Ok::<u32, InternalError>(u32::try_from(rendered_rows.len()).unwrap_or(u32::MAX))
    });
    let _row_count = row_count?;

    let total_local_instructions = prepare_projection_local_instructions
        .saturating_add(scalar_runtime_local_instructions)
        .saturating_add(materialize_projection_local_instructions)
        .saturating_add(result_rows_local_instructions);

    Ok(SqlProjectionTextExecutorAttribution {
        prepare_projection: prepare_projection_local_instructions,
        scalar_runtime: scalar_runtime_local_instructions,
        materialize_projection: materialize_projection_local_instructions,
        result_rows: result_rows_local_instructions,
        total: total_local_instructions,
    })
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
    if let Some(projected) =
        try_execute_covering_sql_projection_rows_for_canister(db, authority, &plan)?
    {
        let projected = finalize_sql_projection_rows(&plan, projected)?;
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    if let Some(projected) =
        try_execute_hybrid_covering_sql_projection_rows_for_canister(db, authority, &plan)?
    {
        let projected = finalize_sql_projection_rows(&plan, projected)?;
        let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

        return Ok(SqlProjectionRows::new(projected, row_count));
    }

    let row_layout = authority.row_layout();
    let prepared_projection = prepare_projection_shape_from_plan(authority.model(), &plan);

    // Execute the canonical scalar runtime and then shape the resulting
    // structural page into projected SQL values.
    let page =
        execute_initial_scalar_retained_slot_page_for_canister(db, debug, authority, plan.clone())?;
    let projected = project_structural_sql_projection_page(row_layout, &prepared_projection, page)?;
    let projected = finalize_sql_projection_rows(&plan, projected)?;
    let row_count = u32::try_from(projected.len()).unwrap_or(u32::MAX);

    Ok(SqlProjectionRows::new(projected, row_count))
}

#[cfg(feature = "sql")]
fn try_execute_covering_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 0: this SQL-side shortcut only owns index-backed covering scans.
    // Planner-proven full-scan / primary-key covering routes still flow
    // through the structural executor path, which already knows how to shape
    // those rows without pretending there are covering index components.
    if plan.access.as_index_prefix_path().is_none() && plan.access.as_index_range_path().is_none() {
        return Ok(None);
    }
    if plan.has_residual_predicate() {
        return Ok(None);
    }

    // Phase 1: admit only planner-proven pure covering routes that need no
    // row-backed fields in SQL projection materialization.
    let Some(covering) = covering_read_execution_plan_from_fields(
        authority.model().fields(),
        plan,
        authority.primary_key_name(),
        true,
    ) else {
        return Ok(None);
    };
    if covering
        .fields
        .iter()
        .any(|field| matches!(field.source, CoveringReadFieldSource::RowField))
    {
        return Ok(None);
    }

    let component_indices = covering_projection_component_indices(covering.fields.as_slice());
    let store = db.recovered_store(authority.store_path())?;
    let index_prefix_specs = lower_index_prefix_specs(authority.entity_tag(), &plan.access)?;
    let index_range_specs = lower_index_range_specs(authority.entity_tag(), &plan.access)?;

    // Phase 2: scan the covering component payloads under the same planner
    // order contract the executor already exposes in EXPLAIN EXECUTION.
    let scan_direction = covering_projection_scan_direction(covering.order_contract);
    let scan_limit = pure_covering_scan_limit(
        covering.order_contract,
        covering.existing_row_mode,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let raw_pairs = resolve_covering_projection_components_from_lowered_specs(
        authority.entity_tag(),
        index_prefix_specs.as_slice(),
        index_range_specs.as_slice(),
        scan_direction,
        scan_limit,
        component_indices.as_slice(),
        |index| db.recovered_store(index.store()),
    )?;

    // Phase 3: reuse the executor-owned covering decode contract so planner-
    // proven routes avoid row-store reads entirely while row-check-required
    // routes still preserve missing-row consistency rules.
    let Some(decoded_rows) = decode_covering_projection_pairs(
        raw_pairs,
        store,
        plan.scalar_plan().consistency,
        covering.existing_row_mode,
        Ok::<Vec<Value>, InternalError>,
    )?
    else {
        return Ok(None);
    };
    let mut projected_rows: Vec<(DataKey, Vec<Value>)> = decoded_rows
        .into_iter()
        .map(|(data_key, decoded_values)| {
            let decoded_components =
                covering_decoded_component_map(component_indices.as_slice(), decoded_values)?;
            let projected_row =
                project_covering_row(&data_key, covering.fields.as_slice(), &decoded_components)?;

            Ok::<(DataKey, Vec<Value>), InternalError>((data_key, projected_row))
        })
        .collect::<Result<Vec<_>, _>>()?;
    reorder_covering_projection_pairs(covering.order_contract, projected_rows.as_mut_slice());
    if !plan.scalar_plan().distinct
        && let Some(page) = plan.scalar_plan().page.as_ref()
    {
        apply_sql_projection_page_window(&mut projected_rows, page.offset, page.limit);
    }

    Ok(Some(
        projected_rows
            .into_iter()
            .map(|(_data_key, row)| row)
            .collect(),
    ))
}

#[cfg(feature = "sql")]
fn try_execute_hybrid_covering_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<Option<Vec<Vec<Value>>>, InternalError>
where
    C: CanisterKind,
{
    // Phase 0: hybrid SQL projection mixes index-backed covering components
    // with sparse row-backed field reads, so it only applies to genuine
    // secondary-index access paths.
    if plan.access.as_index_prefix_path().is_none() && plan.access.as_index_range_path().is_none() {
        return Ok(None);
    }

    // Phase 1: admit only the planner-owned direct projection shapes that mix
    // covering-backed fields with row-backed sparse reads over one index path.
    let Some(hybrid) = covering_hybrid_projection_plan_from_fields(
        authority.model().fields(),
        plan,
        authority.primary_key_name(),
    ) else {
        return Ok(None);
    };

    let component_indices = covering_projection_component_indices(hybrid.fields.as_slice());
    let row_field_slots = hybrid_projection_row_field_slots(hybrid.fields.as_slice());
    let store = db.recovered_store(authority.store_path())?;
    let index_prefix_specs = lower_index_prefix_specs(authority.entity_tag(), &plan.access)?;
    let index_range_specs = lower_index_range_specs(authority.entity_tag(), &plan.access)?;

    // Phase 2: read the covering-backed component payloads in the order
    // implied by the planner-owned covering order contract.
    let scan_direction = covering_projection_scan_direction(hybrid.order_contract);
    let scan_limit = hybrid_covering_scan_limit(
        hybrid.order_contract,
        plan.scalar_plan().distinct,
        plan.scalar_plan().page.as_ref(),
    );
    let raw_pairs = resolve_covering_projection_components_from_lowered_specs(
        authority.entity_tag(),
        index_prefix_specs.as_slice(),
        index_range_specs.as_slice(),
        scan_direction,
        scan_limit,
        component_indices.as_slice(),
        |index| db.recovered_store(index.store()),
    )?;

    // Phase 3: assemble final projected rows by mixing decoded covering
    // values with sparse row-backed field reads for uncovered slots.
    #[cfg(any(test, feature = "structural-read-metrics"))]
    record_sql_projection_hybrid_covering_path_hit();
    let mut projected_rows = store.with_data(|data_store| {
        let mut projected_rows = Vec::with_capacity(raw_pairs.len());

        for (data_key, _existence_witness, components) in raw_pairs {
            let decoded_components =
                decode_hybrid_covering_components(component_indices.as_slice(), components)?;
            let sparse_row_fields = read_hybrid_projection_row_fields_from_store(
                authority.row_layout(),
                data_store,
                &data_key,
                row_field_slots.as_slice(),
            )?;
            let Some(sparse_row_fields) = sparse_row_fields else {
                continue;
            };
            let projected_row = project_hybrid_covering_row(
                &data_key,
                hybrid.fields.as_slice(),
                &decoded_components,
                &sparse_row_fields,
            )?;

            projected_rows.push((data_key, projected_row));
        }

        Ok::<Vec<(DataKey, Vec<Value>)>, InternalError>(projected_rows)
    })?;
    reorder_covering_projection_pairs(hybrid.order_contract, projected_rows.as_mut_slice());
    if !plan.scalar_plan().distinct
        && let Some(page) = plan.scalar_plan().page.as_ref()
    {
        apply_sql_projection_page_window(&mut projected_rows, page.offset, page.limit);
    }

    Ok(Some(
        projected_rows
            .into_iter()
            .map(|(_data_key, row)| row)
            .collect(),
    ))
}

#[cfg(feature = "sql")]
fn covering_projection_component_indices(fields: &[CoveringReadField]) -> Vec<usize> {
    let mut component_indices = Vec::new();

    for field in fields {
        let CoveringReadFieldSource::IndexComponent { component_index } = &field.source else {
            continue;
        };
        if component_indices.contains(component_index) {
            continue;
        }

        component_indices.push(*component_index);
    }

    component_indices
}

#[cfg(feature = "sql")]
fn pure_covering_scan_limit(
    order_contract: CoveringProjectionOrder,
    existing_row_mode: CoveringExistingRowMode,
    distinct: bool,
    page: Option<&PageSpec>,
) -> usize {
    if distinct {
        return usize::MAX;
    }
    if existing_row_mode != CoveringExistingRowMode::ProvenByPlanner {
        return usize::MAX;
    }

    let Some(page) = page else {
        return usize::MAX;
    };
    if !matches!(order_contract, CoveringProjectionOrder::IndexOrder(_)) {
        return usize::MAX;
    }
    let Some(limit) = page.limit else {
        return usize::MAX;
    };

    page.offset
        .saturating_add(limit)
        .max(1)
        .try_into()
        .unwrap_or(usize::MAX)
}

#[cfg(feature = "sql")]
fn hybrid_covering_scan_limit(
    order_contract: CoveringProjectionOrder,
    distinct: bool,
    page: Option<&PageSpec>,
) -> usize {
    if distinct {
        return usize::MAX;
    }

    let Some(page) = page else {
        return usize::MAX;
    };
    if !matches!(order_contract, CoveringProjectionOrder::IndexOrder(_)) {
        return usize::MAX;
    }
    let Some(limit) = page.limit else {
        return usize::MAX;
    };

    page.offset
        .saturating_add(limit)
        .max(1)
        .try_into()
        .unwrap_or(usize::MAX)
}

#[cfg(feature = "sql")]
fn covering_decoded_component_map(
    component_indices: &[usize],
    decoded_values: Vec<Value>,
) -> Result<BTreeMap<usize, Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant(
            "covering SQL projection component decode arity mismatch",
        ));
    }

    Ok(component_indices
        .iter()
        .copied()
        .zip(decoded_values)
        .collect())
}

#[cfg(feature = "sql")]
fn hybrid_projection_row_field_slots(fields: &[CoveringReadField]) -> Vec<usize> {
    let mut row_field_slots = Vec::new();

    for field in fields {
        if !matches!(field.source, CoveringReadFieldSource::RowField) {
            continue;
        }
        if row_field_slots.contains(&field.field_slot.index()) {
            continue;
        }

        row_field_slots.push(field.field_slot.index());
    }

    row_field_slots
}

#[cfg(feature = "sql")]
fn decode_hybrid_covering_components(
    component_indices: &[usize],
    components: Vec<Vec<u8>>,
) -> Result<BTreeMap<usize, Value>, InternalError> {
    let mut decoded = BTreeMap::new();

    for (component_index, component) in component_indices.iter().copied().zip(components) {
        let Some(value) = decode_covering_projection_component(component.as_slice())? else {
            return Err(InternalError::query_executor_invariant(
                "hybrid SQL projection expected one decodable covering component payload",
            ));
        };
        decoded.insert(component_index, value);
    }

    Ok(decoded)
}

#[cfg(feature = "sql")]
fn project_covering_row(
    data_key: &crate::db::data::DataKey,
    fields: &[CoveringReadField],
    decoded_components: &BTreeMap<usize, Value>,
) -> Result<Vec<Value>, InternalError> {
    let mut projected = Vec::with_capacity(fields.len());

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index } => decoded_components
                .get(component_index)
                .cloned()
                .ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "covering SQL projection missing decoded covering component",
                    )
                })?,
            CoveringReadFieldSource::PrimaryKey => data_key.storage_key().as_value(),
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant(
                    "pure covering SQL projection unexpectedly reached row-backed field source",
                ));
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

#[cfg(feature = "sql")]
fn read_hybrid_projection_row_fields_from_store(
    row_layout: RowLayout,
    data_store: &DataStore,
    data_key: &DataKey,
    row_field_slots: &[usize],
) -> Result<Option<BTreeMap<usize, Value>>, InternalError> {
    // Phase 1: empty row-backed hybrids stay on the covering-only path.
    if row_field_slots.is_empty() {
        return Ok(Some(BTreeMap::new()));
    }

    // Phase 2: fetch the persisted row once. The store boundary still returns
    // one owned `RawRow`, so hybrid selective reads reduce decode work here
    // but do not yet avoid the full row fetch itself.
    let raw_key = data_key.to_raw()?;

    // Phase 3: request the caller-declared slot list through the data-layer
    // selective read boundary. The storage layer still chooses the narrower
    // one-field decode path internally when possible.
    let selective = data_store.read_slot_values(
        &raw_key,
        row_layout.contract(),
        data_key.storage_key(),
        row_field_slots,
    )?;
    let Some(decoded) = selective.into_present() else {
        return Ok(None);
    };
    // Phase 4: rebuild the field-slot map expected by the hybrid projection
    // row shaper from the compact storage-owned selective read result.
    let mut row_fields = BTreeMap::new();

    for (slot, value) in row_field_slots.iter().copied().zip(decoded) {
        let Some(value) = value else {
            return Err(InternalError::query_executor_invariant(
                "hybrid SQL projection sparse row decode expected declared direct field value",
            ));
        };
        row_fields.insert(slot, value);
    }

    Ok(Some(row_fields))
}

#[cfg(feature = "sql")]
fn project_hybrid_covering_row(
    data_key: &crate::db::data::DataKey,
    fields: &[CoveringReadField],
    decoded_components: &BTreeMap<usize, Value>,
    row_fields: &BTreeMap<usize, Value>,
) -> Result<Vec<Value>, InternalError> {
    let mut projected = Vec::with_capacity(fields.len());

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index } => {
                #[cfg(any(test, feature = "structural-read-metrics"))]
                record_sql_projection_hybrid_covering_index_field_access();

                decoded_components
                    .get(component_index)
                    .cloned()
                    .ok_or_else(|| {
                        InternalError::query_executor_invariant(
                            "hybrid SQL projection missing decoded covering component",
                        )
                    })?
            }
            CoveringReadFieldSource::PrimaryKey => data_key.storage_key().as_value(),
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                #[cfg(any(test, feature = "structural-read-metrics"))]
                record_sql_projection_hybrid_covering_row_field_access();

                row_fields
                    .get(&field.field_slot.index())
                    .cloned()
                    .ok_or_else(|| {
                        InternalError::query_executor_invariant(
                            "hybrid SQL projection missing sparse row-backed field value",
                        )
                    })?
            }
        };
        projected.push(value);
    }

    Ok(projected)
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

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
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

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
fn render_projected_sql_rows_text(rows: Vec<Vec<Value>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| render_sql_projection_value_text(&value))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
fn render_sql_projection_blob(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(sql_projection_hex_encode(bytes).as_str());

    rendered
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
fn render_sql_projection_duration(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");

    rendered
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
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

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
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

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
fn sql_projection_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
}

#[cfg(all(feature = "sql", feature = "perf-attribution"))]
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
// Shape one retained slot-row page through either direct field-slot copies or
// the compiled projection evaluator while keeping one row loop.
fn shape_slot_rows_from_projection_structural<T>(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
    if let Some(field_slots) = prepared_projection.retained_slot_direct_projection_field_slots() {
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
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        let mut emit_value = std::convert::identity;

        return shape_data_rows_from_direct_field_slots(
            rows,
            row_layout,
            field_slots,
            &mut emit_value,
        );
    }

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
// Shape one raw data-row page through direct field-slot copies only.
fn shape_data_rows_from_direct_field_slots<T>(
    rows: &[DataRow],
    row_layout: RowLayout,
    field_slots: &[(String, usize)],
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
    let mut shaped_rows = Vec::with_capacity(rows.len());

    // Phase 1: open each structural row once, then decode only the declared
    // direct field slots into the final output representation.
    for (data_key, raw_row) in rows {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut shaped = Vec::with_capacity(field_slots.len());
        for (_field_name, slot) in field_slots {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_data_rows_slot_access(true);

            let value = row_fields.required_value_by_contract(*slot)?;
            shaped.push(emit_value(value));
        }
        shaped_rows.push(shaped);
    }

    Ok(shaped_rows)
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

#[cfg(feature = "sql")]
fn finalize_sql_projection_rows(
    plan: &AccessPlannedQuery,
    rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if !plan.scalar_plan().distinct {
        return Ok(rows);
    }

    // Phase 1: apply DISTINCT at the outward projected-row boundary so
    // deduplication is defined over final SQL values rather than structural rows.
    let mut distinct_rows = crate::db::executor::group::GroupKeySet::new();
    let mut deduped_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if distinct_rows
            .insert_value(&Value::List(row.clone()))
            .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)?
        {
            deduped_rows.push(row);
        }
    }

    // Phase 2: apply LIMIT/OFFSET only after projected-row deduplication so
    // DISTINCT paging matches SQL semantics.
    if let Some(page) = plan.scalar_plan().page.as_ref() {
        apply_sql_projection_page_window(&mut deduped_rows, page.offset, page.limit);
    }

    Ok(deduped_rows)
}

#[cfg(feature = "sql")]
fn apply_sql_projection_page_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let offset = usize::min(rows.len(), usize::try_from(offset).unwrap_or(usize::MAX));
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        if rows.len() > limit {
            rows.truncate(limit);
        }
    }
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
    pub hybrid_covering_path_hits: u64,
    pub hybrid_covering_index_field_accesses: u64,
    pub hybrid_covering_row_field_accesses: u64,
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
fn record_sql_projection_hybrid_covering_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_path_hits = metrics.hybrid_covering_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_hybrid_covering_index_field_access() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_index_field_accesses = metrics
            .hybrid_covering_index_field_accesses
            .saturating_add(1);
    });
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_sql_projection_hybrid_covering_row_field_access() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_row_field_accesses =
            metrics.hybrid_covering_row_field_accesses.saturating_add(1);
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
pub(crate) fn with_sql_projection_materialization_metrics<T>(
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
pub(crate) fn with_sql_projection_materialization_metrics<T>(
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
                projection_eval_data_row_for_materialize_tests,
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
            Some(vec![("rank".to_string(), 1)]),
            vec![false, true, false, false],
        )
    }

    fn repeated_direct_rank_projection_shape() -> PreparedProjectionShape {
        PreparedProjectionShape::from_test_parts(
            ProjectionSpec::from_fields_for_test(vec![
                ProjectionField::Scalar {
                    expr: Expr::Field(FieldId::new("rank")),
                    alias: None,
                },
                ProjectionField::Scalar {
                    expr: Expr::Field(FieldId::new("rank")),
                    alias: None,
                },
            ]),
            PreparedProjectionPlan::Scalar(Vec::new()),
            false,
            None,
            Some(vec![("rank".to_string(), 1), ("rank".to_string(), 1)]),
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

    #[test]
    fn sql_projection_materialization_prefers_direct_data_row_field_copies() {
        let row_layout = projection_eval_row_layout_for_materialize_tests();
        let page = StructuralCursorPage::new(
            vec![projection_eval_data_row_for_materialize_tests(41, 19, true)],
            None,
        );
        let prepared_projection = direct_rank_projection_shape();

        let (payload, metrics) = with_sql_projection_materialization_metrics(|| {
            project_structural_sql_projection_page(row_layout, &prepared_projection, page)
        });
        let payload = payload.expect("data-row SQL projection materialization should succeed");

        assert_eq!(payload, vec![vec![Value::Int(19)]]);

        assert_eq!(
            metrics.data_rows_path_hits, 1,
            "data-row projection should stay on the raw-row path",
        );
        assert_eq!(
            metrics.data_rows_scalar_fallback_hits, 0,
            "direct data-row field copies should avoid the scalar fallback path",
        );
        assert_eq!(
            metrics.data_rows_projected_slot_accesses, 1,
            "direct data-row field copies should decode only the declared projected slot",
        );
        assert_eq!(
            metrics.data_rows_non_projected_slot_accesses, 0,
            "direct data-row field copies should avoid unrelated slot reads",
        );
    }

    #[test]
    fn sql_projection_materialization_prefers_direct_data_row_field_copies_for_repeated_fields() {
        let row_layout = projection_eval_row_layout_for_materialize_tests();
        let page = StructuralCursorPage::new(
            vec![projection_eval_data_row_for_materialize_tests(41, 19, true)],
            None,
        );
        let prepared_projection = repeated_direct_rank_projection_shape();

        let (payload, metrics) = with_sql_projection_materialization_metrics(|| {
            project_structural_sql_projection_page(row_layout, &prepared_projection, page)
        });
        let payload =
            payload.expect("repeated data-row SQL projection materialization should succeed");

        assert_eq!(payload, vec![vec![Value::Int(19), Value::Int(19)]]);

        assert_eq!(
            metrics.data_rows_path_hits, 1,
            "repeated data-row projection should stay on the raw-row path",
        );
        assert_eq!(
            metrics.data_rows_scalar_fallback_hits, 0,
            "repeated direct data-row fields should avoid the scalar fallback path",
        );
        assert_eq!(
            metrics.data_rows_projected_slot_accesses, 2,
            "repeated direct data-row fields should read only the repeated projected slot",
        );
        assert_eq!(
            metrics.data_rows_non_projected_slot_accesses, 0,
            "repeated direct data-row fields should avoid unrelated slot reads",
        );
    }
}
