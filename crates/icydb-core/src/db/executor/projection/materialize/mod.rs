//! Module: db::executor::projection::materialize
//! Responsibility: shared projection materialization helpers that are used by both structural and typed row flows.
//! Does not own: adapter DTO shaping or expression evaluation semantics.
//! Boundary: keeps validation, grouped projection materialization, and shared row-walk helpers behind one executor-owned boundary.

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
            StructuralCursorPage,
            group::{GroupKeySet, KeyCanonicalError},
            terminal::{RetainedSlotRow, RowLayout},
        },
        query::plan::{AccessPlannedQuery, PageSpec, expr::ProjectionSpec},
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};
use std::borrow::Cow;

#[cfg(test)]
use crate::db::executor::projection::eval::eval_scalar_projection_expr_with_value_reader;
use crate::db::executor::projection::eval::{
    ProjectionEvalError, ScalarProjectionExpr,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    eval_scalar_projection_expr_with_value_ref_reader,
};
#[cfg(test)]
use crate::db::query::plan::expr::compile_scalar_projection_expr;

///
/// PreparedProjectionPlan
///
/// PreparedProjectionPlan is the executor-owned projection materialization plan
/// shared by typed row projection, slot-row validation, and higher-level
/// structural row shaping. Production paths consume only planner-compiled
/// scalar programs so projection execution no longer carries a generic
/// field-resolve fallback.
///

#[derive(Debug)]
pub(in crate::db) enum PreparedProjectionPlan {
    Scalar(Vec<ScalarProjectionExpr>),
}

///
/// PreparedProjectionShape
///
/// PreparedProjectionShape is the executor-owned prepared projection contract
/// shared by slot-row validation and higher-level structural row shaping.
/// It freezes the canonical projection semantic spec plus the derived direct
/// slot layouts needed by compiled scalar projection flow.
///

#[derive(Debug)]
pub(in crate::db) struct PreparedProjectionShape {
    projection: ProjectionSpec,
    prepared: PreparedProjectionPlan,
    projection_is_model_identity: bool,
    retained_slot_direct_projection_field_slots: Option<Vec<(String, usize)>>,
    data_row_direct_projection_field_slots: Option<Vec<(String, usize)>>,
    #[cfg(any(test, feature = "diagnostics"))]
    projected_slot_mask: Vec<bool>,
}

impl PreparedProjectionShape {
    #[must_use]
    pub(in crate::db) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    #[must_use]
    pub(in crate::db) const fn prepared(&self) -> &PreparedProjectionPlan {
        &self.prepared
    }

    #[must_use]
    pub(in crate::db) const fn scalar_projection_exprs(&self) -> &[ScalarProjectionExpr] {
        let PreparedProjectionPlan::Scalar(compiled_fields) = self.prepared();

        compiled_fields.as_slice()
    }

    #[must_use]
    pub(in crate::db::executor) const fn projection_is_model_identity(&self) -> bool {
        self.projection_is_model_identity
    }

    #[must_use]
    pub(in crate::db) fn retained_slot_direct_projection_field_slots(
        &self,
    ) -> Option<&[(String, usize)]> {
        self.retained_slot_direct_projection_field_slots.as_deref()
    }

    #[must_use]
    pub(in crate::db) fn data_row_direct_projection_field_slots(
        &self,
    ) -> Option<&[(String, usize)]> {
        self.data_row_direct_projection_field_slots.as_deref()
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[must_use]
    pub(in crate::db) const fn projected_slot_mask(&self) -> &[bool] {
        self.projected_slot_mask.as_slice()
    }

    /// Build one projection shape directly from test-owned prepared parts.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn from_test_parts(
        projection: ProjectionSpec,
        prepared: PreparedProjectionPlan,
        projection_is_model_identity: bool,
        retained_slot_direct_projection_field_slots: Option<Vec<(String, usize)>>,
        data_row_direct_projection_field_slots: Option<Vec<(String, usize)>>,
        projected_slot_mask: Vec<bool>,
    ) -> Self {
        Self {
            projection,
            prepared,
            projection_is_model_identity,
            retained_slot_direct_projection_field_slots,
            data_row_direct_projection_field_slots,
            projected_slot_mask,
        }
    }
}

///
/// PreparedSlotProjectionValidation
///
/// PreparedSlotProjectionValidation is the executor-owned slot-row projection
/// validation bundle reused by page kernels and retained-slot row shaping.
/// It freezes the canonical projection semantic spec plus the compiled
/// validation/evaluation shape so execute no longer recomputes that plan at
/// each slot-row validation boundary.
///

pub(in crate::db::executor) type PreparedSlotProjectionValidation = PreparedProjectionShape;

///
/// ProjectionMaterializationMetricsRecorder
///
/// Executor callback bundle for structural projection materialization counters.
/// This keeps projection row shaping in executor ownership while allowing
/// adapter layers to own their diagnostic counter storage and labels.
///

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct ProjectionMaterializationMetricsRecorder {
    slot_rows_path_hit: fn(),
    data_rows_path_hit: fn(),
    data_rows_scalar_fallback_hit: fn(),
    data_rows_slot_access: fn(bool),
    distinct_candidate_row: fn(),
    distinct_bounded_stop: fn(),
}

#[cfg(any(test, feature = "diagnostics"))]
impl ProjectionMaterializationMetricsRecorder {
    /// Construct one observer from adapter-owned materialization counters.
    pub(in crate::db) const fn new(
        slot_rows_path_hit: fn(),
        data_rows_path_hit: fn(),
        data_rows_scalar_fallback_hit: fn(),
        data_rows_slot_access: fn(bool),
        distinct_candidate_row: fn(),
        distinct_bounded_stop: fn(),
    ) -> Self {
        Self {
            slot_rows_path_hit,
            data_rows_path_hit,
            data_rows_scalar_fallback_hit,
            data_rows_slot_access,
            distinct_candidate_row,
            distinct_bounded_stop,
        }
    }

    fn record_slot_rows_path_hit(self) {
        (self.slot_rows_path_hit)();
    }

    fn record_data_rows_path_hit(self) {
        (self.data_rows_path_hit)();
    }

    fn record_data_rows_scalar_fallback_hit(self) {
        (self.data_rows_scalar_fallback_hit)();
    }

    fn record_data_rows_slot_access(self, projected_slot: bool) {
        (self.data_rows_slot_access)(projected_slot);
    }

    fn record_distinct_candidate_row(self) {
        (self.distinct_candidate_row)();
    }

    fn record_distinct_bounded_stop(self) {
        (self.distinct_bounded_stop)();
    }
}

///
/// ProjectionMaterializationMetricsRecorder
///
/// Zero-sized no-op recorder used when materialization diagnostics are not
/// compiled. Keeping the type available avoids cfg-heavy executor signatures.
///

#[cfg(not(any(test, feature = "diagnostics")))]
#[derive(Clone, Copy)]
pub(in crate::db) struct ProjectionMaterializationMetricsRecorder;

#[cfg(not(any(test, feature = "diagnostics")))]
impl ProjectionMaterializationMetricsRecorder {
    /// Construct one no-op structural projection materialization observer.
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    const fn record_slot_rows_path_hit(self) {
        let _ = self;
    }

    const fn record_data_rows_path_hit(self) {
        let _ = self;
    }

    const fn record_data_rows_scalar_fallback_hit(self) {
        let _ = self;
    }

    const fn record_data_rows_slot_access(self, projected_slot: bool) {
        let _ = (self, projected_slot);
    }

    const fn record_distinct_candidate_row(self) {
        let _ = self;
    }

    const fn record_distinct_bounded_stop(self) {
        let _ = self;
    }
}

///
/// MaterializedProjectionRows
///
/// MaterializedProjectionRows is the executor-owned transport wrapper for one
/// structurally projected page. It keeps nested value-row storage an executor
/// implementation detail until an adapter consumes the page for DTO
/// shaping.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct MaterializedProjectionRows(Vec<Vec<Value>>);

#[cfg(feature = "sql")]
impl MaterializedProjectionRows {
    const fn new(rows: Vec<Vec<Value>>) -> Self {
        Self(rows)
    }

    #[must_use]
    pub(in crate::db) fn into_value_rows(self) -> Vec<Vec<Value>> {
        self.0
    }
}

///
/// ProjectionValidationRow
///
/// ProjectionValidationRow is the deliberately narrow row-read contract for
/// shared projection validation only.
/// This abstraction exists to keep retained-slot layout and row payload choice
/// as executor-local representation decisions rather than semantic
/// requirements of the validator itself.
/// It is intentionally not a generic executor row API for predicates,
/// ordering, projection materialization, or adapter rendering.
///

pub(in crate::db::executor) trait ProjectionValidationRow {
    /// Borrow one slot value for projection-expression validation.
    #[must_use]
    fn projection_validation_slot_value(&self, slot: usize) -> Option<&Value>;
}

/// Build one executor-owned prepared projection shape from planner-frozen metadata.
#[must_use]
pub(in crate::db) fn prepare_projection_shape_from_plan(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> PreparedProjectionShape {
    let projection = plan.frozen_projection_spec().clone();
    let prepared = PreparedProjectionPlan::Scalar(
        plan.scalar_projection_plan()
            .expect(
                "scalar execution projection shapes must carry one planner-compiled scalar program",
            )
            .to_vec(),
    );
    let retained_slot_direct_projection_field_slots =
        retained_slot_direct_projection_field_slots_from_projection(
            &projection,
            plan.frozen_direct_projection_slots(),
        );
    let data_row_direct_projection_field_slots =
        data_row_direct_projection_field_slots_from_projection(model, &projection);
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask =
        projected_slot_mask_from_slots(model.fields().len(), plan.projected_slot_mask());

    PreparedProjectionShape {
        projection,
        prepared,
        projection_is_model_identity: plan.projection_is_model_identity(),
        retained_slot_direct_projection_field_slots,
        data_row_direct_projection_field_slots,
        #[cfg(any(test, feature = "diagnostics"))]
        projected_slot_mask,
    }
}

/// Validate projection expressions against one row-domain that can expose
/// borrowed slot values by field slot.
pub(in crate::db::executor) fn validate_prepared_projection_row(
    prepared_validation: &PreparedSlotProjectionValidation,
    row: &impl ProjectionValidationRow,
) -> Result<(), InternalError> {
    if prepared_validation.projection_is_model_identity() {
        return Ok(());
    }

    let PreparedProjectionPlan::Scalar(compiled_fields) = prepared_validation.prepared();
    for compiled in compiled_fields {
        let mut read_slot = |slot| row.projection_validation_slot_value(slot);
        eval_scalar_projection_expr_with_value_ref_reader(compiled, &mut read_slot)
            .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)?;
    }

    Ok(())
}

fn retained_slot_direct_projection_field_slots_from_projection(
    projection: &ProjectionSpec,
    direct_projection_slots: Option<&[usize]>,
) -> Option<Vec<(String, usize)>> {
    let direct_projection_slots = direct_projection_slots?;
    let mut field_slots = Vec::with_capacity(direct_projection_slots.len());

    for (field, slot) in projection
        .fields()
        .zip(direct_projection_slots.iter().copied())
    {
        let field_name = field.direct_field_name()?;
        field_slots.push((field_name.to_string(), slot));
    }

    Some(field_slots)
}

fn data_row_direct_projection_field_slots_from_projection(
    model: &EntityModel,
    projection: &ProjectionSpec,
) -> Option<Vec<(String, usize)>> {
    let mut field_slots = Vec::with_capacity(projection.len());

    // Phase 1: preserve canonical output order exactly as declared, but allow
    // duplicate source slots because raw-row decoding can borrow the same slot
    // repeatedly without the retained-slot `take()` constraint.
    for field in projection.fields() {
        let field_name = field.direct_field_name()?;
        let slot = model.resolve_field_slot(field_name)?;
        field_slots.push((field_name.to_string(), slot));
    }

    Some(field_slots)
}

#[cfg(feature = "sql")]
pub(in crate::db) fn project_structural_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<MaterializedProjectionRows, InternalError> {
    shape_structural_projection_page(
        row_layout,
        prepared_projection,
        page,
        metrics,
        project_slot_rows_from_projection_structural,
        project_data_rows_from_projection_structural,
    )
    .map(MaterializedProjectionRows::new)
}

#[cfg(feature = "sql")]
pub(in crate::db) fn project_distinct_structural_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    plan: &AccessPlannedQuery,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<MaterializedProjectionRows, InternalError> {
    let window = ProjectionDistinctWindow::from_page(plan.scalar_plan().page.as_ref());

    // Phase 1: choose the structural payload once, then run a bounded
    // DISTINCT projector over that shape. The projector owns the
    // post-projection window so it can stop when LIMIT has been satisfied.
    page.consume_projection_rows(
        |slot_rows| {
            metrics.record_slot_rows_path_hit();

            project_distinct_slot_rows_from_projection_structural(
                prepared_projection,
                slot_rows,
                window,
                metrics,
            )
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            project_distinct_data_rows_from_projection_structural(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
                window,
                metrics,
            )
        },
    )
    .map(MaterializedProjectionRows::new)
}

#[cfg(feature = "sql")]
fn project_distinct_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    window: ProjectionDistinctWindow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = prepared_projection.retained_slot_direct_projection_field_slots() {
        return project_distinct_slot_rows_from_direct_field_slots(
            rows,
            field_slots,
            window,
            metrics,
        );
    }

    project_distinct_dense_slot_rows_from_projection_structural(
        prepared_projection,
        rows,
        window,
        metrics,
    )
}

#[cfg(feature = "sql")]
fn project_distinct_dense_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    window: ProjectionDistinctWindow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let projection = prepared_projection.projection();

    collect_bounded_distinct_projected_rows(window, rows.iter(), metrics, |row| {
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
    })
}

#[cfg(feature = "sql")]
fn project_distinct_slot_rows_from_direct_field_slots(
    rows: Vec<RetainedSlotRow>,
    field_slots: &[(String, usize)],
    window: ProjectionDistinctWindow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    collect_bounded_distinct_projected_rows(window, rows, metrics, |mut row| {
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
    })
}

#[cfg(feature = "sql")]
fn project_distinct_data_rows_from_projection_structural(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    rows: &[DataRow],
    window: ProjectionDistinctWindow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        return project_distinct_data_rows_from_direct_field_slots(
            rows,
            row_layout,
            field_slots,
            window,
            metrics,
        );
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "diagnostics")))]
    let projected_slot_mask = &[];

    metrics.record_data_rows_scalar_fallback_hit();
    project_distinct_scalar_data_rows_from_projection_structural(
        compiled_fields,
        rows,
        row_layout,
        projected_slot_mask,
        window,
        metrics,
    )
}

#[cfg(feature = "sql")]
fn project_distinct_data_rows_from_direct_field_slots(
    rows: &[DataRow],
    row_layout: RowLayout,
    field_slots: &[(String, usize)],
    window: ProjectionDistinctWindow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    collect_bounded_distinct_projected_rows(window, rows.iter(), metrics, |(data_key, raw_row)| {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut shaped = Vec::with_capacity(field_slots.len());
        for (_field_name, slot) in field_slots {
            metrics.record_data_rows_slot_access(true);

            shaped.push(row_fields.required_value_by_contract(*slot)?);
        }

        Ok(shaped)
    })
}

#[cfg(feature = "sql")]
fn project_distinct_scalar_data_rows_from_projection_structural(
    compiled_fields: &[ScalarProjectionExpr],
    rows: &[DataRow],
    row_layout: RowLayout,
    projected_slot_mask: &[bool],
    window: ProjectionDistinctWindow,
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = projected_slot_mask;

    collect_bounded_distinct_projected_rows(window, rows.iter(), metrics, |(data_key, raw_row)| {
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
    })
}

///
/// ProjectionDistinctWindow
///
/// ProjectionDistinctWindow carries projected-row DISTINCT paging after
/// structural projection. It lets the row projector skip OFFSET rows and stop
/// at the LIMIT horizon while preserving the existing projected-row DISTINCT
/// equality contract.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProjectionDistinctWindow {
    offset: usize,
    limit: Option<usize>,
}

#[cfg(feature = "sql")]
impl ProjectionDistinctWindow {
    fn from_page(page: Option<&PageSpec>) -> Self {
        Self {
            offset: page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX)),
            limit: page.and_then(|page| {
                page.limit
                    .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
            }),
        }
    }

    const fn output_is_empty(self) -> bool {
        matches!(self.limit, Some(0))
    }

    fn output_capacity(self) -> usize {
        self.limit.unwrap_or(0)
    }

    fn stop_after_distinct_count(self) -> Option<usize> {
        self.limit.map(|limit| self.offset.saturating_add(limit))
    }
}

///
/// DistinctProjectionAccumulator
///
/// DistinctProjectionAccumulator owns the projected-row DISTINCT set and
/// post-DISTINCT window state for one materialization pass. Callers feed rows
/// in final execution order and stop when `consider_row` returns false.
///

#[cfg(feature = "sql")]
struct DistinctProjectionAccumulator {
    distinct_rows: GroupKeySet,
    output_rows: Vec<Vec<Value>>,
    window: ProjectionDistinctWindow,
    distinct_seen: usize,
}

#[cfg(feature = "sql")]
impl DistinctProjectionAccumulator {
    fn new(window: ProjectionDistinctWindow) -> Self {
        Self {
            distinct_rows: GroupKeySet::new(),
            output_rows: Vec::with_capacity(window.output_capacity()),
            window,
            distinct_seen: 0,
        }
    }

    fn consider_row(
        &mut self,
        row: Vec<Value>,
        metrics: ProjectionMaterializationMetricsRecorder,
    ) -> Result<bool, InternalError> {
        let inserted = self
            .distinct_rows
            .insert_value(&Value::List(row.clone()))
            .map_err(KeyCanonicalError::into_internal_error)?;
        if !inserted {
            return Ok(true);
        }

        let distinct_index = self.distinct_seen;
        self.distinct_seen = self.distinct_seen.saturating_add(1);
        if distinct_index >= self.window.offset {
            self.output_rows.push(row);
        }

        let Some(stop_after) = self.window.stop_after_distinct_count() else {
            return Ok(true);
        };
        if self.distinct_seen >= stop_after {
            metrics.record_distinct_bounded_stop();

            return Ok(false);
        }

        Ok(true)
    }

    fn into_rows(self) -> Vec<Vec<Value>> {
        self.output_rows
    }
}

#[cfg(feature = "sql")]
fn collect_bounded_distinct_projected_rows<I>(
    window: ProjectionDistinctWindow,
    rows: impl IntoIterator<Item = I>,
    metrics: ProjectionMaterializationMetricsRecorder,
    mut project_row: impl FnMut(I) -> Result<Vec<Value>, InternalError>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if window.output_is_empty() {
        return Ok(Vec::new());
    }

    let mut accumulator = DistinctProjectionAccumulator::new(window);

    // Phase 1: project rows in final execution order and feed each projected
    // tuple into the DISTINCT/window accumulator. A bounded LIMIT can stop the
    // projector before later structural rows are decoded.
    for row in rows {
        let projected = project_row(row)?;
        metrics.record_distinct_candidate_row();

        if !accumulator.consider_row(projected, metrics)? {
            break;
        }
    }

    Ok(accumulator.into_rows())
}

#[cfg(feature = "sql")]
fn shape_structural_projection_page<T>(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    page: StructuralCursorPage,
    metrics: ProjectionMaterializationMetricsRecorder,
    shape_slot_rows: impl FnOnce(
        &PreparedProjectionShape,
        Vec<RetainedSlotRow>,
    ) -> Result<Vec<Vec<T>>, InternalError>,
    shape_data_rows: impl FnOnce(
        RowLayout,
        &PreparedProjectionShape,
        &[DataRow],
        ProjectionMaterializationMetricsRecorder,
    ) -> Result<Vec<Vec<T>>, InternalError>,
) -> Result<Vec<Vec<T>>, InternalError> {
    // Phase 1: choose the structural payload once, then keep the row loop
    // inside the selected shaping path.
    page.consume_projection_rows(
        |slot_rows| {
            metrics.record_slot_rows_path_hit();

            shape_slot_rows(prepared_projection, slot_rows)
        },
        |data_rows| {
            metrics.record_data_rows_path_hit();

            shape_data_rows(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
                metrics,
            )
        },
    )
}

#[cfg(feature = "sql")]
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
    metrics: ProjectionMaterializationMetricsRecorder,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        let mut emit_value = std::convert::identity;

        return shape_data_rows_from_direct_field_slots(
            rows,
            row_layout,
            field_slots,
            metrics,
            &mut emit_value,
        );
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "diagnostics")))]
    let projected_slot_mask = &[];

    metrics.record_data_rows_scalar_fallback_hit();
    let mut emit_value = std::convert::identity;
    shape_scalar_data_rows_from_projection_structural(
        compiled_fields,
        rows,
        row_layout,
        projected_slot_mask,
        metrics,
        &mut emit_value,
    )
}

#[cfg(feature = "sql")]
// Shape one raw data-row page through direct field-slot copies only.
fn shape_data_rows_from_direct_field_slots<T>(
    rows: &[DataRow],
    row_layout: RowLayout,
    field_slots: &[(String, usize)],
    metrics: ProjectionMaterializationMetricsRecorder,
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
            metrics.record_data_rows_slot_access(true);

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
    metrics: ProjectionMaterializationMetricsRecorder,
    emit_value: &mut impl FnMut(Value) -> T,
) -> Result<Vec<Vec<T>>, InternalError> {
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
            shaped.push(emit_value(value.into_owned()));
        }
        shaped_rows.push(shaped);
    }

    Ok(shaped_rows)
}

#[cfg(any(test, feature = "diagnostics"))]
fn projected_slot_mask_from_slots(field_count: usize, projected_slots: &[bool]) -> Vec<bool> {
    let mut mask = vec![false; field_count];

    for (slot, projected) in projected_slots.iter().copied().enumerate() {
        if projected && let Some(entry) = mask.get_mut(slot) {
            *entry = true;
        }
    }

    mask
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
