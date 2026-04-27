use crate::{
    db::{
        data::{CanonicalSlotReader, DataRow},
        executor::{
            StructuralCursorPage,
            projection::{
                PreparedProjectionShape, ProjectionEvalError, ScalarProjectionExpr,
                eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
                visit_prepared_projection_values_with_required_value_reader_cow,
            },
            terminal::{RetainedSlotRow, RowLayout},
        },
        query::plan::{AccessPlannedQuery, PageSpec},
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;
#[cfg(any(test, feature = "diagnostics"))]
use std::cell::RefCell;

#[cfg(feature = "sql")]
pub(in crate::db::session::sql::projection::runtime) fn project_distinct_structural_sql_projection_page(
    row_layout: RowLayout,
    prepared_projection: &PreparedProjectionShape,
    plan: &AccessPlannedQuery,
    page: StructuralCursorPage,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let window = SqlProjectionDistinctWindow::from_page(plan.scalar_plan().page.as_ref());

    // Phase 1: choose the structural payload once, then run a bounded
    // DISTINCT projector over that shape. The projector owns the SQL
    // post-projection window so it can stop when LIMIT has been satisfied.
    page.consume_projection_rows(
        |slot_rows| {
            #[cfg(any(test, feature = "diagnostics"))]
            record_sql_projection_slot_rows_path_hit();

            project_distinct_slot_rows_from_projection_structural(
                prepared_projection,
                slot_rows,
                window,
            )
        },
        |data_rows| {
            #[cfg(any(test, feature = "diagnostics"))]
            record_sql_projection_data_rows_path_hit();

            project_distinct_data_rows_from_projection_structural(
                row_layout,
                prepared_projection,
                data_rows.as_slice(),
                window,
            )
        },
    )
}

#[cfg(feature = "sql")]
fn project_distinct_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    window: SqlProjectionDistinctWindow,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = prepared_projection.retained_slot_direct_projection_field_slots() {
        return project_distinct_slot_rows_from_direct_field_slots(rows, field_slots, window);
    }

    project_distinct_dense_slot_rows_from_projection_structural(prepared_projection, rows, window)
}

#[cfg(feature = "sql")]
fn project_distinct_dense_slot_rows_from_projection_structural(
    prepared_projection: &PreparedProjectionShape,
    rows: Vec<RetainedSlotRow>,
    window: SqlProjectionDistinctWindow,
) -> Result<Vec<Vec<Value>>, InternalError> {
    let projection = prepared_projection.projection();

    collect_bounded_distinct_projected_rows(window, rows.iter(), |row| {
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
    window: SqlProjectionDistinctWindow,
) -> Result<Vec<Vec<Value>>, InternalError> {
    collect_bounded_distinct_projected_rows(window, rows, |mut row| {
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
    window: SqlProjectionDistinctWindow,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if let Some(field_slots) = prepared_projection.data_row_direct_projection_field_slots() {
        return project_distinct_data_rows_from_direct_field_slots(
            rows,
            row_layout,
            field_slots,
            window,
        );
    }

    let compiled_fields = prepared_projection.scalar_projection_exprs();
    #[cfg(any(test, feature = "diagnostics"))]
    let projected_slot_mask = prepared_projection.projected_slot_mask();
    #[cfg(not(any(test, feature = "diagnostics")))]
    let projected_slot_mask = &[];

    #[cfg(any(test, feature = "diagnostics"))]
    record_sql_projection_data_rows_scalar_fallback_hit();
    project_distinct_scalar_data_rows_from_projection_structural(
        compiled_fields,
        rows,
        row_layout,
        projected_slot_mask,
        window,
    )
}

#[cfg(feature = "sql")]
fn project_distinct_data_rows_from_direct_field_slots(
    rows: &[DataRow],
    row_layout: RowLayout,
    field_slots: &[(String, usize)],
    window: SqlProjectionDistinctWindow,
) -> Result<Vec<Vec<Value>>, InternalError> {
    collect_bounded_distinct_projected_rows(window, rows.iter(), |(data_key, raw_row)| {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut shaped = Vec::with_capacity(field_slots.len());
        for (_field_name, slot) in field_slots {
            #[cfg(any(test, feature = "diagnostics"))]
            record_sql_projection_data_rows_slot_access(true);

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
    window: SqlProjectionDistinctWindow,
) -> Result<Vec<Vec<Value>>, InternalError> {
    #[cfg(not(any(test, feature = "diagnostics")))]
    let _ = projected_slot_mask;

    collect_bounded_distinct_projected_rows(window, rows.iter(), |(data_key, raw_row)| {
        let row_fields = row_layout.open_raw_row(raw_row)?;
        row_fields.validate_storage_key(data_key)?;

        let mut shaped = Vec::with_capacity(compiled_fields.len());
        for compiled in compiled_fields {
            let value = eval_canonical_scalar_projection_expr_with_required_value_reader_cow(
                compiled,
                &mut |slot| {
                    #[cfg(any(test, feature = "diagnostics"))]
                    record_sql_projection_data_rows_slot_access(
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
/// SqlProjectionDistinctWindow
///
/// SqlProjectionDistinctWindow carries SQL DISTINCT paging after projection.
/// It lets the row projector skip OFFSET rows and stop at the LIMIT horizon
/// while preserving the existing projected-row DISTINCT equality contract.
///

#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlProjectionDistinctWindow {
    offset: usize,
    limit: Option<usize>,
}

#[cfg(feature = "sql")]
impl SqlProjectionDistinctWindow {
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
/// SqlDistinctProjectionAccumulator
///
/// SqlDistinctProjectionAccumulator owns the SQL projected-row DISTINCT set
/// and post-DISTINCT window state for one materialization pass.
/// Callers feed rows in final execution order and stop when `consider_row`
/// returns false.
///

#[cfg(feature = "sql")]
struct SqlDistinctProjectionAccumulator {
    distinct_rows: crate::db::executor::group::GroupKeySet,
    output_rows: Vec<Vec<Value>>,
    window: SqlProjectionDistinctWindow,
    distinct_seen: usize,
}

#[cfg(feature = "sql")]
impl SqlDistinctProjectionAccumulator {
    fn new(window: SqlProjectionDistinctWindow) -> Self {
        Self {
            distinct_rows: crate::db::executor::group::GroupKeySet::new(),
            output_rows: Vec::with_capacity(window.output_capacity()),
            window,
            distinct_seen: 0,
        }
    }

    fn consider_row(&mut self, row: Vec<Value>) -> Result<bool, InternalError> {
        let inserted = self
            .distinct_rows
            .insert_value(&Value::List(row.clone()))
            .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)?;
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
            #[cfg(any(test, feature = "diagnostics"))]
            record_sql_projection_distinct_bounded_stop();

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
    window: SqlProjectionDistinctWindow,
    rows: impl IntoIterator<Item = I>,
    mut project_row: impl FnMut(I) -> Result<Vec<Value>, InternalError>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if window.output_is_empty() {
        return Ok(Vec::new());
    }

    let mut accumulator = SqlDistinctProjectionAccumulator::new(window);

    // Phase 1: project rows in final execution order and feed each projected
    // tuple into the DISTINCT/window accumulator. A bounded LIMIT can stop the
    // projector before later structural rows are decoded.
    for row in rows {
        let projected = project_row(row)?;

        #[cfg(any(test, feature = "diagnostics"))]
        record_sql_projection_distinct_candidate_row();

        if !accumulator.consider_row(projected)? {
            break;
        }
    }

    Ok(accumulator.into_rows())
}

#[cfg(feature = "sql")]
pub(in crate::db::session::sql::projection::runtime) fn finalize_sql_projection_rows(
    plan: &AccessPlannedQuery,
    rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>, InternalError> {
    if !plan.scalar_plan().distinct {
        return Ok(rows);
    }

    collect_bounded_distinct_projected_rows(
        SqlProjectionDistinctWindow::from_page(plan.scalar_plan().page.as_ref()),
        rows,
        Ok,
    )
}

///
/// SqlProjectionMaterializationMetrics
///
/// SqlProjectionMaterializationMetrics aggregates one test-scoped view of the
/// row-backed SQL projection path selection and fallback slot access behavior.
/// It lets perf probes distinguish retained projected rows, retained slot
/// rows, and `data_rows` fallback execution without changing runtime policy.
///

#[cfg(any(test, feature = "diagnostics"))]
#[cfg_attr(all(test, not(feature = "diagnostics")), allow(unreachable_pub))]
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
    pub distinct_candidate_rows: u64,
    pub distinct_bounded_stop_hits: u64,
}

#[cfg(any(test, feature = "diagnostics"))]
std::thread_local! {
    static SQL_PROJECTION_MATERIALIZATION_METRICS: RefCell<Option<SqlProjectionMaterializationMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(any(test, feature = "diagnostics"))]
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

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_slot_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.slot_rows_path_hits = metrics.slot_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_data_rows_path_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_path_hits = metrics.data_rows_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_sql_projection_hybrid_covering_path_hit()
 {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_path_hits = metrics.hybrid_covering_path_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_sql_projection_hybrid_covering_index_field_access()
 {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_index_field_accesses = metrics
            .hybrid_covering_index_field_accesses
            .saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn record_sql_projection_hybrid_covering_row_field_access()
 {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.hybrid_covering_row_field_accesses =
            metrics.hybrid_covering_row_field_accesses.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_data_rows_scalar_fallback_hit() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.data_rows_scalar_fallback_hits =
            metrics.data_rows_scalar_fallback_hits.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn record_sql_projection_data_rows_slot_access(projected_slot: bool) {
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

#[cfg(any(test, feature = "diagnostics"))]
fn record_sql_projection_distinct_candidate_row() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.distinct_candidate_rows = metrics.distinct_candidate_rows.saturating_add(1);
    });
}

#[cfg(any(test, feature = "diagnostics"))]
fn record_sql_projection_distinct_bounded_stop() {
    update_sql_projection_materialization_metrics(|metrics| {
        metrics.distinct_bounded_stop_hits = metrics.distinct_bounded_stop_hits.saturating_add(1);
    });
}

///
/// with_sql_projection_materialization_metrics
///
/// Run one closure while collecting row-backed SQL projection metrics on the
/// current thread, then return the closure result plus the aggregated
/// snapshot.
///

#[cfg(feature = "diagnostics")]
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

#[cfg(all(test, not(feature = "diagnostics")))]
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
