//! Module: executor::load::grouped_output
//! Responsibility: grouped row projection materialization and output finalization.
//! Does not own: grouped stream/fold execution orchestration.
//! Boundary: grouped output shaping + observability finalization helpers.

use crate::{
    db::{
        GroupedRow,
        executor::{
            ExecutionOptimization, ExecutionTrace,
            aggregate::AggregateOutput,
            load::{GroupedCursorPage, GroupedFoldStage, GroupedRouteStage, LoadExecutor},
            plan_metrics::record_rows_scanned,
        },
        query::{
            builder::AggregateExpr,
            plan::{FieldSlot, PlannedProjectionLayout, expr::ProjectionSpec},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Finalize grouped output payloads and observability after grouped fold execution.
    pub(super) fn finalize_grouped_output(
        mut route: GroupedRouteStage<E>,
        folded: GroupedFoldStage,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        let rows_returned = folded.page.rows.len();
        Self::finalize_path_outcome(
            &mut route.execution_context.execution_trace,
            folded.optimization,
            folded.rows_scanned,
            rows_returned,
            folded.index_predicate_applied,
            folded.index_predicate_keys_rejected,
            folded.distinct_keys_deduped,
        );

        let mut span = crate::obs::sink::Span::<E>::new(crate::obs::sink::ExecKind::Load);
        span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
        if folded.check_filtered_rows_upper_bound {
            debug_assert!(
                folded.filtered_rows >= rows_returned,
                "grouped pagination must return at most filtered row cardinality",
            );
        }

        (folded.page, route.execution_context.execution_trace)
    }

    // Evaluate grouped projection semantics for each grouped row while preserving
    // legacy grouped response shape at the public boundary.
    pub(super) fn project_grouped_rows_from_projection(
        projection: &ProjectionSpec,
        projection_layout: &PlannedProjectionLayout,
        group_fields: &[FieldSlot],
        aggregate_exprs: &[AggregateExpr],
        rows: Vec<GroupedRow>,
    ) -> Result<Vec<GroupedRow>, InternalError> {
        let mut projected_rows = Vec::with_capacity(rows.len());
        for row in rows {
            projected_rows.push(Self::project_grouped_row_from_projection(
                projection,
                projection_layout,
                group_fields,
                aggregate_exprs,
                row.group_key(),
                row.aggregate_values(),
            )?);
        }

        Ok(projected_rows)
    }

    // Evaluate one grouped projection expression row and convert it back into
    // legacy grouped `(group_key, aggregate_values)` payload vectors.
    pub(super) fn project_grouped_row_from_projection(
        projection: &ProjectionSpec,
        projection_layout: &PlannedProjectionLayout,
        group_fields: &[FieldSlot],
        aggregate_exprs: &[AggregateExpr],
        group_key_values: &[Value],
        aggregate_values: &[Value],
    ) -> Result<GroupedRow, InternalError> {
        let grouped_row = crate::db::executor::load::projection::GroupedRowView::new(
            group_key_values,
            aggregate_values,
            group_fields,
            aggregate_exprs,
        );
        let projected_values =
            crate::db::executor::load::projection::evaluate_grouped_projection_values(
                projection,
                &grouped_row,
            )
            .map_err(|err| {
                InternalError::query_invalid_logical_plan(format!(
                    "grouped projection evaluation failed: {err}",
                ))
            })?;

        let mut projected_group_key =
            Vec::with_capacity(projection_layout.group_field_positions().len());
        for position in projection_layout.group_field_positions() {
            let Some(value) = projected_values.get(*position) else {
                return Err(super::invariant(format!(
                    "grouped projection layout group-field position out of bounds: position={position}, projected_len={}",
                    projected_values.len()
                )));
            };
            projected_group_key.push(value.clone());
        }

        let mut projected_aggregate_values =
            Vec::with_capacity(projection_layout.aggregate_positions().len());
        for position in projection_layout.aggregate_positions() {
            let Some(value) = projected_values.get(*position) else {
                return Err(super::invariant(format!(
                    "grouped projection layout aggregate position out of bounds: position={position}, projected_len={}",
                    projected_values.len()
                )));
            };
            projected_aggregate_values.push(value.clone());
        }

        Ok(GroupedRow::new(
            projected_group_key,
            projected_aggregate_values,
        ))
    }

    // Convert one aggregate output payload into grouped response value payload.
    pub(super) fn aggregate_output_to_value(output: &AggregateOutput<E>) -> Value {
        match output {
            AggregateOutput::Count(value) => Value::Uint(u64::from(*value)),
            AggregateOutput::Sum(value) => value.map_or(Value::Null, Value::Decimal),
            AggregateOutput::Exists(value) => Value::Bool(*value),
            AggregateOutput::Min(value)
            | AggregateOutput::Max(value)
            | AggregateOutput::First(value)
            | AggregateOutput::Last(value) => value.map_or(Value::Null, Value::from),
        }
    }

    // Record shared observability outcome for any execution path.
    pub(in crate::db::executor::load) fn finalize_path_outcome(
        execution_trace: &mut Option<ExecutionTrace>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        rows_returned: usize,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
    ) {
        record_rows_scanned::<E>(rows_scanned);
        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(
                optimization,
                rows_scanned,
                rows_returned,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
            );
        }
    }
}
