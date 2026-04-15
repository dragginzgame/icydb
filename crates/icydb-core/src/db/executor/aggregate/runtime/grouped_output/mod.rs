//! Module: executor::aggregate::runtime::grouped_output
//! Responsibility: grouped row projection materialization and output finalization.
//! Does not own: grouped stream/fold execution orchestration.
//! Boundary: grouped output shaping + observability finalization helpers.

use crate::{
    db::{
        GroupedRow,
        executor::{
            ExecutionTrace,
            pipeline::contracts::{
                ExecutionOutcomeMetrics, GroupedCursorPage, GroupedFoldStage, GroupedRouteStage,
            },
            plan_metrics::{
                record_rows_aggregated_for_path, record_rows_emitted_for_path,
                record_rows_filtered_for_path, record_rows_scanned_for_path,
            },
            projection::*,
        },
        query::plan::{
            FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout, expr::ProjectionSpec,
        },
    },
    error::InternalError,
    metrics::sink::{ExecKind, PathSpan},
    value::Value,
};

///
/// GroupedOutputRuntimeObserverBindings
///
/// GroupedOutputRuntimeObserverBindings keeps entity-typed grouped output
/// observability behind one narrow function-table boundary.
/// Shared grouped output finalization stays monomorphic and delegates only the
/// entity-bound metrics/span leaf.
///

pub(in crate::db::executor) struct GroupedOutputRuntimeObserverBindings {
    entity_path: &'static str,
}

impl GroupedOutputRuntimeObserverBindings {
    /// Build one grouped output observer bundle from one structural entity path.
    #[must_use]
    pub(in crate::db::executor) const fn for_path(entity_path: &'static str) -> Self {
        Self { entity_path }
    }

    /// Record grouped output metrics and execution-trace outcome for one completed page.
    fn finalize_grouped_observability(
        &self,
        execution_trace: &mut Option<ExecutionTrace>,
        metrics: ExecutionOutcomeMetrics,
        rows_aggregated: usize,
        rows_returned: usize,
        execution_time_micros: u64,
    ) {
        finalize_grouped_observability_for_path(
            self.entity_path,
            execution_trace,
            metrics,
            rows_aggregated,
            rows_returned,
            execution_time_micros,
        );
    }
}

// Finalize grouped output payloads and observability after grouped fold
// execution using a non-generic grouped page/fold contract.
pub(in crate::db::executor) fn finalize_grouped_output_with_observer(
    observer: &GroupedOutputRuntimeObserverBindings,
    mut route: GroupedRouteStage,
    folded: GroupedFoldStage,
    execution_time_micros: u64,
) -> (GroupedCursorPage, Option<ExecutionTrace>) {
    let rows_returned = folded.rows_returned();
    let rows_aggregated = folded.filtered_rows();
    let metrics = ExecutionOutcomeMetrics {
        optimization: folded.optimization(),
        rows_scanned: folded.rows_scanned(),
        post_access_rows: rows_returned,
        index_predicate_applied: folded.index_predicate_applied(),
        index_predicate_keys_rejected: folded.index_predicate_keys_rejected(),
        distinct_keys_deduped: folded.distinct_keys_deduped(),
    };
    observer.finalize_grouped_observability(
        route.execution_trace_mut(),
        metrics,
        rows_aggregated,
        rows_returned,
        execution_time_micros,
    );

    if folded.should_check_filtered_rows_upper_bound() {
        debug_assert!(
            folded.filtered_rows() >= rows_returned,
            "grouped pagination must return at most filtered row cardinality",
        );
    }

    (folded.into_page(), route.into_execution_trace())
}

// Record shared observability outcome for scalar/grouped execution paths.
pub(in crate::db::executor) fn finalize_path_outcome_for_path(
    entity_path: &'static str,
    execution_trace: &mut Option<ExecutionTrace>,
    metrics: ExecutionOutcomeMetrics,
    rows_emitted: usize,
    index_only: bool,
    execution_time_micros: u64,
) {
    let ExecutionOutcomeMetrics {
        optimization,
        rows_scanned,
        post_access_rows: _post_access_rows,
        index_predicate_applied,
        index_predicate_keys_rejected,
        distinct_keys_deduped,
    } = metrics;
    record_rows_scanned_for_path(entity_path, rows_scanned);
    let rows_filtered = rows_scanned.saturating_sub(rows_emitted);
    record_rows_filtered_for_path(entity_path, rows_filtered);
    record_rows_emitted_for_path(entity_path, rows_emitted);

    if let Some(execution_trace) = execution_trace.as_mut() {
        execution_trace.set_path_outcome(
            optimization,
            rows_scanned,
            rows_scanned,
            rows_emitted,
            execution_time_micros,
            index_only,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        );
    }
}

fn finalize_grouped_observability_for_path(
    entity_path: &'static str,
    execution_trace: &mut Option<ExecutionTrace>,
    metrics: ExecutionOutcomeMetrics,
    rows_aggregated: usize,
    rows_returned: usize,
    execution_time_micros: u64,
) {
    let ExecutionOutcomeMetrics {
        optimization,
        rows_scanned,
        post_access_rows,
        index_predicate_applied,
        index_predicate_keys_rejected,
        distinct_keys_deduped,
    } = metrics;
    record_rows_aggregated_for_path(entity_path, rows_aggregated);
    record_rows_scanned_for_path(entity_path, rows_scanned);
    let rows_filtered = rows_scanned.saturating_sub(post_access_rows);
    record_rows_filtered_for_path(entity_path, rows_filtered);
    record_rows_emitted_for_path(entity_path, post_access_rows);

    if let Some(execution_trace) = execution_trace.as_mut() {
        execution_trace.set_path_outcome(
            optimization,
            rows_scanned,
            rows_scanned,
            post_access_rows,
            execution_time_micros,
            false,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        );
    }

    let mut span = PathSpan::new(ExecKind::Load, entity_path);
    span.set_rows(u64::try_from(rows_returned).unwrap_or(u64::MAX));
}

// Evaluate grouped projection semantics for each grouped row while preserving
// grouped response contract at the public boundary.
pub(in crate::db::executor) fn project_grouped_rows_from_projection(
    projection: &ProjectionSpec,
    projection_is_identity: bool,
    projection_layout: &PlannedProjectionLayout,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
    rows: Vec<GroupedRow>,
) -> Result<Vec<GroupedRow>, InternalError> {
    // Phase 1: short-circuit the common grouped identity shape.
    // Grouped logical plans currently lower to canonical `group fields +
    // aggregate terminals` projection order, so paying the generic grouped
    // projection evaluator here only rebuilds rows we already have.
    if projection_is_identity {
        return Ok(rows);
    }

    // Phase 2: retain the generic grouped projection evaluator for any future
    // additive grouped projection shape that is not already row-identical.
    let compiled_projection =
        compile_grouped_projection_plan(projection, group_fields, aggregate_execution_specs)
            .map_err(ProjectionEvalError::into_grouped_projection_internal_error)?;
    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        projected_rows.push(project_grouped_values_from_projection(
            compiled_projection.as_slice(),
            projection_layout,
            group_fields,
            aggregate_execution_specs,
            row.group_key(),
            row.aggregate_values(),
        )?);
    }

    Ok(projected_rows)
}

// Evaluate one grouped projection expression row and convert grouped key +
// aggregate slices directly into grouped output vectors.
pub(in crate::db::executor) fn project_grouped_values_from_projection(
    compiled_projection: &[GroupedProjectionExpr],
    projection_layout: &PlannedProjectionLayout,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
    group_key_values: &[Value],
    aggregate_values: &[Value],
) -> Result<GroupedRow, InternalError> {
    let grouped_row = GroupedRowView::new(
        group_key_values,
        aggregate_values,
        group_fields,
        aggregate_execution_specs,
    );
    let mut projected_group_key =
        Vec::with_capacity(projection_layout.group_field_positions().len());
    let mut projected_aggregate_values =
        Vec::with_capacity(projection_layout.aggregate_positions().len());
    let mut next_group_position = projection_layout.group_field_positions().iter().copied();
    let mut next_aggregate_position = projection_layout.aggregate_positions().iter().copied();
    let mut expected_group_position = next_group_position.next();
    let mut expected_aggregate_position = next_aggregate_position.next();

    // Phase 1: evaluate each compiled projection expression once and route the
    // resulting value directly into the final grouped output buffers.
    for (projection_index, expr) in compiled_projection.iter().enumerate() {
        let projected_value = eval_grouped_projection_expr(expr, &grouped_row)
            .map_err(ProjectionEvalError::into_grouped_projection_internal_error)?;

        if expected_group_position == Some(projection_index) {
            projected_group_key.push(projected_value);
            expected_group_position = next_group_position.next();
            continue;
        }
        if expected_aggregate_position == Some(projection_index) {
            projected_aggregate_values.push(projected_value);
            expected_aggregate_position = next_aggregate_position.next();
        }
    }

    // Phase 2: preserve the old out-of-bounds diagnostics when the planner
    // layout references a projection position that does not exist.
    if let Some(position) = expected_group_position {
        return Err(PlannedProjectionLayout::projected_position_out_of_bounds(
            "group-field",
            position,
            compiled_projection.len(),
        ));
    }
    if let Some(position) = expected_aggregate_position {
        return Err(PlannedProjectionLayout::projected_position_out_of_bounds(
            "aggregate",
            position,
            compiled_projection.len(),
        ));
    }

    Ok(GroupedRow::new(
        projected_group_key,
        projected_aggregate_values,
    ))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            GroupedRow,
            executor::aggregate::runtime::grouped_output::project_grouped_rows_from_projection,
            query::{
                builder::aggregate::{count, max_by},
                plan::{
                    AggregateKind, FieldSlot, GroupedAggregateExecutionSpec,
                    PlannedProjectionLayout,
                    expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
                },
            },
        },
        value::Value,
    };

    #[test]
    fn grouped_identity_projection_fast_path_preserves_rows() {
        let projection = ProjectionSpec::from_fields_for_test(vec![
            ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("age")),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(count()),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(max_by("score")),
                alias: None,
            },
        ]);
        let projection_layout = PlannedProjectionLayout {
            group_field_positions: vec![0],
            aggregate_positions: vec![1, 2],
        };
        let group_fields = [FieldSlot::from_parts_for_test(0, "age")];
        let aggregate_execution_specs = [
            GroupedAggregateExecutionSpec::from_parts_for_test(
                AggregateKind::Count,
                None,
                None,
                false,
            ),
            GroupedAggregateExecutionSpec::from_parts_for_test(
                AggregateKind::Max,
                Some(FieldSlot::from_parts_for_test(1, "score")),
                Some("score"),
                false,
            ),
        ];
        let rows = vec![
            GroupedRow::new(vec![Value::Uint(21)], vec![Value::Uint(2), Value::Uint(90)]),
            GroupedRow::new(vec![Value::Uint(35)], vec![Value::Uint(1), Value::Uint(70)]),
        ];

        let projected_rows = project_grouped_rows_from_projection(
            &projection,
            true,
            &projection_layout,
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
            rows.clone(),
        )
        .expect("grouped identity projection should preserve grouped rows");

        assert_eq!(projected_rows, rows);
    }

    #[test]
    fn grouped_non_identity_projection_reorders_aggregate_outputs() {
        let projection = ProjectionSpec::from_fields_for_test(vec![
            ProjectionField::Scalar {
                expr: Expr::Field(FieldId::new("age")),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(max_by("score")),
                alias: None,
            },
            ProjectionField::Scalar {
                expr: Expr::Aggregate(count()),
                alias: None,
            },
        ]);
        let projection_layout = PlannedProjectionLayout {
            group_field_positions: vec![0],
            aggregate_positions: vec![1, 2],
        };
        let group_fields = [FieldSlot::from_parts_for_test(0, "age")];
        let aggregate_execution_specs = [
            GroupedAggregateExecutionSpec::from_parts_for_test(
                AggregateKind::Count,
                None,
                None,
                false,
            ),
            GroupedAggregateExecutionSpec::from_parts_for_test(
                AggregateKind::Max,
                Some(FieldSlot::from_parts_for_test(1, "score")),
                Some("score"),
                false,
            ),
        ];
        let rows = vec![
            GroupedRow::new(vec![Value::Uint(21)], vec![Value::Uint(2), Value::Uint(90)]),
            GroupedRow::new(vec![Value::Uint(35)], vec![Value::Uint(1), Value::Uint(70)]),
        ];

        let projected_rows = project_grouped_rows_from_projection(
            &projection,
            false,
            &projection_layout,
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
            rows,
        )
        .expect("grouped reordered projection should evaluate through compiled grouped plan");

        assert_eq!(
            projected_rows,
            vec![
                GroupedRow::new(vec![Value::Uint(21)], vec![Value::Uint(90), Value::Uint(2)]),
                GroupedRow::new(vec![Value::Uint(35)], vec![Value::Uint(70), Value::Uint(1)]),
            ],
        );
    }
}
