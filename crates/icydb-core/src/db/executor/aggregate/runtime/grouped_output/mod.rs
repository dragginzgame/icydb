//! Module: executor::aggregate::runtime::grouped_output
//! Responsibility: grouped row projection materialization and output finalization.
//! Does not own: grouped stream/fold execution orchestration.
//! Boundary: grouped output shaping helpers.

#[cfg(test)]
mod tests;

use crate::{
    db::executor::{
        RuntimeGroupedRow,
        aggregate::{GroupedAggregateExecutionSpec, PlannedProjectionLayout, ProjectionSpec},
        pipeline::contracts::GroupedCursorPage,
        pipeline::runtime::GroupedFoldStage,
        projection::*,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

// Finalize grouped output after grouped fold execution.
pub(in crate::db::executor) fn finalize_grouped_output(
    folded: GroupedFoldStage,
) -> GroupedCursorPage {
    if folded.should_check_filtered_rows_upper_bound() {
        debug_assert!(
            folded.filtered_rows() >= folded.page_row_count(),
            "grouped pagination must return at most filtered row cardinality",
        );
    }

    folded.into_page()
}

// Evaluate grouped projection semantics for each grouped row while preserving
// grouped response contract at the public boundary.
pub(in crate::db::executor) fn project_grouped_rows_from_projection(
    projection: &ProjectionSpec,
    projection_is_identity: bool,
    projection_layout: &PlannedProjectionLayout,
    group_fields: &crate::db::query::plan::GroupFieldSet,
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
    rows: Vec<RuntimeGroupedRow>,
) -> Result<Vec<RuntimeGroupedRow>, InternalError> {
    let Some(compiled_projection) = compile_grouped_projection_plan_if_needed(
        projection,
        projection_is_identity,
        projection_layout,
        group_fields,
        aggregate_execution_specs,
    )?
    else {
        return Ok(rows);
    };

    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        projected_rows.push(project_grouped_values_from_compiled_projection(
            &compiled_projection,
            row.group_key(),
            row.aggregate_values(),
        )?);
    }

    Ok(projected_rows)
}

// Evaluate one grouped projection expression row and convert grouped key +
// aggregate slices directly into grouped output vectors.
pub(in crate::db::executor) fn project_grouped_values_from_compiled_projection(
    compiled_projection: &CompiledGroupedProjectionPlan<'_>,
    group_key_values: &[Value],
    aggregate_values: &[Value],
) -> Result<RuntimeGroupedRow, InternalError> {
    let grouped_row = GroupedRowView::new(group_key_values, aggregate_values);
    let mut projected_group_key = Vec::with_capacity(
        compiled_projection
            .projection_layout()
            .group_field_positions()
            .len(),
    );
    let mut projected_aggregate_values = Vec::with_capacity(
        compiled_projection
            .projection_layout()
            .aggregate_positions()
            .len(),
    );
    let mut next_group_position = compiled_projection
        .projection_layout()
        .group_field_positions()
        .iter()
        .copied();
    let mut next_aggregate_position = compiled_projection
        .projection_layout()
        .aggregate_positions()
        .iter()
        .copied();
    let mut expected_group_position = next_group_position.next();
    let mut expected_aggregate_position = next_aggregate_position.next();

    // Phase 1: evaluate each compiled projection expression once and route the
    // resulting value directly into the final grouped output buffers.
    for (projection_index, expr) in compiled_projection.compiled_projection().iter().enumerate() {
        let projected_value = expr
            .evaluate(&grouped_row)
            .map(Cow::into_owned)
            .map_err(ProjectionEvalError::into_internal_error)?;

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
            compiled_projection.compiled_projection().len(),
        ));
    }
    if let Some(position) = expected_aggregate_position {
        return Err(PlannedProjectionLayout::projected_position_out_of_bounds(
            "aggregate",
            position,
            compiled_projection.compiled_projection().len(),
        ));
    }

    Ok(RuntimeGroupedRow::new(
        projected_group_key,
        projected_aggregate_values,
    ))
}
