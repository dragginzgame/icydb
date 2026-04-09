//! Module: db::executor::aggregate::runtime::grouped_output::projection
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_output::projection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        GroupedRow,
        executor::projection::*,
        query::plan::{
            FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
            expr::{Expr, ProjectionField, ProjectionSpec},
        },
    },
    error::InternalError,
    value::Value,
};

// Evaluate grouped projection semantics for each grouped row while preserving
// grouped response contract at the public boundary.
pub(in crate::db::executor) fn project_grouped_rows_from_projection(
    projection: &ProjectionSpec,
    projection_layout: &PlannedProjectionLayout,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
    rows: Vec<GroupedRow>,
) -> Result<Vec<GroupedRow>, InternalError> {
    // Phase 1: short-circuit the common grouped identity shape.
    // Grouped logical plans currently lower to canonical `group fields +
    // aggregate terminals` projection order, so paying the generic grouped
    // projection evaluator here only rebuilds rows we already have.
    if projection_is_identity_grouped_projection(
        projection,
        group_fields,
        aggregate_execution_specs,
    ) {
        return Ok(rows);
    }

    // Phase 2: retain the generic grouped projection evaluator for any future
    // additive grouped projection shape that is not already row-identical.
    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        projected_rows.push(project_grouped_row_from_projection(
            projection,
            projection_layout,
            group_fields,
            aggregate_execution_specs,
            row.group_key(),
            row.aggregate_values(),
        )?);
    }

    Ok(projected_rows)
}

// Detect the canonical grouped identity projection so grouped output shaping
// can return the already-materialized rows unchanged.
fn projection_is_identity_grouped_projection(
    projection: &ProjectionSpec,
    group_fields: &[FieldSlot],
    aggregate_execution_specs: &[GroupedAggregateExecutionSpec],
) -> bool {
    if projection.len()
        != group_fields
            .len()
            .saturating_add(aggregate_execution_specs.len())
    {
        return false;
    }

    let mut projection_fields = projection.fields();

    for expected_group_field in group_fields {
        let Some(ProjectionField::Scalar { expr, .. }) = projection_fields.next() else {
            return false;
        };

        if !matches!(
            expression_without_alias(expr),
            Expr::Field(field_id) if field_id.as_str() == expected_group_field.field.as_str()
        ) {
            return false;
        }
    }

    for expected_aggregate_execution_spec in aggregate_execution_specs {
        let Some(ProjectionField::Scalar { expr, .. }) = projection_fields.next() else {
            return false;
        };

        if !matches!(
            expression_without_alias(expr),
            Expr::Aggregate(actual_aggregate_expr)
                if expected_aggregate_execution_spec.matches_aggregate_expr(actual_aggregate_expr)
        ) {
            return false;
        }
    }

    true
}

// Evaluate one grouped projection expression row and convert it into grouped
// `(group_key, aggregate_values)` payload vectors.
fn project_grouped_row_from_projection(
    projection: &ProjectionSpec,
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
    let projected_values = evaluate_grouped_projection_values(projection, &grouped_row)
        .map_err(ProjectionEvalError::into_grouped_projection_internal_error)?;
    let projected_group_key = projected_values_for_positions(
        projected_values.as_slice(),
        projection_layout.group_field_positions(),
        "group-field",
    )?;
    let projected_aggregate_values = projected_values_for_positions(
        projected_values.as_slice(),
        projection_layout.aggregate_positions(),
        "aggregate",
    )?;

    Ok(GroupedRow::new(
        projected_group_key,
        projected_aggregate_values,
    ))
}

// Strip alias wrappers so grouped identity detection compares canonical roots.
fn expression_without_alias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias { expr: inner, .. } = expr {
        expr = inner.as_ref();
    }

    expr
}

// Project one stable set of row positions into one cloned value vector. Grouped
// output layout splitting reuses this for both grouped-key and aggregate payloads.
fn projected_values_for_positions(
    projected_values: &[Value],
    positions: &[usize],
    position_kind: &str,
) -> Result<Vec<Value>, InternalError> {
    let mut values = Vec::with_capacity(positions.len());
    for position in positions {
        let Some(value) = projected_values.get(*position) else {
            return Err(PlannedProjectionLayout::projected_position_out_of_bounds(
                position_kind,
                *position,
                projected_values.len(),
            ));
        };
        values.push(value.clone());
    }

    Ok(values)
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
            &projection_layout,
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
            rows.clone(),
        )
        .expect("grouped identity projection should preserve grouped rows");

        assert_eq!(projected_rows, rows);
    }
}
