use crate::{
    db::{
        executor::{
            RuntimeGroupedRow,
            aggregate::runtime::grouped_output::project_grouped_rows_from_projection,
        },
        query::{
            builder::aggregate::{count, max_by},
            plan::{
                AggregateKind, FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
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
        GroupedAggregateExecutionSpec::from_parts_for_test(AggregateKind::Count, None, None, false),
        GroupedAggregateExecutionSpec::from_parts_for_test(
            AggregateKind::Max,
            Some(FieldSlot::from_parts_for_test(1, "score")),
            Some("score"),
            false,
        ),
    ];
    let rows = vec![
        RuntimeGroupedRow::new(vec![Value::Nat(21)], vec![Value::Nat(2), Value::Nat(90)]),
        RuntimeGroupedRow::new(vec![Value::Nat(35)], vec![Value::Nat(1), Value::Nat(70)]),
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
        GroupedAggregateExecutionSpec::from_parts_for_test(AggregateKind::Count, None, None, false),
        GroupedAggregateExecutionSpec::from_parts_for_test(
            AggregateKind::Max,
            Some(FieldSlot::from_parts_for_test(1, "score")),
            Some("score"),
            false,
        ),
    ];
    let rows = vec![
        RuntimeGroupedRow::new(vec![Value::Nat(21)], vec![Value::Nat(2), Value::Nat(90)]),
        RuntimeGroupedRow::new(vec![Value::Nat(35)], vec![Value::Nat(1), Value::Nat(70)]),
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
            RuntimeGroupedRow::new(vec![Value::Nat(21)], vec![Value::Nat(90), Value::Nat(2)]),
            RuntimeGroupedRow::new(vec![Value::Nat(35)], vec![Value::Nat(70), Value::Nat(1)]),
        ],
    );
}
