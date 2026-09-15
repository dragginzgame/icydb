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
    let group_fields =
        crate::db::query::plan::GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "age")]);
    let aggregate_execution_specs = [
        GroupedAggregateExecutionSpec::from_test_inputs(AggregateKind::Count, None, None, false),
        GroupedAggregateExecutionSpec::from_test_inputs(
            AggregateKind::Max,
            Some(FieldSlot::from_test_slot(1, "score")),
            Some("score"),
            false,
        ),
    ];
    let rows = vec![
        RuntimeGroupedRow::new(
            vec![Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(90)],
        ),
        RuntimeGroupedRow::new(
            vec![Value::Nat64(35)],
            vec![Value::Nat64(1), Value::Nat64(70)],
        ),
    ];

    let projected_rows = project_grouped_rows_from_projection(
        &projection,
        true,
        &projection_layout,
        &group_fields,
        aggregate_execution_specs.as_slice(),
        rows.clone(),
    )
    .expect("grouped identity projection should preserve grouped rows");

    assert_eq!(projected_rows, rows);
}

#[test]
fn grouped_non_identity_projection_reorders_aggregate_outputs() {
    use crate::db::{
        QueryError,
        executor::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
        DiagnosticExecutionLane as Lane, DiagnosticFactTag,
    };
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
    let group_fields =
        crate::db::query::plan::GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "age")]);
    let aggregate_execution_specs = [
        GroupedAggregateExecutionSpec::from_test_inputs(AggregateKind::Count, None, None, false),
        GroupedAggregateExecutionSpec::from_test_inputs(
            AggregateKind::Max,
            Some(FieldSlot::from_test_slot(1, "score")),
            Some("score"),
            false,
        ),
    ];
    let rows = vec![
        RuntimeGroupedRow::new(
            vec![Value::Nat64(21)],
            vec![Value::Nat64(2), Value::Nat64(90)],
        ),
        RuntimeGroupedRow::new(
            vec![Value::Nat64(35)],
            vec![Value::Nat64(1), Value::Nat64(70)],
        ),
    ];

    let project = || {
        project_grouped_rows_from_projection(
            &projection,
            false,
            &projection_layout,
            &group_fields,
            &aggregate_execution_specs,
            rows.clone(),
        )
    };
    assert!(
        project().is_err(),
        "non-identity compilation requires execution authority"
    );
    for limit in [0, 16_000_000] {
        let projected_rows = with_query_execution_budget_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
            HardExecutionContext::new(Scope::Execution, Lane::PublicRead, 0),
            || project().map_err(QueryError::execute),
        );
        if limit == 0 {
            assert!(projected_rows.unwrap_err().diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
            continue;
        }
        let projected_rows = projected_rows
            .expect("grouped reordered projection should evaluate through compiled grouped plan");

        assert_eq!(
            projected_rows,
            vec![
                RuntimeGroupedRow::new(
                    vec![Value::Nat64(21)],
                    vec![Value::Nat64(90), Value::Nat64(2)]
                ),
                RuntimeGroupedRow::new(
                    vec![Value::Nat64(35)],
                    vec![Value::Nat64(70), Value::Nat64(1)]
                ),
            ],
        );
    }
}
