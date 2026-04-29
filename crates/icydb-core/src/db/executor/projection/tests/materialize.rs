use super::*;

#[cfg(feature = "sql")]
#[test]
fn projection_hash_alias_identity_matches_evaluated_projection_output() {
    let row = row(5, 42, true);
    let base_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let aliased_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("rank"))),
            name: Alias::new("rank_expr"),
        },
        alias: Some(Alias::new("rank_out")),
    }]);

    let base_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&base_projection, std::slice::from_ref(&row))
            .expect("base projection should evaluate");
    let aliased_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&aliased_projection, std::slice::from_ref(&row))
            .expect("aliased projection should evaluate");

    assert_eq!(
        base_projection.structural_hash_for_test(),
        aliased_projection.structural_hash_for_test(),
        "alias-insensitive projection hash must align with evaluator output identity",
    );
    assert_eq!(
        base_rows[0].values(),
        aliased_rows[0].values(),
        "alias wrappers must not affect evaluated projection values",
    );
    assert_eq!(
        base_rows[0].id(),
        aliased_rows[0].id(),
        "projection identity checks must preserve source row identity",
    );
}

#[cfg(feature = "sql")]
#[test]
fn projection_field_order_preserved_for_multi_field_selection() {
    let rows = [row(51, 7, true), row(52, 9, false)];
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("label")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("flag")),
            alias: None,
        },
    ]);

    let projected = project_rows_from_projection(&projection, rows.as_slice())
        .expect("multi-field projection should evaluate");

    assert_eq!(
        projected[0].values(),
        &[
            output(Value::Text("label-51".to_string())),
            output(Value::Int(7)),
            output(Value::Bool(true)),
        ],
        "projection values must preserve declaration order for the first row",
    );
    assert_eq!(
        projected[1].values(),
        &[
            output(Value::Text("label-52".to_string())),
            output(Value::Int(9)),
            output(Value::Bool(false)),
        ],
        "projection values must preserve declaration order for the second row",
    );
}

#[cfg(feature = "sql")]
#[test]
fn scalar_arithmetic_projection_returns_computed_values() {
    let rows = [row(7, 41, true)];
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);

    let projected = project_rows_from_projection(&projection, rows.as_slice())
        .expect("arithmetic scalar projection should evaluate");
    let only_value = projected[0]
        .values()
        .first()
        .expect("projection should emit one value");
    assert_eq!(
        only_value,
        &output(Value::Decimal(crate::types::Decimal::from(42_u64))),
        "arithmetic scalar projection should emit computed expression result",
    );
}

#[cfg(feature = "sql")]
#[test]
fn ordering_is_preserved_when_projecting_computed_fields() {
    // Input rows are already in execution order; projection must preserve that
    // row ordering while evaluating computed scalar expressions.
    let rows = [row(8, 1, true), row(9, 2, true), row(10, 3, true)];
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(100))),
        },
        alias: None,
    }]);

    let projected = project_rows_from_projection(&projection, rows.as_slice())
        .expect("computed projection should evaluate deterministically");

    let projected_ids: Vec<_> = projected.iter().map(ProjectedRow::id).collect();
    let expected_ids: Vec<_> = rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        projected_ids, expected_ids,
        "projection phase must preserve established row ordering",
    );
    let expected_values = [
        crate::types::Decimal::from(101_u64),
        crate::types::Decimal::from(102_u64),
        crate::types::Decimal::from(103_u64),
    ];
    for (actual, expected) in projected
        .iter()
        .map(|row| row.values()[0].clone())
        .zip(expected_values)
    {
        assert_eq!(
            actual,
            output(Value::Decimal(expected)),
            "computed projection values must align with preserved row order",
        );
    }
}

#[cfg(feature = "sql")]
#[test]
fn expression_projection_column_identity_is_deterministic() {
    let rows = [row(53, 7, true)];
    let base_projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_internal_a"),
            },
            alias: Some(Alias::new("rank_plus_one_a")),
        },
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(2))),
                }),
                name: Alias::new("rank_times_two_internal_a"),
            },
            alias: Some(Alias::new("rank_times_two_a")),
        },
    ]);
    let alias_variant_projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_internal_b"),
            },
            alias: Some(Alias::new("rank_plus_one_b")),
        },
        ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(2))),
                }),
                name: Alias::new("rank_times_two_internal_b"),
            },
            alias: Some(Alias::new("rank_times_two_b")),
        },
    ]);

    let base_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&base_projection, rows.as_slice())
            .expect("base expression projection should evaluate");
    let alias_rows: Vec<ProjectedRow<ProjectionEvalEntity>> =
        project_rows_from_projection(&alias_variant_projection, rows.as_slice())
            .expect("alias-variant expression projection should evaluate");

    assert_eq!(
        base_projection.structural_hash_for_test(),
        alias_variant_projection.structural_hash_for_test(),
        "expression projection identity must remain deterministic across alias-only renames",
    );
    assert_eq!(
        base_rows[0].values(),
        alias_rows[0].values(),
        "expression projection output values must remain deterministic across alias-only renames",
    );
    assert_eq!(base_rows[0].values().len(), 2);
    assert_eq!(
        base_rows[0].values()[0],
        output(Value::Decimal(crate::types::Decimal::from(8_u64))),
        "first expression projection output should preserve deterministic declared order",
    );
    assert_eq!(
        base_rows[0].values()[1],
        output(Value::Decimal(crate::types::Decimal::from(14_u64))),
        "second expression projection output should preserve deterministic declared order",
    );
}

#[cfg(feature = "sql")]
#[test]
fn projection_materialization_exposes_projected_rows_payload() {
    let row = row(6, 19, true);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let projected_rows = project_rows_from_projection::<ProjectionEvalEntity>(
        &projection,
        std::slice::from_ref(&row),
    )
    .expect("projection materialization should succeed for one row");

    assert_eq!(
        projected_rows.len(),
        1,
        "projection payload should preserve row cardinality"
    );
    assert_eq!(
        projected_rows[0].id(),
        row.0,
        "projection payload should preserve row identity"
    );
    assert_eq!(
        projected_rows[0].values(),
        &[output(Value::Int(19))],
        "projection payload should preserve projection value ordering",
    );
}

#[cfg(feature = "sql")]
fn direct_rank_projection_shape_for_materialize_test() -> PreparedProjectionShape {
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

#[cfg(feature = "sql")]
#[test]
fn identity_data_row_materialization_visits_borrowed_row_views() {
    let row_layout = projection_eval_row_layout_for_materialize_tests();
    let rows = [
        projection_eval_data_row_for_materialize_tests(61, 3, true),
        projection_eval_data_row_for_materialize_tests(62, 5, false),
    ];

    let borrowed_rows =
        count_borrowed_identity_data_row_views_for_test(row_layout, rows.as_slice())
            .expect("identity data-row materialization should decode borrowed row views");

    assert_eq!(
        borrowed_rows,
        rows.len(),
        "identity data-row materialization should expose each row as a borrowed RowView",
    );
}

#[cfg(feature = "sql")]
#[test]
fn direct_data_row_materialization_visits_borrowed_row_views() {
    let row_layout = projection_eval_row_layout_for_materialize_tests();
    let prepared_projection = direct_rank_projection_shape_for_materialize_test();
    let rows = [
        projection_eval_data_row_for_materialize_tests(63, 7, true),
        projection_eval_data_row_for_materialize_tests(64, 11, false),
    ];

    let borrowed_rows =
        count_borrowed_data_row_views_for_test(row_layout, &prepared_projection, rows.as_slice())
            .expect("direct data-row materialization should decode borrowed row views");

    assert_eq!(
        borrowed_rows,
        rows.len(),
        "direct data-row materialization should expose each row as a borrowed RowView",
    );
}
