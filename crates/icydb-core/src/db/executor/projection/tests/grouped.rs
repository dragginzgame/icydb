use super::*;

#[test]
fn grouped_projection_arithmetic_over_group_field_evaluates() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs: [GroupedAggregateExecutionSpec; 0] = [];
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Int(2))),
    };

    let value = eval_expr_grouped(&expr, &grouped_row).expect("grouped arithmetic should evaluate");
    assert_eq!(
        value.cmp_numeric(&Value::Int(9)),
        Some(Ordering::Equal),
        "grouped arithmetic projection should evaluate over grouped keys",
    );
}

#[test]
fn grouped_projection_supports_numeric_equality_widening() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs: [GroupedAggregateExecutionSpec; 0] = [];
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Literal(Value::Uint(7))),
    };

    let value = eval_expr_grouped(&expr, &grouped_row)
        .expect("grouped numeric equality should widen deterministically");
    assert_eq!(value, Value::Bool(true));
}

#[test]
fn grouped_projection_rejects_numeric_and_non_numeric_equality_mix() {
    let group_fields = [
        FieldSlot::from_parts_for_test(1, "rank"),
        FieldSlot::from_parts_for_test(2, "label"),
    ];
    let aggregate_execution_specs: [GroupedAggregateExecutionSpec; 0] = [];
    let key_values = [Value::Int(7), Value::Text("label-7".to_string())];
    let grouped_row = GroupedRowView::new(
        key_values.as_slice(),
        &[],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("rank"))),
        right: Box::new(Expr::Field(FieldId::new("label"))),
    };

    let err = eval_expr_grouped(&expr, &grouped_row)
        .expect_err("grouped mixed numeric/non-numeric equality should fail");
    assert!(matches!(
        err,
        crate::db::executor::projection::ProjectionEvalError::InvalidBinaryOperands { op, .. }
            if op == "eq"
    ));
}

#[test]
fn grouped_projection_mixing_aggregate_and_arithmetic_evaluates() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([sum("rank")]);
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[Value::Int(40)],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(sum("rank"))),
        right: Box::new(Expr::Literal(Value::Int(2))),
    };

    let value = eval_expr_grouped(&expr, &grouped_row)
        .expect("grouped aggregate arithmetic projection should evaluate");
    assert_eq!(
        value.cmp_numeric(&Value::Int(42)),
        Some(Ordering::Equal),
        "grouped projections must evaluate aggregate+scalar arithmetic deterministically",
    );
}

#[test]
fn grouped_projection_alias_wrapping_is_semantic_no_op() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([sum("rank")]);
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[Value::Int(40)],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let plain = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(sum("rank"))),
        right: Box::new(Expr::Literal(Value::Int(2))),
    };
    let aliased = Expr::Alias {
        expr: Box::new(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        }),
        name: Alias::new("sum_plus_two"),
    };

    let plain_value =
        eval_expr_grouped(&plain, &grouped_row).expect("plain grouped expression should work");
    let alias_value =
        eval_expr_grouped(&aliased, &grouped_row).expect("aliased grouped expression should work");
    assert_eq!(
        plain_value, alias_value,
        "grouped alias wrapping must not change expression values",
    );
}

#[test]
fn grouped_projection_column_order_is_stable() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([count(), sum("rank")]);
    let grouped_row = GroupedRowView::new(
        &[Value::Int(7)],
        &[Value::Uint(3), Value::Int(40)],
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    );
    let projection = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Aggregate(sum("rank")),
            alias: Some(Alias::new("sum_rank")),
        },
        ProjectionField::Scalar {
            expr: Expr::Aggregate(count()),
            alias: Some(Alias::new("count_all")),
        },
        ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(count())),
                right: Box::new(Expr::Literal(Value::Int(1))),
            },
            alias: Some(Alias::new("count_plus_one")),
        },
    ]);

    let compiled = compile_grouped_projection_plan(
        &projection,
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    )
    .expect("grouped projection should compile once");
    let values = evaluate_grouped_projection_values(compiled.as_slice(), &grouped_row)
        .expect("grouped projection vector should evaluate");

    assert_eq!(
        values.len(),
        3,
        "grouped projection must preserve declared field count",
    );
    assert_eq!(
        values[0].cmp_numeric(&Value::Int(40)),
        Some(Ordering::Equal),
        "first grouped projection output must follow projection declaration order",
    );
    assert_eq!(
        values[1].cmp_numeric(&Value::Uint(3)),
        Some(Ordering::Equal),
        "second grouped projection output must follow projection declaration order",
    );
    assert_eq!(
        values[2].cmp_numeric(&Value::Int(4)),
        Some(Ordering::Equal),
        "third grouped projection output must evaluate computed aggregate expression in order",
    );
}

#[test]
fn grouped_projection_ordering_preserves_input_group_order() {
    let group_fields = [FieldSlot::from_parts_for_test(1, "rank")];
    let aggregate_execution_specs = grouped_execution_specs([sum("rank")]);
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: Some(Alias::new("sum_plus_one")),
    }]);
    let grouped_inputs = vec![
        (vec![Value::Int(1)], vec![Value::Int(10)]),
        (vec![Value::Int(2)], vec![Value::Int(20)]),
        (vec![Value::Int(3)], vec![Value::Int(30)]),
    ];
    let mut observed = Vec::new();
    let compiled = compile_grouped_projection_plan(
        &projection,
        group_fields.as_slice(),
        aggregate_execution_specs.as_slice(),
    )
    .expect("grouped projection should compile once");
    for (key_values, aggregate_values) in grouped_inputs {
        let row_view = GroupedRowView::new(
            key_values.as_slice(),
            aggregate_values.as_slice(),
            group_fields.as_slice(),
            aggregate_execution_specs.as_slice(),
        );
        let evaluated = evaluate_grouped_projection_values(compiled.as_slice(), &row_view)
            .expect("grouped projection should evaluate per-row");
        observed.push(evaluated[0].clone());
    }

    let expected = [Value::Int(11), Value::Int(21), Value::Int(31)];
    for (actual, expected_value) in observed.into_iter().zip(expected) {
        assert_eq!(
            actual.cmp_numeric(&expected_value),
            Some(Ordering::Equal),
            "grouped projection evaluation order must preserve grouped row order",
        );
    }
}
