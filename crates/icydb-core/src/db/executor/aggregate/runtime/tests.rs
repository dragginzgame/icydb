use crate::{
    db::{
        executor::{
            aggregate::runtime::group_matches_having_expr,
            projection::{GroupedRowView, compile_grouped_projection_expr},
        },
        query::{
            builder::aggregate::AggregateExpr,
            plan::{
                AggregateKind, FieldSlot, GroupedAggregateExecutionSpec,
                expr::{BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp},
            },
        },
    },
    types::Decimal,
    value::Value,
};

#[test]
fn grouped_having_runtime_accepts_post_aggregate_round_compare() {
    let expr = Expr::Binary {
        op: BinaryOp::Gte,
        left: Box::new(Expr::FunctionCall {
            function: Function::Round,
            args: vec![
                Expr::Aggregate(AggregateExpr::terminal_for_kind(AggregateKind::Count)),
                Expr::Literal(Value::Uint(2)),
            ],
        }),
        right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(1000, 2)))),
    };
    let specs = [GroupedAggregateExecutionSpec::from_parts_for_test(
        AggregateKind::Count,
        None,
        None,
        false,
    )];
    let compiled = compile_grouped_projection_expr(&expr, &[], &specs)
        .expect("grouped HAVING ROUND compare should compile");
    let aggregate_values = [Value::Decimal(Decimal::new(10049, 3))];
    let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
    let matched = group_matches_having_expr(&compiled, &grouped_row)
        .expect("grouped HAVING ROUND compare should evaluate");

    assert!(matched);
}

#[test]
fn grouped_having_runtime_accepts_post_aggregate_arithmetic_compare() {
    let aggregate_expr = AggregateExpr::terminal_for_kind(AggregateKind::Count);
    let expr = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(aggregate_expr)),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        }),
        right: Box::new(Expr::Literal(Value::Uint(5))),
    };
    let specs = [GroupedAggregateExecutionSpec::from_parts_for_test(
        AggregateKind::Count,
        None,
        None,
        false,
    )];
    let compiled = compile_grouped_projection_expr(&expr, &[], &specs)
        .expect("grouped HAVING arithmetic compare should compile");
    let aggregate_values = [Value::Uint(5)];
    let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
    let matched = group_matches_having_expr(&compiled, &grouped_row)
        .expect("grouped HAVING arithmetic compare should evaluate");

    assert!(matched);
}

#[test]
fn grouped_having_runtime_accepts_and_over_group_keys_and_aggregates() {
    let group_field = FieldSlot::from_parts_for_test(1, "class_name");
    let aggregate_expr = AggregateExpr::terminal_for_kind(AggregateKind::Count);
    let expr = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new(group_field.field()))),
            right: Box::new(Expr::Literal(Value::Text("Mage".to_string()))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Aggregate(aggregate_expr)),
            right: Box::new(Expr::Literal(Value::Uint(10))),
        }),
    };

    let group_fields = [group_field];
    let specs = [GroupedAggregateExecutionSpec::from_parts_for_test(
        AggregateKind::Count,
        None,
        None,
        false,
    )];
    let compiled = compile_grouped_projection_expr(&expr, &group_fields, &specs)
        .expect("grouped HAVING AND expression should compile");
    let group_key_values = [Value::Text("Mage".to_string())];
    let aggregate_values = [Value::Uint(11)];
    let grouped_row = GroupedRowView::new(&group_key_values, &aggregate_values, &group_fields, &[]);
    let matched = group_matches_having_expr(&compiled, &grouped_row)
        .expect("grouped HAVING AND expression should evaluate");

    assert!(matched);
}

#[test]
fn grouped_having_runtime_accepts_post_aggregate_case_and_not() {
    let aggregate_expr = AggregateExpr::terminal_for_kind(AggregateKind::Count);
    let expr = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(Expr::Literal(Value::Bool(false))),
                },
                Expr::Aggregate(aggregate_expr),
            )],
            else_expr: Box::new(Expr::Literal(Value::Uint(0))),
        }),
        right: Box::new(Expr::Literal(Value::Uint(5))),
    };
    let specs = [GroupedAggregateExecutionSpec::from_parts_for_test(
        AggregateKind::Count,
        None,
        None,
        false,
    )];
    let compiled = compile_grouped_projection_expr(&expr, &[], &specs)
        .expect("grouped HAVING CASE should compile");
    let aggregate_values = [Value::Uint(6)];
    let grouped_row = GroupedRowView::new(&[], &aggregate_values, &[], &[]);
    let matched = group_matches_having_expr(&compiled, &grouped_row)
        .expect("grouped HAVING CASE should evaluate");

    assert!(matched);
}
