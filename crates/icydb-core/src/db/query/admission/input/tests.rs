use super::*;
use crate::db::query::{
    expr::{CompareOperator, OrderTerm, SetOperator},
    plan::{
        AggregateKind, AggregateShape,
        expr::{CaseWhenArm, UnaryOp},
    },
};

fn nested_filter(depth: usize) -> FilterExpr {
    let mut filter = FilterExpr::Constant(true);
    for _ in 1..depth {
        filter = FilterExpr::Not(Box::new(filter));
    }
    filter
}

#[test]
fn input_depth_is_checked_before_descent() {
    for (depth, expected) in [
        (MAX_QUERY_INPUT_DEPTH, Ok(())),
        (
            MAX_QUERY_INPUT_DEPTH + 1,
            Err(QueryReadAdmissionCode::InputDepthExceeded),
        ),
    ] {
        let request = DynamicQuery::new("E").filter(nested_filter(depth));
        assert_eq!(validate_dynamic_query_input(&request), expected);
    }
}

#[test]
fn input_nodes_include_cumulative_fields_and_membership_values() {
    let request = |count| {
        DynamicQuery::new("E").filter(FilterExpr::Set {
            operator: SetOperator::In,
            field: "id".into(),
            values: vec![FilterValue::Null; count],
        })
    };
    // Entity, filter and field consume three nodes before the membership values.
    assert_eq!(
        validate_dynamic_query_input(&request(MAX_QUERY_INPUT_NODES - 3)),
        Ok(())
    );
    assert_eq!(
        validate_dynamic_query_input(&request(MAX_QUERY_INPUT_NODES - 2)),
        Err(QueryReadAdmissionCode::InputNodesExceeded)
    );
    let combined = request(MAX_QUERY_INPUT_NODES - 3).select(["id"]);
    assert_eq!(
        validate_dynamic_query_input(&combined),
        Err(QueryReadAdmissionCode::InputNodesExceeded)
    );
}

#[test]
fn input_bytes_are_cumulative_content_not_allocation_capacity() {
    let request = |count| DynamicQuery::new("E").filter(FilterExpr::eq("id", "x".repeat(count)));
    assert_eq!(
        validate_dynamic_query_input(&request(MAX_QUERY_INPUT_BYTES - 3)),
        Ok(())
    );
    assert_eq!(
        validate_dynamic_query_input(&request(MAX_QUERY_INPUT_BYTES - 2)),
        Err(QueryReadAdmissionCode::InputBytesExceeded)
    );
    let mut field = String::with_capacity(MAX_QUERY_INPUT_BYTES + 1);
    field.push('x');
    assert_eq!(
        validate_dynamic_query_input(&DynamicQuery::new("E").select([field])),
        Ok(())
    );
    let combined = request(MAX_QUERY_INPUT_BYTES - 3).cursor("x");
    assert_eq!(
        validate_dynamic_query_input(&combined),
        Err(QueryReadAdmissionCode::InputBytesExceeded)
    );
}

#[test]
fn admission_inspects_branches_that_simplification_would_discard() {
    let request = DynamicQuery::new("E").filter(FilterExpr::and(vec![
        FilterExpr::Constant(false),
        nested_filter(MAX_QUERY_INPUT_DEPTH),
    ]));
    assert_eq!(
        validate_dynamic_query_input(&request),
        Err(QueryReadAdmissionCode::InputDepthExceeded)
    );
}

#[test]
fn nested_filter_values_share_expression_depth() {
    let mut value = FilterValue::Null;
    for _ in 1..MAX_QUERY_INPUT_DEPTH {
        value = FilterValue::List(vec![value]);
    }
    let request = DynamicQuery::new("E").filter(FilterExpr::Compare {
        operator: CompareOperator::Eq,
        field: "id".into(),
        value,
    });
    assert_eq!(
        validate_dynamic_query_input(&request),
        Err(QueryReadAdmissionCode::InputDepthExceeded)
    );
}

#[test]
fn ordering_and_aggregate_children_share_the_input_budget() {
    let aggregate = AggregateExpr::from_shape(
        AggregateShape::terminal(AggregateKind::Count).with_filter_expr(Expr::Literal(
            Value::Text("x".repeat(MAX_QUERY_INPUT_BYTES)),
        )),
    );
    let request = DynamicQuery::new("E").aggregate(aggregate.clone());
    assert_eq!(
        validate_dynamic_query_input(&request),
        Err(QueryReadAdmissionCode::InputBytesExceeded)
    );
    let request = DynamicQuery::new("E").order_by(OrderTerm::asc(aggregate));
    assert_eq!(
        validate_dynamic_query_input(&request),
        Err(QueryReadAdmissionCode::InputBytesExceeded)
    );
}

#[test]
fn runtime_values_charge_nested_keys_payloads_and_wide_containers() {
    let mut budget = QueryInputBudget::new();
    let value = Value::Map(vec![(
        Value::Text("x".repeat(MAX_QUERY_INPUT_BYTES)),
        Value::Enum(crate::value::ValueEnum::test_payload(
            1,
            1,
            Value::Text("x".into()),
        )),
    )]);
    assert_eq!(
        budget.value(&value, 1),
        Err(QueryReadAdmissionCode::InputBytesExceeded)
    );
    assert_eq!(
        QueryInputBudget::new().value(&Value::List(vec![Value::Null; MAX_QUERY_INPUT_NODES]), 1),
        Err(QueryReadAdmissionCode::InputNodesExceeded)
    );
}

#[test]
fn rejected_owned_requests_drop_without_recursive_stack_growth() {
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(|| {
            let request = DynamicQuery::new("E").filter(nested_filter(20_000));
            assert_eq!(
                validate_dynamic_query_input(&request),
                Err(QueryReadAdmissionCode::InputDepthExceeded)
            );
            drop(request);

            let mut value = FilterValue::Null;
            for _ in 0..20_000 {
                value = FilterValue::List(vec![value]);
            }
            let request = DynamicQuery::new("E").filter(FilterExpr::Compare {
                operator: CompareOperator::Eq,
                field: "id".into(),
                value,
            });
            assert_eq!(
                validate_dynamic_query_input(&request),
                Err(QueryReadAdmissionCode::InputDepthExceeded)
            );
            drop(request);

            // Exercise owned order/aggregate syntax and CASE child detachment too.
            let mut expr = Expr::Literal(Value::Null);
            for _ in 0..10_000 {
                expr = Expr::Case {
                    when_then_arms: vec![CaseWhenArm::new(Expr::Literal(Value::Bool(true)), expr)],
                    else_expr: Box::new(Expr::Literal(Value::Null)),
                };
            }
            let aggregate = AggregateExpr::from_shape(
                AggregateShape::terminal(AggregateKind::Count).with_filter_expr(expr),
            );
            let request = DynamicQuery::new("E").order_by(OrderTerm::asc(aggregate));
            assert_eq!(
                validate_dynamic_query_input(&request),
                Err(QueryReadAdmissionCode::InputDepthExceeded)
            );
            drop(request);

            let mut value = Value::Null;
            for _ in 0..20_000 {
                value = Value::List(vec![value]);
            }
            let expr = Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Literal(value)),
            };
            let aggregate = AggregateExpr::from_shape(
                AggregateShape::terminal(AggregateKind::Count).with_filter_expr(expr),
            );
            let request = DynamicQuery::new("E").aggregate(aggregate);
            assert_eq!(
                validate_dynamic_query_input(&request),
                Err(QueryReadAdmissionCode::InputDepthExceeded)
            );
            drop(request);
        })
        .expect("spawn small-stack rejection probe")
        .join()
        .expect("bounded rejection and owned cleanup");
}
