use crate::{
    db::query::{
        builder::aggregate::count,
        plan::expr::{BinaryOp, CaseWhenArm, Expr, Function, UnaryOp},
    },
    value::Value,
};

#[test]
fn planner_expression_owners_drop_deep_intermediates_on_a_small_stack() {
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(|| {
            for family in 0..5 {
                let mut expr = Expr::Literal(Value::Null);
                for _ in 0..20_000 {
                    expr = match family {
                        0 => Expr::Unary {
                            op: UnaryOp::Not,
                            expr: Box::new(expr),
                        },
                        1 => Expr::Binary {
                            op: BinaryOp::And,
                            left: Box::new(expr),
                            right: Box::new(Expr::Literal(Value::Bool(true))),
                        },
                        2 => Expr::Case {
                            when_then_arms: vec![CaseWhenArm::new(
                                expr,
                                Expr::Literal(Value::Null),
                            )],
                            else_expr: Box::new(Expr::Literal(Value::Null)),
                        },
                        3 => Expr::FunctionCall {
                            function: Function::Coalesce,
                            args: vec![expr],
                        },
                        _ => Expr::Aggregate(count().with_filter_expr(expr)),
                    };
                }
                drop(expr);
            }
            let mut value = Value::Null;
            for _ in 0..20_000 {
                value = Value::List(vec![value]);
            }
            drop(Expr::Literal(value));
        })
        .expect("small-stack expression owner probe")
        .join()
        .expect("iterative cleanup");
}
