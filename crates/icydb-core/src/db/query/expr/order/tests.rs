use super::{OrderExpr, OrderTerm};
use crate::{
    db::query::{
        builder::sum,
        plan::{OrderDirection, expr::Expr},
    },
    value::Value,
};

#[test]
fn owned_order_lowering_preserves_operands_and_direction() {
    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        let aggregate = sum("amount")
            .with_filter_expr(Expr::Literal(Value::Text("x".repeat(4096))))
            .distinct();
        let input_address = std::ptr::from_ref(aggregate.input_expr().expect("input"));
        let filter_address = std::ptr::from_ref(aggregate.filter_expr().expect("filter"));
        let expected = Expr::Aggregate(aggregate.clone());
        let term = OrderTerm {
            expr: OrderExpr::from(aggregate),
            direction,
        };

        let lowered = term.lower();
        assert_eq!(lowered.direction(), direction);
        assert_eq!(lowered.expr(), &expected);
        let Expr::Aggregate(aggregate) = lowered.expr() else {
            panic!("aggregate order expression");
        };
        assert_eq!(
            std::ptr::from_ref(aggregate.input_expr().expect("input")),
            input_address
        );
        assert_eq!(
            std::ptr::from_ref(aggregate.filter_expr().expect("filter")),
            filter_address
        );
    }
}
