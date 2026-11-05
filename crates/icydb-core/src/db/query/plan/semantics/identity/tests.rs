use crate::value::Value;

use super::*;

#[test]
fn borrowed_semantic_keys_preserve_owned_identity_and_filter_equivalence() {
    use crate::db::query::plan::AggregateShape;

    let mut expressions = Vec::new();
    for kind in [
        AggregateKind::Count,
        AggregateKind::Sum,
        AggregateKind::Avg,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::Exists,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        for distinct in [false, true] {
            for input in [
                None,
                Some(Expr::Literal(Value::Null)),
                Some(Expr::Literal(Value::Nat64(1))),
                Some(Expr::Literal(Value::Nat64(2))),
                Some(Expr::Field("amount".into())),
                Some(Expr::Field("rank".into())),
            ] {
                for filter in [
                    None,
                    Some(Expr::Literal(Value::Bool(true))),
                    Some(Expr::Literal(Value::Bool(false))),
                ] {
                    let mut shape = input
                        .clone()
                        .map_or_else(
                            || AggregateShape::terminal(kind),
                            |input| AggregateShape::from_expression_input(kind, input),
                        )
                        .with_raw_distinct(distinct);
                    if let Some(filter) = filter {
                        shape = shape.with_filter_expr(filter);
                    }
                    expressions.push(AggregateExpr::from_shape(shape));
                }
            }
        }
    }
    let owned: Vec<_> = expressions
        .iter()
        .map(AggregateSemanticKey::from_aggregate_expr)
        .collect();
    for (left_expr, left) in expressions.iter().zip(&owned) {
        let borrowed = AggregateSemanticKeyRef::from_aggregate_expr(left_expr);
        assert_eq!(borrowed, left.as_ref());
        // Canonical views share retained expression nodes; normalization
        // may omit COUNT's input, but never copies or rewrites the source.
        if let Some(input) = borrowed.input_expr {
            assert!(std::ptr::eq(input, left_expr.input_expr().unwrap()));
        }
        if let Some(filter) = borrowed.filter_expr {
            assert!(std::ptr::eq(filter, left_expr.filter_expr().unwrap()));
        }
        for (right_expr, right) in expressions.iter().zip(&owned) {
            let expected = left.identity == right.identity && left.filter_expr == right.filter_expr;
            assert_eq!(
                borrowed == AggregateSemanticKeyRef::from_aggregate_expr(right_expr),
                expected
            );
            assert_eq!(left == right, expected);
        }
    }
}

#[test]
fn aggregate_identity_normalizes_only_non_distinct_count_non_null_literals() {
    let literal_count = AggregateIdentity::from_kind_input_and_distinct(
        AggregateKind::Count,
        Some(Expr::Literal(Value::Nat64(1))),
        false,
    );
    let null_count = AggregateIdentity::from_kind_input_and_distinct(
        AggregateKind::Count,
        Some(Expr::Literal(Value::Null)),
        false,
    );
    let distinct_literal_count = AggregateIdentity::from_kind_input_and_distinct(
        AggregateKind::Count,
        Some(Expr::Literal(Value::Nat64(1))),
        true,
    );

    assert!(matches!(
        literal_count,
        AggregateIdentity::Count {
            input_expr: None,
            distinct: false,
        }
    ));
    assert!(matches!(
        null_count.input_expr(),
        Some(Expr::Literal(Value::Null))
    ));
    assert!(matches!(
        distinct_literal_count,
        AggregateIdentity::Count {
            input_expr: Some(Expr::Literal(Value::Nat64(1))),
            distinct: true
        }
    ));
}
