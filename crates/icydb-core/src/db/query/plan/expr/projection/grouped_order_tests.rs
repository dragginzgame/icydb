//! Grouped ordering keeps shallow key proofs separate from full Top-K admission.

use super::*;
use crate::db::{
    query::{
        builder::aggregate::{count as count_expr, sum},
        plan::{
            FieldSlot, GroupField,
            expr::{CaseWhenArm, FieldPath, Function, UnaryOp},
        },
    },
    schema::AcceptedFieldKind,
};

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn abs(expr: Expr) -> Expr {
    Expr::FunctionCall {
        function: Function::Abs,
        args: vec![expr],
    }
}

fn count() -> Expr {
    Expr::Aggregate(count_expr())
}

fn direct_fields() -> GroupFieldSet {
    GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
        0,
        "key",
        AcceptedFieldKind::Int32,
    )])
}

#[test]
fn grouped_order_canonical_proof_keeps_field_and_offset_rejections_distinct() {
    let direct = direct_fields();
    let paths = GroupFieldSet::PathAware(vec![GroupField::scalar_path_for_test(
        "profile.key",
        "profile",
        vec!["key".into()],
        1,
        AcceptedFieldKind::Text { max_len: Some(64) },
    )]);
    for (fields, field, other) in [
        (
            direct,
            Expr::Field("key".into()),
            Expr::Field("other".into()),
        ),
        (
            paths,
            Expr::FieldPath(FieldPath::new("profile", vec!["key".into()])),
            Expr::FieldPath(FieldPath::new("profile", vec!["other".into()])),
        ),
    ] {
        let expected = fields.get(0).unwrap();
        for (expr, admission) in [
            (
                field.clone(),
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::CanonicalGroupField,
                ),
            ),
            (other.clone(), GroupedOrderTermAdmissibility::PrefixMismatch),
            (
                binary(BinaryOp::Add, field.clone(), Expr::Literal(Value::Nat64(1))),
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::GroupFieldPlusConstant,
                ),
            ),
            (
                binary(
                    BinaryOp::Sub,
                    field.clone(),
                    Expr::Literal(Value::Int64(-1)),
                ),
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::GroupFieldMinusConstant,
                ),
            ),
            (
                binary(BinaryOp::Add, other.clone(), Expr::Literal(Value::Nat64(1))),
                GroupedOrderTermAdmissibility::PrefixMismatch,
            ),
            (
                binary(BinaryOp::Sub, other, Expr::Literal(Value::Text("1".into()))),
                GroupedOrderTermAdmissibility::UnsupportedExpression,
            ),
            (
                binary(BinaryOp::Mul, field.clone(), Expr::Literal(Value::Nat64(1))),
                GroupedOrderTermAdmissibility::UnsupportedExpression,
            ),
            (
                binary(BinaryOp::Add, Expr::Literal(Value::Nat64(1)), field.clone()),
                GroupedOrderTermAdmissibility::UnsupportedExpression,
            ),
            (
                abs(field),
                GroupedOrderTermAdmissibility::UnsupportedExpression,
            ),
        ] {
            assert_eq!(
                classify_grouped_order_term_for_field(&expr, expected),
                admission
            );
        }
    }
}

#[test]
fn grouped_order_top_k_checks_siblings_after_the_heap_trigger() {
    let fields = direct_fields();
    let key = Expr::Field("key".into());
    let outside = Expr::Field("amount".into());
    let case = |result| Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(Expr::Literal(Value::Bool(true)), result)],
        else_expr: Box::new(key.clone()),
    };
    for (expr, heap, admission) in [
        (
            key.clone(),
            false,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
        (count(), true, GroupedTopKOrderTermAdmissibility::Admissible),
        (
            abs(key.clone()),
            false,
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression,
        ),
        (
            abs(count()),
            true,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
        (
            binary(BinaryOp::Add, count(), abs(key.clone())),
            true,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
        (
            abs(binary(BinaryOp::Add, abs(key.clone()), count())),
            true,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
        (
            abs(binary(
                BinaryOp::Add,
                abs(key.clone()),
                Expr::Literal(Value::Nat64(1)),
            )),
            false,
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression,
        ),
        (
            Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![],
            },
            false,
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression,
        ),
        (
            binary(BinaryOp::Add, count(), outside.clone()),
            true,
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference,
        ),
        (
            binary(BinaryOp::Add, outside.clone(), count()),
            true,
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference,
        ),
        (
            case(key.clone()),
            true,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
        (
            case(outside.clone()),
            true,
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference,
        ),
        (
            Expr::Aggregate(sum("amount").with_filter_expr(outside)),
            true,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
        (
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(count()),
            },
            true,
            GroupedTopKOrderTermAdmissibility::Admissible,
        ),
    ] {
        assert_eq!(grouped_top_k_order_term_requires_heap(&expr), heap);
        assert_eq!(classify_grouped_top_k_order_term(&expr, &fields), admission);
    }
}

#[test]
fn grouped_order_nested_terms_keep_narrow_canonical_and_broad_top_k_proofs() {
    let fields = direct_fields();
    for leaf in [Expr::Field("key".into()), count()] {
        let heap = matches!(leaf, Expr::Aggregate(_));
        let mut expr = leaf;
        for _ in 1..crate::db::query::admission::input::MAX_QUERY_INPUT_DEPTH {
            expr = binary(BinaryOp::Add, expr, Expr::Literal(Value::Nat64(1)));
        }
        assert_eq!(grouped_top_k_order_term_requires_heap(&expr), heap);
        assert_eq!(
            classify_grouped_order_term_for_field(&expr, fields.get(0).unwrap()),
            GroupedOrderTermAdmissibility::UnsupportedExpression,
        );
        assert_eq!(
            classify_grouped_top_k_order_term(&expr, &fields),
            GroupedTopKOrderTermAdmissibility::Admissible,
        );
    }
}
