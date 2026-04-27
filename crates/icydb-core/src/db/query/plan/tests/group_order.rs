//! Module: db::query::plan::tests::group_order
//! Covers grouped `ORDER BY` admissibility rules that belong to the planner's
//! grouped contract boundary.
//! Does not own: scalar projection semantics outside grouped order handling.
//! Boundary: keeps grouped order policy over already-lowered expressions under
//! the planner `tests` boundary instead of leaf `expr` modules.

use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            AggregateKind,
            expr::{
                BinaryOp, Expr, FieldId, Function, GroupedOrderExprClass,
                GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility, UnaryOp,
                classify_grouped_order_term_for_field, classify_grouped_top_k_order_term,
                grouped_top_k_order_term_requires_heap,
            },
        },
    },
    value::Value,
};

fn field(name: &str) -> Expr {
    Expr::Field(FieldId::new(name))
}

fn int(value: i64) -> Expr {
    Expr::Literal(Value::Int(value))
}

fn function(function: Function, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall { function, args }
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn unary(op: UnaryOp, expr: Expr) -> Expr {
    Expr::Unary {
        op,
        expr: Box::new(expr),
    }
}

fn aggregate(kind: AggregateKind, expr: Expr) -> Expr {
    Expr::Aggregate(AggregateExpr::from_expression_input(kind, expr))
}

fn avg(expr: Expr) -> Expr {
    aggregate(AggregateKind::Avg, expr)
}

fn count_all_with_filter(filter_expr: Expr) -> Expr {
    Expr::Aggregate(
        AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false)
            .with_filter_expr(filter_expr),
    )
}

fn round(expr: Expr) -> Expr {
    function(Function::Round, vec![expr, int(2)])
}

fn wrapped_avg_score() -> Expr {
    function(
        Function::Coalesce,
        vec![
            function(Function::NullIf, vec![avg(field("score")), int(40)]),
            int(99),
        ],
    )
}

#[test]
fn grouped_order_classifier_accepts_canonical_group_field() {
    let expr = field("score");

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::CanonicalGroupField),
    );
    assert!(matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_accepts_group_field_plus_constant() {
    let expr = binary(BinaryOp::Add, field("score"), int(1));

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldPlusConstant),
    );
    assert!(matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_accepts_group_field_minus_constant() {
    let expr = binary(BinaryOp::Sub, field("score"), int(2));

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldMinusConstant),
    );
    assert!(matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_rejects_non_preserving_computed_order() {
    let expr = binary(BinaryOp::Add, field("score"), field("score"));

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::UnsupportedExpression,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_reports_prefix_mismatch_for_other_field() {
    let expr = binary(BinaryOp::Add, field("other_score"), int(1));

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::PrefixMismatch,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_rejects_wrapper_function_without_proof() {
    let expr = round(field("score"));

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::UnsupportedExpression,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_rejects_unary_wrapper_without_proof() {
    let expr = unary(UnaryOp::Not, field("score"));

    assert_eq!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::UnsupportedExpression,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field(&expr, "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_top_k_classifier_accepts_aggregate_leaf_terms() {
    let expr = avg(field("score"));

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_post_aggregate_round_terms() {
    let expr = round(avg(field("score")));

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_wrapped_post_aggregate_value_selection_terms() {
    let expr = wrapped_avg_score();

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_group_field_scalar_composition() {
    let expr = binary(BinaryOp::Add, field("score"), field("score"));

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_rejects_non_group_field_leaves() {
    let expr = binary(BinaryOp::Add, avg(field("score")), field("other_score"));

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["score"]),
        GroupedTopKOrderTermAdmissibility::NonGroupFieldReference,
    );
}

#[test]
fn grouped_top_k_classifier_rejects_unsupported_wrapper_functions() {
    let expr = function(Function::Lower, vec![field("score")]);

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["score"]),
        GroupedTopKOrderTermAdmissibility::UnsupportedExpression,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_filtered_aggregate_null_test_terms() {
    let expr = count_all_with_filter(function(Function::IsNotNull, vec![field("guild_rank")]));

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["class_name", "guild_rank"],),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_filtered_aggregate_null_test_boolean_compositions() {
    let expr = count_all_with_filter(binary(
        BinaryOp::And,
        function(Function::IsNotNull, vec![field("guild_rank")]),
        binary(BinaryOp::Gte, field("level"), int(10)),
    ));

    assert_eq!(
        classify_grouped_top_k_order_term(&expr, &["class_name", "guild_rank", "level"],),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_heap_gate_requires_aggregate_leaf() {
    assert!(grouped_top_k_order_term_requires_heap(&avg(field("score"))));
    assert!(grouped_top_k_order_term_requires_heap(&round(avg(field(
        "score"
    )))));
    assert!(!grouped_top_k_order_term_requires_heap(&binary(
        BinaryOp::Add,
        field("score"),
        field("score"),
    )));
    assert!(!grouped_top_k_order_term_requires_heap(&field("score")));
}
