//! Module: db::query::plan::tests::group_order
//! Covers grouped `ORDER BY` parsing and admissibility rules that belong to
//! the planner's grouped contract boundary.
//! Does not own: scalar projection semantics outside grouped order handling.
//! Boundary: keeps grouped order policy and grouped post-aggregate expression
//! reconstruction under the planner `tests/` boundary instead of leaf `expr`
//! modules.

use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            AggregateKind,
            expr::{
                BinaryOp, CaseWhenArm, Expr, FieldId, Function, GroupedOrderExprClass,
                GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility, UnaryOp,
                classify_grouped_order_term_for_field, classify_grouped_top_k_order_term,
                grouped_top_k_order_term_requires_heap, parse_grouped_post_aggregate_order_expr,
                parse_supported_order_expr,
            },
        },
    },
    value::Value,
};

fn parse(expr: &str) -> Expr {
    parse_supported_order_expr(expr)
        .expect("supported grouped ORDER BY test expression should parse")
}

fn parse_top_k(expr: &str) -> Expr {
    parse_grouped_post_aggregate_order_expr(expr)
        .expect("supported grouped Top-K ORDER BY test expression should parse")
}

#[test]
fn grouped_order_parser_preserves_expression_aggregate_input_shape() {
    let expr = parse_grouped_post_aggregate_order_expr("ROUND(AVG(rank + score), 2)")
        .expect("grouped order expression with aggregate input should parse");

    assert_eq!(
        expr,
        Expr::FunctionCall {
            function: Function::Round,
            args: vec![
                Expr::Aggregate(AggregateExpr::from_expression_input(
                    AggregateKind::Avg,
                    Expr::Binary {
                        op: BinaryOp::Add,
                        left: Box::new(Expr::Field(FieldId::new("rank"))),
                        right: Box::new(Expr::Field(FieldId::new("score"))),
                    },
                )),
                Expr::Literal(Value::Int(2)),
            ],
        },
        "aggregate input expressions should stay on the planner expression spine instead of collapsing back to one field-only target",
    );
}

#[test]
fn grouped_order_parser_preserves_parenthesized_expression_aggregate_input_shape() {
    let expr = parse_grouped_post_aggregate_order_expr("ROUND(AVG((rank + score) / 2), 2)")
        .expect("grouped order expression with parenthesized aggregate input should parse");

    assert_eq!(
        expr,
        Expr::FunctionCall {
            function: Function::Round,
            args: vec![
                Expr::Aggregate(AggregateExpr::from_expression_input(
                    AggregateKind::Avg,
                    Expr::Binary {
                        op: BinaryOp::Div,
                        left: Box::new(Expr::Binary {
                            op: BinaryOp::Add,
                            left: Box::new(Expr::Field(FieldId::new("rank"))),
                            right: Box::new(Expr::Field(FieldId::new("score"))),
                        }),
                        right: Box::new(Expr::Literal(Value::Int(2))),
                    },
                )),
                Expr::Literal(Value::Int(2)),
            ],
        },
        "parenthesized aggregate-input arithmetic should preserve nested grouped order expression structure",
    );
}

#[test]
fn grouped_order_parser_preserves_case_aggregate_input_shape() {
    let expr =
        parse_grouped_post_aggregate_order_expr("SUM(CASE WHEN level >= 20 THEN 1 ELSE 0 END)")
            .expect("grouped order expression with searched CASE aggregate input should parse");

    assert_eq!(
        expr,
        Expr::Aggregate(AggregateExpr::from_expression_input(
            AggregateKind::Sum,
            Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(
                    Expr::Binary {
                        op: BinaryOp::Gte,
                        left: Box::new(Expr::Field(FieldId::new("level"))),
                        right: Box::new(Expr::Literal(Value::Int(20))),
                    },
                    Expr::Literal(Value::Int(1)),
                )],
                else_expr: Box::new(Expr::Literal(Value::Int(0))),
            },
        )),
        "searched CASE aggregate inputs should stay on the grouped post-aggregate order expression spine instead of collapsing to an unknown field label",
    );
}

#[test]
fn grouped_order_parser_preserves_filtered_aggregate_shape() {
    let expr = parse_grouped_post_aggregate_order_expr("COUNT(*) FILTER (WHERE age >= 20)")
        .expect("grouped order expression with filtered aggregate should parse");

    assert_eq!(
        expr,
        Expr::Aggregate(
            AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false).with_filter_expr(
                Expr::Binary {
                    op: BinaryOp::Gte,
                    left: Box::new(Expr::Field(FieldId::new("age"))),
                    right: Box::new(Expr::Literal(Value::Int(20))),
                },
            ),
        ),
        "filtered grouped aggregate terms should preserve FILTER semantics instead of collapsing back to a bare aggregate shell",
    );
}

#[test]
fn grouped_order_parser_preserves_filtered_unary_not_aggregate_shape() {
    let expr = parse_grouped_post_aggregate_order_expr("SUM(strength) FILTER (WHERE NOT is_npc)")
        .expect("grouped order expression with filtered unary-not aggregate should parse");

    assert_eq!(
        expr,
        Expr::Aggregate(
            AggregateExpr::from_expression_input(
                AggregateKind::Sum,
                Expr::Field(FieldId::new("strength")),
            )
            .with_filter_expr(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Field(FieldId::new("is_npc"))),
            }),
        ),
        "filtered grouped aggregate terms should preserve unary NOT filter semantics instead of collapsing back to an unknown field label",
    );
}

#[test]
fn grouped_order_parser_preserves_filtered_null_test_aggregate_shape() {
    let expr =
        parse_grouped_post_aggregate_order_expr("COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))")
            .expect("grouped order expression with filtered null-test aggregate should parse");

    assert_eq!(
        expr,
        Expr::Aggregate(
            AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false).with_filter_expr(
                Expr::FunctionCall {
                    function: Function::IsNotNull,
                    args: vec![Expr::Field(FieldId::new("guild_rank"))],
                },
            ),
        ),
        "filtered grouped aggregate terms should preserve null-test FILTER semantics instead of collapsing back to an unknown field label",
    );
}

#[test]
fn grouped_order_parser_preserves_filtered_null_test_boolean_composition_shape() {
    let expr = parse_grouped_post_aggregate_order_expr(
        "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank) AND level >= 10)",
    )
    .expect("grouped order expression with filtered null-test boolean composition should parse");

    assert_eq!(
        expr,
        Expr::Aggregate(
            AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false).with_filter_expr(
                Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(Expr::FunctionCall {
                        function: Function::IsNotNull,
                        args: vec![Expr::Field(FieldId::new("guild_rank"))],
                    }),
                    right: Box::new(Expr::Binary {
                        op: BinaryOp::Gte,
                        left: Box::new(Expr::Field(FieldId::new("level"))),
                        right: Box::new(Expr::Literal(Value::Int(10))),
                    }),
                },
            ),
        ),
        "filtered grouped aggregate terms should preserve null-test boolean composition semantics through grouped order parsing",
    );
}

#[test]
fn grouped_order_parser_preserves_wrapped_post_aggregate_value_selection_shape() {
    let expr = parse_grouped_post_aggregate_order_expr("COALESCE(NULLIF(AVG(score), 40), 99)")
        .expect(
            "grouped order expression with wrapped post-aggregate value selection should parse",
        );

    assert_eq!(
        expr,
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![
                Expr::FunctionCall {
                    function: Function::NullIf,
                    args: vec![
                        Expr::Aggregate(AggregateExpr::from_expression_input(
                            AggregateKind::Avg,
                            Expr::Field(FieldId::new("score")),
                        )),
                        Expr::Literal(Value::Int(40)),
                    ],
                },
                Expr::Literal(Value::Int(99)),
            ],
        },
        "wrapped post-aggregate value-selection terms should stay on the grouped order expression spine",
    );
}

#[test]
fn grouped_order_classifier_accepts_canonical_group_field() {
    let _expr = parse("score");

    assert_eq!(
        classify_grouped_order_term_for_field("score", "score"),
        GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::CanonicalGroupField),
    );
    assert!(matches!(
        classify_grouped_order_term_for_field("score", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_accepts_group_field_plus_constant() {
    let _expr = parse("score + 1");

    assert_eq!(
        classify_grouped_order_term_for_field("score + 1", "score"),
        GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldPlusConstant),
    );
    assert!(matches!(
        classify_grouped_order_term_for_field("score + 1", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_accepts_group_field_minus_constant() {
    let _expr = parse("score - 2");

    assert_eq!(
        classify_grouped_order_term_for_field("score - 2", "score"),
        GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldMinusConstant),
    );
    assert!(matches!(
        classify_grouped_order_term_for_field("score - 2", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_rejects_non_preserving_computed_order() {
    let _expr = parse("score + score");

    assert_eq!(
        classify_grouped_order_term_for_field("score + score", "score"),
        GroupedOrderTermAdmissibility::UnsupportedExpression,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field("score + score", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_reports_prefix_mismatch_for_other_field() {
    let _expr = parse("other_score + 1");

    assert_eq!(
        classify_grouped_order_term_for_field("other_score + 1", "score"),
        GroupedOrderTermAdmissibility::PrefixMismatch,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field("other_score + 1", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_rejects_wrapper_function_without_proof() {
    let _expr = parse("ROUND(score, 2)");

    assert_eq!(
        classify_grouped_order_term_for_field("ROUND(score, 2)", "score"),
        GroupedOrderTermAdmissibility::UnsupportedExpression,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field("ROUND(score, 2)", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_order_classifier_rejects_unary_wrapper_without_proof() {
    let _expr = parse("NOT score");

    assert_eq!(
        classify_grouped_order_term_for_field("NOT score", "score"),
        GroupedOrderTermAdmissibility::UnsupportedExpression,
    );
    assert!(!matches!(
        classify_grouped_order_term_for_field("NOT score", "score"),
        GroupedOrderTermAdmissibility::Preserves(_)
    ));
}

#[test]
fn grouped_top_k_classifier_accepts_aggregate_leaf_terms() {
    let _expr = parse_top_k("AVG(score)");

    assert_eq!(
        classify_grouped_top_k_order_term("AVG(score)", &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_post_aggregate_round_terms() {
    let _expr = parse_top_k("ROUND(AVG(score), 2)");

    assert_eq!(
        classify_grouped_top_k_order_term("ROUND(AVG(score), 2)", &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_wrapped_post_aggregate_value_selection_terms() {
    let _expr = parse_top_k("COALESCE(NULLIF(AVG(score), 40), 99)");

    assert_eq!(
        classify_grouped_top_k_order_term("COALESCE(NULLIF(AVG(score), 40), 99)", &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_group_field_scalar_composition() {
    let _expr = parse_top_k("score + score");

    assert_eq!(
        classify_grouped_top_k_order_term("score + score", &["score"]),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_rejects_non_group_field_leaves() {
    let _expr = parse_top_k("AVG(score) + other_score");

    assert_eq!(
        classify_grouped_top_k_order_term("AVG(score) + other_score", &["score"]),
        GroupedTopKOrderTermAdmissibility::NonGroupFieldReference,
    );
}

#[test]
fn grouped_top_k_classifier_rejects_unsupported_wrapper_functions() {
    assert_eq!(
        classify_grouped_top_k_order_term("LOWER(score)", &["score"]),
        GroupedTopKOrderTermAdmissibility::UnsupportedExpression,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_filtered_aggregate_null_test_terms() {
    let _expr = parse_top_k("COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))");

    assert_eq!(
        classify_grouped_top_k_order_term(
            "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))",
            &["class_name", "guild_rank"],
        ),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_classifier_accepts_filtered_aggregate_null_test_boolean_compositions() {
    let _expr = parse_top_k("COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank) AND level >= 10)");

    assert_eq!(
        classify_grouped_top_k_order_term(
            "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank) AND level >= 10)",
            &["class_name", "guild_rank", "level"],
        ),
        GroupedTopKOrderTermAdmissibility::Admissible,
    );
}

#[test]
fn grouped_top_k_heap_gate_requires_aggregate_leaf() {
    assert!(grouped_top_k_order_term_requires_heap("AVG(score)"));
    assert!(grouped_top_k_order_term_requires_heap(
        "ROUND(AVG(score), 2)"
    ));
    assert!(!grouped_top_k_order_term_requires_heap("score + score"));
    assert!(!grouped_top_k_order_term_requires_heap("score"));
}
