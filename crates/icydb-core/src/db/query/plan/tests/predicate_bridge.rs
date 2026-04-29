//! Module: db::query::plan::tests::predicate_bridge
//! Covers the planner-owned runtime-predicate bridge and compiler behavior.
//! Does not own: predicate module API shape or lowering-specific WHERE rules.
//! Boundary: keeps predicate-bridge regression coverage on the planner owner
//! `tests/` boundary instead of under a predicate-side test-only shim module.

use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, ComparePredicate, Predicate},
        query::plan::expr::{
            BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp,
            canonicalize_grouped_having_bool_expr, canonicalize_runtime_predicate_via_bool_expr,
            canonicalize_scalar_where_bool_expr, compile_normalized_bool_expr_to_predicate,
            is_normalized_bool_expr, normalize_bool_expr, predicate_to_runtime_bool_expr_for_test,
        },
    },
    value::{Value, ValueEnum},
};

#[test]
fn predicate_bridge_roundtrip_covers_every_live_predicate_variant() {
    for predicate in representative_predicates() {
        let expr = predicate_to_runtime_bool_expr_for_test(&predicate);
        assert!(
            expr_has_no_opaque_nodes(&expr),
            "predicate lowered through opaque expr shape: {predicate:?}"
        );

        let normalized = normalize_bool_expr(expr);
        assert!(
            is_normalized_bool_expr(&normalized),
            "predicate did not lower to normalized bool expr: {predicate:?}"
        );

        let round_tripped = compile_normalized_bool_expr_to_predicate(&normalized);
        let rerendered = predicate_to_runtime_bool_expr_for_test(&round_tripped);
        assert!(
            expr_has_no_opaque_nodes(&rerendered),
            "round-tripped predicate reintroduced opaque expr shape: {round_tripped:?}"
        );
    }
}

#[test]
fn predicate_bridge_roundtrip_is_idempotent() {
    for predicate in representative_predicates() {
        let once = canonicalize_runtime_predicate_via_bool_expr(predicate.clone());
        let twice = canonicalize_runtime_predicate_via_bool_expr(once.clone());

        assert_eq!(
            twice, once,
            "predicate bridge was not idempotent: {predicate:?}"
        );
        assert_eq!(
            canonicalize_runtime_predicate_via_bool_expr(predicate),
            once,
            "predicate-facing canonicalizer should stay a thin wrapper over the planner-owned bridge",
        );
    }
}

#[test]
fn predicate_bridge_canonicalizes_equivalent_membership_and_logical_shapes() {
    let unsorted_in = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        crate::db::predicate::CompareOp::In,
        Value::List(vec![Value::Uint(3), Value::Uint(1), Value::Uint(3)]),
        CoercionId::Strict,
    ));
    let sorted_in = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        crate::db::predicate::CompareOp::In,
        Value::List(vec![Value::Uint(1), Value::Uint(3)]),
        CoercionId::Strict,
    ));
    let swapped_eq_fields = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        "rhs".to_string(),
        crate::db::predicate::CompareOp::Eq,
        "lhs".to_string(),
        CoercionId::Strict,
    ));
    let ordered_eq_fields = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
        "lhs".to_string(),
        crate::db::predicate::CompareOp::Eq,
        "rhs".to_string(),
        CoercionId::Strict,
    ));
    let nested_and = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        Predicate::And(vec![Predicate::Compare(ComparePredicate::eq(
            "a".to_string(),
            Value::Int(1),
        ))]),
    ]);
    let flat_and = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
    ]);

    assert_eq!(
        canonicalize_runtime_predicate_via_bool_expr(unsorted_in),
        canonicalize_runtime_predicate_via_bool_expr(sorted_in)
    );
    assert_eq!(
        canonicalize_runtime_predicate_via_bool_expr(swapped_eq_fields),
        canonicalize_runtime_predicate_via_bool_expr(ordered_eq_fields)
    );
    assert_eq!(
        canonicalize_runtime_predicate_via_bool_expr(nested_and),
        canonicalize_runtime_predicate_via_bool_expr(flat_and)
    );
}

#[test]
fn predicate_bridge_preserves_special_predicate_variants() {
    let text_contains_ci = Predicate::TextContainsCi {
        field: "name".to_string(),
        value: Value::Text("al".to_string()),
    };
    let is_missing = Predicate::IsMissing {
        field: "nickname".to_string(),
    };
    let contains = Predicate::Compare(ComparePredicate::with_coercion(
        "tags",
        crate::db::predicate::CompareOp::Contains,
        Value::Text("mage".to_string()),
        CoercionId::Strict,
    ));

    assert!(matches!(
        canonicalize_runtime_predicate_via_bool_expr(text_contains_ci),
        Predicate::TextContainsCi { .. }
    ));
    assert!(matches!(
        canonicalize_runtime_predicate_via_bool_expr(is_missing),
        Predicate::IsMissing { .. }
    ));
    assert!(matches!(
        canonicalize_runtime_predicate_via_bool_expr(contains),
        Predicate::Compare(compare)
            if compare.op() == crate::db::predicate::CompareOp::Contains
    ));
}

#[test]
fn normalize_bool_expr_canonicalizes_equivalent_and_tree_shapes() {
    let nested_left = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
    };
    let nested_right = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
        }),
    };

    let normalized_left = normalize_bool_expr(nested_left);
    let normalized_right = normalize_bool_expr(nested_right);

    assert_eq!(
        normalized_left, normalized_right,
        "equivalent AND trees should normalize onto one canonical shape",
    );
    assert!(
        is_normalized_bool_expr(&normalized_left),
        "canonicalized AND tree should satisfy the normalized bool-expression contract",
    );
}

#[test]
fn normalize_bool_expr_collapses_duplicate_and_children() {
    let expr = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
    };

    let normalized = normalize_bool_expr(expr);

    assert_eq!(
        normalized,
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        },
        "duplicate AND children should collapse onto one canonical child",
    );
}

#[test]
fn normalize_bool_expr_canonicalizes_equivalent_or_tree_shapes() {
    let nested_left = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
    };
    let nested_right = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
        }),
    };

    let normalized_left = normalize_bool_expr(nested_left);
    let normalized_right = normalize_bool_expr(nested_right);

    assert_eq!(
        normalized_left, normalized_right,
        "equivalent OR trees should normalize onto one canonical shape",
    );
    assert!(
        is_normalized_bool_expr(&normalized_left),
        "canonicalized OR tree should satisfy the normalized bool-expression contract",
    );
}

#[test]
fn normalize_bool_expr_collapses_duplicate_or_children() {
    let expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
    };

    let normalized = normalize_bool_expr(expr);

    assert_eq!(
        normalized,
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        },
        "duplicate OR children should collapse onto one canonical child",
    );
}

#[test]
fn normalize_bool_expr_canonicalizes_literal_left_extractable_compare_shapes() {
    let left = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Literal(Value::Int(20))),
        right: Box::new(Expr::Field(FieldId::new("age"))),
    };
    let right = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("age"))),
        right: Box::new(Expr::Literal(Value::Int(20))),
    };

    let normalized_left = normalize_bool_expr(left);
    let normalized_right = normalize_bool_expr(right);

    assert_eq!(
        normalized_left, normalized_right,
        "literal-left extractable compares should normalize onto one canonical shape",
    );
    assert!(
        is_normalized_bool_expr(&normalized_left),
        "canonicalized literal-left extractable compare should satisfy the normalized bool-expression contract",
    );
}

#[test]
fn normalize_bool_expr_canonicalizes_literal_left_residual_compare_shapes() {
    let left = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Literal(Value::Text("AlphA".to_string()))),
        right: Box::new(Expr::FunctionCall {
            function: Function::Replace,
            args: vec![
                Expr::Field(FieldId::new("name")),
                Expr::Literal(Value::Text("a".to_string())),
                Expr::Literal(Value::Text("A".to_string())),
            ],
        }),
    };
    let right = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FunctionCall {
            function: Function::Replace,
            args: vec![
                Expr::Field(FieldId::new("name")),
                Expr::Literal(Value::Text("a".to_string())),
                Expr::Literal(Value::Text("A".to_string())),
            ],
        }),
        right: Box::new(Expr::Literal(Value::Text("AlphA".to_string()))),
    };

    let normalized_left = normalize_bool_expr(left);
    let normalized_right = normalize_bool_expr(right);

    assert_eq!(
        normalized_left, normalized_right,
        "literal-left residual compares should normalize onto one canonical shape",
    );
    assert!(
        is_normalized_bool_expr(&normalized_left),
        "canonicalized literal-left residual compare should satisfy the normalized bool-expression contract",
    );
}

#[test]
fn compile_bool_expr_to_predicate_flattens_equivalent_and_tree_shapes() {
    let left = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
    };
    let right = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
        }),
    };

    let predicate_left = compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(left));
    let predicate_right = compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(right));

    assert_eq!(
        predicate_left, predicate_right,
        "equivalent AND trees should compile to one identical predicate shape",
    );
    assert!(
        matches!(predicate_left, Predicate::And(ref children) if children.len() == 3),
        "compiled canonical AND predicate should stay flattened instead of preserving nested shells",
    );
}

#[test]
fn compile_bool_expr_to_predicate_flattens_equivalent_or_tree_shapes() {
    let left = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
    };
    let right = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
        }),
    };

    let predicate_left = compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(left));
    let predicate_right = compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(right));

    assert_eq!(
        predicate_left, predicate_right,
        "equivalent OR trees should compile to one identical predicate shape",
    );
    assert!(
        matches!(predicate_left, Predicate::Or(ref children) if children.len() == 3),
        "compiled canonical OR predicate should stay flattened instead of preserving nested shells",
    );
}

#[test]
fn normalize_bool_expr_canonicalizes_equivalent_mixed_boolean_tree_shapes() {
    let left = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
    };
    let right = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
        }),
    };

    let normalized_left = normalize_bool_expr(left);
    let normalized_right = normalize_bool_expr(right);

    assert_eq!(
        normalized_left, normalized_right,
        "equivalent mixed boolean trees should normalize onto one canonical shape",
    );
    assert!(
        is_normalized_bool_expr(&normalized_left),
        "canonicalized mixed boolean tree should satisfy the normalized bool-expression contract",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_canonicalizes_boolean_searched_case_to_first_match_form() {
    let case_expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gte,
                left: Box::new(Expr::Field(FieldId::new("age"))),
                right: Box::new(Expr::Literal(Value::Int(30))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
    };
    let canonical_expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![
                Expr::Binary {
                    op: BinaryOp::Gte,
                    left: Box::new(Expr::Field(FieldId::new("age"))),
                    right: Box::new(Expr::Literal(Value::Int(30))),
                },
                Expr::Literal(Value::Bool(false)),
            ],
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(Expr::FunctionCall {
                    function: Function::Coalesce,
                    args: vec![
                        Expr::Binary {
                            op: BinaryOp::Gte,
                            left: Box::new(Expr::Field(FieldId::new("age"))),
                            right: Box::new(Expr::Literal(Value::Int(30))),
                        },
                        Expr::Literal(Value::Bool(false)),
                    ],
                }),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("age"))),
                right: Box::new(Expr::Literal(Value::Int(20))),
            }),
        }),
    };

    assert_eq!(
        canonicalize_scalar_where_bool_expr(case_expr),
        canonicalize_scalar_where_bool_expr(canonical_expr),
        "boolean searched CASE should normalize onto the exact first-match canonical boolean shape",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_does_not_merge_non_equivalent_case_and_boolean_shapes() {
    let case_expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Field(FieldId::new("a")),
            Expr::Field(FieldId::new("b")),
        )],
        else_expr: Box::new(Expr::Field(FieldId::new("c"))),
    };
    let wrong_boolean_expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Field(FieldId::new("a"))),
            right: Box::new(Expr::Field(FieldId::new("b"))),
        }),
        right: Box::new(Expr::Field(FieldId::new("c"))),
    };

    assert_ne!(
        canonicalize_scalar_where_bool_expr(case_expr),
        canonicalize_scalar_where_bool_expr(wrong_boolean_expr),
        "searched CASE canonicalization must preserve first-match distinctness instead of collapsing to a superficially similar boolean tree",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_collapses_true_truth_wrapper_to_bare_field() {
    let wrapped_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("active"))),
        right: Box::new(Expr::Literal(Value::Bool(true))),
    };
    let canonical_expr = Expr::Field(FieldId::new("active"));

    assert_eq!(
        canonicalize_scalar_where_bool_expr(wrapped_expr),
        canonicalize_scalar_where_bool_expr(canonical_expr),
        "scalar truth wrappers should canonicalize through the same planner-owned truth-condition form as the bare bool field",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_collapses_false_truth_wrapper_to_not_field() {
    let wrapped_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("active"))),
        right: Box::new(Expr::Literal(Value::Bool(false))),
    };
    let canonical_expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Field(FieldId::new("active"))),
    };

    assert_eq!(
        canonicalize_scalar_where_bool_expr(wrapped_expr),
        canonicalize_scalar_where_bool_expr(canonical_expr),
        "scalar false truth wrappers should canonicalize through the same planner-owned truth-condition form as NOT <bool expr>",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_collapses_false_null_test_wrapper_to_not_null_test() {
    let wrapped_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FunctionCall {
            function: Function::IsNull,
            args: vec![Expr::FunctionCall {
                function: Function::Trim,
                args: vec![Expr::Field(FieldId::new("name"))],
            }],
        }),
        right: Box::new(Expr::Literal(Value::Bool(false))),
    };
    let canonical_expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::FunctionCall {
            function: Function::IsNull,
            args: vec![Expr::FunctionCall {
                function: Function::Trim,
                args: vec![Expr::Field(FieldId::new("name"))],
            }],
        }),
    };

    assert_eq!(
        canonicalize_scalar_where_bool_expr(wrapped_expr),
        canonicalize_scalar_where_bool_expr(canonical_expr),
        "scalar false null-test wrappers should canonicalize through the same planner-owned truth-condition form as NOT <null test>",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_collapses_true_compare_wrapper_with_richer_operand_family() {
    let wrapped_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Field(FieldId::new("name"))],
            }),
            right: Box::new(Expr::Literal(Value::Text("ada".to_string()))),
        }),
        right: Box::new(Expr::Literal(Value::Bool(true))),
    };
    let canonical_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Field(FieldId::new("name"))],
        }),
        right: Box::new(Expr::Literal(Value::Text("ada".to_string()))),
    };

    assert_eq!(
        canonicalize_scalar_where_bool_expr(wrapped_expr),
        canonicalize_scalar_where_bool_expr(canonical_expr),
        "scalar true compare wrappers should canonicalize through the same planner-owned truth-condition form as the admitted compare family",
    );
}

#[test]
fn canonicalize_grouped_having_bool_expr_collapses_boolean_truth_wrapper_inside_case_condition() {
    let grouped_case = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Uint(1))),
                }),
                right: Box::new(Expr::Literal(Value::Bool(true))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Bool(false))),
    };
    let canonical_case = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Uint(1))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Bool(false))),
    };

    assert_eq!(
        canonicalize_grouped_having_bool_expr(grouped_case),
        canonicalize_grouped_having_bool_expr(canonical_case),
        "grouped searched CASE should collapse redundant `= TRUE` wrappers inside boolean branch conditions",
    );
}

#[test]
fn canonicalize_grouped_having_bool_expr_collapses_false_truth_wrapper_to_not_compare() {
    let grouped_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Aggregate(crate::db::count())),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        }),
        right: Box::new(Expr::Literal(Value::Bool(false))),
    };
    let canonical_expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Aggregate(crate::db::count())),
            right: Box::new(Expr::Literal(Value::Uint(1))),
        }),
    };

    assert_eq!(
        canonicalize_grouped_having_bool_expr(grouped_expr),
        canonicalize_grouped_having_bool_expr(canonical_expr),
        "grouped false truth wrappers should canonicalize through the same planner-owned truth-condition form as NOT <bool expr>",
    );
}

#[test]
fn canonicalize_scalar_where_bool_expr_is_idempotent() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gte,
                left: Box::new(Expr::Field(FieldId::new("age"))),
                right: Box::new(Expr::Literal(Value::Int(30))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
    };

    let once = canonicalize_scalar_where_bool_expr(expr);
    let twice = canonicalize_scalar_where_bool_expr(once.clone());

    assert_eq!(
        twice, once,
        "scalar searched CASE canonicalization must be idempotent once the canonical form is reached",
    );
}

#[test]
fn canonicalize_grouped_having_bool_expr_is_idempotent_for_explicit_else_case() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Uint(1))),
                }),
                right: Box::new(Expr::Literal(Value::Bool(true))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Bool(false))),
    };

    let once = canonicalize_grouped_having_bool_expr(expr);
    let twice = canonicalize_grouped_having_bool_expr(once.clone());

    assert_eq!(
        twice, once,
        "grouped searched CASE canonicalization must be idempotent for the explicit-ELSE shipped family",
    );
}

#[test]
fn canonicalize_grouped_having_bool_expr_is_idempotent_for_omitted_else_case() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Aggregate(crate::db::count())),
                right: Box::new(Expr::Literal(Value::Uint(1))),
            },
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Null)),
    };

    let once = canonicalize_grouped_having_bool_expr(expr);
    let twice = canonicalize_grouped_having_bool_expr(once.clone());

    assert_eq!(
        twice, once,
        "grouped searched CASE canonicalization must be idempotent even when the omitted-ELSE family stays fail-closed",
    );
}

#[test]
fn compile_bool_expr_to_predicate_keeps_equivalent_mixed_boolean_tree_shapes_identical() {
    let left = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
    };
    let right = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("c"))),
            right: Box::new(Expr::Literal(Value::Int(3))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("b"))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            }),
        }),
    };

    let predicate_left = compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(left));
    let predicate_right = compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(right));

    assert_eq!(
        predicate_left, predicate_right,
        "equivalent mixed boolean trees should compile to one identical predicate shape",
    );
    assert!(
        matches!(
            predicate_left,
            Predicate::Or(ref children)
                if children.len() == 2
                    && children.iter().any(|child| matches!(child, Predicate::And(and_children) if and_children.len() == 2))
        ),
        "compiled canonical mixed boolean predicate should preserve the bounded mixed OR-of-AND shape",
    );
}

fn representative_predicates() -> Vec<Predicate> {
    vec![
        Predicate::True,
        Predicate::False,
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("age".to_string(), Value::Int(5))),
            Predicate::Not(Box::new(Predicate::IsNull {
                field: "name".to_string(),
            })),
        ]),
        Predicate::Or(vec![
            Predicate::TextContains {
                field: "name".to_string(),
                value: Value::Text("al".to_string()),
            },
            Predicate::IsEmpty {
                field: "tags".to_string(),
            },
        ]),
        Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            crate::db::predicate::CompareOp::Eq,
            Value::Enum(ValueEnum::loose("Active")),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            crate::db::predicate::CompareOp::Lt,
            Value::Int(10),
            CoercionId::NumericWiden,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![Value::Uint(3), Value::Uint(1), Value::Uint(3)]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            crate::db::predicate::CompareOp::NotIn,
            Value::List(vec![Value::Uint(7), Value::Uint(2)]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            crate::db::predicate::CompareOp::Contains,
            Value::Text("mage".to_string()),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::TextCasefold,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            crate::db::predicate::CompareOp::EndsWith,
            Value::Text("ce".to_string()),
            CoercionId::Strict,
        )),
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "rhs".to_string(),
            crate::db::predicate::CompareOp::Eq,
            "lhs".to_string(),
            CoercionId::Strict,
        )),
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "level".to_string(),
            crate::db::predicate::CompareOp::Gt,
            "rank".to_string(),
            CoercionId::NumericWiden,
        )),
        Predicate::IsNull {
            field: "deleted_at".to_string(),
        },
        Predicate::IsNotNull {
            field: "name".to_string(),
        },
        Predicate::IsMissing {
            field: "nickname".to_string(),
        },
        Predicate::IsEmpty {
            field: "tags".to_string(),
        },
        Predicate::IsNotEmpty {
            field: "tags".to_string(),
        },
        Predicate::TextContains {
            field: "name".to_string(),
            value: Value::Text("li".to_string()),
        },
        Predicate::TextContainsCi {
            field: "name".to_string(),
            value: Value::Text("al".to_string()),
        },
    ]
}

fn expr_has_no_opaque_nodes(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FieldPath(_) => false,
        Expr::Unary { expr, .. } => expr_has_no_opaque_nodes(expr),
        Expr::Binary { left, right, .. } => {
            expr_has_no_opaque_nodes(left) && expr_has_no_opaque_nodes(right)
        }
        Expr::FunctionCall { args, .. } => args.iter().all(expr_has_no_opaque_nodes),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                expr_has_no_opaque_nodes(arm.condition()) && expr_has_no_opaque_nodes(arm.result())
            }) && expr_has_no_opaque_nodes(else_expr)
        }
        Expr::Aggregate(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

#[test]
fn predicate_bridge_preserves_strict_ordered_text_compares() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name".to_string(),
        crate::db::predicate::CompareOp::Gte,
        Value::Text("Ada".to_string()),
        CoercionId::Strict,
    ));

    assert_eq!(
        canonicalize_runtime_predicate_via_bool_expr(predicate.clone()),
        predicate
    );
}

#[test]
fn predicate_bridge_preserves_strict_uint_ordered_compares() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "rank".to_string(),
        crate::db::predicate::CompareOp::Gt,
        Value::Uint(10),
        CoercionId::Strict,
    ));

    assert_eq!(
        canonicalize_runtime_predicate_via_bool_expr(predicate.clone()),
        predicate
    );
}

#[test]
fn predicate_bridge_promotes_ordered_decimal_literal_compares_to_numeric_widen() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "dodge_chance".to_string(),
        crate::db::predicate::CompareOp::Gte,
        Value::Decimal(crate::types::Decimal::new(20, 2)),
        CoercionId::Strict,
    ));

    assert_eq!(
        canonicalize_runtime_predicate_via_bool_expr(predicate),
        Predicate::Compare(ComparePredicate::with_coercion(
            "dodge_chance".to_string(),
            crate::db::predicate::CompareOp::Gte,
            Value::Decimal(crate::types::Decimal::new(20, 2)),
            CoercionId::NumericWiden,
        )),
        "ordered decimal literal compares should canonicalize onto numeric widening so float-backed fields do not fail strict literal validation",
    );
}
