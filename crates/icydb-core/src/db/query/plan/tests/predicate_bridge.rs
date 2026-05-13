//! Module: db::query::plan::tests::predicate_bridge
//! Covers the planner-owned runtime-predicate bridge and compiler behavior.
//! Does not own: predicate module API shape or lowering-specific WHERE rules.
//! Boundary: keeps predicate-bridge regression coverage on the planner owner
//! `tests/` boundary instead of under a predicate-side test-only shim module.

use super::PlanFilteredEntity;
use crate::{
    db::{
        predicate::{
            CoercionId, CompareFieldsPredicate, ComparePredicate, Predicate, PredicateProgram,
        },
        query::plan::expr::{
            BinaryOp, CanonicalExpr, CaseWhenArm, Expr, ExprType, FieldId, Function, UnaryOp,
            canonicalize_grouped_having_bool_expr, canonicalize_runtime_predicate_via_bool_expr,
            canonicalize_scalar_where_bool_expr, collapse_true_only_boolean_admission,
            compile_canonical_bool_expr_to_compiled_predicate,
            compile_normalized_bool_expr_to_predicate, eval_builder_expr_for_value_preview,
            infer_expr_type, is_normalized_bool_expr, normalize_bool_expr,
            normalize_bool_expr_artifact, predicate_to_runtime_bool_expr_for_test,
        },
        schema::SchemaInfo,
    },
    traits::EntitySchema,
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
        Value::List(vec![Value::Nat(3), Value::Nat(1), Value::Nat(3)]),
        CoercionId::Strict,
    ));
    let sorted_in = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        crate::db::predicate::CompareOp::In,
        Value::List(vec![Value::Nat(1), Value::Nat(3)]),
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
fn predicate_compile_does_not_collapse_mixed_membership_coercions() {
    let mixed_coercion_expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Field(FieldId::new("name"))],
            }),
            right: Box::new(Expr::Literal(Value::Text("a".to_string()))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("name"))),
            right: Box::new(Expr::Literal(Value::Text("b".to_string()))),
        }),
    };

    let predicate =
        compile_normalized_bool_expr_to_predicate(&normalize_bool_expr(mixed_coercion_expr));

    assert!(
        predicate_contains_no_membership_compare(&predicate),
        "mixed strict/casefold OR leaves must stay expanded instead of collapsing to IN: {predicate:?}",
    );
}

#[test]
fn predicate_compile_preserves_bounded_case_lowering_equivalence() {
    let case_expr = scalar_case_expr(
        vec![(
            Expr::Field(FieldId::new("active")),
            Expr::Literal(Value::Bool(false)),
        )],
        Expr::Literal(Value::Bool(false)),
    );

    let direct = compile_normalized_bool_expr_to_predicate(&case_expr);
    let lowered = canonicalize_scalar_where_bool_expr(case_expr);
    let lowered_predicate = compile_normalized_bool_expr_to_predicate(&lowered);

    assert!(
        !expr_contains_case(&lowered),
        "bounded scalar CASE should lower before the ordinary predicate compile path: {lowered:?}",
    );
    assert_eq!(
        lowered,
        Expr::Literal(Value::Bool(false)),
        "this invariant case should lower completely without admitting COALESCE into predicate compilation",
    );
    assert_eq!(
        direct, lowered_predicate,
        "direct CASE predicate compilation must stay semantically equivalent to explicit CASE lowering",
    );
    assert!(
        predicate_contains_only_runtime_predicate_nodes(&direct)
            && predicate_contains_only_runtime_predicate_nodes(&lowered_predicate),
        "predicate compilation must fully lower CASE paths without leaking planner Expr nodes: direct={direct:?} lowered={lowered_predicate:?}",
    );
}

#[test]
fn planner_expr_pipeline_order_preserves_stage_contracts() {
    let raw_expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Literal(Value::Bool(true))),
            right: Box::new(Expr::Field(FieldId::new("active"))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Field(FieldId::new("tag"))],
            }),
            right: Box::new(Expr::Literal(Value::Text("mage".to_string()))),
        }),
    };

    let canonical = canonicalize_scalar_where_bool_expr(raw_expr);
    assert!(
        is_normalized_bool_expr(&canonical),
        "canonicalization must hand type inference a normalized boolean expression",
    );
    assert_eq!(
        canonical,
        normalize_bool_expr(canonical.clone()),
        "canonicalized expression should already be at the normalized fixed point",
    );

    let canonical_before_type_inference = canonical.clone();
    let schema =
        SchemaInfo::cached_for_generated_entity_model(<PlanFilteredEntity as EntitySchema>::MODEL);
    let typed = infer_expr_type(&canonical, schema)
        .expect("canonical boolean expression should type-check against plan test schema");
    assert_eq!(
        typed,
        ExprType::Bool,
        "type inference should classify canonical filter shape without mutating it",
    );
    assert_eq!(
        canonical, canonical_before_type_inference,
        "type inference must preserve expression shape by borrowing the canonical tree",
    );

    let predicate = compile_normalized_bool_expr_to_predicate(&canonical);
    assert!(
        predicate_contains_only_runtime_predicate_nodes(&predicate),
        "predicate compilation should emit only runtime Predicate nodes: {predicate:?}",
    );

    let projection_expr = Expr::FunctionCall {
        function: Function::Length,
        args: vec![Expr::Field(FieldId::new("tag"))],
    };
    let projected =
        eval_builder_expr_for_value_preview(&projection_expr, "tag", &Value::Text("mage".into()))
            .expect("projection evaluation should remain a scalar Value boundary");
    assert!(
        projection_value_contains_no_predicate_constructs(&projected),
        "projection evaluation should return scalar values, not predicate constructs: {projected:?}",
    );
}

#[test]
fn compiled_predicates_match_shared_truth_value_admission() {
    let simple_compare = normalize_bool_expr_artifact(Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("active"))),
        right: Box::new(Expr::Literal(Value::Bool(true))),
    });
    assert_compiled_predicate_matches_truth_value(
        "simple compare with NULL input",
        &simple_compare,
        "active",
        &[Value::Bool(true), Value::Bool(false), Value::Null],
    );

    let boolean_composition = normalize_bool_expr_artifact(Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Field(FieldId::new("active"))),
        right: Box::new(Expr::Literal(Value::Bool(false))),
    });
    assert_compiled_predicate_matches_truth_value(
        "boolean composition",
        &boolean_composition,
        "active",
        &[Value::Bool(true), Value::Bool(false), Value::Null],
    );

    let scalar_case = normalize_bool_expr_artifact(scalar_case_expr(
        vec![(
            Expr::Field(FieldId::new("active")),
            Expr::Literal(Value::Bool(true)),
        )],
        Expr::Literal(Value::Bool(false)),
    ));
    assert_compiled_predicate_matches_truth_value(
        "scalar CASE expression",
        &scalar_case,
        "active",
        &[Value::Bool(true), Value::Bool(false), Value::Null],
    );

    let having_like_case = normalize_bool_expr_artifact(scalar_case_expr(
        vec![(
            Expr::Field(FieldId::new("active")),
            Expr::Literal(Value::Bool(true)),
        )],
        Expr::Literal(Value::Null),
    ));
    assert_compiled_predicate_matches_truth_value(
        "HAVING-like CASE expression",
        &having_like_case,
        "active",
        &[Value::Bool(true), Value::Bool(false), Value::Null],
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
fn normalize_bool_expr_is_idempotent_for_mixed_boolean_tree() {
    let expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Literal(Value::Int(2))),
                right: Box::new(Expr::Field(FieldId::new("b"))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("a"))),
                right: Box::new(Expr::Literal(Value::Int(1))),
            }),
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
                left: Box::new(Expr::Literal(Value::Int(2))),
                right: Box::new(Expr::Field(FieldId::new("b"))),
            }),
        }),
    };

    let once = normalize_bool_expr(expr);
    let twice = normalize_bool_expr(once.clone());

    assert_eq!(
        once, twice,
        "boolean normalization must be idempotent after one canonical pass",
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
fn canonicalize_scalar_where_bool_case_semantics_null_condition_falls_through() {
    assert_scalar_case_lowering_preserves_literal_semantics(
        "CASE WHEN NULL THEN TRUE ELSE FALSE END",
        scalar_case_expr(
            vec![(Expr::Literal(Value::Null), Expr::Literal(Value::Bool(true)))],
            Expr::Literal(Value::Bool(false)),
        ),
        Value::Bool(false),
    );
}

#[test]
fn canonicalize_scalar_where_bool_case_semantics_preserve_nested_case() {
    let nested_case = scalar_case_expr(
        vec![(Expr::Literal(Value::Null), Expr::Literal(Value::Bool(true)))],
        Expr::Literal(Value::Bool(false)),
    );

    assert_scalar_case_lowering_preserves_literal_semantics(
        "nested CASE with NULL inner condition",
        scalar_case_expr(
            vec![(Expr::Literal(Value::Bool(true)), nested_case)],
            Expr::Literal(Value::Bool(true)),
        ),
        Value::Bool(false),
    );
}

#[test]
fn canonicalize_scalar_where_bool_case_semantics_preserve_case_inside_and_or() {
    let null_condition_case = scalar_case_expr(
        vec![(Expr::Literal(Value::Null), Expr::Literal(Value::Bool(true)))],
        Expr::Literal(Value::Bool(false)),
    );

    assert_scalar_case_lowering_preserves_literal_semantics(
        "CASE inside AND",
        Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(null_condition_case.clone()),
            right: Box::new(Expr::Literal(Value::Bool(true))),
        },
        Value::Bool(false),
    );
    assert_scalar_case_lowering_preserves_literal_semantics(
        "CASE inside OR",
        Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(null_condition_case),
            right: Box::new(Expr::Literal(Value::Bool(true))),
        },
        Value::Bool(true),
    );
}

#[test]
fn canonicalize_scalar_where_bool_case_semantics_preserve_returning_null_branch() {
    assert_scalar_case_lowering_preserves_literal_semantics(
        "CASE returning NULL from selected branch",
        scalar_case_expr(
            vec![(Expr::Literal(Value::Bool(true)), Expr::Literal(Value::Null))],
            Expr::Literal(Value::Bool(false)),
        ),
        Value::Null,
    );
}

#[test]
fn canonicalize_scalar_where_bool_case_size_guard_keeps_large_case_fail_closed() {
    let case_expr = scalar_case_expr(
        (0..9)
            .map(|index| {
                let condition = if index == 8 {
                    Expr::Literal(Value::Bool(true))
                } else if index % 2 == 0 {
                    Expr::Literal(Value::Null)
                } else {
                    Expr::Literal(Value::Bool(false))
                };

                (condition, Expr::Literal(Value::Bool(index == 8)))
            })
            .collect(),
        Expr::Literal(Value::Bool(false)),
    );
    let original_value = eval_literal_bool_expr(&case_expr);
    let canonical = canonicalize_scalar_where_bool_expr(case_expr);
    let canonical_value = eval_literal_bool_expr(&canonical);

    assert_eq!(
        original_value,
        Value::Bool(true),
        "source large CASE should select only the first TRUE condition",
    );
    assert_eq!(
        canonical_value, original_value,
        "large CASE fail-closed path must preserve literal SQL boolean semantics",
    );
    assert!(
        matches!(&canonical, Expr::Case { when_then_arms, .. } if when_then_arms.len() == 9),
        "large CASE should stay in top-level CASE form instead of being partially or fully lowered: {canonical:?}",
    );
    assert!(
        is_normalized_bool_expr(&canonical),
        "large fail-closed CASE should still satisfy the normalized bool-expression contract",
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
                    right: Box::new(Expr::Literal(Value::Nat(1))),
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
                right: Box::new(Expr::Literal(Value::Nat(1))),
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
            right: Box::new(Expr::Literal(Value::Nat(1))),
        }),
        right: Box::new(Expr::Literal(Value::Bool(false))),
    };
    let canonical_expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Aggregate(crate::db::count())),
            right: Box::new(Expr::Literal(Value::Nat(1))),
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
                    right: Box::new(Expr::Literal(Value::Nat(1))),
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
                right: Box::new(Expr::Literal(Value::Nat(1))),
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
            Value::List(vec![Value::Nat(3), Value::Nat(1), Value::Nat(3)]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            crate::db::predicate::CompareOp::NotIn,
            Value::List(vec![Value::Nat(7), Value::Nat(2)]),
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

// Verify the predicate compilation boundary only emits runtime predicate AST
// nodes. This keeps expression-planner nodes from leaking beyond the predicate
// compile stage when pipeline-order tests exercise the full sequence.
fn predicate_contains_only_runtime_predicate_nodes(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => true,
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .all(predicate_contains_only_runtime_predicate_nodes),
        Predicate::Not(child) => predicate_contains_only_runtime_predicate_nodes(child),
    }
}

// Detect compact membership predicates so the mixed-coercion guard can assert
// that predicate compilation fails closed instead of picking one coercion mode.
fn predicate_contains_no_membership_compare(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Compare(compare) => !matches!(
            compare.op(),
            crate::db::predicate::CompareOp::In | crate::db::predicate::CompareOp::NotIn
        ),
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .all(predicate_contains_no_membership_compare),
        Predicate::Not(child) => predicate_contains_no_membership_compare(child),
        Predicate::True
        | Predicate::False
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => true,
    }
}

// Projection evaluation must end at scalar `Value` output. The guard uses the
// known `LENGTH('mage')` result so predicate constructs cannot hide behind a
// permissive shape-only assertion.
fn projection_value_contains_no_predicate_constructs(value: &Value) -> bool {
    matches!(value, Value::Nat(4))
}

// Build a compact searched `CASE` expression for canonicalization tests that
// need to focus on SQL truth semantics instead of constructor boilerplate.
fn scalar_case_expr(arms: Vec<(Expr, Expr)>, else_expr: Expr) -> Expr {
    Expr::Case {
        when_then_arms: arms
            .into_iter()
            .map(|(condition, result)| CaseWhenArm::new(condition, result))
            .collect(),
        else_expr: Box::new(else_expr),
    }
}

// Assert the literal-only searched-`CASE` lowering contract directly. The
// helper intentionally evaluates both the source and canonical form so future
// rewrites cannot accidentally treat `NULL` conditions as branch matches.
fn assert_scalar_case_lowering_preserves_literal_semantics(
    label: &str,
    expr: Expr,
    expected: Value,
) {
    let original_value = eval_literal_bool_expr(&expr);
    let canonical = canonicalize_scalar_where_bool_expr(expr);
    let canonical_value = eval_literal_bool_expr(&canonical);

    assert_eq!(
        original_value, expected,
        "source expression should have expected literal SQL boolean value for {label}",
    );
    assert_eq!(
        canonical_value, expected,
        "canonicalized expression should preserve literal SQL boolean value for {label}: {canonical:?}",
    );
    assert!(
        is_normalized_bool_expr(&canonical),
        "canonicalized CASE expression should satisfy the normalized bool-expression contract for {label}",
    );
    assert!(
        !expr_contains_case(&canonical),
        "bounded searched CASE should lower out of CASE form for {label}: {canonical:?}",
    );
}

// Compare the actual compiled predicate runtime against expression evaluation
// followed by the shared TRUE-only truth-value boundary. This catches drift
// between predicate_compile's truth-set encoding and the central admission
// contract without reimplementing predicate execution in the test.
fn assert_compiled_predicate_matches_truth_value(
    label: &str,
    canonical: &CanonicalExpr,
    field_name: &str,
    values: &[Value],
) {
    let predicate = compile_canonical_bool_expr_to_compiled_predicate(canonical).into_predicate();
    let program = PredicateProgram::compile_for_model_only(
        <PlanFilteredEntity as EntitySchema>::MODEL,
        &predicate,
    );
    let field_slot = <PlanFilteredEntity as EntitySchema>::MODEL
        .resolve_field_slot(field_name)
        .expect("predicate bridge test field should exist");

    for value in values {
        let evaluated = eval_builder_expr_for_value_preview(canonical.as_expr(), field_name, value)
            .expect("canonical boolean expression should evaluate in preview");
        let expected = collapse_true_only_boolean_admission(evaluated, |found| {
            format!("canonical boolean expression produced non-boolean value: {found:?}")
        })
        .expect("canonical boolean expression should collapse through truth_value");
        let actual = program
            .eval_with_slot_value_ref_reader(&mut |slot| (slot == field_slot).then_some(value));

        assert_eq!(
            actual,
            expected,
            "{label}: compiled predicate must match evaluated expression truth admission for value={value:?}; predicate={predicate:?}; expr={:?}",
            canonical.as_expr(),
        );
    }
}

// Evaluate the small literal-only expression family emitted by searched-CASE
// lowering. It models SQL three-valued boolean logic: `TRUE` selects a CASE
// branch, while `FALSE` and `NULL` both fall through.
fn eval_literal_bool_expr(expr: &Expr) -> Value {
    match expr {
        Expr::Literal(value) => value.clone(),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => eval_sql_not(eval_literal_bool_expr(expr)),
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => eval_sql_and(eval_literal_bool_expr(left), eval_literal_bool_expr(right)),
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => eval_sql_or(eval_literal_bool_expr(left), eval_literal_bool_expr(right)),
        Expr::FunctionCall {
            function: Function::Coalesce,
            args,
        } => args
            .iter()
            .map(eval_literal_bool_expr)
            .find(|value| !matches!(value, Value::Null))
            .unwrap_or(Value::Null),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => when_then_arms
            .iter()
            .find_map(|arm| match eval_literal_bool_expr(arm.condition()) {
                Value::Bool(true) => Some(eval_literal_bool_expr(arm.result())),
                Value::Bool(false) | Value::Null => None,
                value => panic!("CASE condition evaluated to non-bool literal: {value:?}"),
            })
            .unwrap_or_else(|| eval_literal_bool_expr(else_expr)),
        other => panic!("literal CASE semantic test cannot evaluate expression: {other:?}"),
    }
}

// Apply SQL three-valued `NOT` for the literal-only evaluator above.
fn eval_sql_not(value: Value) -> Value {
    match value {
        Value::Bool(value) => Value::Bool(!value),
        Value::Null => Value::Null,
        value => panic!("NOT operand evaluated to non-bool literal: {value:?}"),
    }
}

// Apply SQL three-valued `AND`, including `FALSE` dominance over `NULL`.
fn eval_sql_and(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
        (Value::Bool(true), value) | (value, Value::Bool(true)) => value,
        (Value::Null, Value::Null) => Value::Null,
        (left, right) => panic!("AND operands evaluated to non-bool literals: {left:?}, {right:?}"),
    }
}

// Apply SQL three-valued `OR`, including `TRUE` dominance over `NULL`.
fn eval_sql_or(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
        (Value::Bool(false), value) | (value, Value::Bool(false)) => value,
        (Value::Null, Value::Null) => Value::Null,
        (left, right) => panic!("OR operands evaluated to non-bool literals: {left:?}, {right:?}"),
    }
}

// Report whether a canonicalized expression still contains a searched `CASE`
// node after the bounded lowering pass has had a chance to run.
fn expr_contains_case(expr: &Expr) -> bool {
    match expr {
        Expr::Case { .. } => true,
        Expr::Unary { expr, .. } => expr_contains_case(expr),
        Expr::Binary { left, right, .. } => expr_contains_case(left) || expr_contains_case(right),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_case),
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) | Expr::Aggregate(_) => false,
        #[cfg(test)]
        Expr::Alias { expr, .. } => expr_contains_case(expr),
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
fn predicate_bridge_preserves_strict_nat_ordered_compares() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "rank".to_string(),
        crate::db::predicate::CompareOp::Gt,
        Value::Nat(10),
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
