use super::{budget::expansion_fits, normalize_bool_case_expr};
use crate::{
    db::query::plan::expr::{
        BinaryOp, CaseWhenArm, Expr, FieldId, Function,
        canonicalize::canonicalize_scalar_where_bool_expr_artifact,
        derive_normalized_bool_expr_predicate_subset, eval_builder_expr_for_value_preview,
    },
    value::{Value, ValueEnum},
};

fn field(name: &str) -> Expr {
    Expr::Field(FieldId::new(name))
}

fn nested_case(levels: usize) -> Expr {
    let mut expr = field("condition");
    for level in 0..levels {
        expr = Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(expr, field(&format!("yes_{level}")))],
            else_expr: Box::new(field(&format!("no_{level}"))),
        };
    }
    expr
}

#[test]
fn rewrite_budget_counts_both_condition_copies_at_the_boundary() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let result = field("");
        // Ten wrapper units + two condition nodes + result + ELSE = fourteen.
        // The remaining 242 units admit 121 payload blocks copied twice.
        for (bytes, fits) in [(121 * 64, true), (121 * 64 + 1, false)] {
            let arms = [CaseWhenArm::new(field(&"x".repeat(bytes)), result.clone())];
            assert_eq!(expansion_fits(&arms, &result), fits);
            assert_eq!(
                matches!(
                    normalize_bool_case_expr(arms.to_vec(), result.clone(), false, work)
                        .expect("canonical preparation"),
                    Expr::Case { .. }
                ),
                !fits
            );
        }
        let arms = (0..8)
            .map(|i| CaseWhenArm::new(field(&format!("c{i}")), field(&format!("r{i}"))))
            .collect::<Vec<_>>();
        assert!(expansion_fits(&arms, &field("else")));
    });
}

#[test]
fn rewrite_budget_is_content_based_and_covers_large_and_nested_values() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let mut reserved = String::with_capacity(32 * 1024);
        reserved.push_str("small");
        let mut values = Vec::with_capacity(4096);
        values.push(Value::Text(reserved));
        let operands = [
            Value::List(values),
            Value::List(vec![Value::Text("small".into())]),
        ];
        let canonical = |value| {
            let arms = vec![CaseWhenArm::new(
                Expr::FunctionCall {
                    function: Function::IsNull,
                    args: vec![Expr::Literal(value)],
                },
                field("yes"),
            )];
            assert!(expansion_fits(&arms, &field("no")));
            normalize_bool_case_expr(arms, field("no"), false, work).expect("canonical preparation")
        };
        // Move the original containers into admission: Clone may discard capacity.
        let [reserved, compact] = operands;
        assert_eq!(canonical(reserved), canonical(compact));
        let mut nested = Value::Null;
        for _ in 0..300 {
            nested = Value::List(vec![nested]);
        }
        for value in [
            Value::Text("x".repeat(16 * 1024)),
            Value::Blob(vec![0; 16 * 1024]),
            Value::List(vec![Value::Null; 256]),
            Value::Map(vec![(Value::Null, Value::Null); 128]),
            Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Blob(vec![0; 16 * 1024]),
            )),
            nested,
        ] {
            let arms = [CaseWhenArm::new(field("condition"), Expr::Literal(value))];
            assert!(!expansion_fits(&arms, &field("no")));
        }
    });
}

#[test]
fn rewrite_budget_accounts_big_integer_magnitude_without_encoding() {
    for value in [Value::IntBig(1_i64.into()), Value::NatBig(1_u64.into())] {
        let arms = [CaseWhenArm::new(field("condition"), Expr::Literal(value))];
        assert!(expansion_fits(&arms, &field("no")));
    }
    let digits = "9".repeat(40_000);
    for value in [
        Value::IntBig(digits.parse().expect("large integer")),
        Value::NatBig(digits.parse().expect("large natural")),
    ] {
        let arms = [CaseWhenArm::new(field("condition"), Expr::Literal(value))];
        assert!(!expansion_fits(&arms, &field("no")));
    }
}

#[test]
fn nested_case_stays_compact_and_canonicalization_is_idempotent() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for levels in [1, 2, 4, 6, 12] {
            let canonical = canonicalize_scalar_where_bool_expr_artifact(nested_case(levels), work)
                .expect("canonical preparation")
                .into_expr();
            if levels >= 6 {
                assert!(matches!(canonical, Expr::Case { .. }));
            }
            assert_eq!(
                canonicalize_scalar_where_bool_expr_artifact(canonical.clone(), work)
                    .expect("canonical preparation")
                    .into_expr(),
                canonical
            );
        }
    });
}

#[test]
fn compact_case_remains_expression_backed_when_predicate_projection_is_unavailable() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let selector = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(field("tenant")),
            right: Box::new(Expr::Literal(Value::Text("chosen".into()))),
        };
        assert!(derive_normalized_bool_expr_predicate_subset(&selector).is_some());
        let query = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(selector),
            right: Box::new(nested_case(6)),
        };
        let canonical = canonicalize_scalar_where_bool_expr_artifact(query, work)
            .expect("canonical preparation")
            .into_expr();
        // The maintained compiler projects whole representable expressions; it is
        // not an independent-conjunct extractor. Keep the full expression authority.
        assert!(derive_normalized_bool_expr_predicate_subset(&canonical).is_none());
    });
}

#[test]
fn compact_case_preserves_first_match_and_three_valued_results() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let truth = [Value::Bool(true), Value::Bool(false), Value::Null];
        for first in &truth {
            for second in &truth {
                for yes in &truth {
                    for later in &truth {
                        for otherwise in &truth {
                            let arms = vec![
                                CaseWhenArm::new(
                                    Expr::FunctionCall {
                                        function: Function::Coalesce,
                                        args: vec![Expr::Literal(first.clone()); 130],
                                    },
                                    Expr::Literal(yes.clone()),
                                ),
                                CaseWhenArm::new(
                                    Expr::Literal(second.clone()),
                                    Expr::Literal(later.clone()),
                                ),
                            ];
                            let expected = if first == &Value::Bool(true) {
                                yes
                            } else if second == &Value::Bool(true) {
                                later
                            } else {
                                otherwise
                            };
                            for top_level in [false, true] {
                                let actual = normalize_bool_case_expr(
                                    arms.clone(),
                                    Expr::Literal(otherwise.clone()),
                                    top_level,
                                    work,
                                )
                                .expect("canonical preparation");
                                assert!(matches!(actual, Expr::Case { .. }));
                                assert_eq!(
                                    eval_builder_expr_for_value_preview(
                                        &actual,
                                        "unused",
                                        &Value::Null
                                    )
                                    .expect("compiled compact CASE"),
                                    *expected
                                );
                            }
                        }
                    }
                }
            }
        }
    });
}
