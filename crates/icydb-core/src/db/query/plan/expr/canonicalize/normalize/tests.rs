use super::bool_expr_normalized_order;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        query::{
            builder::scalar_projection::render_scalar_projection_expr_plan_label,
            plan::{
                expr::{
                    BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp,
                    canonicalize::{
                        canonicalize_grouped_having_bool_expr_artifact,
                        canonicalize_scalar_where_bool_expr_artifact,
                    },
                    eval_builder_expr_for_value_preview, is_normalized_bool_expr,
                    normalize_bool_expr,
                },
                render_scalar_filter_expr_plan_label,
            },
            preparation::PreparationWork,
        },
        test_support::request_with_limit,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};
use sha2::{Digest as _, Sha256};

#[test]
fn casefold_literal_normalization_is_idempotent_and_preserves_budget_failures() {
    let input = Expr::membership(
        Expr::FunctionCall {
            function: Function::Lower,
            args: vec![field(0)],
        },
        vec![
            Value::Text("ALİ".into()),
            Value::Null,
            Value::Text("ΟΣ".into()),
        ],
        true,
    );
    let before = input.clone();
    let run = |root: &RequestExecutionRoot| -> Result<Expr, QueryError> {
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            normalize_bool_expr(input.clone(), work)
        })
    };
    let generous = RequestExecutionRoot::__new_runtime_root();
    let canonical = run(&generous).unwrap();
    assert_eq!(input, before);
    crate::db::query::preparation::with_preparation_work(|work| {
        assert_eq!(
            normalize_bool_expr(canonical.clone(), work).unwrap(),
            canonical
        );
    });
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let used = generous.observed(resource);
        assert!(used > 0);
        let short = request_with_limit(resource, used - 1);
        let error = run(&short).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        let exact = request_with_limit(resource, used);
        assert_eq!(run(&exact).unwrap(), canonical);
        assert_eq!(exact.observed(resource), used);
        let repeated = request_with_limit(resource, used * 2);
        assert_eq!(run(&repeated).unwrap(), canonical);
        assert_eq!(run(&repeated).unwrap(), canonical);
        assert!(
            run(&repeated)
                .unwrap_err()
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
    }
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn not(expr: Expr) -> Expr {
    Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(expr),
    }
}

fn field(index: usize) -> Expr {
    Expr::Field(FieldId::new(format!("field_{index:04}")))
}

#[test]
fn issue_case_normalization_reaches_conditions_results_and_else() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for op in [BinaryOp::And, BinaryOp::Or] {
            let compound = binary(op, not(not(field(0))), field(1));
            let nested = Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(compound.clone(), compound.clone())],
                else_expr: Box::new(compound.clone()),
            };
            let input = Expr::Case {
                when_then_arms: vec![
                    CaseWhenArm::new(compound.clone(), nested.clone()),
                    CaseWhenArm::new(not(not(field(2))), compound.clone()),
                ],
                else_expr: Box::new(nested),
            };
            let canonical = normalize_bool_expr(input.clone(), work).unwrap();
            assert!(is_normalized_bool_expr(&canonical));
            assert_ne!(canonical, input);
            assert_eq!(
                normalize_bool_expr(canonical.clone(), work).unwrap(),
                canonical
            );
        }
    });
}

#[test]
fn issue_case_normalization_charges_children_before_budget_exhaustion() {
    let input = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(field(0), field(1))],
        else_expr: Box::new(field(2)),
    };
    let run = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
            normalize_bool_expr(input.clone(), work)
        })
    };
    let exact = request_with_limit(Resource::PredicateExpressionSteps, 4);
    assert_eq!(run(&exact).unwrap(), input);
    assert_eq!(exact.observed(Resource::PredicateExpressionSteps), 4);
    let short = request_with_limit(Resource::PredicateExpressionSteps, 3);
    for _ in 0..2 {
        let error = run(&short).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        )));
    }
    assert_eq!(short.observed(Resource::PredicateExpressionSteps), 5);
}

#[test]
fn issue_case_normalization_preserves_context_specific_null_results() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for op in [BinaryOp::And, BinaryOp::Or] {
            let input = Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(
                    binary(op, field(0), Expr::Literal(Value::Null)),
                    Expr::Literal(Value::Bool(true)),
                )],
                else_expr: Box::new(Expr::Literal(Value::Null)),
            };
            let scalar = canonicalize_scalar_where_bool_expr_artifact(input.clone(), work)
                .unwrap()
                .into_expr();
            let grouped = canonicalize_grouped_having_bool_expr_artifact(input.clone(), work)
                .unwrap()
                .into_expr();
            for value in [Value::Bool(true), Value::Bool(false), Value::Null] {
                let evaluate =
                    |expr| eval_builder_expr_for_value_preview(expr, "field_0000", &value).unwrap();
                let expected = evaluate(&input);
                assert_eq!(evaluate(&grouped), expected);
                assert_eq!(
                    evaluate(&scalar),
                    if expected == Value::Null {
                        Value::Bool(false)
                    } else {
                        expected
                    }
                );
            }
        }
    });
}

// Distinct boolean branches prevent constant folding from hiding condition copies.
fn nested_case(levels: usize) -> Expr {
    let mut expr = field(0);
    for level in 0..levels {
        expr = Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(expr, field(level * 2 + 1))],
            else_expr: Box::new(field(level * 2 + 2)),
        };
    }
    expr
}

// Cover normalized operands that can reach ordering, including equal labels
// with different typed values so the structural Debug tie-break is exercised.
fn ordering_terms() -> Vec<Expr> {
    vec![
        field(0),
        not(not(field(1))),
        not(Expr::Literal(Value::Null)),
        Expr::Literal(Value::Bool(false)),
        binary(BinaryOp::Lt, Expr::Literal(Value::Int64(5)), field(0)),
        binary(BinaryOp::Eq, field(0), field(1)),
        binary(BinaryOp::Eq, field(0), Expr::Literal(Value::Int64(5))),
        binary(BinaryOp::Eq, field(0), Expr::Literal(Value::Nat64(5))),
        binary(
            BinaryOp::Eq,
            field(0),
            Expr::Literal(Value::Text("'\\\n".into())),
        ),
        binary(BinaryOp::Or, field(2), field(1)),
        not(binary(BinaryOp::And, field(2), field(0))),
        Expr::membership(field(0), vec![Value::Int64(5), Value::Null], false),
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![binary(BinaryOp::And, field(2), field(1)), field(0)],
        },
        nested_case(2),
    ]
}

#[test]
fn normalized_ordering_preserves_labels_structure_and_canonical_models() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let terms = ordering_terms()
            .into_iter()
            .map(|expr| normalize_bool_expr(expr, work).expect("canonical preparation"))
            .collect::<Vec<_>>();
        for left in &terms {
            assert_eq!(
                normalize_bool_expr(left.clone(), work).expect("canonical preparation"),
                *left
            );
            assert_eq!(
                render_scalar_projection_expr_plan_label(left),
                render_scalar_filter_expr_plan_label(left)
            );
            for right in &terms {
                let expected = render_scalar_filter_expr_plan_label(left)
                    .cmp(&render_scalar_filter_expr_plan_label(right))
                    .then_with(|| format!("{left:?}").cmp(&format!("{right:?}")));
                assert_eq!(bool_expr_normalized_order(left, right), expected);
            }
        }

        // Capture exact canonical-model and label bytes for matched frozen binaries.
        // This receipt is not a new runtime fingerprint or an alternative encoder.
        let mut digest = Sha256::new();
        for op in [BinaryOp::And, BinaryOp::Or] {
            let canonical = normalize_bool_expr(left_chain(op, terms.clone()), work)
                .expect("canonical preparation");
            let mut reversed = terms.clone();
            reversed.reverse();
            reversed.push(terms[0].clone());
            assert_eq!(
                normalize_bool_expr(balanced_chain(op, &reversed), work)
                    .expect("canonical preparation"),
                canonical
            );
            assert!(is_normalized_bool_expr(&canonical));
            digest.update(format!("{canonical:?}\n"));
            digest.update(render_scalar_projection_expr_plan_label(&canonical));
        }
        for levels in 0..=4 {
            let canonical = canonicalize_scalar_where_bool_expr_artifact(nested_case(levels), work)
                .expect("canonical preparation")
                .into_expr();
            assert!(is_normalized_bool_expr(&canonical));
            assert_eq!(
                normalize_bool_expr(canonical.clone(), work).expect("canonical preparation"),
                canonical
            );
            digest.update(format!("{canonical:?}\n"));
            digest.update(render_scalar_projection_expr_plan_label(&canonical));
        }
        let receipt = format!("{:x}", digest.finalize());
        assert_eq!(
            receipt,
            "ac4fd65e7c16849940ffd1352c3ca10433696f2b8ae2fc36cf15ec3eceaf95aa"
        );
        println!("canonical_ordering_receipt={receipt}");
    });
}

fn left_chain(op: BinaryOp, terms: Vec<Expr>) -> Expr {
    terms
        .into_iter()
        .reduce(|left, right| binary(op, left, right))
        .expect("fixtures are nonempty")
}

fn balanced_chain(op: BinaryOp, terms: &[Expr]) -> Expr {
    if let [term] = terms {
        term.clone()
    } else {
        assert!(!terms.is_empty(), "fixtures are nonempty");
        let (left, right) = terms.split_at(terms.len() / 2);
        binary(op, balanced_chain(op, left), balanced_chain(op, right))
    }
}

#[test]
fn associative_groups_preserve_canonical_order_deduplication_and_left_association() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for count in [1, 2, 4, 16, 64, 128] {
            let mut terms = (0..count).rev().map(field).collect::<Vec<_>>();
            terms.extend([
                field(0),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Bool(false)),
            ]);
            let mut canonical_terms = terms.clone();
            canonical_terms.sort_by(bool_expr_normalized_order);
            canonical_terms.dedup();
            for op in [BinaryOp::And, BinaryOp::Or] {
                let expected = left_chain(op, canonical_terms.clone());
                let left = left_chain(op, terms.clone());
                let balanced = balanced_chain(op, &terms);
                let right = terms
                    .iter()
                    .rev()
                    .cloned()
                    .reduce(|right, left| binary(op, left, right))
                    .expect("nonempty fixture");
                for input in [left, balanced, right] {
                    let actual = normalize_bool_expr(input, work).expect("canonical preparation");
                    assert_eq!(actual, expected);
                    assert!(is_normalized_bool_expr(&actual));
                    assert_eq!(
                        normalize_bool_expr(actual.clone(), work).expect("canonical preparation"),
                        actual
                    );
                }
            }
        }
    });
}

#[test]
fn term_normalization_exposes_groups_without_crossing_other_operators() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for op in [BinaryOp::And, BinaryOp::Or] {
            let opposite = if op == BinaryOp::And {
                BinaryOp::Or
            } else {
                BinaryOp::And
            };
            let nested = binary(opposite, field(3), field(2));
            let input = binary(
                op,
                not(not(binary(op, field(1), field(0)))),
                binary(op, not(not(field(0))), nested.clone()),
            );
            let mut expected_terms = vec![
                field(0),
                field(1),
                normalize_bool_expr(nested, work).expect("canonical preparation"),
            ];
            expected_terms.sort_by(bool_expr_normalized_order);
            assert_eq!(
                normalize_bool_expr(input, work).expect("canonical preparation"),
                left_chain(op, expected_terms)
            );
        }
    });
}

#[test]
fn associative_terms_keep_comparison_normalization_and_nulls() {
    crate::db::query::preparation::with_preparation_work(|work| {
        let compare = binary(BinaryOp::Lt, Expr::Literal(Value::Int64(5)), field(0));
        let normalized_compare = binary(BinaryOp::Gt, field(0), Expr::Literal(Value::Int64(5)));
        for op in [BinaryOp::And, BinaryOp::Or] {
            let terms = vec![
                compare.clone(),
                not(Expr::Literal(Value::Null)),
                normalized_compare.clone(),
            ];
            let mut expected_terms = vec![normalized_compare.clone(), Expr::Literal(Value::Null)];
            expected_terms.sort_by(bool_expr_normalized_order);
            assert_eq!(
                normalize_bool_expr(balanced_chain(op, &terms), work)
                    .expect("canonical preparation"),
                left_chain(op, expected_terms)
            );
        }
    });
}
