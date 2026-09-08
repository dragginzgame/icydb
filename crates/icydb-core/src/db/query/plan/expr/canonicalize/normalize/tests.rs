use super::bool_expr_normalized_order;
use crate::{
    db::query::{
        builder::scalar_projection::render_scalar_projection_expr_plan_label,
        plan::{
            expr::{
                BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp,
                canonicalize::canonicalize_scalar_where_bool_expr_artifact,
                is_normalized_bool_expr, normalize_bool_expr,
            },
            render_scalar_filter_expr_plan_label,
        },
    },
    value::Value,
};
use sha2::{Digest as _, Sha256};
use std::{hint::black_box, time::Instant};

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

// Build and freeze matched binaries with debug assertions disabled. Keep this
// separate from correctness tests: the baseline's debug checks amplify re-entry.
#[cfg(not(debug_assertions))]
#[test]
#[ignore = "manual native nested-CASE preparation microbenchmark; disable debug assertions"]
fn nested_case_normalization_native_timing() {
    crate::db::query::preparation::with_preparation_work(|work| {
        for levels in [0, 2, 4, 6] {
            let input = nested_case(levels);
            drop(black_box(
                canonicalize_scalar_where_bool_expr_artifact(input.clone(), work)
                    .expect("canonical preparation"),
            ));
            let mut samples = Vec::new();
            for _ in 0..3 {
                let inputs = (0..3).map(|_| input.clone()).collect::<Vec<_>>();
                let start = Instant::now();
                for expr in inputs {
                    drop(black_box(
                        canonicalize_scalar_where_bool_expr_artifact(black_box(expr), work)
                            .expect("canonical preparation"),
                    ));
                }
                samples.push(start.elapsed().as_nanos() / 3);
            }
            samples.sort_unstable();
            println!(
                "nested_case_native levels={levels} median_ns={}",
                samples[1]
            );
        }
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

// Manual native timing probe, not a correctness or IC-cycle gate. Input-tree
// allocation/cloning is outside the timer; canonicalization, its public debug
// checks and output disposal are inside. No rows or planner metadata are read.
#[test]
#[ignore = "manual native boolean-normalization microbenchmark"]
fn boolean_normalization_native_timing() {
    for count in [4, 16, 64, 128] {
        let terms = (0..count).rev().map(field).collect::<Vec<_>>();
        for (shape, expr) in [
            ("left", left_chain(BinaryOp::And, terms.clone())),
            ("balanced", balanced_chain(BinaryOp::And, &terms)),
        ] {
            // Each workload uses one finite request outside the timer;
            // unrelated benchmark shapes do not consume its allowance.
            crate::db::query::preparation::with_preparation_work(|work| {
                drop(black_box(
                    normalize_bool_expr(expr.clone(), work).expect("canonical preparation"),
                ));
                let mut samples = Vec::new();
                for _ in 0..7 {
                    let inputs = (0..64).map(|_| expr.clone()).collect::<Vec<_>>();
                    let start = Instant::now();
                    for input in inputs {
                        drop(black_box(
                            normalize_bool_expr(black_box(input), work)
                                .expect("canonical preparation"),
                        ));
                    }
                    samples.push(start.elapsed().as_nanos() / 64);
                }
                samples.sort_unstable();
                println!(
                    "normalization_native count={count} shape={shape} median_ns={}",
                    samples[3]
                );
            });
        }
    }
}
