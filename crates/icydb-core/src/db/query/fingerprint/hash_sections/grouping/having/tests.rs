//! Pin planner-owned HAVING identity bytes.

use super::*;
use crate::{
    db::{
        codec::{hex::encode_hex_lower, new_hash_sha256},
        query::{
            builder::{count, min_by, sum},
            fingerprint::finalize_sha256_digest,
            plan::{
                FieldSlot,
                expr::{FieldPath, Function},
            },
        },
    },
    value::Value,
};

fn aggregates() -> Vec<AggregateExpr> {
    let sum = sum("amount").with_filter_expr(Expr::Literal(Value::Bool(true)));
    vec![min_by("amount").distinct(), sum.clone(), sum]
}

#[test]
fn having_hash_propagates_literal_failure() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    let source = GroupHavingFingerprintSource {
        expr: &Expr::Literal(Value::Bool(true)),
        group_fields: &GroupFieldSet::Direct(vec![]),
        aggregates: &[],
    };
    with_test_hash_override(Err(test_hash_budget_error), || {
        let error =
            hash_group_having_projection(&mut new_hash_sha256(), Some(&source)).unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

fn cases() -> Vec<Expr> {
    let mut cases = vec![
        Expr::Field("owner".into()),
        Expr::Field("missing".into()),
        Expr::FieldPath(FieldPath::new("profile", vec!["tag".into()])),
        Expr::Literal(Value::Text("quote's λ".into())),
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(Value::Bool(false))),
        },
        Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![Expr::Field("owner".into()), Expr::Literal(Value::Null)],
        },
        Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Literal(Value::Bool(true)),
                Expr::Aggregate(sum("amount")),
            )],
            else_expr: Box::new(Expr::Literal(Value::Nat64(1))),
        },
        Expr::Aggregate(count()),
        Expr::Aggregate(sum("amount").with_filter_expr(Expr::Literal(Value::Bool(false)))),
        Expr::Aggregate(AggregateExpr::from_expression_input(
            crate::db::query::plan::AggregateKind::Sum,
            Expr::Literal(Value::Text("λ'\\n".repeat(64))),
        )),
    ];
    cases.extend(aggregates().into_iter().map(Expr::Aggregate));
    for op in [
        BinaryOp::Eq,
        BinaryOp::Ne,
        BinaryOp::Lt,
        BinaryOp::Lte,
        BinaryOp::Gt,
        BinaryOp::Gte,
        BinaryOp::And,
        BinaryOp::Or,
        BinaryOp::Add,
        BinaryOp::Sub,
        BinaryOp::Mul,
        BinaryOp::Div,
    ] {
        cases.push(Expr::Binary {
            op,
            left: Box::new(Expr::Field("owner".into())),
            right: Box::new(Expr::Literal(Value::Nat64(7))),
        });
    }
    cases
}

#[test]
fn having_hash_preserves_planner_expression_grammar() {
    let aggregates = aggregates();
    let planned: Vec<_> = aggregates
        .iter()
        .cloned()
        .map(GroupAggregateSpec::from_aggregate_expr)
        .collect();
    let plan_fields = GroupFieldSet::Direct(vec![FieldSlot::unresolved(2, "owner")]);
    let mut plan_hash = new_hash_sha256();
    hash_group_having_projection(&mut plan_hash, None).unwrap();
    for expr in cases() {
        hash_group_having_projection(
            &mut plan_hash,
            Some(&GroupHavingFingerprintSource {
                expr: &expr,
                group_fields: &plan_fields,
                aggregates: &planned,
            }),
        )
        .unwrap();
    }
    assert_eq!(
        encode_hex_lower(&finalize_sha256_digest(plan_hash)),
        "d32de1031cc115b3dd2e6bf3928cf9511717b0957c8dd01dd073ad5de0587960",
    );
}
