//! Shared query hashing admission, semantic parity and continuation propagation.

use super::request;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        codec::{new_hash_sha256, write_hash_tag_u8},
        predicate::MissingRowPolicy,
        query::{
            builder::{aggregate::AggregateExpr, count},
            fingerprint::{
                finalize_sha256_digest,
                hash_sections::{grouping::hash_projection_spec, hash_scalar_semantic_filter},
                projection_hash::{
                    admission::admit_expr_hash, hash_projection_structural_fingerprint,
                    hash_scalar_filter_expr_structural_fingerprint,
                },
            },
            plan::{
                AccessPlannedQuery, AggregateKind, LogicalPlan,
                expr::{
                    Alias, BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function,
                    ProjectionField, ProjectionSpec, UnaryOp,
                },
            },
            preparation::PreparationWork,
        },
    },
    value::{Value, test_hash_budget_error, with_test_hash_override},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn projection(expr: &Expr) -> ProjectionSpec {
    ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: expr.clone(),
        alias: Some(Alias::new("display_only")),
    }])
}

fn query_hash(
    expr: &Expr,
    as_projection: bool,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<[u8; 32], QueryError> {
    let spec = projection(expr);
    let plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    PreparationWork::run(&root.scope(), lane, |work| {
        let mut hasher = new_hash_sha256();
        if as_projection {
            hash_projection_spec(&mut hasher, &spec, &plan, work)
        } else {
            hash_scalar_semantic_filter(&mut hasher, Some(expr), None, work)
        }
        .map_err(QueryError::execute)?;
        Ok(finalize_sha256_digest(hasher))
    })
}

fn reference_hash(expr: &Expr, as_projection: bool) -> [u8; 32] {
    let mut hasher = new_hash_sha256();
    if as_projection {
        hash_projection_structural_fingerprint(&mut hasher, &projection(expr)).unwrap();
    } else {
        write_hash_tag_u8(&mut hasher, 0x21);
        hash_scalar_filter_expr_structural_fingerprint(&mut hasher, expr).unwrap();
    }
    finalize_sha256_digest(hasher)
}

#[test]
fn query_expression_hashes_preserve_bytes_under_exact_and_cumulative_admission() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Literal(Value::Bool(false))),
            },
            Expr::Aggregate(
                AggregateExpr::from_expression_input(
                    AggregateKind::Sum,
                    Expr::Binary {
                        op: BinaryOp::Add,
                        left: Box::new(Expr::Field(FieldId::new("amount"))),
                        right: Box::new(Expr::Literal(Value::IntBig(
                            "18446744073709551616".parse().unwrap(),
                        ))),
                    },
                )
                .distinct()
                .with_filter_expr(Expr::FunctionCall {
                    function: Function::IsNotNull,
                    args: vec![Expr::FieldPath(FieldPath::new(
                        "meta",
                        vec!["国家".into(), "a.b".into()],
                    ))],
                }),
            ),
        )],
        else_expr: Box::new(Expr::Literal(Value::Map(vec![
            (Value::Text("z".into()), Value::Blob(vec![7; 80])),
            (
                Value::Text("a".into()),
                Value::List(vec![Value::Bool(true); 20]),
            ),
        ]))),
    };
    let before = expr.clone();
    for as_projection in [false, true] {
        let expected = reference_hash(&expr, as_projection);
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let measured = request(Resource::TemporaryBytes, 16_000_000);
            assert_eq!(
                query_hash(&expr, as_projection, &measured, lane).unwrap(),
                expected
            );
            for resource in [
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
                Resource::TemporaryBytes,
            ] {
                let exact = measured.observed(resource);
                assert!(exact > 0);
                for limit in [exact - 1, exact, 2 * exact] {
                    let root = request(resource, limit);
                    for attempt in 1..=3 {
                        let result = query_hash(&expr, as_projection, &root, lane);
                        if attempt * exact <= limit {
                            assert_eq!(result.unwrap(), expected);
                        } else {
                            let facts = result.unwrap_err().diagnostic_facts();
                            assert!(
                                facts
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            assert!(
                                facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                            );
                            break;
                        }
                    }
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                    assert_eq!(root.observed(Resource::QueryExecutions), 0);
                }
            }
        }
    }
    assert_eq!(expr, before);
}

#[test]
fn query_hash_admission_uses_canonical_count_operands_and_ignores_alias_bytes() {
    let count_rows = Expr::Aggregate(count());
    let count_literal = Expr::Aggregate(AggregateExpr::from_expression_input(
        AggregateKind::Count,
        Expr::Literal(Value::Text("x".repeat(8192))),
    ));
    for as_projection in [false, true] {
        let root = request(Resource::NestedValueSteps, 0);
        assert_eq!(
            query_hash(&count_rows, as_projection, &root, Lane::Diagnostic).unwrap(),
            query_hash(&count_literal, as_projection, &root, Lane::Diagnostic).unwrap(),
        );
        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
    let field = Expr::Field(FieldId::new("账户"));
    let root = request(Resource::TemporaryBytes, 0);
    let alias = Expr::Alias {
        expr: Box::new(field.clone()),
        name: Alias::new("x".repeat(8192)),
    };
    assert_eq!(
        query_hash(&field, true, &root, Lane::Diagnostic).unwrap(),
        query_hash(&alias, true, &root, Lane::Diagnostic).unwrap(),
    );
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn admission_finishes_before_encoding_and_does_not_hash_literals_twice() {
    let expr = Expr::Literal(Value::Map(vec![(Value::Bool(true), Value::Bool(false))]));
    let root = request(Resource::TemporaryBytes, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        let mut hasher = new_hash_sha256();
        let before = finalize_sha256_digest(hasher.clone());
        assert!(hash_scalar_semantic_filter(&mut hasher, Some(&expr), None, work).is_err());
        assert_eq!(finalize_sha256_digest(hasher), before);
        Ok(())
    })
    .unwrap();
    with_test_hash_override(Err(test_hash_budget_error), || {
        let root = request(Resource::TemporaryBytes, 16_000_000);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            admit_expr_hash(&expr, work).map_err(QueryError::execute)
        })
        .unwrap();
        // Admission succeeded without calling the injected failing hash writer.
        let error = query_hash(&expr, true, &root, Lane::Diagnostic).unwrap_err();
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

#[test]
fn filter_hash_failure_prevents_continuation_return_and_fresh_retry_succeeds() {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.filter_expr = Some(Expr::Literal(Value::Text("account".into())));
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    let error = build(&request(Resource::NestedValueSteps, 0)).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::NestedValueSteps.raw()
    )));
    let measured = request(Resource::PredicateExpressionSteps, 16_000_000);
    let expected = build(&measured).unwrap().unwrap().continuation_signature();
    let exact = measured.observed(Resource::PredicateExpressionSteps);
    assert!(build(&request(Resource::PredicateExpressionSteps, exact - 1)).is_err());
    let root = request(Resource::PredicateExpressionSteps, 2 * exact);
    for _ in 0..2 {
        assert_eq!(
            build(&root).unwrap().unwrap().continuation_signature(),
            expected
        );
    }
    assert!(build(&root).is_err());
    assert_eq!(
        build(&request(Resource::PredicateExpressionSteps, exact))
            .unwrap()
            .unwrap()
            .continuation_signature(),
        expected
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::QueryExecutions), 0);
}
