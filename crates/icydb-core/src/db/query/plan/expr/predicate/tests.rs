//! Predicate extraction preserves capability absence and shares request admission.

use super::derive_normalized_bool_expr_predicate_subset as derive;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CompareOp, Predicate},
        query::{
            plan::expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn extract(root: &RequestExecutionRoot, expr: &Expr) -> Result<Option<Predicate>, QueryError> {
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        derive(expr, work)
    })
}

fn compare(value: Value) -> Expr {
    Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("label"))),
        right: Box::new(Expr::Literal(value)),
    }
}

fn assert_resource(error: QueryError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
    );
}

#[test]
fn extraction_admits_owned_leaf_once_without_copying_the_source_expression() {
    let expr = compare(Value::Text("x".repeat(1024)));
    let expected = Predicate::eq("label".into(), Value::Text("x".repeat(1024)));
    let generous = request(Resource::TemporaryBytes, 16_000_000);
    assert_eq!(extract(&generous, &expr).unwrap(), Some(expected.clone()));
    // One field and one payload, with no temporary copied CanonicalExpr.
    let bytes = generous.observed(Resource::TemporaryBytes);
    assert_eq!(bytes, 5 + 1024);
    let exact = request(Resource::TemporaryBytes, bytes);
    assert_eq!(extract(&exact, &expr).unwrap(), Some(expected));
    assert_resource(
        extract(&exact, &expr).unwrap_err(),
        Resource::TemporaryBytes,
    );
    assert_resource(
        extract(&request(Resource::TemporaryBytes, bytes - 1), &expr).unwrap_err(),
        Resource::TemporaryBytes,
    );
}

#[test]
fn extraction_exhaustion_never_becomes_an_unsupported_predicate() {
    let expr = compare(Value::Text("payload".into()));
    for resource in [
        Resource::TemporaryBytes,
        Resource::PredicateExpressionSteps,
        Resource::NestedValueSteps,
    ] {
        assert_resource(extract(&request(resource, 0), &expr).unwrap_err(), resource);
    }
    let unsupported = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::FunctionCall {
            function: Function::Upper,
            args: vec![Expr::Field(FieldId::new("label"))],
        }),
        right: Box::new(Expr::Literal(Value::Text("X".into()))),
    };
    assert_eq!(
        extract(&request(Resource::TemporaryBytes, 0), &unsupported).unwrap(),
        None
    );
    assert_eq!(
        extract(
            &request(Resource::TemporaryBytes, 0),
            &Expr::Literal(Value::Bool(true))
        )
        .unwrap(),
        Some(Predicate::True)
    );
}

#[test]
fn membership_and_truth_shells_share_cumulative_construction_admission() {
    let chain = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(compare(Value::Text("a".repeat(128)))),
        right: Box::new(compare(Value::Text("b".repeat(128)))),
    };
    for expr in [
        chain.clone(),
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(chain),
        },
    ] {
        let generous = request(Resource::TemporaryBytes, 16_000_000);
        let expected = extract(&generous, &expr)
            .unwrap()
            .expect("membership extraction");
        assert!(
            matches!(&expected, Predicate::Compare(compare) if matches!(compare.op(), CompareOp::In | CompareOp::NotIn))
        );
        let bytes = generous.observed(Resource::TemporaryBytes);
        let exact = request(Resource::TemporaryBytes, bytes);
        assert_eq!(extract(&exact, &expr).unwrap(), Some(expected));
        assert_resource(
            extract(&request(Resource::TemporaryBytes, bytes - 1), &expr).unwrap_err(),
            Resource::TemporaryBytes,
        );
        assert_resource(
            extract(&exact, &expr).unwrap_err(),
            Resource::TemporaryBytes,
        );
    }
}
