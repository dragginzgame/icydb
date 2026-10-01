//! Predicate extraction preserves capability absence and shares request admission.

use super::derive_normalized_bool_expr_predicate_subset as derive;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        predicate::{CompareOp, Predicate},
        query::{
            plan::expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
            preparation::PreparationWork,
        },
        test_support::request_with_limit,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

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
    let generous = request_with_limit(Resource::TemporaryBytes, 16_000_000);
    assert_eq!(extract(&generous, &expr).unwrap(), Some(expected.clone()));
    // One field and one payload, with no temporary copied CanonicalExpr.
    let bytes = generous.observed(Resource::TemporaryBytes);
    assert_eq!(bytes, 5 + 1024);
    let exact = request_with_limit(Resource::TemporaryBytes, bytes);
    assert_eq!(extract(&exact, &expr).unwrap(), Some(expected));
    assert_resource(
        extract(&exact, &expr).unwrap_err(),
        Resource::TemporaryBytes,
    );
    assert_resource(
        extract(
            &request_with_limit(Resource::TemporaryBytes, bytes - 1),
            &expr,
        )
        .unwrap_err(),
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
        assert_resource(
            extract(&request_with_limit(resource, 0), &expr).unwrap_err(),
            resource,
        );
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
        extract(
            &request_with_limit(Resource::TemporaryBytes, 0),
            &unsupported
        )
        .unwrap(),
        None
    );
    assert_eq!(
        extract(
            &request_with_limit(Resource::TemporaryBytes, 0),
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
        let generous = request_with_limit(Resource::TemporaryBytes, 16_000_000);
        let expected = extract(&generous, &expr)
            .unwrap()
            .expect("membership extraction");
        assert!(
            matches!(&expected, Predicate::Compare(compare) if matches!(compare.op(), CompareOp::In | CompareOp::NotIn))
        );
        let bytes = generous.observed(Resource::TemporaryBytes);
        let exact = request_with_limit(Resource::TemporaryBytes, bytes);
        assert_eq!(extract(&exact, &expr).unwrap(), Some(expected));
        assert_resource(
            extract(
                &request_with_limit(Resource::TemporaryBytes, bytes - 1),
                &expr,
            )
            .unwrap_err(),
            Resource::TemporaryBytes,
        );
        assert_resource(
            extract(&exact, &expr).unwrap_err(),
            Resource::TemporaryBytes,
        );
    }
}

#[test]
fn nullable_false_truth_guards_obey_cumulative_construction_admission() {
    for leaf in [
        compare(Value::Text("archived".into())),
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("peer"))),
            right: Box::new(Expr::Field(FieldId::new("label"))),
        },
        Expr::FunctionCall {
            function: Function::Contains,
            args: vec![
                Expr::Field(FieldId::new("label")),
                Expr::Literal(Value::Text("a".into())),
            ],
        },
    ] {
        let expr = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(leaf),
        };
        let generous = request_with_limit(Resource::TemporaryBytes, 16_000_000);
        let expected = extract(&generous, &expr).unwrap().unwrap();
        let bytes = generous.observed(Resource::TemporaryBytes);
        let exact = request_with_limit(Resource::TemporaryBytes, bytes);
        assert_eq!(extract(&exact, &expr).unwrap(), Some(expected));
        assert_resource(
            extract(
                &request_with_limit(Resource::TemporaryBytes, bytes - 1),
                &expr,
            )
            .unwrap_err(),
            Resource::TemporaryBytes,
        );
        assert_resource(
            extract(&exact, &expr).unwrap_err(),
            Resource::TemporaryBytes,
        );
    }
}
