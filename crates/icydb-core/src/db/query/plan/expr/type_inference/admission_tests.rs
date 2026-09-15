//! Inference retains error order and result types under current request limits.

use super::{ExprType, infer_expr_type};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::{AggregateExpr, count},
            plan::{
                AggregateKind, PlanError, exact_metadata_schema,
                expr::{BinaryOp, CaseWhenArm, Expr, FieldPath, Function},
                validate::ExprPlanError,
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag, QueryFieldRole,
};

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn infer(expr: &Expr, root: &RequestExecutionRoot, lane: Lane) -> Result<ExprType, QueryError> {
    let schema = exact_metadata_schema(&[], &[]);
    PreparationWork::run(&root.scope(), lane, |work| {
        infer_expr_type(expr, &schema, work)
            .map_err(|error| error.attach_query_field(QueryFieldRole::Projection))
    })
}

fn assert_resource(error: QueryError, resource: Resource) {
    assert!(matches!(error, QueryError::Execute(_)));
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
    );
    assert_eq!(error.query_field_context(), None);
}

fn assert_missing(error: QueryError, field: &str) {
    let expected = QueryError::from(PlanError::from(ExprPlanError::unknown_expr_field(field)))
        .attach_query_field(QueryFieldRole::Projection);
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    assert_eq!(error.query_field_context(), expected.query_field_context());
}

#[test]
fn inference_admits_argument_storage_and_cumulative_visits_without_rewriting() {
    let expr = Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![Expr::Literal(Value::Null), Expr::Field("age".into())],
    };
    let before = expr.clone();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let baseline = root(Resource::TemporaryBytes, 16_000_000);
        let expected = infer(&expr, &baseline, lane).unwrap();
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let exact = baseline.observed(resource);
            assert!(exact > 0);
            let request = root(resource, exact);
            assert_eq!(infer(&expr, &request, lane).unwrap(), expected);
            assert_resource(infer(&expr, &request, lane).unwrap_err(), resource);
            assert_resource(
                infer(&expr, &root(resource, exact - 1), lane).unwrap_err(),
                resource,
            );
            assert_eq!(
                infer(&expr, &root(resource, exact), lane).unwrap(),
                expected
            );
        }
        assert_eq!(baseline.observed(Resource::RowsVisited), 0);
    }
    assert_eq!(expr, before);
}

#[test]
fn inference_preserves_case_function_and_binary_first_errors() {
    let missing = |name: &str| Expr::Field(name.into());
    for (expr, field) in [
        (
            Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(missing("condition"), missing("result"))],
                else_expr: Box::new(missing("else")),
            },
            "else",
        ),
        // Arity/family errors belong after inference of all visited arguments.
        (
            Expr::FunctionCall {
                function: Function::Abs,
                args: vec![
                    Expr::Literal(Value::Text("bad family".into())),
                    missing("argument"),
                ],
            },
            "argument",
        ),
        (
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(missing("left")),
                right: Box::new(missing("right")),
            },
            "left",
        ),
    ] {
        assert_missing(
            infer(
                &expr,
                &root(Resource::TemporaryBytes, 16_000_000),
                Lane::Diagnostic,
            )
            .unwrap_err(),
            field,
        );
    }
}

#[test]
fn inference_labels_are_admitted_and_literal_payloads_stay_unvisited() {
    for expr in [
        Expr::Field("missing".into()),
        Expr::FieldPath(FieldPath::new("missing", vec!["child".into()])),
    ] {
        assert_resource(
            infer(&expr, &root(Resource::TemporaryBytes, 0), Lane::PublicRead).unwrap_err(),
            Resource::TemporaryBytes,
        );
        assert_missing(
            infer(
                &expr,
                &root(Resource::TemporaryBytes, 16_000_000),
                Lane::PublicRead,
            )
            .unwrap_err(),
            "missing",
        );
    }
    for expr in [
        Expr::Literal(Value::Text("x".repeat(8192))),
        Expr::Aggregate(
            AggregateExpr::from_expression_input(
                AggregateKind::Count,
                Expr::Field("row_operand".into()),
            )
            .with_filter_expr(Expr::Field("row_filter".into())),
        ),
        Expr::Aggregate(count()),
    ] {
        let request = root(Resource::TemporaryBytes, 0);
        infer(&expr, &request, Lane::Diagnostic).unwrap();
        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 1);
        assert_eq!(request.observed(Resource::NestedValueSteps), 0);
    }
    // An existing scalar root with no nested metadata keeps its Unknown result.
    assert_eq!(
        infer(
            &Expr::FieldPath(FieldPath::new("age", vec!["child".into()])),
            &root(Resource::TemporaryBytes, 0),
            Lane::Diagnostic
        )
        .unwrap(),
        ExprType::Unknown
    );
}

#[test]
fn projection_type_gate_preserves_resource_failures_and_field_context() {
    use crate::db::query::plan::{
        expr::{ProjectionField, ProjectionSpec},
        validate::grouped::validate_projection_expr_types,
    };
    let projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field("missing".into()),
        alias: None,
    }]);
    for limit in [0, 16_000_000] {
        let root = root(Resource::TemporaryBytes, limit);
        let schema = exact_metadata_schema(&[], &[]);
        let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            validate_projection_expr_types(&schema, &projection, work)
        })
        .unwrap_err();
        if limit == 0 {
            assert_resource(error, Resource::TemporaryBytes);
        } else {
            assert_missing(error, "missing");
        }
    }
}
