use super::newtype_query_schema;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::expr::{
                BinaryOp, CaseWhenArm, CompiledExpr, CompiledExprCaseArm, Expr, FieldId, FieldPath,
                Function, UnaryOp, compile_scalar_projection_expr_with_schema,
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
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

fn compile(expr: &Expr, root: &RequestExecutionRoot) -> Result<Option<CompiledExpr>, QueryError> {
    let schema = newtype_query_schema();
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        compile_scalar_projection_expr_with_schema(&schema, expr, work).map_err(QueryError::execute)
    })
}

#[test]
fn scalar_construction_charges_exact_work_and_actual_specialized_backing() {
    let slot = size_of::<CompiledExpr>() as u64;
    let arm = size_of::<CompiledExprCaseArm>() as u64;
    let field = || Expr::Field(FieldId::new("id"));
    let literal = || Expr::Literal(Value::Bool(true));
    let cases = [
        (Expr::Literal(Value::Text("abc".into())), 4, 1, 3),
        (field(), 7, 0, 2),
        (
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(field()),
                right: Box::new(field()),
            },
            15,
            0,
            4,
        ),
        (
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(literal()),
                right: Box::new(literal()),
            },
            3,
            2,
            2 * slot,
        ),
        (
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(literal()),
            },
            2,
            1,
            slot,
        ),
        (
            Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Literal(Value::Text("abc".into()))],
            },
            5,
            1,
            slot + 3,
        ),
        (
            Expr::Case {
                when_then_arms: vec![CaseWhenArm::new(literal(), literal())],
                else_expr: Box::new(literal()),
            },
            4,
            3,
            arm,
        ),
        (
            Expr::Case {
                when_then_arms: vec![],
                else_expr: Box::new(literal()),
            },
            2,
            1,
            slot,
        ),
        (
            Expr::FieldPath(FieldPath::new("profile", vec!["name".into()])),
            31,
            0,
            7 + size_of::<String>() as u64 + size_of::<Box<[u8]>>() as u64 + 4 + 4 + 14,
        ),
    ];
    for (expr, steps, values, bytes) in cases {
        let original = expr.clone();
        for (resource, exact) in [
            (Resource::PredicateExpressionSteps, steps),
            (Resource::NestedValueSteps, values),
            (Resource::TemporaryBytes, bytes),
        ] {
            let request = root(resource, exact);
            assert!(compile(&expr, &request).unwrap().is_some(), "{expr:?}");
            assert_eq!(request.observed(resource), exact, "{expr:?}");
            assert_eq!(request.observed(Resource::RowsVisited), 0);
            if exact > 0 {
                let request = root(resource, exact - 1);
                let error = compile(&expr, &request).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw())),
                    "{expr:?}"
                );
            }
        }
        assert_eq!(expr, original);
    }
}

#[test]
fn scalar_budget_failure_is_not_unsupported_syntax_and_is_cumulative() {
    let expr = Expr::Field(FieldId::new("missing"));
    assert!(
        compile(&expr, &root(Resource::PredicateExpressionSteps, 9))
            .unwrap()
            .is_none()
    );
    assert!(compile(&expr, &root(Resource::PredicateExpressionSteps, 8)).is_err());

    let expr = Expr::Literal(Value::Text("abc".into()));
    let request = root(Resource::TemporaryBytes, 5);
    let first = compile(&expr, &request).unwrap().unwrap();
    assert!(compile(&expr, &request).is_err());
    assert_eq!(first, CompiledExpr::Literal(Value::Text("abc".into())));
    assert!(
        compile(&expr, &root(Resource::TemporaryBytes, 3))
            .unwrap()
            .is_some()
    );
}

#[test]
fn scalar_discarded_case_payloads_are_charged_before_specialization() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Literal(Value::Bool(true)),
            Expr::Literal(Value::Nat64(1)),
        )],
        else_expr: Box::new(Expr::Literal(Value::Text("discarded".into()))),
    };
    let exact = size_of::<CompiledExprCaseArm>() as u64 + 9;
    assert!(compile(&expr, &root(Resource::TemporaryBytes, exact - 1)).is_err());
    assert_eq!(
        compile(&expr, &root(Resource::TemporaryBytes, exact)).unwrap(),
        Some(CompiledExpr::Literal(Value::Nat64(1)))
    );
}

#[test]
fn scalar_runtime_compilation_requires_and_charges_active_execution() {
    use crate::db::executor::budget::{
        ExecutionConstructionBudget, HardExecutionContext, with_query_execution_budget_for_tests,
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetScope;
    let schema = newtype_query_schema();
    let expr = Expr::Literal(Value::Text("abc".into()));
    assert!(
        compile_scalar_projection_expr_with_schema(&schema, &expr, &ExecutionConstructionBudget)
            .is_err()
    );
    for limit in [2, 3] {
        let budget = HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, limit);
        let result = with_query_execution_budget_for_tests(
            budget,
            HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::TrustedRead,
                0,
            ),
            || {
                compile_scalar_projection_expr_with_schema(
                    &schema,
                    &expr,
                    &ExecutionConstructionBudget,
                )
                .map_err(QueryError::execute)
            },
        );
        if limit == 3 {
            assert_eq!(
                result.unwrap(),
                Some(CompiledExpr::Literal(Value::Text("abc".into())))
            );
        } else {
            assert!(result.unwrap_err().diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
        }
    }
}
