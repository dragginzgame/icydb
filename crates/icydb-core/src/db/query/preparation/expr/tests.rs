use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            admission::input::{MAX_QUERY_INPUT_DEPTH, QueryInputBudget},
            builder::min_by,
            plan::expr::{BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function, UnaryOp},
            preparation::PreparationWork,
        },
    },
    value::{Value, ValueEnum},
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

fn copy(expr: &Expr, root: &RequestExecutionRoot) -> Result<Expr, QueryError> {
    QueryInputBudget::new()
        .expr(expr, 1)
        .expect("input admitted before copying");
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        work.copy_expr(expr)
    })
}

#[test]
fn field_clause_copy_reserves_exact_backing_and_preserves_duplicates() {
    let names = ["z", "a", "z"];
    let bytes = (names.len() * size_of::<FieldId>() + 3) as u64;
    let request = root(Resource::TemporaryBytes, bytes);
    let copied = PreparationWork::run(
        &request.scope(),
        DiagnosticExecutionLane::PublicRead,
        |work| work.copy_slice(&names, |name| Ok(FieldId::new(work.copy_text(name)?))),
    )
    .unwrap();
    assert_eq!(
        copied.iter().map(FieldId::as_str).collect::<Vec<_>>(),
        names
    );
    assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 6);
}

#[test]
fn rejected_clause_backing_does_not_visit_any_fields() {
    let names = ["z", "a", "z"];
    let bytes = (names.len() * size_of::<FieldId>()) as u64;
    let request = root(Resource::TemporaryBytes, bytes - 1);
    let result = PreparationWork::run(
        &request.scope(),
        DiagnosticExecutionLane::PublicRead,
        |work| work.copy_slice(&names, |name| Ok(FieldId::new(work.copy_text(name)?))),
    );
    assert!(result.is_err());
    assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 0);
}

#[test]
fn expression_copy_preserves_all_raw_shapes_and_nested_value_identity() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::FieldPath(FieldPath::new(
                    "record",
                    vec!["inner".into(), "name".into()],
                ))),
                right: Box::new(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(Expr::Literal(Value::Null)),
                }),
            },
            Expr::Aggregate(
                min_by("rank")
                    .distinct()
                    .with_filter_expr(Expr::Literal(Value::Bool(true))),
            ),
        )],
        else_expr: Box::new(Expr::FunctionCall {
            function: Function::Coalesce,
            args: vec![Expr::Alias {
                expr: Box::new(Expr::Literal(Value::Enum(ValueEnum::test_payload(
                    3,
                    4,
                    Value::Map(vec![
                        (Value::Nat64(2), Value::Text("b".into())),
                        (Value::Nat64(2), Value::Blob(vec![1, 2, 3])),
                    ]),
                )))),
                name: "label".into(),
            }],
        }),
    };
    let request = root(Resource::TemporaryBytes, 16_000_000);
    assert_eq!(copy(&expr, &request).unwrap(), expr);
    assert!(request.observed(Resource::TemporaryBytes) > 0);
    assert!(request.observed(Resource::NestedValueSteps) > 0);
}

#[test]
fn exact_copy_backing_and_exhaustion_preserve_the_source() {
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field("abc".into())),
        right: Box::new(Expr::Literal(Value::Text("payload".into()))),
    };
    let bytes = 2 * size_of::<Expr>() as u64 + 3 + 7;
    let exact = root(Resource::TemporaryBytes, bytes);
    assert_eq!(copy(&expr, &exact).unwrap(), expr);
    assert_eq!(exact.observed(Resource::TemporaryBytes), bytes);
    let short = root(Resource::TemporaryBytes, bytes - 1);
    let error = copy(&expr, &short).expect_err("last payload allocation rejects");
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert_eq!(
        copy(&expr, &root(Resource::TemporaryBytes, bytes)).unwrap(),
        expr
    );
}

#[test]
fn failed_expression_visit_stops_before_siblings_and_retries_keep_charges() {
    let expr = Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![
            Expr::Literal(Value::Text("first".into())),
            Expr::Literal(Value::Text("later".into())),
        ],
    };
    let request = root(Resource::PredicateExpressionSteps, 1);
    for expected in [2, 3] {
        let error = copy(&expr, &request).expect_err("copy visit exhausts current request");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw()
        )));
        assert_eq!(
            request.observed(Resource::PredicateExpressionSteps),
            expected
        );
        assert_eq!(request.observed(Resource::NestedValueSteps), 0);
    }
    assert_eq!(
        request.observed(Resource::TemporaryBytes),
        2 * size_of::<Expr>() as u64
    );
}

#[test]
fn admitted_depth_copy_and_failure_cleanup_fit_the_input_boundary() {
    let mut expr = Expr::Literal(Value::Null);
    // The literal's Value occupies the final input level.
    for _ in 1..MAX_QUERY_INPUT_DEPTH - 1 {
        expr = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        };
    }
    assert_eq!(
        copy(&expr, &root(Resource::TemporaryBytes, 16_000_000)).unwrap(),
        expr
    );
    assert!(copy(&expr, &root(Resource::PredicateExpressionSteps, 64)).is_err());
}
