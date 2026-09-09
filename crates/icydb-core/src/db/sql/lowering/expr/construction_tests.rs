use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::expr::{CaseWhenArm, Expr},
            preparation::PreparationWork,
        },
        sql::{
            lowering::{
                SqlLoweringError,
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::{
                SqlAggregateCall, SqlAggregateKind, SqlCaseArm, SqlExpr, SqlExprBinaryOp,
                SqlExprUnaryOp, SqlMembershipValue, SqlScalarFunction,
            },
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

fn lower(input: &SqlExpr, root: &RequestExecutionRoot) -> Result<Expr, QueryError> {
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        lower_sql_expr(input, SqlExprPhase::Where, work).map_err(|error| match error {
            SqlLoweringError::Query(error) => *error,
            other => panic!("unexpected lowering error: {other:?}"),
        })
    })
}

fn field() -> SqlExpr {
    SqlExpr::Field("name".to_string())
}
fn literal() -> SqlExpr {
    SqlExpr::Literal(Value::Nat64(1))
}

#[test]
fn aggregate_operands_are_borrowed_and_retained_backing_is_charged() {
    for (input, filtered) in [
        (None, false),
        (Some(field()), false),
        (Some(SqlExpr::Literal(Value::Text("abc".into()))), false),
        (None, true),
    ] {
        let bytes = match &input {
            None => 0,
            Some(SqlExpr::Field(_)) => 4 + size_of::<Expr>() as u64,
            Some(_) => 3 + size_of::<Expr>() as u64,
        } + if filtered {
            size_of::<Expr>() as u64
        } else {
            0
        };
        let syntax = SqlExpr::Aggregate(SqlAggregateCall {
            kind: SqlAggregateKind::Count,
            input: input.map(Box::new),
            filter_expr: filtered.then(|| Box::new(SqlExpr::Literal(Value::Bool(true)))),
            distinct: false,
        });
        let original = syntax.clone();
        let run = |request: &RequestExecutionRoot| {
            PreparationWork::run(
                &request.scope(),
                DiagnosticExecutionLane::PublicRead,
                |work| {
                    lower_sql_expr(&syntax, SqlExprPhase::PostAggregate, work).map_err(|error| {
                        match error {
                            SqlLoweringError::Query(error) => *error,
                            other => panic!("unexpected lowering error: {other:?}"),
                        }
                    })
                },
            )
        };
        let exact = root(Resource::TemporaryBytes, bytes);
        let expected = run(&exact).unwrap();
        assert_eq!(exact.observed(Resource::TemporaryBytes), bytes);
        if bytes != 0 {
            assert!(run(&root(Resource::TemporaryBytes, bytes - 1)).is_err());
        }
        assert_eq!(syntax, original);
        assert_eq!(
            run(&root(Resource::TemporaryBytes, bytes)).unwrap(),
            expected
        );
    }
}

#[test]
fn literals_and_membership_charge_nested_payload_not_only_slots() {
    let value = Value::List(vec![Value::Text("abc".to_string())]);
    let bytes = size_of::<Value>() as u64 + 3;
    for (input, required) in [
        (SqlExpr::Literal(value.clone()), bytes),
        (
            SqlExpr::Membership {
                expr: Box::new(field()),
                values: vec![SqlMembershipValue::Literal(value)],
                negated: false,
            },
            4 + size_of::<Value>() as u64 + bytes + 2 * size_of::<Expr>() as u64,
        ),
    ] {
        let exact = root(Resource::TemporaryBytes, required);
        assert!(lower(&input, &exact).is_ok());
        assert_eq!(exact.observed(Resource::TemporaryBytes), required);
        assert!(lower(&input, &root(Resource::TemporaryBytes, required - 1)).is_err());
    }
}

#[test]
fn scalar_construction_charges_exact_requested_backing() {
    let slot = size_of::<Expr>() as u64;
    let cases = [
        (field(), 4),
        (
            SqlExpr::FieldPath {
                root: "name".to_string(),
                segments: vec!["a".to_string(), "bb".to_string()],
            },
            7 + 2 * size_of::<String>() as u64,
        ),
        (
            SqlExpr::Binary {
                op: SqlExprBinaryOp::Eq,
                left: Box::new(field()),
                right: Box::new(literal()),
            },
            4 + 2 * slot,
        ),
        (
            SqlExpr::Unary {
                op: SqlExprUnaryOp::Not,
                expr: Box::new(literal()),
            },
            slot,
        ),
        (
            SqlExpr::NullTest {
                expr: Box::new(field()),
                negated: false,
            },
            4 + slot,
        ),
        (
            SqlExpr::FunctionCall {
                function: SqlScalarFunction::Lower,
                args: vec![field()],
            },
            4 + slot,
        ),
        (
            SqlExpr::FunctionCall {
                function: SqlScalarFunction::Round,
                args: vec![literal()],
            },
            2 * slot,
        ),
        (
            SqlExpr::FunctionCall {
                function: SqlScalarFunction::Round,
                args: vec![literal(), literal()],
            },
            2 * slot,
        ),
        (
            SqlExpr::Case {
                arms: vec![SqlCaseArm {
                    condition: literal(),
                    result: literal(),
                }],
                else_expr: None,
            },
            size_of::<CaseWhenArm>() as u64 + slot,
        ),
        (
            SqlExpr::Membership {
                expr: Box::new(field()),
                values: vec![
                    SqlMembershipValue::Literal(Value::Nat64(1)),
                    SqlMembershipValue::Literal(Value::Nat64(2)),
                ],
                negated: true,
            },
            4 + 2 * size_of::<Value>() as u64 + 3 * slot,
        ),
        (
            SqlExpr::Like {
                expr: Box::new(field()),
                pattern: "ab%".to_string(),
                negated: true,
                casefold: true,
            },
            6 + 4 * slot,
        ),
    ];
    for (input, bytes) in cases {
        let exact = root(Resource::TemporaryBytes, bytes);
        assert!(lower(&input, &exact).is_ok(), "{input:?}");
        assert_eq!(exact.observed(Resource::TemporaryBytes), bytes, "{input:?}");
        let short = root(Resource::TemporaryBytes, bytes - 1);
        let error = lower(&input, &short).expect_err("must reject before last backing allocation");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
    }
}

#[test]
fn expression_visits_and_identifier_copy_work_have_exact_limits() {
    let input = SqlExpr::FieldPath {
        root: "name".to_string(),
        segments: vec!["a".to_string(), "bb".to_string()],
    };
    // One root visit, two path-segment visits and seven copied bytes.
    let exact = root(Resource::PredicateExpressionSteps, 10);
    lower(&input, &exact).unwrap();
    assert_eq!(exact.observed(Resource::PredicateExpressionSteps), 10);
    assert!(lower(&input, &root(Resource::PredicateExpressionSteps, 9)).is_err());
    assert!(lower(&literal(), &root(Resource::PredicateExpressionSteps, 0)).is_err());
}

#[test]
fn failed_construction_preserves_syntax_and_accumulates_retry_work() {
    let input = SqlExpr::Binary {
        op: SqlExprBinaryOp::Eq,
        left: Box::new(field()),
        right: Box::new(field()),
    };
    let original = input.clone();
    // Permit the first field and box, but reject the second field copy.
    let request = root(Resource::TemporaryBytes, 4 + size_of::<Expr>() as u64);
    let mut observed = 0;
    for _ in 0..2 {
        assert!(lower(&input, &request).is_err());
        assert_eq!(input, original);
        assert!(request.observed(Resource::TemporaryBytes) > observed);
        observed = request.observed(Resource::TemporaryBytes);
    }
    assert!(
        lower(
            &input,
            &root(Resource::TemporaryBytes, 8 + 2 * size_of::<Expr>() as u64)
        )
        .is_ok()
    );
}
