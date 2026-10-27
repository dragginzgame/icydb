//! Literal construction borrows preparation's cumulative request authority.

use super::*;
use crate::db::{
    QueryError, RequestExecutionRoot,
    query::preparation::PreparationWork,
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::{DiagnosticExecutionLane as Lane, DiagnosticFactTag as Fact};

fn replace(input: Expr, from: &str, to: &str) -> Expr {
    Expr::FunctionCall {
        function: Function::Replace,
        args: vec![
            input,
            Expr::Literal(Value::Text(from.into())),
            Expr::Literal(Value::Text(to.into())),
        ],
    }
}

fn fold(expr: &Expr, limit: u64, lane: Lane) -> (Result<Option<Value>, QueryError>, u64) {
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            1_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, limit),
    );
    let result = PreparationWork::run(&root.scope(), lane, |work| {
        eval_literal_only_expr_value(expr, work).map_err(QueryError::execute)
    });
    (result, root.observed(Resource::TemporaryBytes))
}

#[test]
fn literal_replacement_admits_exact_backing_and_shares_nested_construction() {
    let expr = replace(Expr::Literal(Value::Text("aaaa".into())), "a", "zz");
    let exact = 3 * size_of::<Value>() as u64 + 7 + 8;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [exact - 1, exact, exact + 1] {
            let (result, observed) = fold(&expr, limit, lane);
            assert_eq!(observed, exact);
            if limit < exact {
                let facts = result.unwrap_err().diagnostic_facts();
                assert!(facts.contains(&(Fact::BudgetResource, Resource::TemporaryBytes.raw())));
                assert!(facts.contains(&(Fact::Limit, limit)));
                assert!(facts.contains(&(Fact::Actual, exact)));
                assert!(facts.contains(&(Fact::ExecutionLane, lane.raw())));
            } else {
                assert_eq!(result.unwrap(), Some(Value::Text("zzzzzzzz".into())));
            }
        }
        let nested = replace(expr.clone(), "z", "yy");
        let nested_exact = 6 * size_of::<Value>() as u64 + 7 + 8 + 3 + 16;
        for limit in [nested_exact - 1, nested_exact] {
            let (result, observed) = fold(&nested, limit, lane);
            assert_eq!(observed, nested_exact);
            if limit < nested_exact {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), Some(Value::Text("y".repeat(16))));
            }
        }
    }
}

#[test]
fn literal_case_skips_unreachable_construction_and_retains_unfoldable_results() {
    let expr = Expr::Case {
        when_then_arms: vec![CaseWhenArm::new(
            Expr::Literal(Value::Bool(true)),
            Expr::Literal(Value::Null),
        )],
        else_expr: Box::new(replace(
            Expr::Literal(Value::Text("a".repeat(64))),
            "a",
            &"z".repeat(64),
        )),
    };
    let (result, observed) = fold(&expr, 0, Lane::TrustedRead);
    assert_eq!(result.unwrap(), Some(Value::Null));
    assert_eq!(observed, 0);
    let (result, _) = fold(
        &Expr::Field(crate::db::query::plan::expr::FieldId::from("label")),
        0,
        Lane::TrustedRead,
    );
    assert_eq!(result.unwrap(), None);
}

#[test]
fn literal_text_transforms_share_preparation_admission_and_nested_charges() {
    let text = " é🍋界 ";
    for (function, args, expected) in [
        (
            Function::Lower,
            vec![Value::Text("ΟΣ\u{301}".into())],
            "ΟΣ\u{301}".to_lowercase(),
        ),
        (
            Function::Upper,
            vec![Value::Text("ΐ".into())],
            "ΐ".to_uppercase(),
        ),
        (
            Function::Trim,
            vec![Value::Text(text.into())],
            text.trim().into(),
        ),
        (
            Function::Ltrim,
            vec![Value::Text(text.into())],
            text.trim_start().into(),
        ),
        (
            Function::Rtrim,
            vec![Value::Text(text.into())],
            text.trim_end().into(),
        ),
        (
            Function::Left,
            vec![Value::Text("é🍋界".into()), Value::Int64(2)],
            "é🍋".into(),
        ),
        (
            Function::Right,
            vec![Value::Text("é🍋界".into()), Value::Int64(2)],
            "🍋界".into(),
        ),
        (
            Function::Substring,
            vec![
                Value::Text("é🍋界".into()),
                Value::Int64(2),
                Value::Int64(1),
            ],
            "🍋".into(),
        ),
    ] {
        let expr = Expr::FunctionCall {
            function,
            args: args.into_iter().map(Expr::Literal).collect(),
        };
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for nested in [false, true] {
                let expr = if nested {
                    Expr::FunctionCall {
                        function: Function::Upper,
                        args: vec![expr.clone()],
                    }
                } else {
                    expr.clone()
                };
                let expected = if nested {
                    expected.to_uppercase()
                } else {
                    expected.clone()
                };
                let (result, exact) = fold(&expr, 1_000_000, lane);
                assert_eq!(result.unwrap(), Some(Value::Text(expected.clone())));
                for limit in [exact - 1, exact, exact + 1] {
                    let (result, observed) = fold(&expr, limit, lane);
                    assert_eq!(observed, exact);
                    if limit < exact {
                        let facts = result.unwrap_err().diagnostic_facts();
                        for fact in [
                            (Fact::BudgetResource, Resource::TemporaryBytes.raw()),
                            (Fact::Limit, limit),
                            (Fact::Actual, exact),
                            (Fact::ExecutionLane, lane.raw()),
                        ] {
                            assert!(facts.contains(&fact));
                        }
                    } else {
                        assert_eq!(result.unwrap(), Some(Value::Text(expected.clone())));
                    }
                }
            }
        }
    }
}
