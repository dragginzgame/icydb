//! Text construction uses one budget policy for direct and compiled evaluation.

use super::*;
use crate::{
    db::resource::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        current_execution_budget_usage, with_query_execution_budget_for_tests,
    },
    value::{lower_text_construction_allowance, upper_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
    DiagnosticExecutionLane as Lane, DiagnosticFactTag as Fact,
};

fn cases() -> Vec<(Function, Vec<Value>, Value, u64)> {
    let text = " é🍋界 ";
    let mut cases = [
        (
            Function::Lower,
            "ΟΣ\u{301}",
            "ΟΣ\u{301}".to_lowercase(),
            lower_text_construction_allowance("ΟΣ\u{301}".len()).0,
        ),
        (
            Function::Upper,
            "ΐ",
            "ΐ".to_uppercase(),
            upper_text_construction_allowance("ΐ".len()).0,
        ),
        (
            Function::Trim,
            text,
            text.trim().into(),
            text.trim().len() as u64,
        ),
        (
            Function::Ltrim,
            text,
            text.trim_start().into(),
            text.trim_start().len() as u64,
        ),
        (
            Function::Rtrim,
            text,
            text.trim_end().into(),
            text.trim_end().len() as u64,
        ),
    ]
    .into_iter()
    .map(|(function, input, expected, backing)| {
        (
            function,
            vec![Value::Text(input.into())],
            Value::Text(expected),
            backing,
        )
    })
    .collect::<Vec<_>>();
    for (function, args, expected) in [
        (
            Function::Left,
            vec![Value::Text("é🍋界".into()), Value::Int64(2)],
            "é🍋",
        ),
        (
            Function::Right,
            vec![Value::Text("é🍋界".into()), Value::Int64(2)],
            "🍋界",
        ),
        (
            Function::Substring,
            vec![
                Value::Text("é🍋界".into()),
                Value::Int64(2),
                Value::Int64(1),
            ],
            "🍋",
        ),
        (
            Function::Substring,
            vec![Value::Text("é🍋界".into()), Value::Int64(2)],
            "🍋界",
        ),
    ] {
        cases.push((
            function,
            args,
            Value::Text(expected.into()),
            expected.len() as u64,
        ));
    }
    cases
}

#[test]
fn text_construction_preserves_exact_cumulative_refusals_and_typed_facts() {
    for (function, args, expected, backing) in cases() {
        let compiled = CompiledExpr::FunctionCall {
            function,
            args: args.iter().cloned().map(CompiledExpr::Literal).collect(),
        };
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for compiled_route in [false, true] {
                for (limit, repeats) in [
                    (backing - 1, 1),
                    (backing, 1),
                    (backing + 1, 1),
                    (2 * backing - 1, 2),
                    (2 * backing, 2),
                ] {
                    let result = with_query_execution_budget_for_tests(
                        HardExecutionBudget::uniform_for_tests(
                            1_000_000,
                            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                        )
                        .with_limit_for_tests(Resource::TemporaryBytes, limit),
                        HardExecutionContext::new(Scope::Execution, lane, 42),
                        || {
                            let mut value = Value::Null;
                            for _ in 0..repeats {
                                value = if compiled_route {
                                    compiled
                                        .evaluate(&PreviewParityRow { value: Value::Null })
                                        .map_err(|error| {
                                            QueryError::execute(error.into_internal_error())
                                        })?
                                        .into_owned()
                                } else {
                                    eval_projection_function_call(function, &args)?
                                };
                            }
                            assert_eq!(
                                current_execution_budget_usage()
                                    .map_err(QueryError::execute)?
                                    .observed(Resource::TemporaryBytes),
                                repeats * backing
                            );
                            Ok(value)
                        },
                    );
                    if limit >= repeats * backing {
                        assert_eq!(result.unwrap(), expected);
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        for fact in [
                            (Fact::BudgetResource, Resource::TemporaryBytes.raw()),
                            (Fact::Limit, limit),
                            (Fact::Actual, repeats * backing),
                            (Fact::ExecutionBudgetScope, Scope::Execution.raw()),
                            (Fact::ExecutionLane, lane.raw()),
                            (Fact::QueryShapeFingerprintPrefix, 42),
                        ] {
                            assert!(facts.contains(&fact), "{function:?}");
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn text_walks_admit_work_before_visiting_input_and_null_constructs_nothing() {
    for (function, args, _, _) in cases() {
        for null in [false, true] {
            let mut args = args.clone();
            if null {
                args[0] = Value::Null;
            }
            let result = with_query_execution_budget_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    1_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::PredicateExpressionSteps, 0)
                .with_limit_for_tests(Resource::TemporaryBytes, 0),
                HardExecutionContext::new(Scope::Execution, Lane::TrustedRead, 0),
                || eval_projection_function_call(function, &args),
            );
            if null {
                assert_eq!(result.unwrap(), Value::Null);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    Fact::BudgetResource,
                    Resource::PredicateExpressionSteps.raw()
                )));
            }
        }
    }
}

#[test]
fn borrowed_text_slices_match_character_oracles_at_empty_and_extreme_coordinates() {
    for text in ["", "ascii", "é🍋界", "e\u{301}界", "\0x\0"] {
        for count in [i64::MIN, -1, 0, 1, 2, 32, i64::MAX] {
            let take = usize::try_from(count).unwrap_or(if count < 0 { 0 } else { usize::MAX });
            let chars = text.chars().count();
            for (function, expected) in [
                (Function::Left, text.chars().take(take).collect::<String>()),
                (
                    Function::Right,
                    text.chars().skip(chars.saturating_sub(take)).collect(),
                ),
            ] {
                assert_eq!(
                    eval_projection_function_call(
                        function,
                        &[Value::Text(text.into()), Value::Int64(count),]
                    )
                    .unwrap(),
                    Value::Text(expected)
                );
            }
            for length in [
                None,
                Some(i64::MIN),
                Some(0),
                Some(1),
                Some(2),
                Some(i64::MAX),
            ] {
                let expected = if count <= 0 || length.is_some_and(|length| length <= 0) {
                    String::new()
                } else {
                    let start = usize::try_from(count - 1).unwrap_or(usize::MAX);
                    let take = length.map_or(usize::MAX, |length| {
                        usize::try_from(length).unwrap_or(usize::MAX)
                    });
                    text.chars().skip(start).take(take).collect()
                };
                let mut args = vec![Value::Text(text.into()), Value::Int64(count)];
                if let Some(length) = length {
                    args.push(Value::Int64(length));
                }
                assert_eq!(
                    eval_projection_function_call(Function::Substring, &args).unwrap(),
                    Value::Text(expected)
                );
            }
        }
    }
}
