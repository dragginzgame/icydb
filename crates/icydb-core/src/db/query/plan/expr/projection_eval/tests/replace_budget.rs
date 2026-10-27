//! Exact replacement construction and typed runtime budget propagation.

use super::*;
use crate::db::resource::budget::{
    HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
    current_execution_budget_usage, with_query_execution_budget_for_tests,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
    DiagnosticExecutionLane as Lane, DiagnosticFactTag as Fact,
};

#[test]
fn replacement_sizing_and_construction_match_the_independent_string_oracle() {
    for text in ["", "aaaa", "ababa", "éé", "e\u{301}界", "x\0x"] {
        for from in ["", "aa", "aba", "é", "界", "\0", "missing"] {
            for to in ["", "a", "🍋", "aaaa", "\0"] {
                let expected = if from.is_empty() {
                    text.to_owned()
                } else {
                    text.replace(from, to)
                };
                assert_eq!(
                    Function::replace_text_output_len(text, from, to),
                    expected.len()
                );
                assert_eq!(
                    eval_projection_function_call(
                        Function::Replace,
                        &[
                            Value::Text(text.into()),
                            Value::Text(from.into()),
                            Value::Text(to.into()),
                        ]
                    )
                    .unwrap(),
                    Value::Text(expected),
                );
            }
        }
    }
}

#[test]
fn replace_output_admission_preserves_exact_cumulative_limits_and_error_facts() {
    let args = [
        Value::Text("aaaa".into()),
        Value::Text("a".into()),
        Value::Text("zz".into()),
    ];
    let compiled = CompiledExpr::FunctionCall {
        function: Function::Replace,
        args: args.iter().cloned().map(CompiledExpr::Literal).collect(),
    };
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for use_compiled in [false, true] {
            // One output owns eight bytes. Two calls must share the same
            // counter, and refusal retains the attempted cumulative charge.
            for (limit, repeats, succeeds, observed) in [
                (0, 1, false, 8),
                (7, 1, false, 8),
                (8, 1, true, 8),
                (9, 1, true, 8),
                (15, 2, false, 16),
                (16, 2, true, 16),
            ] {
                let result = with_query_execution_budget_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        1_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(Resource::TemporaryBytes, limit),
                    HardExecutionContext::new(Scope::Execution, lane, 42),
                    || {
                        let mut output = Value::Null;
                        for _ in 0..repeats {
                            output = if use_compiled {
                                compiled
                                    .evaluate(&PreviewParityRow { value: Value::Null })
                                    .map_err(|error| {
                                        QueryError::execute(error.into_internal_error())
                                    })?
                                    .into_owned()
                            } else {
                                eval_projection_function_call(Function::Replace, &args)?
                            };
                        }
                        assert_eq!(
                            current_execution_budget_usage()
                                .map_err(QueryError::execute)?
                                .observed(Resource::TemporaryBytes),
                            observed
                        );
                        Ok(output)
                    },
                );
                if succeeds {
                    assert_eq!(result.unwrap(), Value::Text("zzzzzzzz".into()));
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(
                        error.diagnostic().detail(),
                        Some(&DiagnosticDetail::RuntimeBoundary {
                            boundary:
                                icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
                        })
                    );
                    for fact in [
                        (Fact::BudgetResource, Resource::TemporaryBytes.raw()),
                        (Fact::Limit, limit),
                        (Fact::Actual, observed),
                        (Fact::ExecutionBudgetScope, Scope::Execution.raw()),
                        (Fact::ExecutionLane, lane.raw()),
                        (Fact::QueryShapeFingerprintPrefix, 42),
                    ] {
                        assert!(error.diagnostic_facts().contains(&fact));
                    }
                }
            }
        }
    }
}

#[test]
fn null_replace_arguments_do_not_construct_or_charge_output() {
    for null_index in 0..3 {
        let mut args = [
            Value::Text("aaaa".into()),
            Value::Text("a".into()),
            Value::Text("zz".into()),
        ];
        args[null_index] = Value::Null;
        with_query_execution_budget_for_tests(
            HardExecutionBudget::uniform_for_tests(
                1_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, 0)
            .with_limit_for_tests(Resource::PredicateExpressionSteps, 0),
            HardExecutionContext::new(Scope::Execution, Lane::TrustedRead, 0),
            || {
                assert_eq!(
                    eval_projection_function_call(Function::Replace, &args)?,
                    Value::Null
                );
                assert_eq!(
                    current_execution_budget_usage()
                        .map_err(QueryError::execute)?
                        .observed(Resource::TemporaryBytes),
                    0
                );
                Ok(())
            },
        )
        .unwrap();
    }
}
