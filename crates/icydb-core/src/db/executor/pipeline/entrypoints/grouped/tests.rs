use super::*;
use crate::{
    db::{
        QueryError,
        cursor::{ContinuationSignature, GroupedContinuationToken, encode_cursor},
        direction::Direction,
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetScope, DiagnosticFactTag};

fn budget() -> HardExecutionBudget {
    HardExecutionBudget::uniform_for_tests(1_000_000, HardExecutionFailureHeadroom::new(1, 1))
}

fn context() -> HardExecutionContext {
    HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0,
    )
}

#[test]
fn grouped_cursor_charges_exact_binary_and_base64_bounds() {
    // Exercise all three unpadded Base64 remainder lengths with real tokens.
    for length in 1..=3 {
        let token = GroupedContinuationToken::new_with_direction(
            ContinuationSignature::from_bytes([1; 32]),
            vec![Value::Text("x".repeat(length))],
            Direction::Asc,
            0,
        );
        let encoded = token.encode(&[2; 32]).unwrap();
        for (resource, exact) in [
            (DiagnosticExecutionBudgetResource::CursorSteps, 1),
            (
                DiagnosticExecutionBudgetResource::TemporaryBytes,
                encoded.len() as u64,
            ),
            (
                DiagnosticExecutionBudgetResource::ResultBytes,
                encode_cursor(&encoded).len() as u64,
            ),
        ] {
            for (limit, accepted) in [(exact, true), (exact - 1, false)] {
                let result = with_query_execution_budget_for_tests(
                    budget().with_limit_for_tests(resource, limit),
                    context(),
                    || charge_grouped_cursor_bytes(&encoded).map_err(QueryError::execute),
                );
                if accepted {
                    result.expect("exact resource bound must admit");
                } else {
                    let error = result.expect_err("one byte/step below must reject");
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                }
            }
        }
    }
}

#[test]
fn grouped_page_without_cursor_consumes_no_cursor_bytes_or_steps() {
    let budget = budget()
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::CursorSteps, 0)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0)
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, 0);
    let page = GroupedCursorPage {
        rows: vec![],
        next_cursor: None,
    };
    let result = with_query_execution_budget_for_tests(budget, context(), || {
        charge_grouped_page_result(&page).map_err(QueryError::execute)
    })
    .unwrap();
    assert!(result.is_none());
}
