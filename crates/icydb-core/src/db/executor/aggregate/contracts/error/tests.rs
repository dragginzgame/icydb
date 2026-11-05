use super::{GroupBudgetResourceCode, GroupError};
use crate::db::{
    QueryError,
    resource::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        with_query_execution_budget_for_tests,
    },
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope,
    DiagnosticExecutionLane, DiagnosticFactTag, RuntimeBoundaryCode,
};

#[test]
fn grouped_limits_remain_distinct_from_hard_counter_exhaustion() {
    for resource in [
        GroupBudgetResourceCode::Groups,
        GroupBudgetResourceCode::DistinctValuesPerGroup,
        GroupBudgetResourceCode::DistinctValuesTotal,
        GroupBudgetResourceCode::EstimatedBytes,
    ] {
        for grouped in [
            GroupError::memory_limit_exceeded(resource, 11, 10),
            GroupError::distinct_budget_exceeded(resource, 11, 10),
        ] {
            let budget = HardExecutionBudget::uniform_for_tests(
                u64::MAX,
                HardExecutionFailureHeadroom::new(500, 256),
            );
            let context = HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::TrustedRead,
                17,
            );
            let error = with_query_execution_budget_for_tests(budget, context, || {
                let hard = crate::db::resource::budget::current_execution_budget_exceeded(
                    resource.execution_resource(),
                    10,
                    11,
                );
                let error = grouped.into_internal_error();
                assert_eq!(error.diagnostic_code(), hard.diagnostic_code());
                let facts = error.diagnostic_facts();
                assert_eq!(&facts[..6], hard.diagnostic_facts());
                assert_eq!(facts[6], (DiagnosticFactTag::GroupedLimit, resource.raw()));
                Err::<(), _>(QueryError::execute(error))
            })
            .unwrap_err();
            assert_eq!(
                error.diagnostic_facts().last(),
                Some(&(DiagnosticFactTag::GroupedLimit, resource.raw()))
            );
        }
    }
    let error = GroupError::memory_limit_exceeded(GroupBudgetResourceCode::Groups, 11, 10)
        .into_internal_error();
    assert_eq!(
        error.diagnostic_code(),
        crate::error::InternalError::query_executor_invariant().diagnostic_code()
    );
}

#[test]
fn grouped_local_limit_exhaustion_preserves_typed_execution_context() {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    );
    let context = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x6772_6f75_702d_6361,
    );
    let error = with_query_execution_budget_for_tests(budget, context, || {
        Err::<(), _>(QueryError::execute(
            GroupError::memory_limit_exceeded(
                GroupBudgetResourceCode::EstimatedBytes,
                2_048,
                1_024,
            )
            .into_internal_error(),
        ))
    })
    .expect_err("grouped local state exhaustion should remain typed");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (
                DiagnosticFactTag::BudgetResource,
                DiagnosticExecutionBudgetResource::GroupDistinctStateBytes.raw(),
            ),
            (DiagnosticFactTag::Limit, 1_024),
            (DiagnosticFactTag::Actual, 2_048),
            (DiagnosticFactTag::ExecutionBudgetScope, 1),
            (DiagnosticFactTag::ExecutionLane, 2),
            (
                DiagnosticFactTag::QueryShapeFingerprintPrefix,
                0x6772_6f75_702d_6361,
            ),
            (
                DiagnosticFactTag::GroupedLimit,
                GroupBudgetResourceCode::EstimatedBytes.raw()
            ),
        ],
    );
}
