use super::*;
use crate::db::{
    QueryError,
    resource::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        charge_current_execution_budget, read_hard_budget_limit_for_tests,
        with_query_execution_budget_for_tests,
    },
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, RuntimeBoundaryCode,
};

const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
    DiagnosticExecutionBudgetScope::Execution,
    DiagnosticExecutionLane::TrustedRead,
    0x2210_0007_0000_0001,
);

#[test]
fn production_units_preserve_inactive_and_nested_admission() {
    assert!(begin_production_scalar_page_unit(usize::MAX).unwrap());
    finish_production_scalar_page_unit().unwrap();
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        with_production_scalar_page_work(PageWorkEnvelope::default_scalar(), || {
            assert!(begin_production_scalar_page_unit(1)?);
            let error = begin_production_scalar_page_unit(1).unwrap_err();
            assert_eq!(
                error.diagnostic_code(),
                InternalError::query_executor_invariant().diagnostic_code(),
            );
            charge_test_production_candidate(1)?;
            finish_production_scalar_page_unit()?;
            assert!(begin_production_scalar_page_unit(1)?);
            finish_production_scalar_page_unit()
        })
        .map_err(QueryError::execute)
    })
    .expect("nested refusal must preserve the original unit and subsequent admission");
}

#[test]
fn output_hard_failure_preserves_charge_order_without_a_page_receipt() {
    use DiagnosticExecutionBudgetResource as Resource;

    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(Resource::MaterializedBytes, 32);
    with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        let mut output =
            ProductionScalarOutputWork::new(PageWorkEnvelope::default_scalar(), None).unwrap();
        let before = current_execution_budget_usage().unwrap();
        let error = output
            .admit_row(&[Value::Text("a".to_string())])
            .unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
            Resource::MaterializedBytes.raw(),
        )));
        let usage = current_execution_budget_usage().unwrap();
        for resource in Resource::ALL {
            assert_eq!(output.receipt().observed(resource), 0);
            let expected = match resource {
                Resource::NestedValueSteps => 1,
                Resource::MaterializedBytes => 33,
                _ => 0,
            };
            assert_eq!(
                usage.observed(resource) - before.observed(resource),
                expected,
                "{resource:?}",
            );
        }
        assert!(!output.envelope_stopped());
        Ok::<_, QueryError>(())
    })
    .expect("inspect the failed charge within its hard execution scope");
}

#[test]
fn candidate_reservation_preserves_all_resource_amounts_and_overflow_rejection() {
    use DiagnosticExecutionBudgetResource as Resource;

    for bound in [0, 1, 64, 1_000] {
        let work = scalar_candidate_inspection_reservation(bound).unwrap();
        let entries = bound as u64;
        let index_bytes = IndexKey::MAX_STORED_SIZE_BYTES * entries;
        for resource in Resource::ALL {
            let expected = match resource {
                Resource::KeyIndexEntriesVisited => entries,
                Resource::RowsVisited => 1,
                Resource::StoredBytesRead | Resource::DecodedBytes => {
                    index_bytes + u64::from(MAX_ROW_BYTES)
                }
                Resource::PredicateExpressionSteps => entries + 1,
                Resource::NestedValueSteps => MAX_SCALAR_ROW_NESTED_STEPS,
                Resource::MaterializedBytes => u64::from(MAX_ROW_BYTES),
                Resource::CursorSteps => entries * 3 + 1,
                Resource::TemporaryBytes => index_bytes,
                _ => 0,
            };
            assert_eq!(
                work.amount(resource),
                expected,
                "{resource:?}, bound {bound}"
            );
        }
    }
    // On narrower hosts the largest usize remains representable after
    // scaling. On 64-bit hosts this route bound must reject, not saturate.
    if let Ok(bound) = usize::try_from(u64::MAX) {
        let error = scalar_candidate_inspection_reservation(bound).unwrap_err();
        let expected = InternalError::query_executor_invariant();
        assert_eq!(error.class(), expected.class());
        assert_eq!(error.origin(), expected.origin());
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    }
}

#[test]
fn production_units_reserve_one_output_row_before_cumulative_row_bytes_cross_the_envelope() {
    const OBSERVED_ROW_BYTES: u64 = 3 * 1_024 * 1_024;
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        with_production_scalar_page_work(PageWorkEnvelope::default_scalar(), || {
            let mut completed = 0_u64;
            while begin_production_scalar_page_unit(1)? {
                charge_test_production_candidate(OBSERVED_ROW_BYTES)?;
                finish_production_scalar_page_unit()?;
                completed = completed.saturating_add(1);
            }
            Ok(completed)
        })
        .map_err(QueryError::execute)
    })
    .expect("the page envelope should stop successfully");

    assert_eq!(result.value, 3);
    assert!(result.envelope_stopped);
    assert_eq!(
        result
            .receipt
            .observed(DiagnosticExecutionBudgetResource::StoredBytesRead),
        3 * OBSERVED_ROW_BYTES,
    );
    assert_eq!(
        result
            .receipt
            .observed(DiagnosticExecutionBudgetResource::PredicateExpressionSteps),
        3,
    );
}

#[test]
fn active_page_publishes_its_finite_access_entry_limit() {
    assert_eq!(
        production_scalar_page_access_entry_limit()
            .expect("inactive page authority should be readable"),
        None,
    );
    let envelope = PageWorkEnvelope::default_scalar()
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 4);
    let result =
        with_production_scalar_page_work(envelope, production_scalar_page_access_entry_limit)
            .expect("active page authority should publish its entry limit");

    assert_eq!(result.value, Some(4));
    assert!(!result.envelope_stopped);
}

#[test]
fn production_units_stop_before_cumulative_expression_work_can_cross_the_envelope() {
    let envelope = PageWorkEnvelope::default_scalar().with_limit_for_tests(
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        5,
    );
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        with_production_scalar_page_work(envelope, || {
            let mut completed = 0_u64;
            while begin_production_scalar_page_unit(1)? {
                charge_test_production_candidate(1)?;
                finish_production_scalar_page_unit()?;
                completed = completed.saturating_add(1);
            }
            Ok(completed)
        })
        .map_err(QueryError::execute)
    })
    .expect("expression work should stop as successful page progress");

    assert_eq!(result.value, 4);
    assert!(result.envelope_stopped);
    assert_eq!(
        result
            .receipt
            .observed(DiagnosticExecutionBudgetResource::PredicateExpressionSteps),
        4,
    );
}

#[test]
fn production_unit_hard_failure_returns_no_successful_page() {
    const OBSERVED_ROW_BYTES: u64 = 3 * 1_024 * 1_024;
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            2 * 1_024 * 1_024,
        );
    let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        with_production_scalar_page_work(PageWorkEnvelope::default_scalar(), || {
            if !begin_production_scalar_page_unit(1)? {
                return Err(InternalError::query_executor_invariant());
            }
            charge_test_production_candidate(OBSERVED_ROW_BYTES)?;
            finish_production_scalar_page_unit()
        })
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("hard exhaustion must discard page progress")
    };

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
}

#[test]
fn production_unit_rejects_a_route_pull_larger_than_an_empty_page() {
    // Index reads must fit alongside the worst-case row reservation.
    let envelope = PageWorkEnvelope::default_scalar().with_limit_for_tests(
        DiagnosticExecutionBudgetResource::StoredBytesRead,
        u64::from(MAX_ROW_BYTES),
    );
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        with_production_scalar_page_work(envelope, || {
            let _ = begin_production_scalar_page_unit(1_000)?;
            Ok(())
        })
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("an unfit route pull must fail before storage work")
    };

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::PageUnitTooLarge,
        })
    ));
}

#[test]
fn production_output_stops_before_crossing_result_bytes_and_charges_discarded_work() {
    let envelope = PageWorkEnvelope::default_scalar()
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, 50);
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let (receipt, usage) = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        (|| {
            let mut output = ProductionScalarOutputWork::new(envelope, None)?;
            assert!(output.admit_row(&[Value::Text("a".to_string())])?);
            assert!(!output.admit_row(&[Value::Text("b".to_string())])?);
            assert!(output.envelope_stopped());
            Ok::<_, InternalError>((output.receipt(), current_execution_budget_usage()?))
        })()
        .map_err(QueryError::execute)
    })
    .expect("output bytes should end the page without a hard failure");

    assert_eq!(
        receipt.observed(DiagnosticExecutionBudgetResource::ResultRows),
        1,
    );
    assert_eq!(
        receipt.observed(DiagnosticExecutionBudgetResource::ResultBytes),
        33,
    );
    assert_eq!(
        usage.observed(DiagnosticExecutionBudgetResource::ResultBytes),
        33,
        "the unreturned row must not be claimed as result output",
    );
    assert_eq!(
        usage.observed(DiagnosticExecutionBudgetResource::MaterializedBytes),
        66,
        "both the returned row and bounded discarded row were materialized",
    );
}

#[test]
fn production_output_continues_the_same_envelope_as_candidate_scanning() {
    const OBSERVED_ROW_BYTES: u64 = 3 * 1_024 * 1_024;
    let envelope = PageWorkEnvelope::default_scalar();
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let receipt = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        (|| {
            let scan = with_production_scalar_page_work(envelope, || {
                let mut completed = 0_u64;
                while begin_production_scalar_page_unit(1)? {
                    charge_test_production_candidate(OBSERVED_ROW_BYTES)?;
                    finish_production_scalar_page_unit()?;
                    completed = completed.saturating_add(1);
                }
                Ok(completed)
            })?;
            assert_eq!(scan.value, 3);

            let mut output = ProductionScalarOutputWork::new(envelope, Some(scan.receipt))?;
            assert!(output.admit_row(&[Value::Blob(vec![0; MAX_ROW_BYTES as usize])])?);
            Ok::<_, InternalError>(output.receipt())
        })()
        .map_err(QueryError::execute)
    })
    .expect("scan and output should share one cumulative page envelope");

    assert_eq!(
        receipt.observed(DiagnosticExecutionBudgetResource::MaterializedBytes),
        3 * OBSERVED_ROW_BYTES + u64::from(MAX_ROW_BYTES) + RUNTIME_VALUE_NODE_OVERHEAD_BYTES,
    );
    assert_eq!(
        receipt.observed(DiagnosticExecutionBudgetResource::ResultRows),
        1,
    );
}

#[test]
fn production_output_rejects_one_row_that_cannot_fit_an_empty_page() {
    let envelope = PageWorkEnvelope::default_scalar()
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, 32);
    let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
        (|| {
            let mut output = ProductionScalarOutputWork::new(envelope, None)?;
            output.admit_row(&[Value::Text("a".to_string())])
        })()
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("one oversized output row must fail instead of returning a stalled page")
    };

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::PageUnitTooLarge,
        })
    ));
}

#[test]
fn default_page_limits_are_strictly_inside_the_read_hard_budget() {
    let envelope = PageWorkEnvelope::default_scalar();
    for resource in DiagnosticExecutionBudgetResource::ALL {
        if let Some(page_limit) = envelope.limit(resource) {
            assert!(
                page_limit < read_hard_budget_limit_for_tests(resource),
                "page resource {resource:?} must preserve hard-budget headroom",
            );
        }
    }
    assert_eq!(
        PageWorkEnvelope::default_scalar().identity(),
        PageWorkEnvelope::default_scalar().identity(),
    );
    assert_ne!(
        envelope.identity(),
        envelope
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultRows, 7)
            .identity(),
    );
}

fn charge_test_production_candidate(row_bytes: u64) -> Result<(), InternalError> {
    for (resource, amount) in [
        (DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1),
        (DiagnosticExecutionBudgetResource::RowsVisited, 1),
        (
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            row_bytes,
        ),
        (
            DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
            1,
        ),
        (DiagnosticExecutionBudgetResource::NestedValueSteps, 1),
        (DiagnosticExecutionBudgetResource::DecodedBytes, row_bytes),
        (
            DiagnosticExecutionBudgetResource::MaterializedBytes,
            row_bytes,
        ),
        (DiagnosticExecutionBudgetResource::CursorSteps, 1),
    ] {
        charge_current_execution_budget(resource, amount)?;
    }

    Ok(())
}
