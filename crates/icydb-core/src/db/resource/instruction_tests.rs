//! Synthetic counter qualification of the real budget sampling seams.
//! These are deterministic accounting controls, not IC performance measurements.

use crate::{
    db::{
        RequestExecutionRoot,
        resource::budget::{
            HardExecutionBudget, HardExecutionBudgetTracker, HardExecutionContext,
            HardExecutionFailureHeadroom, with_read_execution_budget,
        },
    },
    error::InternalError,
    runtime::{local_instruction_counter, with_instruction_counter_for_tests},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope,
    DiagnosticExecutionLane, DiagnosticFactTag,
};

const HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 128);
const CONTEXT: HardExecutionContext = HardExecutionContext::new(
    DiagnosticExecutionBudgetScope::Execution,
    DiagnosticExecutionLane::PublicRead,
    1,
);

#[test]
fn native_counter_scope_restores_after_nested_scope_and_unwind() {
    let original = local_instruction_counter();
    with_instruction_counter_for_tests(100, |outer| {
        assert_eq!(local_instruction_counter(), 100);
        with_instruction_counter_for_tests(200, |inner| {
            inner.set(250);
            assert_eq!(local_instruction_counter(), 250);
        });
        assert_eq!(local_instruction_counter(), 100);
        assert!(
            std::panic::catch_unwind(|| {
                with_instruction_counter_for_tests(300, |_| panic!("test counter unwind"));
            })
            .is_err()
        );
        assert_eq!(local_instruction_counter(), 100);
        outer.set(107);
        assert_eq!(local_instruction_counter(), 107);
    });
    assert_eq!(local_instruction_counter(), original);
}

#[test]
fn runtime_samples_charge_exact_deltas_and_reject_instruction_exhaustion() {
    with_instruction_counter_for_tests(100, |counter| {
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, HEADROOM)
            .with_limit_for_tests(Resource::InstructionUnits, 7);
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, CONTEXT);
        tracker.check_instruction_watermark().unwrap();
        assert_eq!(tracker.observed(Resource::InstructionUnits), 0);
        counter.set(107);
        tracker.check_instruction_watermark().unwrap();
        tracker.check_instruction_watermark().unwrap();
        assert_eq!(tracker.observed(Resource::InstructionUnits), 7);
        counter.set(108);
        let failure = tracker.finish_instruction_watermark().unwrap_err();
        assert_eq!(failure.resource(), Resource::InstructionUnits);
        assert_eq!(failure.limit(), 7);
        assert_eq!(failure.observed(), 8);
    });
}

#[test]
fn read_scope_samples_first_interval_and_finish_against_request_limit() {
    for limit in [6, 7] {
        with_instruction_counter_for_tests(100, |counter| {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(u64::MAX, HEADROOM)
                    .with_limit_for_tests(Resource::InstructionUnits, limit),
            );
            let result = with_read_execution_budget(&root.scope(), CONTEXT, || {
                counter.set(104);
                // Nested entry must neither reset the active owner's baseline
                // nor count the same instruction interval twice.
                with_read_execution_budget(&root.scope(), CONTEXT, || {
                    counter.set(107);
                    Ok::<_, InternalError>(())
                })
            });
            assert_eq!(root.observed(Resource::InstructionUnits), 7);
            if limit == 7 {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::InstructionUnits.raw(),
                )));
            }
        });
    }
}
