//! Synthetic counter qualification of the real budget sampling seams.
//! These are deterministic accounting controls, not IC performance measurements.

use crate::{
    db::{
        RequestExecutionRoot,
        resource::budget::{
            HardExecutionBudget, HardExecutionBudgetTracker, HardExecutionContext,
            HardExecutionFailureHeadroom, MUTATION_EXECUTION_INSTRUCTION_LIMIT,
            MaintenanceConstructionBudget, finish_current_execution_instruction_watermark,
            with_mutation_execution_budget, with_read_execution_budget,
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
const MUTATION_CONTEXT: HardExecutionContext = HardExecutionContext::new(
    DiagnosticExecutionBudgetScope::Execution,
    DiagnosticExecutionLane::Mutation,
    2,
);

fn assert_message_exhaustion(error: InternalError, actual: u64) {
    let facts = error.diagnostic_facts();
    for fact in [
        (
            DiagnosticFactTag::BudgetResource,
            Resource::InstructionUnits.raw(),
        ),
        (
            DiagnosticFactTag::Limit,
            MUTATION_EXECUTION_INSTRUCTION_LIMIT,
        ),
        (DiagnosticFactTag::Actual, actual),
        (
            DiagnosticFactTag::ExecutionBudgetScope,
            DiagnosticExecutionBudgetScope::Execution.raw(),
        ),
    ] {
        assert!(facts.contains(&fact));
    }
}

#[test]
fn mutation_message_ceiling_is_not_reset_by_a_second_advance() {
    let limit = MUTATION_EXECUTION_INSTRUCTION_LIMIT;
    with_instruction_counter_for_tests(limit - 1, |counter| {
        with_mutation_execution_budget(
            MUTATION_CONTEXT,
            || {
                counter.set(limit);
                finish_current_execution_instruction_watermark()
            },
            std::convert::identity,
        )
        .unwrap();

        counter.set(limit + 1);
        let mut entered = false;
        let error = with_mutation_execution_budget(
            MUTATION_CONTEXT,
            || {
                entered = true;
                Ok::<_, InternalError>(())
            },
            std::convert::identity,
        )
        .unwrap_err();
        assert!(!entered);
        assert_message_exhaustion(error, limit + 1);
    });
}

#[test]
fn mutation_message_ceiling_checks_prior_work_at_publication() {
    let limit = MUTATION_EXECUTION_INSTRUCTION_LIMIT;
    with_instruction_counter_for_tests(limit - 2, |counter| {
        let mut published = false;
        let error = with_mutation_execution_budget(
            MUTATION_CONTEXT,
            || {
                counter.set(limit + 1);
                finish_current_execution_instruction_watermark()?;
                published = true;
                Ok::<_, InternalError>(())
            },
            std::convert::identity,
        )
        .unwrap_err();
        assert!(!published);
        assert_message_exhaustion(error, limit + 1);
    });
}

#[test]
fn maintenance_message_ceiling_is_not_reset_by_a_fresh_budget() {
    let limit = MUTATION_EXECUTION_INSTRUCTION_LIMIT;
    with_instruction_counter_for_tests(limit - 1, |counter| {
        let first = MaintenanceConstructionBudget::new();
        first
            .run(
                |_| {
                    counter.set(limit);
                    Ok::<_, InternalError>(())
                },
                std::convert::identity,
            )
            .unwrap();

        counter.set(limit + 1);
        let second = MaintenanceConstructionBudget::new();
        let mut entered = false;
        let error = second
            .run(
                |_| {
                    entered = true;
                    Ok::<_, InternalError>(())
                },
                std::convert::identity,
            )
            .unwrap_err();
        assert!(!entered);
        assert_message_exhaustion(error, limit + 1);
    });
}

#[test]
fn mutation_message_ceiling_preserves_per_operation_instruction_usage() {
    with_instruction_counter_for_tests(5, |counter| {
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, HEADROOM)
            .with_limit_for_tests(Resource::InstructionUnits, 7);
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, MUTATION_CONTEXT);
        tracker.check_instruction_watermark().unwrap();
        assert_eq!(tracker.observed(Resource::InstructionUnits), 0);
        counter.set(7);
        tracker.check_instruction_watermark().unwrap();
        assert_eq!(tracker.observed(Resource::InstructionUnits), 2);
        counter.set(8);
        let error = tracker.finish_instruction_watermark().unwrap_err();
        assert_eq!(error.resource(), Resource::InstructionUnits);
        assert_eq!(error.limit(), 7);
        assert_eq!(error.observed(), 8);
        assert_eq!(tracker.observed(Resource::InstructionUnits), 3);
    });
}

#[test]
fn maintenance_message_ceiling_refuses_completed_work_before_handoff() {
    let limit = MUTATION_EXECUTION_INSTRUCTION_LIMIT;
    with_instruction_counter_for_tests(limit - 1, |counter| {
        let budget = MaintenanceConstructionBudget::new();
        let error = budget
            .run(
                |_| {
                    counter.set(limit + 1);
                    Ok::<_, InternalError>(())
                },
                std::convert::identity,
            )
            .unwrap_err();
        assert_message_exhaustion(error, limit + 1);
        let mut entered = false;
        let error = budget
            .run(
                |_| {
                    entered = true;
                    Ok::<_, InternalError>(())
                },
                std::convert::identity,
            )
            .unwrap_err();
        assert!(!entered);
        assert_message_exhaustion(error, limit + 1);
    });
}

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
