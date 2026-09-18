//! Completed optional programs retain their policy, absence and owned backing.

use super::*;
use crate::{
    db::{
        index::{IndexCompareOp, IndexLiteral},
        predicate::{ExecutablePredicate, MissingRowPolicy},
        query::preparation::with_preparation_work,
    },
    retained::RetainedBytes,
};
use std::cell::Cell;

const POLICIES: [IndexCompilePolicy; 2] = [
    IndexCompilePolicy::ConservativeSubset,
    IndexCompilePolicy::StrictAllOrNone,
];

#[test]
fn completed_unsupported_programs_do_not_recompile_after_reuse_or_clone() {
    let predicate = ExecutablePredicate::IsNull {
        field_slot: Some(0),
    };
    for policy in POLICIES {
        let prepared = PreparedIndexProgram {
            policy,
            program: with_preparation_work(|work| {
                compile_index_program(&predicate, &[0], policy, work)
            })
            .unwrap(),
        };
        assert!(prepared.program.is_none());
        for resident in [&prepared, &prepared.clone()] {
            for _ in 0..3 {
                assert!(
                    PreparedIndexProgram::resolve(Some(resident), policy, || {
                        panic!("a completed unsupported result must not compile again")
                    })
                    .unwrap()
                    .is_none()
                );
            }
        }
    }
}

#[test]
fn completed_supported_programs_are_borrowed() {
    for policy in POLICIES {
        let prepared = PreparedIndexProgram {
            policy,
            program: Some(IndexPredicateProgram::False),
        };
        let resolved = PreparedIndexProgram::resolve(Some(&prepared), policy, || {
            panic!("a completed program must be reused")
        })
        .unwrap()
        .expect("the prepared program should remain available");
        let Cow::Borrowed(program) = resolved else {
            panic!("reuse must borrow the existing program");
        };
        assert!(std::ptr::eq(program, prepared.program.as_ref().unwrap()));
    }
}

#[test]
fn missing_or_different_policy_compiles_once_without_reusing_another_policy_result() {
    // Conservative compilation can retain false; strict compilation must
    // reject the unsupported null test. Neither result may stand in for the other.
    let predicate = ExecutablePredicate::And(vec![
        ExecutablePredicate::False,
        ExecutablePredicate::IsNull {
            field_slot: Some(0),
        },
    ]);
    for policy in POLICIES {
        let prepared = PreparedIndexProgram {
            policy,
            program: with_preparation_work(|work| {
                compile_index_program(&predicate, &[0], policy, work)
            })
            .unwrap(),
        };
        let other = if policy == IndexCompilePolicy::ConservativeSubset {
            IndexCompilePolicy::StrictAllOrNone
        } else {
            IndexCompilePolicy::ConservativeSubset
        };
        let expected =
            with_preparation_work(|work| compile_index_program(&predicate, &[0], other, work))
                .unwrap();
        assert_ne!(prepared.program, expected);
        for resident in [None, Some(&prepared)] {
            let calls = Cell::new(0);
            let resolved = PreparedIndexProgram::resolve(resident, other, || {
                calls.set(calls.get() + 1);
                with_preparation_work(|work| compile_index_program(&predicate, &[0], other, work))
            })
            .unwrap();
            assert_eq!(calls.get(), 1);
            assert_eq!(resolved.as_deref(), expected.as_ref());
            assert!(resolved.is_none_or(|program| matches!(program, Cow::Owned(_))));
        }
        assert_eq!(
            prepared.program,
            with_preparation_work(|work| compile_index_program(&predicate, &[0], policy, work))
                .unwrap()
        );
    }
}

#[test]
fn preparation_constructors_retain_completion_only_for_the_requested_policy() {
    let plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let aggregate =
        with_preparation_work(|work| ExecutionPreparation::from_plan(&plan, None, work)).unwrap();
    let scalar =
        with_preparation_work(|work| ExecutionPreparation::from_runtime_plan(&plan, None, work))
            .unwrap();
    assert!(matches!(
        aggregate.index_program,
        Some(PreparedIndexProgram {
            policy: IndexCompilePolicy::StrictAllOrNone,
            program: None
        })
    ));
    assert!(matches!(
        scalar.index_program,
        Some(PreparedIndexProgram {
            policy: IndexCompilePolicy::ConservativeSubset,
            program: None
        })
    ));
    for preparation in [aggregate, scalar] {
        for policy in POLICIES {
            assert!(preparation.prepared_index_program(policy).is_none());
            assert!(
                with_preparation_work(|work| preparation.resolve_index_program(policy, work))
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn plan_preparation_preserves_target_precedence_and_optional_inputs() {
    use crate::db::{
        predicate::{IndexCompileTargetKind, IndexPredicateCapability, Predicate},
        query::plan::exact_metadata_schema,
    };
    use crate::value::Value;

    let schema = exact_metadata_schema(&[], &[]);
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    assert!(predicate_capability_profile_for_plan(&plan).is_none());
    with_preparation_work(|work| {
        let projection = plan.prepare_projection(&schema, work)?;
        plan.finalize_static_execution_planning_contract_with_schema(&schema, projection, work)
    })
    .unwrap();
    let target = IndexCompileTarget {
        component_index: 0,
        field_slot: 1,
        kind: IndexCompileTargetKind::Field,
    };
    let predicate = PredicateProgram::compile_with_schema_info(
        &schema,
        &Predicate::eq("age".into(), Value::Int64(1)),
    );
    for (targets, slots, expected) in [
        // Explicit targets remain authoritative even if a slot map disagrees.
        (
            Some(vec![target]),
            Some(vec![2]),
            Some(IndexPredicateCapability::FullyIndexable),
        ),
        (
            None,
            Some(vec![1]),
            Some(IndexPredicateCapability::FullyIndexable),
        ),
        (
            None,
            Some(vec![2]),
            Some(IndexPredicateCapability::RequiresFullScan),
        ),
        (None, None, None),
    ] {
        let contract = plan.static_execution_planning_contract.as_mut().unwrap();
        contract.execution_preparation_compiled_predicate = Some(predicate.clone());
        contract.index_compile_targets = targets;
        contract.slot_map = slots;
        let profile = predicate_capability_profile_for_plan(&plan);
        assert_eq!(profile.map(PredicateCapabilityProfile::index), expected);
        let prepared = with_preparation_work(|work| {
            ExecutionPreparation::from_plan(&plan, slot_map_for_model_plan(&plan), work)
        })
        .unwrap();
        assert_eq!(profile, prepared.predicate_capability_profile());
        assert_strict_plan_compilation(
            &plan,
            prepared.prepared_index_program(IndexCompilePolicy::StrictAllOrNone),
        );
    }

    let contract = plan.static_execution_planning_contract.as_mut().unwrap();
    contract.execution_preparation_compiled_predicate = None;
    contract.index_compile_targets = Some(vec![target]);
    contract.slot_map = Some(vec![1]);
    assert!(predicate_capability_profile_for_plan(&plan).is_none());
    assert_strict_plan_compilation(&plan, None);
}

fn assert_strict_plan_compilation(
    plan: &AccessPlannedQuery,
    expected: Option<&IndexPredicateProgram>,
) {
    use crate::db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::preparation::PreparationWork,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    let program =
        with_preparation_work(|work| compile_strict_index_program_for_plan(plan, work)).unwrap();
    assert_eq!(program.as_ref(), expected);
    if expected.is_none() {
        return;
    }

    // Direct and bundled strict compilation must both reject before allocating
    // encoded literals when the caller's construction allowance is exhausted.
    for direct in [true, false] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, 0),
        );
        let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            if direct {
                compile_strict_index_program_for_plan(plan, work).map(|_| ())
            } else {
                ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan), work)
                    .map(|_| ())
            }
            .map_err(QueryError::execute)
        })
        .unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
    }
}

#[test]
fn completed_program_retention_includes_literal_capacity() {
    let bytes = vec![0x2A; 32];
    let expected = size_of::<PreparedIndexProgram>() + bytes.capacity();
    let prepared = PreparedIndexProgram {
        policy: IndexCompilePolicy::StrictAllOrNone,
        program: Some(IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::Eq,
            literal: IndexLiteral::One(bytes),
        }),
    };
    assert_eq!(RetainedBytes::measure(&prepared, expected), Some(expected));
    assert_eq!(RetainedBytes::measure(&prepared, expected - 1), None);
    let absent = PreparedIndexProgram {
        policy: prepared.policy,
        program: None,
    };
    assert_eq!(
        RetainedBytes::measure(&absent, usize::MAX),
        Some(size_of_val(&absent))
    );
}
