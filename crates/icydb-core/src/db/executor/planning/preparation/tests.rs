//! Completed optional programs retain their policy, absence and owned backing.

use super::*;
use crate::{
    db::{
        index::{IndexCompareOp, IndexLiteral},
        predicate::{ExecutablePredicate, MissingRowPolicy},
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
            program: compile_index_program(&predicate, &[0], policy),
        };
        assert!(prepared.program.is_none());
        for resident in [&prepared, &prepared.clone()] {
            for _ in 0..3 {
                assert!(
                    PreparedIndexProgram::resolve(Some(resident), policy, || {
                        panic!("a completed unsupported result must not compile again")
                    })
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
            program: compile_index_program(&predicate, &[0], policy),
        };
        let other = if policy == IndexCompilePolicy::ConservativeSubset {
            IndexCompilePolicy::StrictAllOrNone
        } else {
            IndexCompilePolicy::ConservativeSubset
        };
        let expected = compile_index_program(&predicate, &[0], other);
        assert_ne!(prepared.program, expected);
        for resident in [None, Some(&prepared)] {
            let calls = Cell::new(0);
            let resolved = PreparedIndexProgram::resolve(resident, other, || {
                calls.set(calls.get() + 1);
                compile_index_program(&predicate, &[0], other)
            });
            assert_eq!(calls.get(), 1);
            assert_eq!(resolved.as_deref(), expected.as_ref());
            assert!(resolved.is_none_or(|program| matches!(program, Cow::Owned(_))));
        }
        assert_eq!(
            prepared.program,
            compile_index_program(&predicate, &[0], policy)
        );
    }
}

#[test]
fn preparation_constructors_retain_completion_only_for_the_requested_policy() {
    let plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let aggregate = ExecutionPreparation::from_plan(&plan, None);
    let scalar = ExecutionPreparation::from_runtime_plan(&plan, None);
    let route = ExecutionPreparation::from_covering_route_plan(&plan, None);
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
    assert!(route.index_program.is_none());
    for preparation in [aggregate, scalar, route] {
        for policy in POLICIES {
            assert!(preparation.prepared_index_program(policy).is_none());
            assert!(preparation.resolve_index_program(policy).is_none());
        }
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
