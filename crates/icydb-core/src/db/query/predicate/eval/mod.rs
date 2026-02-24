use crate::{
    db::{
        index::predicate::IndexPredicateProgram,
        query::predicate::{CompareOp, Predicate, coercion::CoercionSpec},
    },
    traits::{EntityKind, EntityValue},
    value::Value,
};

mod index_compile;
mod resolve;
mod runtime;

#[cfg(test)]
pub(crate) use runtime::{FieldPresence, Row, eval};

///
/// PredicateFieldSlots
///
/// Slot-resolved predicate program for runtime row filtering.
/// Field names are resolved once during setup; evaluation is slot-only.
///

#[derive(Clone, Debug)]
pub(crate) struct PredicateFieldSlots {
    resolved: ResolvedPredicate,
    #[cfg_attr(not(test), allow(dead_code))]
    required_slots: Vec<usize>,
}

///
/// ResolvedComparePredicate
///
/// One comparison node with a pre-resolved field slot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedComparePredicate {
    field_slot: Option<usize>,
    op: CompareOp,
    value: Value,
    coercion: CoercionSpec,
}

///
/// ResolvedPredicate
///
/// Predicate AST compiled to field slots for execution hot paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedPredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ResolvedComparePredicate),
    IsNull {
        field_slot: Option<usize>,
    },
    IsMissing {
        field_slot: Option<usize>,
    },
    IsEmpty {
        field_slot: Option<usize>,
    },
    IsNotEmpty {
        field_slot: Option<usize>,
    },
    TextContains {
        field_slot: Option<usize>,
        value: Value,
    },
    TextContainsCi {
        field_slot: Option<usize>,
        value: Value,
    },
}

impl PredicateFieldSlots {
    /// Resolve a predicate into a slot-based executable form.
    #[must_use]
    pub(crate) fn resolve<E: EntityKind>(predicate: &Predicate) -> Self {
        let resolved = resolve_predicate_slots::<E>(predicate);
        let required_slots = collect_required_slots(&resolved);

        Self {
            resolved,
            required_slots,
        }
    }

    /// Return all unique field slots referenced by this compiled predicate.
    ///
    /// Contract:
    /// - sorted ascending
    /// - deduplicated
    /// - excludes unresolved/missing field references
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const fn required_slots(&self) -> &[usize] {
        self.required_slots.as_slice()
    }

    // Compile this predicate into an index-component evaluator program for one
    // concrete index field-slot ordering.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) fn compile_index_program(
        &self,
        index_slots: &[usize],
    ) -> Option<IndexPredicateProgram> {
        compile_index_program_from_resolved(&self.resolved, index_slots)
    }

    // Compile this predicate into an index-component evaluator program only
    // when every predicate node is supported by index-only evaluation.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) fn compile_index_program_strict(
        &self,
        index_slots: &[usize],
    ) -> Option<IndexPredicateProgram> {
        compile_index_program_from_resolved_full(&self.resolved, index_slots)
    }
}

// Collect every resolved field slot referenced by one compiled predicate tree.
fn collect_required_slots(predicate: &ResolvedPredicate) -> Vec<usize> {
    resolve::collect_required_slots(predicate)
}

// Compile one resolved predicate tree into one index-only program.
#[cfg_attr(not(test), allow(dead_code))]
fn compile_index_program_from_resolved(
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    index_compile::compile_index_program_from_resolved(predicate, index_slots)
}

// Compile one resolved predicate tree only when every node is supported.
fn compile_index_program_from_resolved_full(
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    index_compile::compile_index_program_from_resolved_full(predicate, index_slots)
}

// Resolve one predicate tree from field names to field slots.
fn resolve_predicate_slots<E: EntityKind>(predicate: &Predicate) -> ResolvedPredicate {
    resolve::resolve_predicate_slots::<E>(predicate)
}

/// Evaluate one predicate against one entity using pre-resolved field slots.
#[must_use]
pub(crate) fn eval_with_slots<E: EntityValue>(entity: &E, slots: &PredicateFieldSlots) -> bool {
    runtime::eval_with_resolved_slots(entity, &slots.resolved)
}

/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        PredicateFieldSlots, ResolvedComparePredicate, ResolvedPredicate, collect_required_slots,
        compile_index_program_from_resolved,
    };
    use crate::{
        db::{
            index::{
                EncodedValue,
                predicate::{
                    IndexCompareOp, IndexLiteral, IndexPredicateProgram, eval_index_compare,
                },
            },
            query::predicate::{
                CompareOp,
                coercion::{CoercionId, CoercionSpec},
            },
        },
        value::Value,
    };

    #[test]
    fn collect_required_slots_dedups_and_sorts_slots() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(4),
                op: CompareOp::Eq,
                value: Value::Uint(42),
                coercion: CoercionSpec::default(),
            }),
            ResolvedPredicate::Or(vec![
                ResolvedPredicate::IsNull {
                    field_slot: Some(1),
                },
                ResolvedPredicate::IsMissing {
                    field_slot: Some(4),
                },
            ]),
            ResolvedPredicate::Not(Box::new(ResolvedPredicate::TextContains {
                field_slot: Some(2),
                value: Value::Text("x".to_string()),
            })),
            ResolvedPredicate::IsEmpty { field_slot: None },
        ]);

        let slots = collect_required_slots(&predicate);
        assert_eq!(slots, vec![1, 2, 4]);
    }

    #[test]
    fn required_slots_excludes_unresolved_field_references() {
        let resolved = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: None,
                op: CompareOp::Eq,
                value: Value::Uint(9),
                coercion: CoercionSpec::default(),
            }),
            ResolvedPredicate::TextContainsCi {
                field_slot: None,
                value: Value::Text("x".to_string()),
            },
        ]);
        let required_slots = collect_required_slots(&resolved);
        let slots = PredicateFieldSlots {
            resolved,
            required_slots,
        };

        assert!(slots.required_slots().is_empty());
    }

    #[test]
    fn compile_index_program_maps_field_slot_to_component_index() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(7),
            op: CompareOp::Eq,
            value: Value::Uint(11),
            coercion: CoercionSpec::new(CoercionId::Strict),
        });

        let program = compile_index_program_from_resolved(&predicate, &[3, 7, 9])
            .expect("strict EQ over indexed slot should compile");
        let expected = EncodedValue::try_from_ref(&Value::Uint(11))
            .expect("uint literal should encode")
            .encoded()
            .to_vec();

        assert_eq!(
            program,
            IndexPredicateProgram::Compare {
                component_index: 1,
                op: IndexCompareOp::Eq,
                literal: IndexLiteral::One(expected),
            }
        );
    }

    #[test]
    fn compile_index_program_rejects_non_strict_coercion() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(1),
            op: CompareOp::Eq,
            value: Value::Uint(11),
            coercion: CoercionSpec::new(CoercionId::NumericWiden),
        });

        let program = compile_index_program_from_resolved(&predicate, &[1]);
        assert!(program.is_none());
    }

    #[test]
    fn eval_index_compare_applies_membership_semantics() {
        let component = &[1_u8, 2_u8, 3_u8][..];
        let in_literal = IndexLiteral::Many(vec![vec![9_u8], vec![1_u8, 2_u8, 3_u8]]);
        let not_in_literal = IndexLiteral::Many(vec![vec![0_u8], vec![4_u8]]);

        assert!(eval_index_compare(
            component,
            IndexCompareOp::In,
            &in_literal
        ));
        assert!(eval_index_compare(
            component,
            IndexCompareOp::NotIn,
            &not_in_literal
        ));
    }

    #[test]
    fn compile_index_program_operator_matrix_matches_strict_subset() {
        let eligible = [
            (CompareOp::Eq, Value::Uint(11)),
            (CompareOp::Ne, Value::Uint(11)),
            (CompareOp::Lt, Value::Uint(11)),
            (CompareOp::Lte, Value::Uint(11)),
            (CompareOp::Gt, Value::Uint(11)),
            (CompareOp::Gte, Value::Uint(11)),
            (
                CompareOp::In,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
            (
                CompareOp::NotIn,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
        ];
        for (op, value) in eligible {
            let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op,
                value,
                coercion: CoercionSpec::new(CoercionId::Strict),
            });
            let program = compile_index_program_from_resolved(&predicate, &[1]);

            assert!(
                program.is_some(),
                "strict compare op {op:?} should compile into an index predicate program",
            );
        }

        let ineligible = [
            (CompareOp::Contains, Value::Text("x".to_string())),
            (CompareOp::StartsWith, Value::Text("x".to_string())),
            (CompareOp::EndsWith, Value::Text("x".to_string())),
        ];
        for (op, value) in ineligible {
            let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op,
                value,
                coercion: CoercionSpec::new(CoercionId::Strict),
            });
            let program = compile_index_program_from_resolved(&predicate, &[1]);

            assert!(
                program.is_none(),
                "op {op:?} should stay on fallback execution",
            );
        }
    }

    #[test]
    fn compile_index_program_rejects_non_strict_coercion_across_operator_subset() {
        let operators = [
            (CompareOp::Eq, Value::Uint(11)),
            (CompareOp::Ne, Value::Uint(11)),
            (CompareOp::Lt, Value::Uint(11)),
            (CompareOp::Lte, Value::Uint(11)),
            (CompareOp::Gt, Value::Uint(11)),
            (CompareOp::Gte, Value::Uint(11)),
            (
                CompareOp::In,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
            (
                CompareOp::NotIn,
                Value::List(vec![Value::Uint(11), Value::Uint(12)]),
            ),
        ];

        for (op, value) in operators {
            let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op,
                value,
                coercion: CoercionSpec::new(CoercionId::NumericWiden),
            });
            let program = compile_index_program_from_resolved(&predicate, &[1]);

            assert!(
                program.is_none(),
                "non-strict coercion should reject index-only compile for op {op:?}",
            );
        }
    }

    #[test]
    fn compile_index_program_keeps_safe_and_subset_when_residual_is_uncompilable() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op: CompareOp::Eq,
                value: Value::Uint(7),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            ResolvedPredicate::TextContains {
                field_slot: Some(9),
                value: Value::Text("residual".to_string()),
            },
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(2),
                op: CompareOp::In,
                value: Value::List(vec![Value::Uint(10), Value::Uint(20)]),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
        ]);

        let program = compile_index_program_from_resolved(&predicate, &[1, 2]);
        assert!(
            program.is_some(),
            "AND predicates should keep index-only-safe children as a subset",
        );
    }

    #[test]
    fn compile_index_program_rejects_or_with_uncompilable_child() {
        let predicate = ResolvedPredicate::Or(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op: CompareOp::Eq,
                value: Value::Uint(7),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            ResolvedPredicate::TextContains {
                field_slot: Some(9),
                value: Value::Text("residual".to_string()),
            },
        ]);

        let program = compile_index_program_from_resolved(&predicate, &[1, 2]);
        assert!(
            program.is_none(),
            "OR predicates must fail closed when any child is not index-only-safe",
        );
    }
}
