use crate::{
    db::{
        contracts::{CoercionId, CompareOp},
        executor::predicate_runtime::{
            PredicateFieldSlots, ResolvedComparePredicate, ResolvedPredicate,
        },
        index::{
            IndexCompareOp, IndexLiteral, IndexPredicateProgram,
            predicate::literal_index_component_bytes,
        },
    },
    value::Value,
};

///
/// IndexPredicateCompileMode
///
/// Predicate compile policy for index-only prefilter programs.
/// `ConservativeSubset` keeps load behavior by compiling safe AND-subsets.
/// `StrictAllOrNone` compiles only when every predicate node is supported.
///

#[derive(Clone, Copy)]
pub(crate) enum IndexPredicateCompileMode {
    ConservativeSubset,
    StrictAllOrNone,
}

/// Compile one optional index-only predicate program from pre-resolved slots.
/// This is the single compile-mode switch boundary for subset vs strict policy.
#[must_use]
pub(crate) fn compile_index_predicate_program_from_slots(
    predicate_slots: &PredicateFieldSlots,
    index_slots: &[usize],
    mode: IndexPredicateCompileMode,
) -> Option<IndexPredicateProgram> {
    match mode {
        IndexPredicateCompileMode::ConservativeSubset => {
            compile_index_program_from_resolved(predicate_slots.resolved_predicate(), index_slots)
        }
        IndexPredicateCompileMode::StrictAllOrNone => compile_index_program_from_resolved_full(
            predicate_slots.resolved_predicate(),
            index_slots,
        ),
    }
}

// Compile one resolved predicate tree into one index-only program.
fn compile_index_program_from_resolved(
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    // Compile a safe AND-subset: unsupported AND children are dropped so
    // index-only filtering remains conservative (no false negatives).
    if let ResolvedPredicate::And(children) = predicate {
        return compile_index_program_and_subset(children, index_slots);
    }

    compile_index_program_from_resolved_full(predicate, index_slots)
}

// Compile an AND node by retaining only safely compilable children.
fn compile_index_program_and_subset(
    children: &[ResolvedPredicate],
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    let mut compiled = Vec::new();
    for child in children {
        let child_program = match child {
            // Nested AND nodes can also be safely reduced to a conjunction subset.
            ResolvedPredicate::And(nested) => compile_index_program_and_subset(nested, index_slots),
            _ => compile_index_program_from_resolved_full(child, index_slots),
        };

        let Some(child_program) = child_program else {
            continue;
        };
        match child_program {
            IndexPredicateProgram::True => {}
            IndexPredicateProgram::False => return Some(IndexPredicateProgram::False),
            other => compiled.push(other),
        }
    }

    match compiled.len() {
        0 => None,
        1 => compiled.pop(),
        _ => Some(IndexPredicateProgram::And(compiled)),
    }
}

// Compile one resolved predicate tree only when every node is supported.
fn compile_index_program_from_resolved_full(
    predicate: &ResolvedPredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    match predicate {
        ResolvedPredicate::True => Some(IndexPredicateProgram::True),
        ResolvedPredicate::False => Some(IndexPredicateProgram::False),
        ResolvedPredicate::And(children) => Some(IndexPredicateProgram::And(
            children
                .iter()
                .map(|child| compile_index_program_from_resolved_full(child, index_slots))
                .collect::<Option<Vec<_>>>()?,
        )),
        ResolvedPredicate::Or(children) => Some(IndexPredicateProgram::Or(
            children
                .iter()
                .map(|child| compile_index_program_from_resolved_full(child, index_slots))
                .collect::<Option<Vec<_>>>()?,
        )),
        ResolvedPredicate::Not(inner) => Some(IndexPredicateProgram::Not(Box::new(
            compile_index_program_from_resolved_full(inner, index_slots)?,
        ))),
        ResolvedPredicate::Compare(cmp) => compile_compare_index_node(cmp, index_slots),
        ResolvedPredicate::IsNull { .. }
        | ResolvedPredicate::IsMissing { .. }
        | ResolvedPredicate::IsEmpty { .. }
        | ResolvedPredicate::IsNotEmpty { .. }
        | ResolvedPredicate::TextContains { .. }
        | ResolvedPredicate::TextContainsCi { .. } => None,
    }
}

// Compile one resolved compare node into index-only compare bytes.
fn compile_compare_index_node(
    cmp: &ResolvedComparePredicate,
    index_slots: &[usize],
) -> Option<IndexPredicateProgram> {
    if cmp.coercion.id != CoercionId::Strict {
        return None;
    }
    let field_slot = cmp.field_slot?;
    let component_index = index_slots.iter().position(|slot| *slot == field_slot)?;

    match cmp.op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => {
            let literal = literal_index_component_bytes(&cmp.value)?;
            let op = match cmp.op {
                CompareOp::Eq => IndexCompareOp::Eq,
                CompareOp::Ne => IndexCompareOp::Ne,
                CompareOp::Lt => IndexCompareOp::Lt,
                CompareOp::Lte => IndexCompareOp::Lte,
                CompareOp::Gt => IndexCompareOp::Gt,
                CompareOp::Gte => IndexCompareOp::Gte,
                CompareOp::In
                | CompareOp::NotIn
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => unreachable!("op branch must match index compare subset"),
            };

            Some(IndexPredicateProgram::Compare {
                component_index,
                op,
                literal: IndexLiteral::One(literal),
            })
        }
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(items) = &cmp.value else {
                return None;
            };
            if items.is_empty() {
                return None;
            }
            let literals = items
                .iter()
                .map(literal_index_component_bytes)
                .collect::<Option<Vec<_>>>()?;
            let op = match cmp.op {
                CompareOp::In => IndexCompareOp::In,
                CompareOp::NotIn => IndexCompareOp::NotIn,
                CompareOp::Eq
                | CompareOp::Ne
                | CompareOp::Lt
                | CompareOp::Lte
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => unreachable!("op branch must match index compare subset"),
            };

            Some(IndexPredicateProgram::Compare {
                component_index,
                op,
                literal: IndexLiteral::Many(literals),
            })
        }
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            contracts::{CoercionId, CoercionSpec, CompareOp},
            executor::predicate_runtime::{
                PredicateFieldSlots, ResolvedComparePredicate, ResolvedPredicate,
            },
            index::{
                IndexCompareOp, IndexLiteral, IndexPredicateProgram,
                predicate::literal_index_component_bytes,
            },
        },
        value::Value,
    };

    use super::{IndexPredicateCompileMode, compile_index_predicate_program_from_slots};

    #[test]
    fn compile_index_program_maps_field_slot_to_component_index() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(7),
            op: CompareOp::Eq,
            value: Value::Uint(11),
            coercion: CoercionSpec::new(CoercionId::Strict),
        });

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[3, 7, 9],
            IndexPredicateCompileMode::ConservativeSubset,
        )
        .expect("strict EQ over indexed slot should compile");
        let expected =
            literal_index_component_bytes(&Value::Uint(11)).expect("uint literal should convert");

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

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[1],
            IndexPredicateCompileMode::ConservativeSubset,
        );
        assert!(program.is_none());
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
            let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
            let program = compile_index_predicate_program_from_slots(
                &predicate_slots,
                &[1],
                IndexPredicateCompileMode::ConservativeSubset,
            );

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
            let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
            let program = compile_index_predicate_program_from_slots(
                &predicate_slots,
                &[1],
                IndexPredicateCompileMode::ConservativeSubset,
            );

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
            let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
            let program = compile_index_predicate_program_from_slots(
                &predicate_slots,
                &[1],
                IndexPredicateCompileMode::ConservativeSubset,
            );

            assert!(
                program.is_none(),
                "non-strict coercion for op {op:?} must remain unsupported in index subset",
            );
        }
    }

    #[test]
    fn compile_index_program_rejects_in_with_non_list_literal() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(1),
            op: CompareOp::In,
            value: Value::Uint(11),
            coercion: CoercionSpec::new(CoercionId::Strict),
        });

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[1],
            IndexPredicateCompileMode::ConservativeSubset,
        );
        assert!(program.is_none());
    }

    #[test]
    fn compile_index_program_rejects_in_with_empty_list_literal() {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(1),
            op: CompareOp::In,
            value: Value::List(Vec::new()),
            coercion: CoercionSpec::new(CoercionId::Strict),
        });

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[1],
            IndexPredicateCompileMode::ConservativeSubset,
        );
        assert!(program.is_none());
    }

    #[test]
    fn compile_index_program_and_subset_compiles_supported_children_only() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op: CompareOp::Eq,
                value: Value::Uint(11),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            ResolvedPredicate::TextContains {
                field_slot: Some(1),
                value: Value::Text("x".to_string()),
            },
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(2),
                op: CompareOp::Gt,
                value: Value::Uint(9),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
        ]);

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[1, 2],
            IndexPredicateCompileMode::ConservativeSubset,
        )
        .expect("subset mode should keep supported children");

        let expected_left =
            literal_index_component_bytes(&Value::Uint(11)).expect("left should convert");
        let expected_right =
            literal_index_component_bytes(&Value::Uint(9)).expect("right should convert");

        assert_eq!(
            program,
            IndexPredicateProgram::And(vec![
                IndexPredicateProgram::Compare {
                    component_index: 0,
                    op: IndexCompareOp::Eq,
                    literal: IndexLiteral::One(expected_left),
                },
                IndexPredicateProgram::Compare {
                    component_index: 1,
                    op: IndexCompareOp::Gt,
                    literal: IndexLiteral::One(expected_right),
                },
            ]),
        );
    }

    #[test]
    fn compile_index_program_and_subset_drops_fully_unsupported_and() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::TextContains {
                field_slot: Some(1),
                value: Value::Text("x".to_string()),
            },
            ResolvedPredicate::IsNull {
                field_slot: Some(2),
            },
        ]);

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[1, 2],
            IndexPredicateCompileMode::ConservativeSubset,
        );
        assert!(program.is_none());
    }

    #[test]
    fn compile_index_program_strict_rejects_partial_and_support() {
        let predicate = ResolvedPredicate::And(vec![
            ResolvedPredicate::Compare(ResolvedComparePredicate {
                field_slot: Some(1),
                op: CompareOp::Eq,
                value: Value::Uint(11),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            ResolvedPredicate::TextContains {
                field_slot: Some(1),
                value: Value::Text("x".to_string()),
            },
        ]);

        let predicate_slots = PredicateFieldSlots::from_resolved_for_test(predicate);
        let program = compile_index_predicate_program_from_slots(
            &predicate_slots,
            &[1],
            IndexPredicateCompileMode::StrictAllOrNone,
        );
        assert!(program.is_none());
    }
}
