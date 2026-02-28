use crate::{
    db::{
        contracts::{CoercionId, CoercionSpec, CompareOp},
        index::{
            IndexCompareOp, IndexLiteral, IndexPredicateProgram,
            predicate::literal_index_component_bytes,
        },
        query::predicate::runtime::{
            PredicateProgram, ResolvedComparePredicate, ResolvedPredicate,
        },
    },
    value::Value,
};

use super::{IndexCompilePolicy, compile_index_program};

#[test]
fn compile_index_program_maps_field_slot_to_component_index() {
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(7),
        op: CompareOp::Eq,
        value: Value::Uint(11),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[3, 7, 9],
        IndexCompilePolicy::ConservativeSubset,
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

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[1],
        IndexCompilePolicy::ConservativeSubset,
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
        let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
        let program = compile_index_program(
            predicate_slots.resolved(),
            &[1],
            IndexCompilePolicy::ConservativeSubset,
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
        let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
        let program = compile_index_program(
            predicate_slots.resolved(),
            &[1],
            IndexCompilePolicy::ConservativeSubset,
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
        let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
        let program = compile_index_program(
            predicate_slots.resolved(),
            &[1],
            IndexCompilePolicy::ConservativeSubset,
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

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[1],
        IndexCompilePolicy::ConservativeSubset,
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

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[1],
        IndexCompilePolicy::ConservativeSubset,
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

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[1, 2],
        IndexCompilePolicy::ConservativeSubset,
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

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[1, 2],
        IndexCompilePolicy::ConservativeSubset,
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

    let predicate_slots = PredicateProgram::from_resolved_for_test(predicate);
    let program = compile_index_program(
        predicate_slots.resolved(),
        &[1],
        IndexCompilePolicy::StrictAllOrNone,
    );
    assert!(program.is_none());
}
