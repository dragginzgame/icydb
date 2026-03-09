use crate::{
    db::{
        index::{
            IndexCompareOp, IndexLiteral, IndexPredicateProgram,
            predicate::literal_index_component_bytes,
        },
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ResolvedComparePredicate, ResolvedPredicate,
            compare_eq, compare_order,
        },
    },
    types::Decimal,
    value::Value,
};
use std::cmp::Ordering;

use super::{IndexCompilePolicy, compile_index_program, eval_index_compare};

// Match index compare operations to strict predicate semantics for expected results.
fn expected_strict_compare(
    op: IndexCompareOp,
    left: &Value,
    right: &Value,
    strict: &CoercionSpec,
) -> bool {
    match op {
        IndexCompareOp::Eq => compare_eq(left, right, strict).unwrap_or(false),
        IndexCompareOp::Ne => compare_eq(left, right, strict).is_some_and(|equal| !equal),
        IndexCompareOp::Lt => compare_order(left, right, strict).is_some_and(Ordering::is_lt),
        IndexCompareOp::Lte => compare_order(left, right, strict).is_some_and(Ordering::is_le),
        IndexCompareOp::Gt => compare_order(left, right, strict).is_some_and(Ordering::is_gt),
        IndexCompareOp::Gte => compare_order(left, right, strict).is_some_and(Ordering::is_ge),
        IndexCompareOp::In | IndexCompareOp::NotIn => {
            unreachable!("expected_strict_compare only handles one-literal compare operators")
        }
    }
}

#[test]
fn compile_index_program_maps_field_slot_to_component_index() {
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(7),
        op: CompareOp::Eq,
        value: Value::Uint(11),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(
        &predicate,
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

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
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
        (CompareOp::StartsWith, Value::Text("x".to_string())),
    ];
    for (op, value) in eligible {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(1),
            op,
            value,
            coercion: CoercionSpec::new(CoercionId::Strict),
        });
        let program =
            compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);

        assert!(
            program.is_some(),
            "strict compare op {op:?} should compile into an index predicate program",
        );
    }

    let ineligible = [
        (CompareOp::Contains, Value::Text("x".to_string())),
        (CompareOp::EndsWith, Value::Text("x".to_string())),
    ];
    for (op, value) in ineligible {
        let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: Some(1),
            op,
            value,
            coercion: CoercionSpec::new(CoercionId::Strict),
        });
        let program =
            compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);

        assert!(
            program.is_none(),
            "op {op:?} should stay on fallback execution",
        );
    }
}

#[test]
fn compile_index_program_starts_with_compiles_to_bounded_range_compare_pair() {
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(1),
        op: CompareOp::StartsWith,
        value: Value::Text("foo".to_string()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("strict starts-with should compile for index prefilter");
    let expected_lower =
        literal_index_component_bytes(&Value::Text("foo".to_string())).expect("lower bytes");
    let expected_upper =
        literal_index_component_bytes(&Value::Text("fop".to_string())).expect("upper bytes");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_starts_with_rejects_empty_prefix() {
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(1),
        op: CompareOp::StartsWith,
        value: Value::Text(String::new()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
    assert!(program.is_none());
}

#[test]
fn compile_index_program_starts_with_high_unicode_skips_surrogate_gap_upper_bound() {
    let prefix = format!("foo{}", char::from_u32(0xD7FF).expect("valid scalar"));
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(1),
        op: CompareOp::StartsWith,
        value: Value::Text(prefix.clone()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("strict starts-with should compile for high-unicode prefix");
    let expected_lower =
        literal_index_component_bytes(&Value::Text(prefix)).expect("lower bytes should convert");
    let expected_upper = literal_index_component_bytes(&Value::Text(format!(
        "foo{}",
        char::from_u32(0xE000).expect("valid scalar")
    )))
    .expect("upper bytes should skip surrogate gap");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_starts_with_max_unicode_compiles_to_lower_bound_only() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(1),
        op: CompareOp::StartsWith,
        value: Value::Text(prefix.clone()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset)
        .expect("max-unicode starts-with should compile to one lower-bound compare");
    let expected_lower =
        literal_index_component_bytes(&Value::Text(prefix)).expect("lower bytes should convert");

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::Gte,
            literal: IndexLiteral::One(expected_lower),
        },
    );
}

#[test]
fn compile_index_program_strict_mode_accepts_starts_with_bounded_prefix() {
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(1),
        op: CompareOp::StartsWith,
        value: Value::Text("foo".to_string()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::StrictAllOrNone)
        .expect("strict-all-or-none should compile starts-with when fully index-expressible");
    let expected_lower =
        literal_index_component_bytes(&Value::Text("foo".to_string())).expect("lower bytes");
    let expected_upper =
        literal_index_component_bytes(&Value::Text("fop".to_string())).expect("upper bytes");

    assert_eq!(
        program,
        IndexPredicateProgram::And(vec![
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Gte,
                literal: IndexLiteral::One(expected_lower),
            },
            IndexPredicateProgram::Compare {
                component_index: 0,
                op: IndexCompareOp::Lt,
                literal: IndexLiteral::One(expected_upper),
            },
        ]),
    );
}

#[test]
fn compile_index_program_strict_mode_accepts_starts_with_max_unicode_prefix() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let predicate = ResolvedPredicate::Compare(ResolvedComparePredicate {
        field_slot: Some(1),
        op: CompareOp::StartsWith,
        value: Value::Text(prefix.clone()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::StrictAllOrNone)
        .expect("strict-all-or-none should compile max-unicode starts-with lower-bound form");
    let expected_lower =
        literal_index_component_bytes(&Value::Text(prefix)).expect("lower bytes should convert");

    assert_eq!(
        program,
        IndexPredicateProgram::Compare {
            component_index: 0,
            op: IndexCompareOp::Gte,
            literal: IndexLiteral::One(expected_lower),
        },
    );
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
        let program =
            compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);

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

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
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

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::ConservativeSubset);
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

    let program =
        compile_index_program(&predicate, &[1, 2], IndexCompilePolicy::ConservativeSubset)
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

    let program =
        compile_index_program(&predicate, &[1, 2], IndexCompilePolicy::ConservativeSubset);
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

    let program = compile_index_program(&predicate, &[1], IndexCompilePolicy::StrictAllOrNone);
    assert!(program.is_none());
}

#[test]
fn eval_index_compare_matches_strict_semantics_for_one_literal_ops() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let cases = vec![
        (Value::Int(-2), Value::Int(7)),
        (
            Value::Decimal(Decimal::new(10, 1)),
            Value::Decimal(Decimal::new(1, 0)),
        ),
        (
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ),
    ];
    let operators = [
        IndexCompareOp::Eq,
        IndexCompareOp::Ne,
        IndexCompareOp::Lt,
        IndexCompareOp::Lte,
        IndexCompareOp::Gt,
        IndexCompareOp::Gte,
    ];

    for (left, right) in cases {
        let component = literal_index_component_bytes(&left).expect("left value should encode");
        let literal = IndexLiteral::One(
            literal_index_component_bytes(&right).expect("right value should encode"),
        );

        for op in operators {
            let expected = expected_strict_compare(op, &left, &right, &strict);
            let actual = eval_index_compare(component.as_slice(), op, &literal);

            assert_eq!(
                actual, expected,
                "index compare drifted from strict predicate semantics for op={op:?} left={left:?} right={right:?}",
            );
        }
    }
}

#[test]
fn eval_index_compare_in_and_not_in_match_strict_membership_semantics() {
    let strict = CoercionSpec::new(CoercionId::Strict);
    let target = Value::Text("beta".to_string());
    let candidates = [
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
        Value::Text("gamma".to_string()),
    ];
    let component = literal_index_component_bytes(&target).expect("target should encode");
    let literal = IndexLiteral::Many(
        candidates
            .iter()
            .map(literal_index_component_bytes)
            .collect::<Option<Vec<_>>>()
            .expect("all candidate literals should encode"),
    );

    let expected_in = candidates
        .iter()
        .any(|candidate| compare_eq(&target, candidate, &strict).unwrap_or(false));
    let expected_not_in = candidates
        .iter()
        .all(|candidate| compare_eq(&target, candidate, &strict).is_some_and(|eq| !eq));

    assert_eq!(
        eval_index_compare(component.as_slice(), IndexCompareOp::In, &literal),
        expected_in,
    );
    assert_eq!(
        eval_index_compare(component.as_slice(), IndexCompareOp::NotIn, &literal),
        expected_not_in,
    );
}
