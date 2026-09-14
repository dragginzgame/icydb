//! Conservative pair eligibility, retained operands and conjunction reductions.

use super::simplify_and_compare_constraints;
use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, normalize},
    types::Decimal,
    value::Value,
};

fn compare(field: &str, op: CompareOp, value: i64) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        Value::Int64(value),
        CoercionId::Strict,
    ))
}

#[test]
fn removals_visit_shifted_right_operands_and_shifted_left_rows() {
    let guard = Predicate::IsNotNull {
        field: "guard".into(),
    };
    let tightest = compare("rank", CompareOp::Gt, 7);
    assert_eq!(
        simplify_and_compare_constraints(vec![
            guard.clone(),
            compare("rank", CompareOp::Gt, 3),
            compare("rank", CompareOp::Gt, 1),
            compare("rank", CompareOp::Gt, 2),
            tightest.clone(),
        ]),
        Some(vec![guard.clone(), tightest])
    );

    // Removing rank > 1 shifts an unrelated equality into the left position.
    // It must still meet its later conflicting equality, not be skipped.
    assert_eq!(
        simplify_and_compare_constraints(vec![
            guard,
            compare("rank", CompareOp::Gt, 1),
            compare("other", CompareOp::Eq, 1),
            compare("rank", CompareOp::Gt, 2),
            compare("other", CompareOp::Eq, 2),
        ]),
        None
    );
}

#[test]
fn removals_preserve_unrelated_prefixes_and_first_equivalent_representation() {
    let prefix = (0..64)
        .map(|i| compare(&format!("field_{i}"), CompareOp::Eq, i))
        .collect::<Vec<_>>();
    for ascending in [false, true] {
        let mut input = prefix.clone();
        input.extend(
            (0..64).map(|i| compare("rank", CompareOp::Gte, if ascending { i } else { 63 - i })),
        );
        let mut expected = prefix.clone();
        expected.push(compare("rank", CompareOp::Gte, 63));
        assert_eq!(simplify_and_compare_constraints(input), Some(expected));
    }

    let equality = |value| {
        Predicate::Compare(ComparePredicate::with_coercion(
            "amount",
            CompareOp::Eq,
            value,
            CoercionId::NumericWiden,
        ))
    };
    for values in [
        [Value::Nat64(7), Value::Decimal(Decimal::new(70, 1))],
        [Value::Decimal(Decimal::new(70, 1)), Value::Nat64(7)],
    ] {
        let first = equality(values[0].clone());
        assert_eq!(
            simplify_and_compare_constraints(vec![first.clone(), equality(values[1].clone())]),
            Some(vec![first])
        );
    }
}

#[test]
fn replacements_keep_lower_operand_identity_and_reconsider_survivors() {
    let guard = compare("other", CompareOp::Eq, 9);
    let lower = Predicate::Compare(ComparePredicate::with_coercion(
        "amount",
        CompareOp::Gte,
        Value::Decimal(Decimal::new(70, 1)),
        CoercionId::NumericWiden,
    ));
    let upper = Predicate::Compare(ComparePredicate::with_coercion(
        "amount",
        CompareOp::Lte,
        Value::Nat64(7),
        CoercionId::NumericWiden,
    ));
    let expected = Predicate::Compare(ComparePredicate::with_coercion(
        "amount",
        CompareOp::Eq,
        Value::Decimal(Decimal::new(70, 1)),
        CoercionId::NumericWiden,
    ));
    for bounds in [[lower.clone(), upper.clone()], [upper, lower]] {
        let mut input = vec![guard.clone()];
        input.extend(bounds);
        assert_eq!(
            simplify_and_compare_constraints(input.clone()),
            Some(vec![guard.clone(), expected.clone()])
        );
        input.push(Predicate::Compare(ComparePredicate::with_coercion(
            "amount",
            CompareOp::Gt,
            Value::Nat64(8),
            CoercionId::NumericWiden,
        )));
        assert_eq!(simplify_and_compare_constraints(input), None);
    }
}

#[test]
fn equal_bounds_reuse_lower_text_and_coercion_allocations_in_both_orders() {
    for lower_first in [false, true] {
        let mut lower = ComparePredicate::with_coercion(
            "label".repeat(32),
            CompareOp::Gte,
            Value::Text("payload".repeat(128)),
            CoercionId::Strict,
        );
        lower.coercion.params = vec![("key".repeat(16), "value".repeat(32))];
        let mut upper = lower.clone();
        upper.op = CompareOp::Lte;
        let field = lower.field.as_ptr();
        let Value::Text(text) = &lower.value else {
            unreachable!()
        };
        let payload = text.as_ptr();
        let params = lower.coercion.params.as_ptr();
        let param_name = lower.coercion.params[0].0.as_ptr();
        let param_value = lower.coercion.params[0].1.as_ptr();
        let bounds = if lower_first {
            vec![Predicate::Compare(lower), Predicate::Compare(upper)]
        } else {
            vec![Predicate::Compare(upper), Predicate::Compare(lower)]
        };

        let result = simplify_and_compare_constraints(bounds).expect("compatible bounds");
        let [Predicate::Compare(compare)] = result.as_slice() else {
            panic!("equal inclusive bounds collapse to one comparison")
        };
        assert_eq!(compare.op, CompareOp::Eq);
        assert_eq!(compare.field.as_ptr(), field);
        let Value::Text(text) = &compare.value else {
            panic!("retained text operand")
        };
        assert_eq!(text.as_ptr(), payload);
        assert_eq!(compare.coercion.params.as_ptr(), params);
        assert_eq!(compare.coercion.params[0].0.as_ptr(), param_name);
        assert_eq!(compare.coercion.params[0].1.as_ptr(), param_value);
    }
}

#[test]
fn unsupported_pairs_and_distinct_coercion_contracts_survive_reduction() {
    let mut first = compare("rank", CompareOp::Eq, 1);
    let mut second = compare("rank", CompareOp::Eq, 2);
    for (predicate, parameter) in [(&mut first, "first"), (&mut second, "second")] {
        let Predicate::Compare(compare) = predicate else {
            unreachable!()
        };
        compare.coercion.params = vec![("domain".into(), parameter.into())];
    }
    let preserved = vec![
        first,
        second,
        Predicate::in_("rank".into(), vec![Value::Int64(1), Value::Int64(2)]),
        Predicate::eq("rank".into(), Value::Text("incomparable".into())),
    ];
    let mut input = preserved.clone();
    input.extend([
        compare("other", CompareOp::Gte, 1),
        compare("other", CompareOp::Gte, 3),
    ]);
    let mut expected = preserved;
    expected.push(compare("other", CompareOp::Gte, 3));
    assert_eq!(simplify_and_compare_constraints(input), Some(expected));
}

#[test]
fn equality_with_non_bound_operators_preserves_both_operands() {
    for coercion in [
        CoercionId::Strict,
        CoercionId::NumericWiden,
        CoercionId::TextCasefold,
        CoercionId::CollectionElement,
    ] {
        for op in [
            CompareOp::Ne,
            CompareOp::In,
            CompareOp::NotIn,
            CompareOp::Contains,
            CompareOp::StartsWith,
            CompareOp::EndsWith,
        ] {
            let value = if coercion == CoercionId::NumericWiden {
                Value::Nat64(7)
            } else {
                Value::Text("MiXeD".repeat(128))
            };
            let operand = if op.is_membership_family() {
                Value::List(vec![value.clone()])
            } else {
                value.clone()
            };
            let equality = Predicate::Compare(ComparePredicate::with_coercion(
                "field",
                CompareOp::Eq,
                value,
                coercion,
            ));
            let constraint = Predicate::Compare(ComparePredicate::with_coercion(
                "field", op, operand, coercion,
            ));
            for input in [
                vec![equality.clone(), constraint.clone()],
                vec![constraint, equality],
            ] {
                assert_eq!(simplify_and_compare_constraints(input.clone()), Some(input));
            }
        }
    }
}

#[test]
fn equality_bound_reduction_preserves_coercion_and_inclusive_endpoints() {
    for (coercion, value, bound) in [
        (CoercionId::Strict, Value::Int64(7), Value::Int64(7)),
        (
            CoercionId::NumericWiden,
            Value::Nat64(7),
            Value::Decimal(Decimal::new(70, 1)),
        ),
        (
            CoercionId::TextCasefold,
            Value::Text("MiXeD".into()),
            Value::Text("mixed".into()),
        ),
    ] {
        let equality = Predicate::Compare(ComparePredicate::with_coercion(
            "field",
            CompareOp::Eq,
            value,
            coercion,
        ));
        for op in [CompareOp::Gt, CompareOp::Gte, CompareOp::Lt, CompareOp::Lte] {
            let constraint = Predicate::Compare(ComparePredicate::with_coercion(
                "field",
                op,
                bound.clone(),
                coercion,
            ));
            let expected =
                matches!(op, CompareOp::Gte | CompareOp::Lte).then(|| vec![equality.clone()]);
            for input in [
                vec![equality.clone(), constraint.clone()],
                vec![constraint, equality.clone()],
            ] {
                assert_eq!(simplify_and_compare_constraints(input), expected);
            }
        }
    }
}

// Independent finite integer interpretation, not a second simplification pass.
fn accepts(predicate: &Predicate, value: i64) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,
        Predicate::And(children) => children.iter().all(|child| accepts(child, value)),
        Predicate::Compare(compare) => {
            let Value::Int64(bound) = compare.value() else {
                panic!("integer fixture")
            };
            match compare.op() {
                CompareOp::Eq => value == *bound,
                CompareOp::Ne => value != *bound,
                CompareOp::Lt => value < *bound,
                CompareOp::Lte => value <= *bound,
                CompareOp::Gt => value > *bound,
                CompareOp::Gte => value >= *bound,
                _ => panic!("integer comparison fixture"),
            }
        }
        _ => panic!("conjunction fixture"),
    }
}

#[test]
fn integer_constraint_triples_preserve_semantics_and_canonical_idempotence() {
    let clauses = [
        CompareOp::Eq,
        CompareOp::Ne,
        CompareOp::Lt,
        CompareOp::Lte,
        CompareOp::Gt,
        CompareOp::Gte,
    ]
    .into_iter()
    .flat_map(|op| (-1..=1).map(move |value| compare("rank", op, value)))
    .collect::<Vec<_>>();
    // Every ordered triple includes duplicates and both lower/upper orientations.
    // Samples span all truth intervals around the authored integer boundaries.
    for first in &clauses {
        for second in &clauses {
            for third in &clauses {
                let input = Predicate::And(vec![first.clone(), second.clone(), third.clone()]);
                let actual = normalize(input.clone());
                for value in -2..=2 {
                    assert_eq!(
                        accepts(&actual, value),
                        accepts(&input, value),
                        "{input:?} at {value}"
                    );
                }
                assert_eq!(normalize(actual.clone()), actual);
            }
        }
    }
}
