use crate::{
    db::predicate::{
        CoercionId, CompareOp, ComparePredicate, Predicate,
        encoding::write_predicate_sort_key,
        fingerprint::predicate_fingerprint,
        membership::{MembershipCompareLeaf, collapse_membership_compare_leaves},
        normalize,
        normalize::collapse_same_field_or_equalities,
    },
    types::{Decimal, Float64},
    value::{Value, ValueEnum},
};

fn equality(value: Value, coercion: CoercionId) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::Eq,
        value,
        coercion,
    ))
}

fn assert_normalized_identity(input: Predicate, expected: &Predicate) {
    let normalized = normalize(input.clone());
    assert_eq!(&normalized, expected);
    assert_eq!(normalize(normalized.clone()), normalized);
    let mut actual_bytes = Vec::new();
    let mut expected_bytes = Vec::new();
    write_predicate_sort_key(&mut actual_bytes, &normalized);
    write_predicate_sort_key(&mut expected_bytes, expected);
    assert_eq!(actual_bytes, expected_bytes);
    assert_eq!(
        predicate_fingerprint(&input),
        predicate_fingerprint(expected)
    );
}

#[test]
fn duplicate_only_or_preserves_equality_and_complete_coercion_metadata() {
    for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
        for value in [
            Value::Null,
            Value::Text("a".repeat(512)),
            Value::Float64(Float64::try_new(-0.0).unwrap()),
            Value::Decimal(Decimal::from_i128_with_scale(100, 2)),
            Value::Enum(ValueEnum::test_payload(1, 1, Value::Null)),
        ] {
            for params in [vec![], vec![("fixture".into(), "value".into())]] {
                let mut leaf =
                    ComparePredicate::with_coercion("tag", CompareOp::Eq, value.clone(), coercion);
                leaf.coercion.params = params;
                let expected = Predicate::Compare(leaf);
                assert_normalized_identity(Predicate::Or(vec![expected.clone(); 3]), &expected);
            }
        }
    }
}

#[test]
fn eligible_or_membership_preserves_canonical_identity_across_input_orders() {
    let cases = [
        (
            vec![
                Value::Int64(3),
                Value::Int64(-2),
                Value::Int64(3),
                Value::Int64(1),
            ],
            vec![Value::Int64(-2), Value::Int64(1), Value::Int64(3)],
        ),
        (
            vec![
                Value::Text("a".into()),
                Value::Text("A".into()),
                Value::Text("a".into()),
                Value::Text("beta".into()),
            ],
            vec![
                Value::Text("A".into()),
                Value::Text("a".into()),
                Value::Text("beta".into()),
            ],
        ),
        (
            vec![
                Value::Decimal(Decimal::from_i128_with_scale(10, 1)),
                Value::Decimal(Decimal::from_i128_with_scale(-2, 0)),
                Value::Decimal(Decimal::from_i128_with_scale(100, 2)),
            ],
            vec![
                Value::Decimal(Decimal::from_i128_with_scale(-2, 0)),
                Value::Decimal(Decimal::from_i128_with_scale(1, 0)),
            ],
        ),
        (
            vec![
                Value::Float64(Float64::try_new(-0.0).unwrap()),
                Value::Float64(Float64::try_new(-1.0).unwrap()),
                Value::Float64(Float64::try_new(0.0).unwrap()),
            ],
            vec![
                Value::Float64(Float64::try_new(-1.0).unwrap()),
                Value::Float64(Float64::try_new(0.0).unwrap()),
            ],
        ),
    ];
    for (values, canonical) in cases {
        for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
            let expected = Predicate::Compare(ComparePredicate::with_coercion(
                "tag",
                CompareOp::In,
                Value::List(canonical.clone()),
                coercion,
            ));
            for offset in 0..values.len() {
                let mut permuted = values.clone();
                permuted.rotate_left(offset);
                for reversed in [false, true] {
                    let mut children = permuted
                        .iter()
                        .cloned()
                        .map(|value| equality(value, coercion))
                        .collect::<Vec<_>>();
                    if reversed {
                        children.reverse();
                    }
                    assert_normalized_identity(Predicate::Or(children), &expected);
                }
            }
        }
    }
}

#[test]
fn equal_values_with_distinct_coercion_metadata_remain_membership() {
    let first = equality(Value::Nat64(7), CoercionId::Strict);
    let mut second = first.clone();
    let Predicate::Compare(compare) = &mut second else {
        panic!("compare fixture")
    };
    compare
        .coercion
        .params
        .push(("fixture".into(), "other".into()));
    let expected = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::In,
        Value::List(vec![Value::Nat64(7)]),
        CoercionId::Strict,
    ));
    for children in [
        vec![first.clone(), second.clone(), first.clone()],
        vec![second, first],
    ] {
        assert_normalized_identity(Predicate::Or(children), &expected);
    }
}

#[test]
fn nested_and_large_or_membership_preserve_compact_identity() {
    let expected = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::In,
        Value::List((0..512).map(Value::Nat64).collect()),
        CoercionId::Strict,
    ));
    let children = (0..512)
        .rev()
        .map(|value| {
            Predicate::Or(vec![
                Predicate::False,
                equality(Value::Nat64(value), CoercionId::Strict),
                equality(Value::Nat64(value), CoercionId::Strict),
            ])
        })
        .collect();
    assert_normalized_identity(Predicate::Or(children), &expected);
}

// Compare allocation identities without dereferencing retained raw pointers.
fn payload_address(value: &Value) -> *const u8 {
    match value {
        Value::Text(value) => value.as_ptr(),
        Value::Blob(value) => value.as_ptr(),
        Value::Enum(value) => payload_address(value.payload().expect("payload enum")),
        _ => panic!("fixture requires a heap-backed operand"),
    }
}

#[test]
fn collapse_moves_field_and_heap_backed_operands() {
    for values in [
        vec![
            Value::Text("beta".repeat(512)),
            Value::Text("alpha".repeat(512)),
        ],
        vec![Value::Blob(vec![2; 4096]), Value::Blob(vec![1; 4096])],
        vec![
            Value::Enum(ValueEnum::test_payload(1, 1, Value::Text("b".repeat(1024)))),
            Value::Enum(ValueEnum::test_payload(1, 1, Value::Text("a".repeat(1024)))),
        ],
    ] {
        let mut addresses = values.iter().map(payload_address).collect::<Vec<_>>();
        addresses.sort_unstable();
        let mut children = values
            .into_iter()
            .map(|value| equality(value, CoercionId::Strict))
            .collect::<Vec<_>>();
        let Predicate::Compare(first) = &children[0] else {
            panic!("compare")
        };
        let field_address = first.field.as_ptr();

        let Predicate::Compare(compare) =
            collapse_same_field_or_equalities(&mut children).expect("eligible membership")
        else {
            panic!("compare")
        };
        assert!(children.is_empty());
        assert_eq!(compare.field.as_ptr(), field_address);
        assert_eq!(compare.op, CompareOp::In);
        let Value::List(values) = compare.value else {
            panic!("membership list")
        };
        let mut retained = values.iter().map(payload_address).collect::<Vec<_>>();
        retained.sort_unstable();
        assert_eq!(retained, addresses);
    }
}

#[test]
fn declined_collapse_preserves_all_children() {
    let strict = CoercionId::Strict;
    let mut different_field =
        ComparePredicate::with_coercion("other", CompareOp::Eq, Value::Int64(3), strict);
    let other_field = Predicate::Compare(different_field.clone());
    different_field.field = "tag".into();
    different_field.op = CompareOp::Ne;
    for last in [
        other_field,
        Predicate::Compare(different_field),
        Predicate::True,
        equality(Value::Int64(3), CoercionId::TextCasefold),
        equality(Value::Int64(3), CoercionId::NumericWiden),
        equality(Value::Int64(3), CoercionId::CollectionElement),
        equality(Value::List(vec![Value::Int64(3)]), strict),
        equality(Value::Map(vec![]), strict),
    ] {
        let mut children = vec![
            equality(Value::Int64(1), strict),
            equality(Value::Int64(2), strict),
            last,
        ];
        let original = children.clone();
        assert!(collapse_same_field_or_equalities(&mut children).is_none());
        assert_eq!(children, original);
    }
    for mut children in [vec![], vec![equality(Value::Int64(1), strict)]] {
        let original = children.clone();
        assert!(collapse_same_field_or_equalities(&mut children).is_none());
        assert_eq!(children, original);
    }
}

#[test]
fn owned_and_expression_membership_share_canonical_sets() {
    for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
        for values in [
            vec![
                Value::Text("beta".into()),
                Value::Text("alpha".into()),
                Value::Text("beta".into()),
            ],
            vec![Value::Text("ADA".into()), Value::Text("ada".into())],
        ] {
            let mut children = values
                .iter()
                .cloned()
                .map(|value| equality(value, coercion))
                .collect();
            let owned =
                collapse_same_field_or_equalities(&mut children).expect("eligible membership");
            let leaves = values
                .into_iter()
                .map(|value| MembershipCompareLeaf::new("tag", value, coercion))
                .collect();
            let expression = collapse_membership_compare_leaves(leaves, CompareOp::In)
                .expect("eligible expression membership");
            assert_eq!(owned, Predicate::Compare(expression));
        }
    }
}
