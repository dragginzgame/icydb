use crate::{
    db::predicate::{
        CoercionId, CompareOp, ComparePredicate, Predicate,
        encoding::{
            canonicalize_compare_literal_for_coercion, encode_compare_value_sort_key_into,
            encode_value_sort_key_into, push_bytes_u64, push_len_u64, push_value_sort_key_framed,
            write_normalized_predicate_sort_key, write_predicate_sort_key,
        },
    },
    value::Value,
};
use std::borrow::Cow;

fn sort_key(predicate: &Predicate) -> Vec<u8> {
    let mut key = Vec::new();
    write_predicate_sort_key(&mut key, predicate);
    key
}

#[test]
fn predicate_key_writers_append_framed_payloads_and_allow_buffer_reuse() {
    let predicate = Predicate::Not(Box::new(Predicate::And(vec![
        Predicate::IsNull {
            field: "name".repeat(128),
        },
        Predicate::eq(
            "payload".to_string(),
            Value::Map(vec![
                (
                    Value::Text("z".to_string()),
                    Value::List(vec![Value::Nat64(7)]),
                ),
                (Value::Text("a".to_string()), Value::Nat64(3)),
            ]),
        ),
    ])));
    let expected = sort_key(&predicate);
    for writer in [
        write_predicate_sort_key,
        write_normalized_predicate_sort_key,
    ] {
        let prefix = vec![17, 23, 42];
        let mut output = prefix.clone();
        writer(&mut output, &predicate);
        assert_eq!(&output[..prefix.len()], prefix);
        assert_eq!(&output[prefix.len()..], expected);

        output.clear();
        let shorter = Predicate::IsNull {
            field: "x".to_string(),
        };
        writer(&mut output, &shorter);
        assert_eq!(output, sort_key(&shorter));
    }
}

#[test]
fn compare_encoding_borrows_unchanged_literals_and_owns_coercions() {
    let nested = Value::List(vec![Value::Map(vec![(
        Value::Text("key".repeat(256)),
        Value::NatBig(crate::types::NatBig::from_biguint(
            num_bigint::BigUint::from(1_u8) << 4096_usize,
        )),
    )])]);
    for coercion in [
        CoercionId::Strict,
        CoercionId::CollectionElement,
        CoercionId::NumericWiden,
        CoercionId::TextCasefold,
    ] {
        let canonical = canonicalize_compare_literal_for_coercion(coercion, &nested);
        assert!(
            matches!(canonical, Cow::Borrowed(value) if std::ptr::eq(value, &raw const nested))
        );
    }
    for (coercion, value, expected) in [
        (
            CoercionId::NumericWiden,
            Value::Int64(7),
            Value::Decimal(crate::types::Decimal::new(7, 0)),
        ),
        (
            CoercionId::TextCasefold,
            Value::Text("ADA".to_string()),
            Value::Text("ada".to_string()),
        ),
    ] {
        let canonical = canonicalize_compare_literal_for_coercion(coercion, &value);
        assert!(matches!(canonical, Cow::Owned(_)));
        assert_eq!(*canonical, expected);
    }
}

#[test]
fn membership_encoding_preserves_canonical_sets_and_coercion_reordering() {
    let nested = Value::Map(vec![(Value::Text("key".repeat(64)), Value::Nat64(7))]);
    for source in [
        vec![],
        vec![nested.clone()],
        (0..128).map(Value::Nat64).collect(),
        vec![
            Value::List(vec![Value::Nat64(1)]),
            Value::List(vec![Value::Nat64(2)]),
        ],
        vec![nested.clone(), nested],
        vec![Value::Nat64(2), Value::Nat64(1), Value::Nat64(2)],
        // Raw text order reverses after casefolding; numeric subtypes can merge.
        vec![Value::Text("Z".to_string()), Value::Text("a".to_string())],
        vec![Value::Text("A".to_string()), Value::Text("a".to_string())],
        vec![Value::Int64(7), Value::Nat64(7)],
    ] {
        for coercion in [
            CoercionId::Strict,
            CoercionId::CollectionElement,
            CoercionId::TextCasefold,
            CoercionId::NumericWiden,
        ] {
            // Owned reference always coerces before canonical ordering. Raw source
            // ordering must not bypass transformations that change order/equality.
            let mut expected_values = source
                .iter()
                .map(|value| {
                    canonicalize_compare_literal_for_coercion(coercion, value).into_owned()
                })
                .collect::<Vec<_>>();
            expected_values.sort_unstable_by(Value::canonical_cmp);
            expected_values.dedup();
            let input = Value::List(source.clone());
            let mut expected = vec![input.canonical_tag().to_u8()];
            push_len_u64(&mut expected, expected_values.len());
            for value in &expected_values {
                push_value_sort_key_framed(&mut expected, value);
            }
            for op in [CompareOp::In, CompareOp::NotIn] {
                let mut actual = Vec::new();
                encode_compare_value_sort_key_into(&mut actual, op, coercion, &input, false);
                assert_eq!(actual, expected);
                assert_eq!(input, Value::List(source.clone()));
            }
        }
    }
}

#[test]
fn borrowed_compare_encoding_preserves_scalar_and_membership_bytes() {
    // Explicit expected values pin coercion separately from the borrowing helper.
    // Large/nested inputs exercise unchanged payloads in scalar and list encoding.
    let nested = Value::Map(vec![(Value::Text("z".repeat(256)), Value::Nat64(7))]);
    let source = vec![
        Value::Text("ADA".to_string()),
        nested.clone(),
        Value::Int64(7),
        Value::Text("ada".to_string()),
        nested,
        Value::Decimal(crate::types::Decimal::new(7, 0)),
    ];
    let snapshot = source.clone();
    for coercion in [
        CoercionId::Strict,
        CoercionId::CollectionElement,
        CoercionId::NumericWiden,
        CoercionId::TextCasefold,
    ] {
        let mut expected_values = source.clone();
        match coercion {
            CoercionId::NumericWiden => {
                expected_values[2] = Value::Decimal(crate::types::Decimal::new(7, 0));
            }
            CoercionId::TextCasefold => {
                expected_values[0] = Value::Text("ada".to_string());
            }
            CoercionId::Strict | CoercionId::CollectionElement => {}
        }
        for (value, expected_value) in source.iter().zip(&expected_values) {
            let mut expected = Vec::new();
            encode_value_sort_key_into(&mut expected, expected_value);
            let mut actual = Vec::new();
            encode_compare_value_sort_key_into(&mut actual, CompareOp::Eq, coercion, value, false);
            assert_eq!(actual, expected);
        }
        for normalized in [false, true] {
            let mut values = expected_values.clone();
            // Independent owned reference: canonical value order and equality
            // remain the set contract, regardless of temporary ownership.
            values.sort_unstable_by(Value::canonical_cmp);
            values.dedup();
            let input = Value::List(if normalized {
                values.clone()
            } else {
                source.clone()
            });
            let mut expected = vec![input.canonical_tag().to_u8()];
            push_len_u64(&mut expected, values.len());
            for value in &values {
                push_value_sort_key_framed(&mut expected, value);
            }
            for op in [CompareOp::In, CompareOp::NotIn] {
                let mut actual = Vec::new();
                encode_compare_value_sort_key_into(&mut actual, op, coercion, &input, normalized);
                assert_eq!(actual, expected);
            }
        }
    }
    assert_eq!(source, snapshot);
}

#[test]
fn bigint_sort_key_stream_preserves_length_and_candid_payload() {
    let magnitude = num_bigint::BigUint::from(1_u8) << 4096_usize;
    let mut unsigned = Vec::new();
    candid::Nat::from(magnitude.clone())
        .encode(&mut unsigned)
        .unwrap();
    let negative = -num_bigint::BigInt::from(magnitude.clone());
    let mut signed = Vec::new();
    candid::Int::from(negative.clone())
        .encode(&mut signed)
        .unwrap();
    for (value, bytes) in [
        (
            Value::NatBig(crate::types::NatBig::from_biguint(magnitude)),
            unsigned,
        ),
        (
            Value::IntBig(crate::types::IntBig::from_bigint(negative)),
            signed,
        ),
    ] {
        let mut expected = vec![value.canonical_tag().to_u8()];
        push_bytes_u64(&mut expected, &bytes);
        let mut actual = Vec::new();
        encode_value_sort_key_into(&mut actual, &value);
        assert_eq!(actual, expected);
    }
}

#[test]
fn predicate_sort_key_normalizes_map_entry_order() {
    let map_a = Value::Map(vec![
        (Value::Text("z".to_string()), Value::Int64(9)),
        (Value::Text("a".to_string()), Value::Int64(1)),
    ]);
    let map_b = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Int64(1)),
        (Value::Text("z".to_string()), Value::Int64(9)),
    ]);
    let predicate_a = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_a));
    let predicate_b = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_b));

    assert_eq!(sort_key(&predicate_a), sort_key(&predicate_b));
}

#[test]
fn predicate_sort_key_normalizes_duplicate_map_keys_by_value_order() {
    let map_a = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Int64(2)),
        (Value::Text("a".to_string()), Value::Int64(1)),
    ]);
    let map_b = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Int64(1)),
        (Value::Text("a".to_string()), Value::Int64(2)),
    ]);
    let predicate_a = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_a));
    let predicate_b = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_b));

    assert_eq!(sort_key(&predicate_a), sort_key(&predicate_b));
}

#[test]
fn predicate_sort_key_normalizes_in_list_literal_order() {
    let predicate_a = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Nat64(3), Value::Nat64(1), Value::Nat64(2)],
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)],
    ));

    assert_eq!(sort_key(&predicate_a), sort_key(&predicate_b));
}

#[test]
fn predicate_sort_key_normalizes_in_list_duplicate_literals() {
    let predicate_a = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![
            Value::Nat64(3),
            Value::Nat64(1),
            Value::Nat64(3),
            Value::Nat64(2),
        ],
    ));
    let predicate_b = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)],
    ));

    assert_eq!(sort_key(&predicate_a), sort_key(&predicate_b));
}

#[test]
fn predicate_sort_key_numeric_widen_treats_equivalent_literal_subtypes_as_identical() {
    let predicate_int = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int64(1),
        CoercionId::NumericWiden,
    ));
    let predicate_decimal = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Decimal(crate::types::Decimal::new(10, 1)),
        CoercionId::NumericWiden,
    ));

    assert_eq!(sort_key(&predicate_int), sort_key(&predicate_decimal));
}

#[test]
fn predicate_sort_key_strict_keeps_numeric_literal_subtypes_distinct() {
    let predicate_int = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int64(1),
        CoercionId::Strict,
    ));
    let predicate_decimal = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Decimal(crate::types::Decimal::new(10, 1)),
        CoercionId::Strict,
    ));

    assert_ne!(sort_key(&predicate_int), sort_key(&predicate_decimal));
}

#[test]
fn predicate_sort_key_text_casefold_treats_case_only_literals_as_identical() {
    let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::TextCasefold,
    ));
    let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ADA".to_string()),
        CoercionId::TextCasefold,
    ));

    assert_eq!(sort_key(&predicate_lower), sort_key(&predicate_upper));
}

#[test]
fn predicate_sort_key_strict_keeps_text_case_variants_distinct() {
    let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ada".to_string()),
        CoercionId::Strict,
    ));
    let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Eq,
        Value::Text("ADA".to_string()),
        CoercionId::Strict,
    ));

    assert_ne!(sort_key(&predicate_lower), sort_key(&predicate_upper));
}

#[test]
fn predicate_sort_key_text_casefold_normalizes_in_list_case_variants() {
    let predicate_mixed = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("ADA".to_string()),
            Value::Text("ada".to_string()),
            Value::Text("Bob".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));
    let predicate_canonical = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::In,
        Value::List(vec![
            Value::Text("ada".to_string()),
            Value::Text("bob".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    assert_eq!(sort_key(&predicate_mixed), sort_key(&predicate_canonical));
}
