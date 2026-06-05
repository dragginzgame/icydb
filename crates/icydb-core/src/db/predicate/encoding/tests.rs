use crate::{
    db::predicate::{
        CoercionId, CompareOp, ComparePredicate, Predicate, encoding::encode_predicate_sort_key,
    },
    value::Value,
};

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

    assert_eq!(
        encode_predicate_sort_key(&predicate_a),
        encode_predicate_sort_key(&predicate_b)
    );
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

    assert_eq!(
        encode_predicate_sort_key(&predicate_a),
        encode_predicate_sort_key(&predicate_b)
    );
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

    assert_eq!(
        encode_predicate_sort_key(&predicate_a),
        encode_predicate_sort_key(&predicate_b)
    );
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

    assert_eq!(
        encode_predicate_sort_key(&predicate_a),
        encode_predicate_sort_key(&predicate_b)
    );
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

    assert_eq!(
        encode_predicate_sort_key(&predicate_int),
        encode_predicate_sort_key(&predicate_decimal)
    );
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

    assert_ne!(
        encode_predicate_sort_key(&predicate_int),
        encode_predicate_sort_key(&predicate_decimal)
    );
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

    assert_eq!(
        encode_predicate_sort_key(&predicate_lower),
        encode_predicate_sort_key(&predicate_upper)
    );
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

    assert_ne!(
        encode_predicate_sort_key(&predicate_lower),
        encode_predicate_sort_key(&predicate_upper)
    );
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

    assert_eq!(
        encode_predicate_sort_key(&predicate_mixed),
        encode_predicate_sort_key(&predicate_canonical)
    );
}
