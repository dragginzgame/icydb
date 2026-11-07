use super::{hash_predicate_structural, predicate_fingerprint};
use crate::{
    db::predicate::{CompareOp, ComparePredicate, Predicate, coercion::CoercionId, normalize},
    value::Value,
};

#[test]
fn hash_predicate_preserves_raw_and_child_order_before_normalization() {
    let left = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int64(1))),
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int64(2))),
    ]);
    let right = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int64(2))),
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int64(1))),
    ]);

    assert_ne!(digest_structural(&left), digest_structural(&right));
}

#[test]
fn canonical_hash_is_order_insensitive_for_and() {
    let left = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int64(1))),
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int64(2))),
    ]);
    let right = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int64(2))),
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int64(1))),
    ]);

    assert_eq!(normalize(left.clone()), normalize(right.clone()));
    assert_eq!(digest(&left), digest(&right));
}

#[test]
fn canonical_hash_is_order_insensitive_for_or() {
    let left = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int64(1))),
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int64(2))),
    ]);
    let right = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int64(2))),
        Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int64(1))),
    ]);

    assert_eq!(normalize(left.clone()), normalize(right.clone()));
    assert_eq!(digest(&left), digest(&right));
}

#[test]
fn canonical_hash_treats_same_field_or_eq_and_in_as_equivalent() {
    let or_eq = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Nat64(3),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Nat64(1),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Nat64(3),
            CoercionId::Strict,
        )),
    ]);
    let in_list = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Nat64(1), Value::Nat64(3)]),
        CoercionId::Strict,
    ));

    assert_eq!(normalize(or_eq.clone()), normalize(in_list.clone()));
    assert_eq!(digest(&or_eq), digest(&in_list));
}

#[test]
fn canonical_hash_is_order_insensitive_for_in_list_literals() {
    let left = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Nat64(3), Value::Nat64(1), Value::Nat64(2)],
    ));
    let right = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)],
    ));

    assert_ne!(normalize(left.clone()), normalize(right.clone()));
    assert_eq!(digest(&left), digest(&right));
}

#[test]
fn canonical_hash_normalizes_in_list_duplicate_literals() {
    let left = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![
            Value::Nat64(3),
            Value::Nat64(1),
            Value::Nat64(3),
            Value::Nat64(2),
        ],
    ));
    let right = Predicate::Compare(ComparePredicate::in_(
        "rank".to_string(),
        vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)],
    ));

    assert_ne!(normalize(left.clone()), normalize(right.clone()));
    assert_eq!(digest(&left), digest(&right));
}

#[test]
fn canonical_hash_treats_implicit_and_explicit_strict_coercion_as_equivalent() {
    let left = Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Int64(7)));
    let right = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int64(7),
        CoercionId::Strict,
    ));

    assert_eq!(normalize(left.clone()), normalize(right.clone()));
    assert_eq!(digest(&left), digest(&right));
}

#[test]
fn canonical_hash_distinguishes_different_coercion_ids() {
    let strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int64(7),
        CoercionId::Strict,
    ));
    let numeric_widen = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Eq,
        Value::Int64(7),
        CoercionId::NumericWiden,
    ));

    assert_ne!(normalize(strict.clone()), normalize(numeric_widen.clone()));
    assert_ne!(digest(&strict), digest(&numeric_widen));
}

fn digest(predicate: &Predicate) -> [u8; 32] {
    predicate_fingerprint(predicate)
}

fn digest_structural(predicate: &Predicate) -> [u8; 32] {
    let mut hasher = crate::db::codec::new_hash_sha256();
    crate::db::query::preparation::with_preparation_work(|work| {
        hash_predicate_structural(&mut hasher, predicate, work).unwrap();
    });
    crate::db::codec::finalize_hash_sha256(hasher)
}

#[test]
fn numeric_fingerprints_preserve_inexact_decimal_distinctions() {
    let predicate = |value| {
        Predicate::Compare(ComparePredicate::with_coercion(
            "amount",
            CompareOp::Eq,
            value,
            CoercionId::NumericWiden,
        ))
    };
    let float = predicate(Value::Float64(crate::types::Float64::try_new(0.3).unwrap()));
    let decimal = predicate(Value::Decimal(crate::types::Decimal::new(3, 1)));
    assert_ne!(digest(&float), digest(&decimal));
}
