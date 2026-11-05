use super::PreparedQueryParameterContract;
use crate::{
    db::{
        Predicate,
        predicate::{CoercionId, CompareOp, ComparePredicate},
        query::preparation::with_preparation_work,
    },
    value::Value,
};

fn contract(predicate: &Predicate) -> Option<PreparedQueryParameterContract> {
    with_preparation_work(|work| {
        PreparedQueryParameterContract::from_normalized_predicate(predicate, work).unwrap()
    })
}

#[test]
fn equality_literals_share_one_parameter_contract() {
    let left = contract(&Predicate::eq("id".to_string(), Value::Nat64(1)));
    let right = contract(&Predicate::eq("id".to_string(), Value::Nat64(2)));

    assert_eq!(left, right);
}

#[test]
fn membership_contract_does_not_depend_on_list_arity() {
    let short = contract(&Predicate::in_("id".to_string(), vec![Value::Nat64(1)]));
    let long = contract(&Predicate::in_(
        "id".to_string(),
        vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)],
    ));

    assert_eq!(short, long);
}

#[test]
fn exact_and_ordered_slots_do_not_alias() {
    let exact = contract(&Predicate::eq("id".to_string(), Value::Nat64(1)));
    let ordered = contract(&Predicate::gte("id".to_string(), Value::Nat64(1)));

    assert_ne!(exact, ordered);
}

#[test]
fn parameter_contracts_keep_comparison_coercions_distinct() {
    for (value, alternative) in [
        (Value::Nat64(7), CoercionId::NumericWiden),
        (Value::Text("MiXeD".into()), CoercionId::TextCasefold),
    ] {
        let [strict, coerced] = [CoercionId::Strict, alternative].map(|coercion| {
            contract(&Predicate::Compare(ComparePredicate::with_coercion(
                "value",
                CompareOp::Eq,
                value.clone(),
                coercion,
            )))
            .expect("comparison has a parameter contract")
        });
        assert_ne!(strict, coerced);
    }
}

#[test]
fn oversized_list_and_payload_do_not_become_templates() {
    let too_many = contract(&Predicate::in_(
        "id".to_string(),
        (0..=super::MAX_PREPARED_QUERY_LIST_PARAMETER_ITEMS)
            .map(|value| Value::Nat64(value as u64))
            .collect(),
    ));
    let too_many_bytes = contract(&Predicate::eq(
        "label".to_string(),
        Value::Text("x".repeat(super::MAX_PREPARED_QUERY_PARAMETER_BYTES as usize + 1)),
    ));

    assert!(too_many.is_none());
    assert!(too_many_bytes.is_none());
}
