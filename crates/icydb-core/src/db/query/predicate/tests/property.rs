use crate::{
    db::query::predicate::{
        CoercionId, CoercionSpec, CompareOp, Predicate,
        ast::ComparePredicate,
        coercion::{compare_eq, compare_order},
        eval,
        eval::{FieldPresence, Row},
        normalize,
    },
    types::{Account, Principal, Ulid},
    value::{Value, ValueEnum},
};
use proptest::prelude::*;
use std::{cmp::Ordering, collections::BTreeMap};

///
/// TestRow
///
/// Simple in-memory row implementation for predicate evaluation.
///
/// This deliberately models only:
/// - field presence vs missing
/// - cloning of values (acceptable for tests)
///

#[derive(Clone, Debug)]
struct TestRow {
    fields: BTreeMap<String, Value>,
}

impl Row for TestRow {
    fn field(&self, name: &str) -> FieldPresence {
        match self.fields.get(name) {
            Some(value) => FieldPresence::Present(value.clone()),
            None => FieldPresence::Missing,
        }
    }
}

const FIELDS: [&str; 4] = ["a", "b", "c", "d"];

fn arb_field() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(FIELDS[0].to_string()),
        Just(FIELDS[1].to_string()),
        Just(FIELDS[2].to_string()),
        Just(FIELDS[3].to_string()),
    ]
}

fn arb_scalar_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(Value::Uint),
        any::<bool>().prop_map(Value::Bool),
        "[a-zA-Z0-9_]{0,8}".prop_map(Value::Text),
        any::<u128>().prop_map(|n| Value::Ulid(Ulid::from_u128(n))),
        any::<u8>().prop_map(|b| Value::Account(Account::dummy(b))),
        any::<u8>().prop_map(|b| Value::Principal(Principal::from_slice(&[b]))),
        prop_oneof![Just("A"), Just("B"), Just("C")]
            .prop_map(|variant| Value::Enum(ValueEnum::new(variant, Some("TestEnum")))),
        Just(Value::Null),
        Just(Value::Unit),
    ]
}

fn arb_list_value() -> impl Strategy<Value = Value> {
    prop::collection::vec(arb_scalar_value(), 0..4).prop_map(Value::List)
}

fn arb_map_key() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(Value::Uint),
        any::<bool>().prop_map(Value::Bool),
        "[a-zA-Z0-9_]{0,8}".prop_map(Value::Text),
        any::<u128>().prop_map(|n| Value::Ulid(Ulid::from_u128(n))),
        any::<u8>().prop_map(|b| Value::Account(Account::dummy(b))),
        any::<u8>().prop_map(|b| Value::Principal(Principal::from_slice(&[b]))),
        prop_oneof![Just("A"), Just("B"), Just("C")]
            .prop_map(|variant| Value::Enum(ValueEnum::new(variant, Some("TestEnum")))),
    ]
}

fn arb_map_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(Value::Uint),
        any::<bool>().prop_map(Value::Bool),
        "[a-zA-Z0-9_]{0,8}".prop_map(Value::Text),
        any::<u128>().prop_map(|n| Value::Ulid(Ulid::from_u128(n))),
        any::<u8>().prop_map(|b| Value::Account(Account::dummy(b))),
        any::<u8>().prop_map(|b| Value::Principal(Principal::from_slice(&[b]))),
        prop_oneof![Just("A"), Just("B"), Just("C")]
            .prop_map(|variant| Value::Enum(ValueEnum::new(variant, Some("TestEnum")))),
        Just(Value::Null),
    ]
}

fn arb_map() -> impl Strategy<Value = Value> {
    prop::collection::vec((arb_map_key(), arb_map_value()), 0..4).prop_filter_map(
        "generated map entries must satisfy map invariants",
        |entries| Value::from_map(entries).ok(),
    )
}

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![arb_scalar_value(), arb_list_value(), arb_map()]
}

fn arb_coercion_spec() -> impl Strategy<Value = CoercionSpec> {
    prop_oneof![
        Just(CoercionId::Strict),
        Just(CoercionId::NumericWiden),
        Just(CoercionId::TextCasefold),
        Just(CoercionId::CollectionElement),
    ]
    .prop_map(CoercionSpec::new)
}

fn arb_compare_op() -> impl Strategy<Value = CompareOp> {
    prop_oneof![
        Just(CompareOp::Eq),
        Just(CompareOp::Ne),
        Just(CompareOp::Lt),
        Just(CompareOp::Lte),
        Just(CompareOp::Gt),
        Just(CompareOp::Gte),
        Just(CompareOp::In),
        Just(CompareOp::NotIn),
        Just(CompareOp::Contains),
        Just(CompareOp::StartsWith),
        Just(CompareOp::EndsWith),
    ]
}

/// Arbitrary predicate generator for supported predicate shapes.
///
/// NOTE: This intentionally generates *semantically invalid* predicates
/// (e.g. ordering on lists, IN with non-lists, text ops on non-text).
///
/// The goal is to assert:
/// - totality (never panic)
/// - determinism
/// - normalization equivalence
fn arb_predicate() -> impl Strategy<Value = Predicate> {
    let leaf = prop_oneof![
        Just(Predicate::True),
        Just(Predicate::False),
        arb_field().prop_map(|field| Predicate::IsNull { field }),
        arb_field().prop_map(|field| Predicate::IsMissing { field }),
        arb_field().prop_map(|field| Predicate::IsEmpty { field }),
        arb_field().prop_map(|field| Predicate::IsNotEmpty { field }),
        (
            arb_field(),
            arb_compare_op(),
            arb_value(),
            arb_coercion_spec(),
        )
            .prop_map(|(field, op, value, coercion)| {
                Predicate::Compare(ComparePredicate {
                    field,
                    op,
                    value,
                    coercion,
                })
            }),
        (arb_field(), arb_scalar_value())
            .prop_map(|(field, value)| Predicate::TextContains { field, value }),
        (arb_field(), arb_scalar_value())
            .prop_map(|(field, value)| Predicate::TextContainsCi { field, value }),
    ];

    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Predicate::And),
            prop::collection::vec(inner.clone(), 0..4).prop_map(Predicate::Or),
            inner.prop_map(|p| Predicate::Not(Box::new(p))),
        ]
    })
}

fn arb_unsupported_map_predicate() -> impl Strategy<Value = Predicate> {
    let leaf = prop_oneof![
        (arb_field(), arb_scalar_value(), arb_coercion_spec()).prop_map(
            |(field, key, coercion)| Predicate::MapContainsKey {
                field,
                key,
                coercion,
            },
        ),
        (arb_field(), arb_scalar_value(), arb_coercion_spec()).prop_map(
            |(field, value, coercion)| Predicate::MapContainsValue {
                field,
                value,
                coercion,
            },
        ),
        (
            arb_field(),
            arb_scalar_value(),
            arb_scalar_value(),
            arb_coercion_spec(),
        )
            .prop_map(|(field, key, value, coercion)| {
                Predicate::MapContainsEntry {
                    field,
                    key,
                    value,
                    coercion,
                }
            }),
    ];

    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Predicate::And),
            prop::collection::vec(inner.clone(), 0..4).prop_map(Predicate::Or),
            inner.prop_map(|p| Predicate::Not(Box::new(p))),
        ]
    })
}

fn arb_row() -> impl Strategy<Value = TestRow> {
    prop::collection::vec(
        prop_oneof![Just(None), arb_value().prop_map(Some)],
        FIELDS.len(),
    )
    .prop_map(|values| {
        let mut fields = BTreeMap::new();
        for (name, value) in FIELDS.iter().zip(values) {
            if let Some(value) = value {
                fields.insert((*name).to_string(), value);
            }
        }
        TestRow { fields }
    })
}

fn scan(rows: &[TestRow], predicate: &Predicate) -> BTreeMap<usize, bool> {
    rows.iter()
        .enumerate()
        .map(|(idx, row)| (idx, eval(row, predicate)))
        .collect()
}

//
// Normalization invariants
//

proptest! {
    #[test]
    fn normalization_equivalence(predicate in arb_predicate(), row in arb_row()) {
        let normalized = normalize(&predicate);
        prop_assert_eq!(
            eval(&row, &predicate),
            eval(&row, &normalized)
        );
    }

    #[test]
    fn normalization_idempotent(predicate in arb_predicate()) {
        let once = normalize(&predicate);
        let twice = normalize(&once);
        prop_assert_eq!(once, twice);
    }

    #[test]
    fn scan_invariance(
        predicate in arb_predicate(),
        rows in prop::collection::vec(arb_row(), 0..10)
    ) {
        let normalized = normalize(&predicate);
        prop_assert_eq!(
            scan(&rows, &predicate),
            scan(&rows, &normalized)
        );
    }
}

//
// Coercion invariants
//

proptest! {
    #[test]
    fn coercion_deterministic(
        lhs in arb_value(),
        rhs in arb_value(),
        spec in arb_coercion_spec()
    ) {
        prop_assert_eq!(
            compare_eq(&lhs, &rhs, &spec),
            compare_eq(&lhs, &rhs, &spec)
        );

        prop_assert_eq!(
            compare_order(&lhs, &rhs, &spec),
            compare_order(&lhs, &rhs, &spec)
        );
    }

    #[test]
    fn symmetric_coercions(lhs in arb_value(), rhs in arb_value()) {
        // All coercions listed here are defined as symmetric and locale-independent.
        let symmetric = [
            CoercionId::Strict,
            CoercionId::NumericWiden,
            CoercionId::CollectionElement,
            CoercionId::TextCasefold,
        ];

        for id in symmetric {
            let spec = CoercionSpec::new(id);

            prop_assert_eq!(
                compare_eq(&lhs, &rhs, &spec),
                compare_eq(&rhs, &lhs, &spec)
            );

            prop_assert_eq!(
                compare_order(&lhs, &rhs, &spec),
                compare_order(&rhs, &lhs, &spec).map(Ordering::reverse)
            );
        }
    }
}

//
// Regression tests
//

#[test]
fn not_in_invalid_values_are_false() {
    let mut fields = BTreeMap::new();
    fields.insert("a".to_string(), Value::Int(5));
    let row = TestRow { fields };

    let not_list = Predicate::Compare(ComparePredicate {
        field: "a".to_string(),
        op: CompareOp::NotIn,
        value: Value::Text("nope".to_string()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });
    assert!(!eval(&row, &not_list));

    let wrong_list = Predicate::Compare(ComparePredicate {
        field: "a".to_string(),
        op: CompareOp::NotIn,
        value: Value::List(vec![Value::Text("nope".to_string())]),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });
    assert!(!eval(&row, &wrong_list));
}

//
// Unsupported-shape invariants (policy-disallowed map predicates)
//

mod unsupported_shapes {
    use super::*;

    proptest! {
        #[test]
        fn map_predicate_normalization_equivalence(
            predicate in arb_unsupported_map_predicate(),
            row in arb_row()
        ) {
            let normalized = normalize(&predicate);
            prop_assert_eq!(
                eval(&row, &predicate),
                eval(&row, &normalized)
            );
        }

        #[test]
        fn map_predicate_scan_invariance(
            predicate in arb_unsupported_map_predicate(),
            rows in prop::collection::vec(arb_row(), 0..10)
        ) {
            let normalized = normalize(&predicate);
            prop_assert_eq!(
                scan(&rows, &predicate),
                scan(&rows, &normalized)
            );
        }
    }
}
