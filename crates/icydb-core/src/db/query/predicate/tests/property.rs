use crate::{
    db::query::predicate::{
        CoercionId, CoercionSpec, CompareOp, Predicate, eval,
        eval::{FieldPresence, Row},
        normalize,
    },
    types::{Account, Principal, Ulid},
    value::{Value, ValueEnum},
};
use proptest::prelude::*;
use std::{cmp::Ordering, collections::BTreeMap};

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
            .prop_map(|variant| { Value::Enum(ValueEnum::new(variant, Some("TestEnum"))) }),
        Just(Value::None),
        Just(Value::Unit),
    ]
}

fn arb_list_value() -> impl Strategy<Value = Value> {
    prop::collection::vec(arb_scalar_value(), 0..4).prop_map(Value::List)
}

fn arb_map_value() -> impl Strategy<Value = Value> {
    prop::collection::vec(
        (arb_scalar_value(), arb_scalar_value()).prop_map(|(k, v)| Value::List(vec![k, v])),
        0..4,
    )
    .prop_map(Value::List)
}

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![arb_scalar_value(), arb_list_value(), arb_map_value()]
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
            arb_coercion_spec()
        )
            .prop_map(|(field, op, value, coercion)| {
                Predicate::Compare(crate::db::query::predicate::ast::ComparePredicate {
                    field,
                    op,
                    value,
                    coercion,
                })
            },),
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
            .prop_map(
                |(field, key, value, coercion)| Predicate::MapContainsEntry {
                    field,
                    key,
                    value,
                    coercion,
                }
            ),
        (arb_field(), arb_scalar_value())
            .prop_map(|(field, value)| { Predicate::TextContains { field, value } }),
        (arb_field(), arb_scalar_value())
            .prop_map(|(field, value)| { Predicate::TextContainsCi { field, value } }),
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

proptest! {
    #[test]
    fn normalization_equivalence(predicate in arb_predicate(), row in arb_row()) {
        let normalized = normalize(&predicate);
        prop_assert_eq!(eval(&row, &predicate), eval(&row, &normalized));
    }

    #[test]
    fn scan_invariance(predicate in arb_predicate(), rows in prop::collection::vec(arb_row(), 0..10)) {
        let normalized = normalize(&predicate);
        let left = scan(&rows, &predicate);
        let right = scan(&rows, &normalized);
        prop_assert_eq!(left, right);
    }
}

proptest! {
    #[test]
    fn coercion_deterministic(lhs in arb_value(), rhs in arb_value(), id in arb_coercion_spec()) {
        use crate::db::query::predicate::coercion::{compare_eq, compare_order};

        let a_eq = compare_eq(&lhs, &rhs, &id);
        let b_eq = compare_eq(&lhs, &rhs, &id);
        prop_assert_eq!(a_eq, b_eq);

        let a_ord = compare_order(&lhs, &rhs, &id);
        let b_ord = compare_order(&lhs, &rhs, &id);
        prop_assert_eq!(a_ord, b_ord);
    }

    #[test]
    fn symmetric_coercions(lhs in arb_value(), rhs in arb_value()) {
        use crate::db::query::predicate::coercion::{compare_eq, compare_order};

        let symmetric = [
            CoercionId::Strict,
            CoercionId::NumericWiden,
            CoercionId::CollectionElement,
            CoercionId::TextCasefold,
        ];

        for id in symmetric {
            let spec = CoercionSpec::new(id);
            let forward_eq = compare_eq(&lhs, &rhs, &spec);
            let backward_eq = compare_eq(&rhs, &lhs, &spec);
            prop_assert_eq!(forward_eq, backward_eq);

            let forward_ord = compare_order(&lhs, &rhs, &spec);
            let backward_ord = compare_order(&rhs, &lhs, &spec);
            prop_assert_eq!(forward_ord, backward_ord.map(Ordering::reverse));
        }
    }
}

#[test]
fn not_in_invalid_values_are_false() {
    let mut fields = BTreeMap::new();
    fields.insert("a".to_string(), Value::Int(5));
    let row = TestRow { fields };

    let not_list = Predicate::Compare(crate::db::query::predicate::ast::ComparePredicate {
        field: "a".to_string(),
        op: CompareOp::NotIn,
        value: Value::Text("nope".to_string()),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });
    assert!(!eval(&row, &not_list));

    let wrong_list = Predicate::Compare(crate::db::query::predicate::ast::ComparePredicate {
        field: "a".to_string(),
        op: CompareOp::NotIn,
        value: Value::List(vec![Value::Text("nope".to_string())]),
        coercion: CoercionSpec::new(CoercionId::Strict),
    });
    assert!(!eval(&row, &wrong_list));
}
