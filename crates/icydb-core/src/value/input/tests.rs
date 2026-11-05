use crate::value::{InputValue, Value, hash_value};
use std::cmp::Ordering;

#[test]
fn input_maps_lower_to_canonical_runtime_values() {
    let entries = vec![
        (Value::Text("z".to_string()), Value::Null),
        (Value::Nat64(1), Value::List(vec![Value::Bool(true)])),
        (Value::Int64(1), Value::Text("signed".to_string())),
        (Value::Text("a".to_string()), Value::Unit),
    ];
    let expected = Value::from_map(entries.clone()).expect("valid map");
    for rotation in 0..entries.len() {
        let mut permuted = entries.clone();
        permuted.rotate_left(rotation);
        for reverse in [false, true] {
            if reverse {
                permuted.reverse();
            }
            let input = InputValue::map(
                permuted
                    .iter()
                    .map(|(key, value)| (InputValue::from(key), InputValue::from(value)))
                    .collect(),
            );
            let actual = input.try_into_runtime_non_enum().expect("valid map");
            assert_eq!(actual, expected);
            assert_eq!(Value::canonical_cmp(&actual, &expected), Ordering::Equal);
            assert_eq!(hash_value(&actual).unwrap(), hash_value(&expected).unwrap());
        }
    }
    assert_eq!(
        InputValue::map(Vec::new()).try_into_runtime_non_enum(),
        Some(Value::from_map(Vec::new()).unwrap()),
    );
}

#[test]
fn input_map_lowering_normalizes_nested_maps() {
    let nested = InputValue::map(vec![
        (InputValue::from("z"), InputValue::null()),
        (InputValue::from("a"), InputValue::nat64(7)),
    ]);
    let expected = Value::from_map(vec![
        (Value::Text("a".to_string()), Value::Nat64(7)),
        (Value::Text("z".to_string()), Value::Null),
    ])
    .unwrap();
    assert_eq!(
        InputValue::list(vec![nested.clone()]).try_into_runtime_non_enum(),
        Some(Value::List(vec![expected.clone()])),
    );
    assert_eq!(
        InputValue::map(vec![(InputValue::from("nested"), nested)]).try_into_runtime_non_enum(),
        Some(Value::from_map(vec![(Value::Text("nested".to_string()), expected)]).unwrap()),
    );
}

#[test]
fn input_map_lowering_rejects_invalid_keys_and_unresolved_enums_recursively() {
    let invalid = [
        vec![(InputValue::null(), InputValue::nat64(1))],
        vec![(InputValue::list(Vec::new()), InputValue::nat64(1))],
        vec![(InputValue::map(Vec::new()), InputValue::nat64(1))],
        vec![
            (InputValue::from("a"), InputValue::nat64(1)),
            (InputValue::from("a"), InputValue::nat64(2)),
        ],
        vec![(InputValue::loose_enum("Active"), InputValue::nat64(1))],
        vec![(InputValue::from("a"), InputValue::loose_enum("Active"))],
    ];
    for entries in invalid {
        let input = InputValue::map(entries);
        assert_eq!(input.clone().try_into_runtime_non_enum(), None);
        assert_eq!(
            InputValue::list(vec![input.clone()]).try_into_runtime_non_enum(),
            None,
        );
        assert_eq!(
            InputValue::map(vec![(InputValue::from("nested"), input)]).try_into_runtime_non_enum(),
            None,
        );
    }
}

#[test]
fn runtime_to_input_value_keeps_recursive_collection_shape() {
    let runtime = Value::List(vec![
        Value::Nat64(7),
        Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
    ]);

    assert_eq!(
        InputValue::from(runtime),
        InputValue::list(vec![
            InputValue::nat64(7),
            InputValue::map(vec![(InputValue::from("x"), InputValue::boolean(true))]),
        ]),
    );
}

#[test]
fn unresolved_enum_input_cannot_lower_without_admission() {
    let direct = InputValue::loose_enum("Active");
    let nested = InputValue::list(vec![direct.clone()]);

    assert_eq!(direct.try_into_runtime_non_enum(), None);
    assert_eq!(nested.try_into_runtime_non_enum(), None);
}
