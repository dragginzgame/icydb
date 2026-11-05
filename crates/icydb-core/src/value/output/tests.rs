use crate::value::{OutputValue, PublicValue, Value};

#[test]
fn output_value_from_runtime_value_keeps_recursive_collection_shape() {
    let runtime = Value::List(vec![
        Value::Nat64(7),
        Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
    ]);

    assert_eq!(
        OutputValue::from(runtime),
        OutputValue::list(vec![
            PublicValue::Nat64(7),
            PublicValue::Map(vec![(
                PublicValue::Text("x".to_string()),
                PublicValue::Bool(true),
            )]),
        ]),
    );
}
