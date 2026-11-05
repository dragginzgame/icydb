use super::{Value, value::Map};

#[test]
fn protocol_shape_remains_recursive_without_database_codecs() {
    let value = Value::Array(vec![
        Box::new(Value::Nat(7)),
        Box::new(Value::Map(Box::new(Map::from(vec![(
            "label",
            Value::text("ready"),
        )])))),
    ]);

    assert!(matches!(value, Value::Array(values) if values.len() == 2));
}
