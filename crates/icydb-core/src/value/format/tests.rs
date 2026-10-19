use super::write_value_debug;
use crate::value::{Value, ValueEnum};
use std::fmt::{self, Write};

fn fixtures() -> Vec<Value> {
    let mut values = vec![
        Value::Null,
        Value::Unit,
        Value::Bool(true),
        Value::Text("quote\"'\\\n\0λ".into()),
        Value::Blob(vec![0; 10]),
        Value::Int64(i64::MIN),
        Value::Nat128(u128::MAX),
        Value::List(vec![Value::Nat128(u128::MAX), Value::Int128(i128::MIN)]),
        Value::Map(vec![]),
        Value::Enum(ValueEnum::test_unit(2, 3)),
    ];
    let nested = Value::List(values.clone());
    values.push(nested.clone());
    values.push(Value::Map(vec![(
        Value::Text("key".into()),
        nested.clone(),
    )]));
    values.push(Value::Enum(ValueEnum::test_payload(2, 3, nested)));
    values
}

#[test]
fn compact_value_labels_preserve_debug_bytes_including_nested_decimal_values() {
    for value in fixtures() {
        let mut out = String::new();
        write_value_debug(&value, &mut out).unwrap();
        assert_eq!(out, format!("{value:?}"));
    }
}

struct RejectingOutput {
    text: String,
    limit: usize,
    failed: bool,
}

impl Write for RejectingOutput {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        assert!(!self.failed);
        if self.text.len() + text.len() > self.limit {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

#[test]
fn nested_value_formatting_stops_at_output_rejection() {
    let value = Value::Enum(ValueEnum::test_payload(
        2,
        3,
        Value::Map(vec![(
            Value::Text("key".into()),
            Value::List(vec![Value::Nat128(u128::MAX), Value::Int128(i128::MIN)]),
        )]),
    ));
    let expected = format!("{value:?}");
    for limit in 0..=expected.len() {
        let mut out = RejectingOutput {
            text: String::new(),
            limit,
            failed: false,
        };
        assert_eq!(
            write_value_debug(&value, &mut out).is_ok(),
            limit == expected.len()
        );
        assert!(expected.starts_with(&out.text));
        assert!(out.text.len() <= limit);
    }
}
