//! Engine operations for the canonical schema-owned `Date` atom.

pub use icydb_schema::Date;

use crate::value::{
    RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value,
};

impl RuntimeValueMeta for Date {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Date {
    fn to_value(&self) -> Value {
        Value::Date(*self)
    }
}

impl RuntimeValueDecode for Date {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Date(value) => Some(*value),
            _ => None,
        }
    }
}
