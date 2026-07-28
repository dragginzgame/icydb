//! Engine operations for the canonical schema-owned `Float64` atom.

pub use icydb_schema::{Float64, Float64DecodeError};

use crate::value::{
    RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value,
};

impl RuntimeValueMeta for Float64 {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Float64 {
    fn to_value(&self) -> Value {
        Value::Float64(*self)
    }
}

impl RuntimeValueDecode for Float64 {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Float64(value) => Some(*value),
            _ => None,
        }
    }
}
