//! Engine operations for the canonical schema-owned `Float32` atom.

pub use icydb_schema::{Float32, Float32DecodeError};

use crate::value::{
    RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value,
};

impl RuntimeValueMeta for Float32 {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Float32 {
    fn to_value(&self) -> Value {
        Value::Float32(*self)
    }
}

impl RuntimeValueDecode for Float32 {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Float32(value) => Some(*value),
            _ => None,
        }
    }
}
