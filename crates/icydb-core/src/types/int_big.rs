//! Engine operations for the canonical schema-owned `IntBig` atom.

pub use icydb_schema::IntBig;

use crate::value::{
    RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value,
};

impl RuntimeValueMeta for IntBig {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for IntBig {
    fn to_value(&self) -> Value {
        Value::IntBig(self.clone())
    }
}

impl RuntimeValueDecode for IntBig {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::IntBig(value) => Some(value.clone()),
            _ => None,
        }
    }
}
