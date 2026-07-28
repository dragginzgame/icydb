//! Engine operations for the canonical schema-owned `NatBig` atom.

pub use icydb_schema::NatBig;

use crate::value::{
    RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value,
};

impl RuntimeValueMeta for NatBig {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for NatBig {
    fn to_value(&self) -> Value {
        Value::NatBig(self.clone())
    }
}

impl RuntimeValueDecode for NatBig {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::NatBig(value) => Some(value.clone()),
            _ => None,
        }
    }
}
