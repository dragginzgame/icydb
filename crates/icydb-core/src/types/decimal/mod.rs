//! Engine operations for the canonical schema-owned `Decimal` atom.

pub use icydb_schema::{Decimal, DecimalParts, ParseDecimalError, ParseDecimalErrorReason};

use crate::value::{
    RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value,
};

impl RuntimeValueMeta for Decimal {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Decimal {
    fn to_value(&self) -> Value {
        Value::Decimal(*self)
    }
}

impl RuntimeValueDecode for Decimal {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Decimal(value) => Some(*value),
            _ => None,
        }
    }
}
