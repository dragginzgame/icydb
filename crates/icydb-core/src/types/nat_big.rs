//! Engine operations for the canonical schema-owned `NatBig` atom.

pub use icydb_schema::NatBig;

use crate::{
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{NormalizeAuto, NormalizeCustom, ValidateAuto, ValidateCustom, Visitable},
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

impl NormalizeAuto for NatBig {}

impl NormalizeCustom for NatBig {}

impl ValidateAuto for NatBig {}

impl ValidateCustom for NatBig {}

impl Visitable for NatBig {}
