//! Engine operations for the canonical schema-owned `Blob` atom.

pub use icydb_schema::Blob;

use crate::{
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{NormalizeAuto, NormalizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

impl RuntimeValueMeta for Blob {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Blob {
    fn to_value(&self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl RuntimeValueDecode for Blob {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Blob(value) => Some(Self::from(value.clone())),
            _ => None,
        }
    }
}

impl NormalizeAuto for Blob {}

impl NormalizeCustom for Blob {}

impl ValidateAuto for Blob {}

impl ValidateCustom for Blob {}

impl Visitable for Blob {}
