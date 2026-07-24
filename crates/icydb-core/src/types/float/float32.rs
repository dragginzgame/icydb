//! Engine operations for the canonical schema-owned `Float32` atom.

pub use icydb_schema::{Float32, Float32DecodeError};

use crate::{
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
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

impl SanitizeAuto for Float32 {}

impl SanitizeCustom for Float32 {}

impl ValidateAuto for Float32 {}

impl ValidateCustom for Float32 {}

impl Visitable for Float32 {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}
