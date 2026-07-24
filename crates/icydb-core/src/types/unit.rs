//! Engine operations for the canonical schema-owned `Unit` atom.

pub use icydb_schema::Unit;

use crate::{
    db::{
        EntityKeyBytes, EntityKeyBytesError, PrimaryKeyComponent, PrimaryKeyDecode,
        PrimaryKeyEncode, PrimaryKeyEncodeError, PrimaryKeyValue, validate_entity_key_bytes_buffer,
    },
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

impl EntityKeyBytes for Unit {
    const BYTE_LEN: usize = 0;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)
    }
}

impl RuntimeValueMeta for () {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for () {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl RuntimeValueDecode for () {
    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(())
    }
}

impl RuntimeValueMeta for Unit {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Unit {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl RuntimeValueDecode for Unit {
    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(Self)
    }
}

impl PrimaryKeyEncode for Unit {
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        Ok(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Unit))
    }
}

impl PrimaryKeyDecode for Unit {
    fn from_primary_key_value(key: &PrimaryKeyValue) -> Result<Self, crate::error::InternalError> {
        match *key {
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Unit) => Ok(Self),
            _ => Err(crate::error::InternalError::store_corruption()),
        }
    }
}

impl SanitizeAuto for Unit {}

impl SanitizeCustom for Unit {}

impl ValidateAuto for Unit {}

impl ValidateCustom for Unit {}

impl Visitable for Unit {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}
