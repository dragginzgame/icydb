//! Engine operations for the canonical schema-owned `Subaccount` atom.

pub use icydb_schema::Subaccount;

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{NormalizeAuto, NormalizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

impl EntityKeyBytes for Subaccount {
    const BYTE_LEN: usize = 32;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.to_bytes());
        Ok(())
    }
}

impl RuntimeValueMeta for Subaccount {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Subaccount {
    fn to_value(&self) -> Value {
        Value::Subaccount(*self)
    }
}

impl RuntimeValueDecode for Subaccount {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Subaccount(value) => Some(*value),
            _ => None,
        }
    }
}

impl NormalizeAuto for Subaccount {}

impl NormalizeCustom for Subaccount {}

impl ValidateAuto for Subaccount {}

impl ValidateCustom for Subaccount {}

impl Visitable for Subaccount {}
