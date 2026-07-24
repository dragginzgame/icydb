//! Engine operations for the canonical schema-owned `Ulid` atom.

mod generator;
#[cfg(test)]
mod tests;

pub use icydb_schema::{Ulid, UlidDecodeError, UlidParseError};

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    types::GenerateKey,
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable, VisitorContext,
    },
};

impl EntityKeyBytes for Ulid {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.to_bytes());
        Ok(())
    }
}

impl RuntimeValueMeta for Ulid {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Ulid {
    fn to_value(&self) -> Value {
        Value::Ulid(*self)
    }
}

impl RuntimeValueDecode for Ulid {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Ulid(value) => Some(*value),
            _ => None,
        }
    }
}

impl GenerateKey for Ulid {
    fn generate() -> Self {
        generator::generate().expect(
            "ULID generation requires initialized randomness and non-overflowing monotonic state",
        )
    }
}

impl SanitizeAuto for Ulid {}

impl SanitizeCustom for Ulid {}

impl ValidateAuto for Ulid {
    fn validate_self(&self, context: &mut dyn VisitorContext) {
        if *self == Self::nil() {
            context.issue("ulid must not be nil");
        }
    }
}

impl ValidateCustom for Ulid {}

impl Visitable for Ulid {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}
