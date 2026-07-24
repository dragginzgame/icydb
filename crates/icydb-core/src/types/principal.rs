//! Engine operations for the canonical schema-owned `Principal` atom.

pub use icydb_schema::{Principal, PrincipalDecodeError, PrincipalEncodeError, PrincipalError};

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

impl EntityKeyBytes for Principal {
    const BYTE_LEN: usize = 1 + Self::MAX_LENGTH_IN_BYTES as usize;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.fill(0);

        let principal =
            self.stored_bytes()
                .map_err(|PrincipalEncodeError::TooLarge { len, max }| {
                    EntityKeyBytesError::ValueTooLong { len, max }
                })?;
        let len = principal.len();
        let (tag, payload) = out
            .split_first_mut()
            .ok_or(EntityKeyBytesError::BufferLength {
                expected: Self::BYTE_LEN,
                actual: 0,
            })?;
        *tag = u8::try_from(len).map_err(|_| EntityKeyBytesError::ValueTooLong {
            len,
            max: usize::from(u8::MAX),
        })?;
        let max = payload.len();
        let payload = payload
            .get_mut(..len)
            .ok_or(EntityKeyBytesError::ValueTooLong { len, max })?;
        payload.copy_from_slice(principal);

        Ok(())
    }
}

impl RuntimeValueMeta for Principal {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Principal {
    fn to_value(&self) -> Value {
        Value::Principal(*self)
    }
}

impl RuntimeValueDecode for Principal {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Principal(value) => Some(*value),
            _ => None,
        }
    }
}

impl SanitizeAuto for Principal {}

impl SanitizeCustom for Principal {}

impl ValidateAuto for Principal {}

impl ValidateCustom for Principal {}

impl Visitable for Principal {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}
