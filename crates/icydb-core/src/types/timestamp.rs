//! Engine operations for the canonical schema-owned `Timestamp` atom.

pub use icydb_schema::Timestamp;

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    runtime::now_millis,
    traits::Repr,
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

/// Runtime clock access for the engine-neutral timestamp atom.
pub trait CurrentTimestamp {
    /// Read the current wall clock in Unix milliseconds.
    #[must_use]
    fn now() -> Self;
}

impl CurrentTimestamp for Timestamp {
    fn now() -> Self {
        i64::try_from(now_millis()).map_or(Self::MAX, Self::from_millis)
    }
}

impl Repr for Timestamp {
    type Inner = i64;

    fn repr(&self) -> Self::Inner {
        self.as_millis()
    }

    fn from_repr(inner: Self::Inner) -> Self {
        Self::from_millis(inner)
    }
}

impl EntityKeyBytes for Timestamp {
    const BYTE_LEN: usize = 8;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.as_millis().to_be_bytes());
        Ok(())
    }
}

impl RuntimeValueMeta for Timestamp {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Timestamp {
    fn to_value(&self) -> Value {
        Value::Timestamp(*self)
    }
}

impl RuntimeValueDecode for Timestamp {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Timestamp(value) => Some(*value),
            _ => None,
        }
    }
}

impl SanitizeAuto for Timestamp {}

impl SanitizeCustom for Timestamp {}

impl ValidateAuto for Timestamp {}

impl ValidateCustom for Timestamp {}

impl Visitable for Timestamp {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}

#[cfg(test)]
mod tests;
