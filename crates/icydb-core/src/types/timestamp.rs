//! Engine operations for the canonical schema-owned `Timestamp` atom.

pub use icydb_schema::Timestamp;

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    runtime::now_millis,
    traits::Repr,
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

#[cfg(test)]
mod tests;
