//! Engine operations for the canonical schema-owned `Ulid` atom.

mod generator;
#[cfg(test)]
mod tests;

pub use icydb_schema::{Ulid, UlidDecodeError, UlidParseError};

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    types::GenerateKey,
};

impl EntityKeyBytes for Ulid {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.to_bytes());
        Ok(())
    }
}

impl GenerateKey for Ulid {
    fn generate() -> Result<Self, crate::error::InternalError> {
        generator::generate().map_err(|_| crate::error::InternalError::executor_internal())
    }
}
