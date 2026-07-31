//! Engine operations for the canonical schema-owned `Subaccount` atom.

pub use icydb_schema::Subaccount;

use crate::db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer};

impl EntityKeyBytes for Subaccount {
    const BYTE_LEN: usize = 32;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.to_bytes());
        Ok(())
    }
}
