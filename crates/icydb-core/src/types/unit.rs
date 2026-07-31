//! Engine operations for the canonical schema-owned `Unit` atom.

pub use icydb_schema::Unit;

use crate::db::{
    EntityKeyBytes, EntityKeyBytesError, PrimaryKeyComponent, PrimaryKeyDecode, PrimaryKeyEncode,
    PrimaryKeyEncodeError, PrimaryKeyValue, validate_entity_key_bytes_buffer,
};

impl EntityKeyBytes for Unit {
    const BYTE_LEN: usize = 0;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)
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
