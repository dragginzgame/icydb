mod cbor;

use crate::error::{ErrorClass, ErrorOrigin, InternalError};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error as ThisError;

///
/// SerializeError
///

#[derive(Debug, ThisError)]
pub enum SerializeError {
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

impl SerializeError {
    pub(crate) const fn class() -> ErrorClass {
        ErrorClass::Internal
    }
}

impl From<SerializeError> for InternalError {
    fn from(err: SerializeError) -> Self {
        Self::new(
            SerializeError::class(),
            ErrorOrigin::Serialize,
            err.to_string(),
        )
    }
}
/// Serialize a value using the default `canic` serializer.
///
/// This helper keeps the error type aligned with the rest of `icydb`.
pub fn serialize<T>(ty: &T) -> Result<Vec<u8>, SerializeError>
where
    T: Serialize,
{
    cbor::serialize(ty)
}

/// Deserialize a value produced by [`serialize`].
pub fn deserialize<T>(bytes: &[u8]) -> Result<T, SerializeError>
where
    T: DeserializeOwned,
{
    cbor::deserialize(bytes)
}
