mod cbor;

use crate::error::{ErrorClass, ErrorOrigin, InternalError};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error as ThisError;

/// Generic CBOR serialization infrastructure.
///
/// This module is format-level only:
/// - No database-layer constants or policy limits are defined here.
/// - Callers that need bounded decode must pass explicit limits.
/// - Engine-specific decode policy belongs in subsystem wrappers (for example, `db::codec`).

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

impl From<SerializeError> for InternalError {
    fn from(err: SerializeError) -> Self {
        Self::new(
            ErrorClass::Internal,
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

/// Deserialize a value produced by [`serialize`], with an explicit size limit.
///
/// Size limits are caller policy, not serialization-format policy.
pub fn deserialize_bounded<T>(bytes: &[u8], max_bytes: usize) -> Result<T, SerializeError>
where
    T: DeserializeOwned,
{
    cbor::deserialize_bounded(bytes, max_bytes)
}
