use crate::Error;
use canic::serialize::{deserialize as canic_deserialize, serialize as canic_serialize};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error as ThisError;

/// Serialization errors surfaced through the `icydb` convenience helpers.
#[derive(Debug, ThisError)]
pub enum SerializeError {
    #[error(transparent)]
    SerializeError(#[from] canic::Error),
}

/// Serialize a value using the default `canic` serializer.
///
/// This helper keeps the error type aligned with the rest of `icydb`.
pub fn serialize<T>(ty: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    canic_serialize(ty)
        .map_err(SerializeError::from)
        .map_err(Error::from)
}

/// Deserialize a value produced by [`serialize`].
pub fn deserialize<T>(bytes: &[u8]) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    canic_deserialize(bytes)
        .map_err(SerializeError::from)
        .map_err(Error::from)
}
