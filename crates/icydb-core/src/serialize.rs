use crate::runtime_error::{ErrorClass, ErrorOrigin, RuntimeError};
use canic_memory::serialize::{deserialize as canic_deserialize, serialize as canic_serialize};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error as ThisError;

///
/// SerializeError
///

#[derive(Debug, ThisError)]
pub enum SerializeError {
    #[error(transparent)]
    SerializeError(#[from] canic_memory::serialize::SerializeError),
}

impl SerializeError {
    pub(crate) const fn class(&self) -> ErrorClass {
        match self {
            Self::SerializeError(_) => ErrorClass::Internal,
        }
    }
}

impl From<SerializeError> for RuntimeError {
    fn from(err: SerializeError) -> Self {
        Self::new(err.class(), ErrorOrigin::Serialize, err.to_string())
    }
}

/// Serialize a value using the default `canic` serializer.
///
/// This helper keeps the error type aligned with the rest of `icydb`.
pub fn serialize<T>(ty: &T) -> Result<Vec<u8>, RuntimeError>
where
    T: Serialize,
{
    canic_serialize(ty)
        .map_err(SerializeError::from)
        .map_err(RuntimeError::from)
}

/// Deserialize a value produced by [`serialize`].
pub fn deserialize<T>(bytes: &[u8]) -> Result<T, RuntimeError>
where
    T: DeserializeOwned,
{
    canic_deserialize(bytes)
        .map_err(SerializeError::from)
        .map_err(RuntimeError::from)
}
