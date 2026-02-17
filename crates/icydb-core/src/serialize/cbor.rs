use crate::serialize::SerializeError;
use serde::{Serialize, de::DeserializeOwned};
use serde_cbor::{from_slice, to_vec};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Serialize a value into CBOR bytes.
pub(super) fn serialize<T>(t: &T) -> Result<Vec<u8>, SerializeError>
where
    T: Serialize,
{
    to_vec(t).map_err(|e| SerializeError::Serialize(e.to_string()))
}

/// Deserialize CBOR bytes into a value without a size limit.
pub(super) fn deserialize<T>(bytes: &[u8]) -> Result<T, SerializeError>
where
    T: DeserializeOwned,
{
    deserialize_bounded(bytes, usize::MAX)
}

/// Deserialize CBOR bytes into a value with a caller-provided size limit.
///
/// Safety guarantees:
/// - Input size is bounded before decode.
/// - Any panic during decode is caught and reported as a deserialize error.
/// - No panic escapes this function.
pub(super) fn deserialize_bounded<T>(bytes: &[u8], max_bytes: usize) -> Result<T, SerializeError>
where
    T: DeserializeOwned,
{
    if bytes.len() > max_bytes {
        return Err(SerializeError::Deserialize(format!(
            "payload exceeds maximum allowed size: {} bytes (limit {max_bytes})",
            bytes.len()
        )));
    }

    let result = catch_unwind(AssertUnwindSafe(|| from_slice(bytes)));

    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(SerializeError::Deserialize(err.to_string())),
        Err(_) => Err(SerializeError::Deserialize(
            "panic during CBOR deserialization".into(),
        )),
    }
}
