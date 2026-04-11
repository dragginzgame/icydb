//! Module: serialize
//!
//! Responsibility: generic serialization and bounded-deserialization helpers.
//! Does not own: database-specific payload policy or storage limits.
//! Boundary: format-level helper layer reused by db codecs and tests.

mod cbor;

use serde::{Serialize, de::DeserializeOwned};
use std::fmt;
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

    #[error("deserialize size limit exceeded: {len} bytes (limit {max_bytes})")]
    DeserializeSizeLimitExceeded { len: usize, max_bytes: usize },
}

///
/// SerializeErrorKind
///
/// Stable error-kind taxonomy for serializer failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SerializeErrorKind {
    Serialize,
    Deserialize,
    DeserializeSizeLimitExceeded,
}

impl SerializeErrorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Serialize => "serialize",
            Self::Deserialize => "deserialize",
            Self::DeserializeSizeLimitExceeded => "deserialize_size_limit_exceeded",
        }
    }
}

impl fmt::Display for SerializeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SerializeError {
    /// Return a stable error kind independent of backend error-message text.
    #[must_use]
    pub const fn kind(&self) -> SerializeErrorKind {
        match self {
            Self::Serialize(_) => SerializeErrorKind::Serialize,
            Self::Deserialize(_) => SerializeErrorKind::Deserialize,
            Self::DeserializeSizeLimitExceeded { .. } => {
                SerializeErrorKind::DeserializeSizeLimitExceeded
            }
        }
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

/// Serialize a value using the default `canic` serializer and stream the
/// encoded bytes directly into one lowercase hex string.
pub fn serialize_hex<T>(ty: &T) -> Result<String, SerializeError>
where
    T: Serialize,
{
    cbor::serialize_hex(ty)
}

/// Return serialized byte length using the default `canic` serializer without
/// allocating an output buffer.
pub fn serialized_len<T>(ty: &T) -> Result<usize, SerializeError>
where
    T: Serialize,
{
    cbor::serialize_len(ty)
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
