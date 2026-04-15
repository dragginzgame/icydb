//! Module: codec::cbor
//! Responsibility: DB-owned CBOR compatibility helpers for the remaining
//! runtime seams that still lack schema-owned binary codecs.
//! Does not own: payload policy, storage limits, or error taxonomy mapping.
//! Boundary: runtime DB modules call into this file instead of importing
//! `serde_cbor` directly.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_cbor::from_slice;
use serde_cbor::to_vec;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Encode one runtime payload into CBOR bytes.
pub(in crate::db) fn encode_cbor_bytes<T>(value: &T) -> Result<Vec<u8>, String>
where
    T: Serialize,
{
    to_vec(value).map_err(|err| err.to_string())
}

/// Decode one runtime payload from CBOR bytes without allowing panics to
/// unwind through the DB runtime.
pub(in crate::db) fn decode_cbor_bytes<T>(bytes: &[u8]) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let result = catch_unwind(AssertUnwindSafe(|| from_slice(bytes)));

    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err("panic during CBOR deserialization".into()),
    }
}
