//! Module: serialize::cbor
//! Responsibility: module-local ownership and contracts for serialize::cbor.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::serialize::SerializeError;
use serde::{Serialize, de::DeserializeOwned};
use serde_cbor::{from_slice, to_vec, to_writer};
use std::{
    io,
    panic::{AssertUnwindSafe, catch_unwind},
};

///
/// ByteCountWriter
///
/// Minimal `io::Write` sink that counts emitted bytes without allocation.
///

#[derive(Default)]
struct ByteCountWriter {
    len: usize,
}

impl ByteCountWriter {
    #[must_use]
    const fn into_len(self) -> usize {
        self.len
    }
}

///
/// HexStringWriter
///
/// Streaming `io::Write` sink that hex-encodes emitted bytes directly into
/// one owned string.
///

struct HexStringWriter {
    out: String,
}

impl HexStringWriter {
    #[must_use]
    fn with_byte_capacity(byte_len: usize) -> Self {
        Self {
            out: String::with_capacity(byte_len.saturating_mul(2)),
        }
    }

    #[must_use]
    fn into_string(self) -> String {
        self.out
    }
}

impl io::Write for ByteCountWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.len = self.len.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Write for HexStringWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        for byte in buf {
            self.out.push(HEX[(byte >> 4) as usize] as char);
            self.out.push(HEX[(byte & 0x0f) as usize] as char);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Serialize a value into CBOR bytes.
pub(super) fn serialize<T>(t: &T) -> Result<Vec<u8>, SerializeError>
where
    T: Serialize,
{
    to_vec(t).map_err(|e| SerializeError::Serialize(e.to_string()))
}

/// Serialize a value to CBOR while streaming the encoded bytes into one hex string.
pub(super) fn serialize_hex<T>(t: &T) -> Result<String, SerializeError>
where
    T: Serialize,
{
    let byte_len = serialize_len(t)?;
    let mut writer = HexStringWriter::with_byte_capacity(byte_len);
    to_writer(&mut writer, t).map_err(|e| SerializeError::Serialize(e.to_string()))?;

    Ok(writer.into_string())
}

/// Serialize a value to CBOR and return encoded byte length without allocating.
pub(super) fn serialize_len<T>(t: &T) -> Result<usize, SerializeError>
where
    T: Serialize,
{
    let mut writer = ByteCountWriter::default();
    to_writer(&mut writer, t).map_err(|e| SerializeError::Serialize(e.to_string()))?;

    Ok(writer.into_len())
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
        return Err(SerializeError::DeserializeSizeLimitExceeded {
            len: bytes.len(),
            max_bytes,
        });
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
