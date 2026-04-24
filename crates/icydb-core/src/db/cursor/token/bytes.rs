//! Module: cursor::token::bytes
//! Responsibility: byte-cursor primitives for cursor token wire encoding.
//! Does not own: token envelope structure, value tags, or continuation semantics.
//! Boundary: token codec orchestration -> bounded byte reads/writes.

use crate::db::cursor::token::TokenWireError;
use std::str;

///
/// ByteCursor
///
/// ByteCursor is the bounded decode reader for the token-owned binary wire
/// format. It never panics on malformed input and reports every truncation or
/// type mismatch through `TokenWireError`.
///

pub(in crate::db::cursor::token) struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    // Start one bounded decode cursor over the provided byte slice.
    pub(in crate::db::cursor::token) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    // Return the remaining unread byte count.
    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    // Read one exact byte window, advancing only on success.
    pub(in crate::db::cursor::token) fn read_exact(
        &mut self,
        len: usize,
    ) -> Result<&'a [u8], TokenWireError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| TokenWireError::decode("cursor token length overflow"))?;

        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(TokenWireError::decode(format!(
                "cursor token truncated: needed {len} bytes with {} remaining",
                self.remaining()
            )));
        };

        self.offset = end;
        Ok(slice)
    }

    // Read one fixed-width primitive through an exact byte window.
    pub(in crate::db::cursor::token) fn read_array<const N: usize>(
        &mut self,
    ) -> Result<[u8; N], TokenWireError> {
        let bytes = self.read_exact(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    // Read one tagged byte.
    pub(in crate::db::cursor::token) fn read_u8(&mut self) -> Result<u8, TokenWireError> {
        Ok(self.read_exact(1)?[0])
    }

    // Read one big-endian u32.
    pub(in crate::db::cursor::token) fn read_u32(&mut self) -> Result<u32, TokenWireError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian u64.
    pub(in crate::db::cursor::token) fn read_u64(&mut self) -> Result<u64, TokenWireError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian i64.
    pub(in crate::db::cursor::token) fn read_i64(&mut self) -> Result<i64, TokenWireError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian i128.
    pub(in crate::db::cursor::token) fn read_i128(&mut self) -> Result<i128, TokenWireError> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian u128.
    pub(in crate::db::cursor::token) fn read_u128(&mut self) -> Result<u128, TokenWireError> {
        Ok(u128::from_be_bytes(self.read_array()?))
    }

    // Read one length-prefixed byte payload.
    pub(in crate::db::cursor::token) fn read_len_prefixed_bytes(
        &mut self,
    ) -> Result<&'a [u8], TokenWireError> {
        let len = usize::try_from(self.read_u32()?)
            .map_err(|_| TokenWireError::decode("cursor token length does not fit usize"))?;

        self.read_exact(len)
    }

    // Read one UTF-8 string from a length-prefixed byte payload.
    pub(in crate::db::cursor::token) fn read_string(&mut self) -> Result<String, TokenWireError> {
        let bytes = self.read_len_prefixed_bytes()?;
        let text = str::from_utf8(bytes)
            .map_err(|err| TokenWireError::decode(format!("cursor token invalid utf-8: {err}")))?;

        Ok(text.to_string())
    }

    // Require full cursor consumption at the end of decode.
    pub(in crate::db::cursor::token) fn finish(self) -> Result<(), TokenWireError> {
        if self.remaining() == 0 {
            return Ok(());
        }

        Err(TokenWireError::decode(format!(
            "cursor token has {} trailing bytes",
            self.remaining()
        )))
    }
}

pub(in crate::db::cursor::token) fn checked_len_u32(len: usize) -> Result<u32, TokenWireError> {
    u32::try_from(len)
        .map_err(|_| TokenWireError::encode("cursor token payload exceeds u32 length"))
}

pub(in crate::db::cursor::token) fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db::cursor::token) fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db::cursor::token) fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db::cursor::token) fn write_i128(out: &mut Vec<u8>, value: i128) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db::cursor::token) fn write_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db::cursor::token) fn write_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), TokenWireError> {
    write_u32(out, checked_len_u32(bytes.len())?);
    out.extend_from_slice(bytes);
    Ok(())
}

pub(in crate::db::cursor::token) fn write_string(
    out: &mut Vec<u8>,
    value: &str,
) -> Result<(), TokenWireError> {
    write_len_prefixed_bytes(out, value.as_bytes())
}
