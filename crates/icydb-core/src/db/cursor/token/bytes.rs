//! Module: cursor::token::bytes
//! Responsibility: primitive writers for cursor token wire encoding.
//! Does not own: token envelope structure, value tags, or continuation semantics.
//! Boundary: token codec orchestration -> checked byte writes.

use crate::db::cursor::token::TokenWireError;

pub(in crate::db::cursor::token) fn checked_len_u32(len: usize) -> Result<u32, TokenWireError> {
    u32::try_from(len).map_err(|_| TokenWireError::encode())
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
