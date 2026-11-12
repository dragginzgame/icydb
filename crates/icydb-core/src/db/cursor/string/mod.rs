//! Module: cursor::string
//! Responsibility: external continuation cursor token string formatting.
//! Does not own: binary token wire encoding or continuation validation semantics.
//! Boundary: cursor-owned binary token bytes -> unpadded URL-safe Base64 external token text.

use crate::db::cursor::token::MAX_CURSOR_TOKEN_BYTES;
#[cfg(test)]
use crate::db::cursor::{GroupedContinuationToken, TokenWireError};
use base64::{DecodeError, Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

// Unpadded Base64 needs ceil(4N/3) symbols for N binary bytes.
const MAX_CURSOR_TOKEN_TEXT_LEN: usize = encoded_cursor_len(MAX_CURSOR_TOKEN_BYTES);

/// Return the unpadded Base64 length, conservatively saturating on overflow.
#[must_use]
pub(in crate::db) const fn encoded_cursor_len(bytes: usize) -> usize {
    match base64::encoded_len(bytes, false) {
        Some(len) => len,
        None => usize::MAX,
    }
}

///
/// CursorDecodeError
///
/// External continuation cursor string decode failures.
///

#[derive(Debug, Eq, PartialEq)]
pub enum CursorDecodeError {
    Empty,

    TooLong { len: usize, max: usize },

    InvalidLength,

    InvalidBase64 { position: usize },
}

/// Encode raw cursor bytes as an unpadded URL-safe Base64 token.
#[must_use]
pub(in crate::db) fn encode_cursor(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Decode a canonical unpadded URL-safe Base64 token into raw bytes.
///
/// The token may include surrounding whitespace, which is trimmed.
pub(in crate::db) fn decode_cursor(token: &str) -> Result<Vec<u8>, CursorDecodeError> {
    // Phase 1: normalize input and enforce envelope-level bounds.
    let token = token.trim();

    if token.is_empty() {
        return Err(CursorDecodeError::Empty);
    }

    if token.len() > MAX_CURSOR_TOKEN_TEXT_LEN {
        return Err(CursorDecodeError::TooLong {
            len: token.len(),
            max: MAX_CURSOR_TOKEN_TEXT_LEN,
        });
    }

    // The text ceiling bounds allocation and implies at most the binary budget.
    // The engine rejects padding and nonzero unused bits in the final symbol.
    URL_SAFE_NO_PAD.decode(token).map_err(|error| match error {
        DecodeError::InvalidLength(_) | DecodeError::InvalidPadding => {
            CursorDecodeError::InvalidLength
        }
        DecodeError::InvalidByte(position, _)
        | DecodeError::InvalidLastSymbol {
            offset: position, ..
        } => CursorDecodeError::InvalidBase64 { position },
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::encode_grouped_cursor_token;
