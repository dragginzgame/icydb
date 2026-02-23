///
/// Cursor codec helpers.
///
/// This module owns the opaque wire-token format used for continuation cursors.
/// It intentionally contains only token encoding/decoding logic and no query semantics.
///

// Defensive decode bound for untrusted cursor token input.
const MAX_CURSOR_TOKEN_HEX_LEN: usize = 8 * 1024;

///
/// CursorDecodeError
///

#[derive(Debug, Eq, thiserror::Error, PartialEq)]
pub enum CursorDecodeError {
    #[error("cursor token is empty")]
    Empty,

    #[error("cursor token exceeds max length: {len} hex chars (max {max})")]
    TooLong { len: usize, max: usize },

    #[error("cursor token must have an even number of hex characters")]
    OddLength,

    #[error("invalid hex character at position {position}")]
    InvalidHex { position: usize },
}

/// Encode raw cursor bytes as a lowercase hex token.
#[must_use]
pub fn encode_cursor(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Decode a lowercase/uppercase hex cursor token into raw bytes.
///
/// The token may include surrounding whitespace, which is trimmed.
pub fn decode_cursor(token: &str) -> Result<Vec<u8>, CursorDecodeError> {
    let token = token.trim();

    if token.is_empty() {
        return Err(CursorDecodeError::Empty);
    }

    if token.len() > MAX_CURSOR_TOKEN_HEX_LEN {
        return Err(CursorDecodeError::TooLong {
            len: token.len(),
            max: MAX_CURSOR_TOKEN_HEX_LEN,
        });
    }

    if !token.len().is_multiple_of(2) {
        return Err(CursorDecodeError::OddLength);
    }

    let mut out = Vec::with_capacity(token.len() / 2);
    let bytes = token.as_bytes();

    for idx in (0..bytes.len()).step_by(2) {
        let hi = decode_hex_nibble(bytes[idx])
            .ok_or(CursorDecodeError::InvalidHex { position: idx + 1 })?;

        let lo = decode_hex_nibble(bytes[idx + 1])
            .ok_or(CursorDecodeError::InvalidHex { position: idx + 2 })?;

        out.push((hi << 4) | lo);
    }

    Ok(out)
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{CursorDecodeError, MAX_CURSOR_TOKEN_HEX_LEN, decode_cursor, encode_cursor};

    #[test]
    fn decode_cursor_rejects_empty_and_whitespace_tokens() {
        let err = decode_cursor("").expect_err("empty token should be rejected");
        assert_eq!(err, CursorDecodeError::Empty);

        let err = decode_cursor("   \n\t").expect_err("whitespace token should be rejected");
        assert_eq!(err, CursorDecodeError::Empty);
    }

    #[test]
    fn decode_cursor_rejects_odd_length_tokens() {
        let err = decode_cursor("abc").expect_err("odd-length token should be rejected");
        assert_eq!(err, CursorDecodeError::OddLength);
    }

    #[test]
    fn decode_cursor_enforces_max_token_length() {
        let accepted = "aa".repeat(MAX_CURSOR_TOKEN_HEX_LEN / 2);
        let accepted_bytes = decode_cursor(&accepted).expect("max-sized token should decode");
        assert_eq!(accepted_bytes.len(), MAX_CURSOR_TOKEN_HEX_LEN / 2);

        let rejected = format!("{accepted}aa");
        let err = decode_cursor(&rejected).expect_err("oversized token should be rejected");
        assert_eq!(
            err,
            CursorDecodeError::TooLong {
                len: MAX_CURSOR_TOKEN_HEX_LEN + 2,
                max: MAX_CURSOR_TOKEN_HEX_LEN
            }
        );
    }

    #[test]
    fn decode_cursor_rejects_invalid_hex_with_position() {
        let err = decode_cursor("0x").expect_err("invalid hex nibble should be rejected");
        assert_eq!(err, CursorDecodeError::InvalidHex { position: 2 });
    }

    #[test]
    fn decode_cursor_accepts_mixed_case_and_surrounding_whitespace() {
        let bytes = decode_cursor("  0aFf10  ").expect("mixed-case hex token should decode");
        assert_eq!(bytes, vec![0x0a, 0xff, 0x10]);
    }

    #[test]
    fn encode_decode_cursor_round_trip_is_stable() {
        let raw = vec![0x00, 0x01, 0x0a, 0xff];
        let encoded = encode_cursor(&raw);
        assert_eq!(encoded, "00010aff");

        let decoded = decode_cursor(&encoded).expect("encoded token should decode");
        assert_eq!(decoded, raw);
    }
}
