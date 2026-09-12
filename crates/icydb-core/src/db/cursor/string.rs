//! Module: cursor::string
//! Responsibility: external continuation cursor token string formatting.
//! Does not own: binary token wire encoding or continuation validation semantics.
//! Boundary: cursor-owned binary token bytes -> unpadded URL-safe Base64 external token text.

use crate::db::cursor::token::MAX_CURSOR_TOKEN_BYTES;
#[cfg(test)]
use crate::db::cursor::{GroupedContinuationToken, TokenWireError};
use base64::{DecodeError, Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

// Unpadded Base64 needs ceil(4N/3) symbols for N binary bytes.
const MAX_CURSOR_TOKEN_TEXT_LEN: usize = (MAX_CURSOR_TOKEN_BYTES * 4).div_ceil(3);

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

/// Encode one grouped continuation token as an external cursor token string.
#[cfg(test)]
pub(in crate::db) fn encode_grouped_cursor_token(
    token: &GroupedContinuationToken,
) -> Result<String, TokenWireError> {
    token
        .encode()
        .map(|encoded| encode_cursor(encoded.as_slice()))
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
        DecodeError::InvalidByte(position, _) | DecodeError::InvalidLastSymbol(position, _) => {
            CursorDecodeError::InvalidBase64 { position }
        }
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_text_round_trips_canonical_vectors_and_whitespace() {
        for (raw, encoded) in [
            (b"f".as_slice(), "Zg"),
            (b"fo".as_slice(), "Zm8"),
            (b"foo".as_slice(), "Zm9v"),
            ([0xfb, 0xff].as_slice(), "-_8"),
            ([0x00, 0x01, 0x0a, 0xff].as_slice(), "AAEK_w"),
        ] {
            assert_eq!(encode_cursor(raw), encoded);
            assert_eq!(decode_cursor(&format!("  {encoded} \n")).unwrap(), raw);
        }
    }

    #[test]
    fn cursor_text_rejects_empty_invalid_length_alphabet_and_trailing_bits() {
        for text in ["", " \n\t"] {
            assert_eq!(decode_cursor(text), Err(CursorDecodeError::Empty));
        }
        assert_eq!(decode_cursor("A"), Err(CursorDecodeError::InvalidLength));
        for text in ["A!", "A/", "A+", "AB", "Aé"] {
            assert_eq!(
                decode_cursor(text),
                Err(CursorDecodeError::InvalidBase64 { position: 1 })
            );
        }
        for text in ["AA=", "AA==", "AA A"] {
            assert!(decode_cursor(text).is_err());
        }
    }

    #[test]
    fn cursor_text_enforces_binary_budget_for_all_final_symbol_lengths() {
        for len in [
            1,
            2,
            3,
            MAX_CURSOR_TOKEN_BYTES - 2,
            MAX_CURSOR_TOKEN_BYTES - 1,
            MAX_CURSOR_TOKEN_BYTES,
        ] {
            let raw = vec![0xff; len];
            let text = encode_cursor(&raw);
            assert_eq!(text.len(), (len * 4).div_ceil(3));
            assert_eq!(decode_cursor(&text).unwrap(), raw);
        }
        let rejected = encode_cursor(&vec![0; MAX_CURSOR_TOKEN_BYTES + 1]);
        assert_eq!(
            decode_cursor(&rejected),
            Err(CursorDecodeError::TooLong {
                len: rejected.len(),
                max: MAX_CURSOR_TOKEN_TEXT_LEN,
            })
        );
    }
}
