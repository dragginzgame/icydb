///
/// Cursor codec helpers.
///
/// This module owns the opaque wire-token format used for continuation cursors.
/// It intentionally contains only token encoding/decoding logic and no query semantics.
///

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
/// Returns a descriptive error string for invalid tokens.
pub fn decode_cursor(token: &str) -> Result<Vec<u8>, String> {
    let token = token.trim();
    if token.is_empty() {
        return Err("cursor token is empty".to_string());
    }
    if !token.len().is_multiple_of(2) {
        return Err("cursor token must have an even number of hex characters".to_string());
    }

    let mut out = Vec::with_capacity(token.len() / 2);
    let bytes = token.as_bytes();
    for idx in (0..bytes.len()).step_by(2) {
        let hi = decode_hex_nibble(bytes[idx])
            .ok_or_else(|| format!("invalid hex character at position {}", idx + 1))?;
        let lo = decode_hex_nibble(bytes[idx + 1])
            .ok_or_else(|| format!("invalid hex character at position {}", idx + 2))?;
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
