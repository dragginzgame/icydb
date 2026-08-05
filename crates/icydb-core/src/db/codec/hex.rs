//! Module: db::codec::hex
//! Responsibility: shared bounded hexadecimal byte encoding and decoding.
//! Does not own: domain-specific token validation or decode error taxonomy.
//! Boundary: pure byte/text primitives reused by DB wire and public-literal surfaces.

/// Decode hexadecimal text while bounding the decoded byte count before allocation.
#[must_use]
pub(in crate::db) fn decode_hex_bounded(input: &str, max_bytes: usize) -> Option<Vec<u8>> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }

    let decoded_len = bytes.len() / 2;
    if decoded_len > max_bytes {
        return None;
    }

    let mut decoded = Vec::with_capacity(decoded_len);
    for pair in bytes.chunks_exact(2) {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        decoded.push((high << 4) | low);
    }
    Some(decoded)
}

/// Encode bytes as lowercase hexadecimal text.
#[must_use]
pub fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    // Keep common hex emission allocation-bounded and formatting-free.
    // Formatting each byte with `"{byte:02x}"` is equivalent on the wire, but
    // it routes through the formatter for every byte on hot cursor/fingerprint
    // display paths.
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));

    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
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
    use crate::db::codec::hex::{decode_hex_bounded, encode_hex_lower};

    #[test]
    fn decode_hex_bounded_accepts_exact_mixed_case_payloads() {
        assert_eq!(
            decode_hex_bounded("00010aFF", 4),
            Some(vec![0x00, 0x01, 0x0a, 0xff]),
        );
    }

    #[test]
    fn decode_hex_bounded_rejects_malformed_and_oversized_payloads() {
        assert_eq!(decode_hex_bounded("0", 1), None);
        assert_eq!(decode_hex_bounded("0g", 1), None);
        assert_eq!(decode_hex_bounded("0001", 1), None);
    }

    #[test]
    fn encode_hex_lower_formats_bytes_without_prefix() {
        assert_eq!(encode_hex_lower(&[0x00, 0x01, 0x0a, 0xff]), "00010aff");
    }

    #[test]
    fn encode_hex_lower_handles_empty_input() {
        assert_eq!(encode_hex_lower(&[]), "");
    }
}
