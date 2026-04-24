//! Module: db::codec::hex
//! Responsibility: shared lowercase hexadecimal byte formatting.
//! Does not own: domain-specific token validation or decode error taxonomy.
//! Boundary: pure byte-to-text primitives reused by DB wire/display surfaces.

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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::codec::hex::encode_hex_lower;

    #[test]
    fn encode_hex_lower_formats_bytes_without_prefix() {
        assert_eq!(encode_hex_lower(&[0x00, 0x01, 0x0a, 0xff]), "00010aff");
    }

    #[test]
    fn encode_hex_lower_handles_empty_input() {
        assert_eq!(encode_hex_lower(&[]), "");
    }
}
