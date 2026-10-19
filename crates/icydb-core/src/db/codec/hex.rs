//! Module: db::codec::hex
//! Responsibility: bounded hexadecimal byte decoding.
//! Does not own: domain-specific token validation or decode error taxonomy.
//! Boundary: pure byte/text primitives reused by DB wire and public-literal surfaces.

/// Decode hexadecimal text while bounding the decoded byte count before allocation.
#[must_use]
pub(in crate::db) fn decode_hex_bounded(input: &[u8], max_bytes: usize) -> Option<Vec<u8>> {
    let bytes = input;
    if !bytes.len().is_multiple_of(2) {
        return None;
    }

    let decoded_len = bytes.len() / 2;
    if decoded_len > max_bytes {
        return None;
    }

    let mut decoded = Vec::with_capacity(decoded_len);
    for pair in bytes.as_chunks::<2>().0 {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        decoded.push((high << 4) | low);
    }
    Some(decoded)
}

/// Decode one ASCII hexadecimal digit without domain-specific admission.
pub(in crate::db) const fn decode_hex_nibble(byte: u8) -> Option<u8> {
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
    use crate::{codec::hex::encode_hex_lower, db::codec::hex::decode_hex_bounded};

    #[test]
    fn decode_hex_bounded_accepts_exact_mixed_case_payloads() {
        assert_eq!(
            decode_hex_bounded(b"00010aFF", 4),
            Some(vec![0x00, 0x01, 0x0a, 0xff]),
        );
    }

    #[test]
    fn decode_hex_bounded_rejects_malformed_and_oversized_payloads() {
        assert_eq!(decode_hex_bounded(b"0", 1), None);
        assert_eq!(decode_hex_bounded(b"0g", 1), None);
        assert_eq!(decode_hex_bounded(b"0001", 1), None);
    }

    #[test]
    fn byte_hex_codec_round_trips_every_byte_and_rejects_nonascii() {
        let bytes: Vec<_> = (0..=u8::MAX).collect();
        let hex = encode_hex_lower(&bytes);
        assert_eq!(
            decode_hex_bounded(hex.as_bytes(), bytes.len()),
            Some(bytes.clone())
        );
        assert_eq!(
            decode_hex_bounded(hex.to_uppercase().as_bytes(), bytes.len()),
            Some(bytes)
        );
        assert_eq!(decode_hex_bounded(&[0xff, b'0'], 1), None);
        assert_eq!(decode_hex_bounded("é".as_bytes(), 1), None);
    }
}
