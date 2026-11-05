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
