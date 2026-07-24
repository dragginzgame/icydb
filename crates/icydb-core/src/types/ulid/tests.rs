//! Module: types::ulid::tests
//! Covers ULID encoding, decoding, and generation invariants.

use super::*;
use crate::types::GenerateKey;

#[test]
fn ulid_max_size_is_bounded() {
    let ulid = Ulid::from_bytes([0xFF; 16]);
    let size = ulid.to_bytes().len();

    assert!(
        size <= Ulid::STORED_SIZE as usize,
        "serialized Ulid too large: got {size} bytes (limit {})",
        Ulid::STORED_SIZE
    );
}

#[test]
fn increment_preserves_none_on_randomness_overflow() {
    let ulid = ulid::Ulid::from_parts(1, (1_u128 << 80) - 1);

    assert!(ulid.increment().is_err());
}

#[test]
fn test_ulid_string_roundtrip() {
    let u1 = Ulid::generate();
    let u2 = u1.to_string().parse::<Ulid>().unwrap();

    assert_eq!(u1, u2);
}

#[test]
fn invalid_ulid_string_returns_parse_error() {
    let err = "not-a-ulid"
        .parse::<Ulid>()
        .expect_err("invalid ULID text should fail parsing");

    assert!(matches!(err, UlidParseError::InvalidString));
}

#[test]
fn ulid_bytes_roundtrip() {
    let ulid = Ulid::generate();
    let bytes = ulid.to_bytes();
    let decoded = Ulid::from_bytes(bytes);
    assert_eq!(ulid, decoded);
}

#[test]
fn ulid_debug_renders_canonical_string() {
    let ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        .parse::<Ulid>()
        .expect("fixture ULID should parse successfully");
    let debug = format!("{ulid:?}");

    assert_eq!(
        debug, "\"01ARZ3NDEKTSV4RRFFQ69G5FAV\"",
        "ULID debug should render the canonical string form",
    );
}
