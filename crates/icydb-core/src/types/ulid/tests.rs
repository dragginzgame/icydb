//! Module: types::ulid::tests
//! Covers ULID encoding, decoding, and generation invariants.

use super::*;

#[test]
fn ulid_max_size_is_bounded() {
    let ulid = Ulid::max_storable();
    let size = ulid.to_bytes().len();

    assert!(
        size <= Ulid::STORED_SIZE as usize,
        "serialized Ulid too large: got {size} bytes (limit {})",
        Ulid::STORED_SIZE
    );
}

#[test]
fn test_ulid_string_roundtrip() {
    let u1 = Ulid::generate();
    let u2 = Ulid::from_str(&u1.to_string()).unwrap();

    assert_eq!(u1, u2);
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
    let ulid = Ulid::from_str("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        .expect("fixture ULID should parse successfully");
    let debug = format!("{ulid:?}");

    assert_eq!(
        debug, "\"01ARZ3NDEKTSV4RRFFQ69G5FAV\"",
        "ULID debug should render the canonical string form",
    );
}
