use super::*;
use crate::types::{Account, Principal, Subaccount, Timestamp, Ulid};

#[test]
fn key_max_size_is_bounded() {
    let key = Key::max_storable();
    let size = key.to_bytes().expect("key encode").len();

    assert!(
        size <= Key::STORED_SIZE,
        "serialized Key too large: got {size} bytes (limit {})",
        Key::STORED_SIZE
    );
}

#[test]
fn key_storable_round_trip() {
    let keys = [
        Key::Account(Account::dummy(1)),
        Key::Int(-42),
        Key::Principal(Principal::anonymous()),
        Key::Principal(Principal::from_slice(&[1, 2, 3])),
        Key::Subaccount(Subaccount::from_array([7; 32])),
        Key::Timestamp(Timestamp::from_seconds(42)),
        Key::Uint(42),
        Key::Ulid(Ulid::from_bytes([9; 16])),
        Key::Unit,
    ];

    for key in keys {
        let bytes = key.to_bytes().expect("key encode");
        let decoded = Key::try_from_bytes(&bytes).unwrap();

        assert_eq!(decoded, key, "Key round trip failed for {key:?}");
    }
}

#[test]
fn key_is_exactly_fixed_size() {
    let keys = [
        Key::Account(Account::dummy(1)),
        Key::Int(0),
        Key::Principal(Principal::anonymous()),
        Key::Subaccount(Subaccount::from_array([0; 32])),
        Key::Timestamp(Timestamp::from_seconds(0)),
        Key::Uint(0),
        Key::Ulid(Ulid::from_bytes([0; 16])),
        Key::Unit,
    ];

    for key in keys {
        let len = key.to_bytes().expect("key encode").len();
        assert_eq!(
            len,
            Key::STORED_SIZE,
            "Key serialized length must be exactly {}",
            Key::STORED_SIZE
        );
    }
}

#[test]
fn key_ordering_is_total_and_stable() {
    let keys = vec![
        Key::Account(Account::new(
            Principal::from_slice(&[1]),
            None::<Subaccount>,
        )),
        Key::Account(Account::new(Principal::from_slice(&[1]), Some([0u8; 32]))),
        Key::Int(-1),
        Key::Int(0),
        Key::Principal(Principal::from_slice(&[1])),
        Key::Subaccount(Subaccount::from_array([1; 32])),
        Key::Uint(0),
        Key::Uint(1),
        Key::Timestamp(Timestamp::from_seconds(1)),
        Key::Ulid(Ulid::from_bytes([9; 16])),
        Key::Unit,
    ];

    let mut sorted_by_ord = keys.clone();
    sorted_by_ord.sort();

    let mut sorted_by_bytes = keys;
    sorted_by_bytes.sort_by_key(|key| key.to_bytes().expect("key encode"));

    assert_eq!(
        sorted_by_ord, sorted_by_bytes,
        "Key Ord and byte ordering diverged"
    );
}

#[test]
fn key_lower_bound_is_global_min() {
    let min = Key::lower_bound();
    assert_eq!(min, Key::MIN, "lower_bound must match Key::MIN");

    let mut keys = vec![
        Key::Account(Account::dummy(0)),
        Key::Int(i64::MIN),
        Key::Principal(Principal::from_slice(&[1])),
        Key::Subaccount(Subaccount::from_array([0; 32])),
        Key::Timestamp(Timestamp::from_seconds(0)),
        Key::Uint(0),
        Key::Ulid(Ulid::from_bytes([0; 16])),
        Key::Unit,
        min,
    ];

    for key in &keys {
        assert!(min <= *key, "lower_bound must be <= {key:?}");
    }

    keys.sort_by_key(|key| key.to_bytes().expect("key encode"));
    assert_eq!(keys[0], min, "lower_bound must be the byte-order minimum");
}

#[test]
fn key_from_bytes_rejects_undersized() {
    let bytes = vec![0u8; Key::STORED_SIZE - 1];
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_oversized() {
    let bytes = vec![0u8; Key::STORED_SIZE + 1];
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_accepts_zero_principal_len() {
    let key = Key::Principal(Principal::anonymous());
    let bytes = key.to_bytes().expect("key encode");
    let decoded = Key::try_from_bytes(&bytes).unwrap();
    assert_eq!(decoded, key);
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn key_from_bytes_rejects_oversized_principal_len() {
    let mut bytes = Key::Principal(Principal::from_slice(&[1]))
        .to_bytes()
        .expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_PRINCIPAL;
    bytes[Key::PAYLOAD_OFFSET] = (Principal::MAX_LENGTH_IN_BYTES as u8) + 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_principal_padding() {
    let mut bytes = Key::Principal(Principal::from_slice(&[1]))
        .to_bytes()
        .expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_PRINCIPAL;
    bytes[Key::PAYLOAD_OFFSET] = 1;
    bytes[Key::PAYLOAD_OFFSET + 2] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_account_padding() {
    let mut bytes = Key::Account(Account::new(
        Principal::from_slice(&[1]),
        None::<Subaccount>,
    ))
    .to_bytes()
    .expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_ACCOUNT;
    bytes[Key::PAYLOAD_OFFSET + Account::STORED_SIZE as usize] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_int_padding() {
    let mut bytes = Key::Int(0).to_bytes().expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_INT;
    bytes[Key::PAYLOAD_OFFSET + Key::INT_SIZE] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_uint_padding() {
    let mut bytes = Key::Uint(0).to_bytes().expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_UINT;
    bytes[Key::PAYLOAD_OFFSET + Key::UINT_SIZE] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_timestamp_padding() {
    let mut bytes = Key::Timestamp(Timestamp::from_seconds(0))
        .to_bytes()
        .expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_TIMESTAMP;
    bytes[Key::PAYLOAD_OFFSET + Key::TIMESTAMP_SIZE] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_subaccount_padding() {
    let mut bytes = Key::Subaccount(Subaccount::from_array([0; 32]))
        .to_bytes()
        .expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_SUBACCOUNT;
    bytes[Key::PAYLOAD_OFFSET + Key::SUBACCOUNT_SIZE] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_ulid_padding() {
    let mut bytes = Key::Ulid(Ulid::from_bytes([0; 16]))
        .to_bytes()
        .expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_ULID;
    bytes[Key::PAYLOAD_OFFSET + Key::ULID_SIZE] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn key_from_bytes_rejects_unit_padding() {
    let mut bytes = Key::Unit.to_bytes().expect("key encode");
    bytes[Key::TAG_OFFSET] = Key::TAG_UNIT;
    bytes[Key::PAYLOAD_OFFSET] = 1;
    assert!(Key::try_from_bytes(&bytes).is_err());
}

#[test]
fn principal_encoding_respects_max_size() {
    let max = Principal::from_slice(&[0xFF; 29]);
    let key = Key::Principal(max);

    let bytes = key.to_bytes().expect("key encode");
    assert_eq!(bytes.len(), Key::STORED_SIZE);
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn key_decode_fuzz_roundtrip_is_canonical() {
    const RUNS: u64 = 1_000;

    let mut seed = 0x1234_5678_u64;
    for _ in 0..RUNS {
        let mut bytes = [0u8; Key::STORED_SIZE];
        for b in &mut bytes {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *b = (seed >> 24) as u8;
        }

        if let Ok(decoded) = Key::try_from_bytes(&bytes) {
            let re = decoded.to_bytes().expect("key encode");
            assert_eq!(bytes, re, "decoded key must be canonical");
        }
    }
}
