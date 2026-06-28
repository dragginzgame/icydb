//! Module: db::key_taxonomy::tests
//! Responsibility: compact key taxonomy encoding, decoding, and ordering fixtures.
//! Does not own: index store or data store integration outside taxonomy boundaries.
//! Boundary: exercises taxonomy-owned raw key wrappers and compact primary-key bytes.

use super::{
    COMPOSITE_PRIMARY_KEY_MAX_SIZE, CompactPrimaryKeyDecodeError, CompactStoreKeyDecodeError,
    CompositePrimaryKeyValue, CompositePrimaryKeyValueError, DataStoreKey, EncodedIndexComponent,
    EncodedPrimaryKey, IndexEntryValue, IndexStoreKey, IndexStoreKeyKind, MAX_PRIMARY_KEY_FIELDS,
    PrimaryKeyComponent, PrimaryKeyKind, PrimaryKeyValue, RawDataStoreKey, RawDataStoreKeyRange,
    RawIndexStoreKey,
};
use crate::{
    db::{
        data::DecodedDataStoreKey,
        index::{IndexId, IndexKey, IndexKeyKind},
    },
    traits::Repr,
    types::{Account, EntityTag, IntBig, NatBig, Principal, Subaccount, Timestamp, Ulid},
    value::Value,
};

fn account_fixture(seed: u8) -> Account {
    Account::from_owner_and_subaccount(
        Principal::from_slice(&[seed]),
        Some(Subaccount::from_array([seed; 32])),
    )
}

fn composite_primary_key_fixture() -> CompositePrimaryKeyValue {
    CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(11)),
    ])
    .expect("composite primary key should construct")
}

fn roundtrip(value: PrimaryKeyComponent) {
    let encoded = EncodedPrimaryKey::encode(value).expect("primary key should encode");
    assert_eq!(encoded.kind().expect("kind should decode"), value.kind());
    assert_eq!(
        encoded
            .decode_component()
            .expect("primary key should decode"),
        value
    );
}

#[test]
fn runtime_values_convert_directly_to_primary_key_components() {
    let account = account_fixture(3);
    let cases = [
        (Value::Nat64(42), PrimaryKeyComponent::Nat64(42)),
        (Value::Int64(-42), PrimaryKeyComponent::Int64(-42)),
        (
            Value::Nat128(u128::MAX),
            PrimaryKeyComponent::Nat128(u128::MAX),
        ),
        (
            Value::Int128(i128::MIN),
            PrimaryKeyComponent::Int128(i128::MIN),
        ),
        (
            Value::Timestamp(Timestamp::from_millis(-42)),
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-42)),
        ),
        (
            Value::Ulid(Ulid::from_u128(42)),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(42)),
        ),
        (
            Value::Principal(Principal::from_slice(&[1, 2, 3])),
            PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
        ),
        (
            Value::Subaccount(Subaccount::from_array([7; 32])),
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([7; 32])),
        ),
        (
            Value::Account(account),
            PrimaryKeyComponent::Account(account),
        ),
        (Value::Unit, PrimaryKeyComponent::Unit),
    ];

    for (value, expected) in cases {
        assert_eq!(
            PrimaryKeyComponent::from_runtime_value(&value),
            Some(expected),
            "value: {value:?}"
        );
    }
}

#[test]
fn runtime_big_integer_values_do_not_convert_to_primary_key_components() {
    assert!(PrimaryKeyComponent::from_runtime_value(&Value::IntBig(IntBig::from(1i32))).is_none());
    assert!(PrimaryKeyComponent::from_runtime_value(&Value::NatBig(NatBig::from(1u32))).is_none());
}

#[test]
fn composite_primary_key_value_keeps_fixed_capacity_components() {
    let components = [
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(9)),
    ];
    let key = CompositePrimaryKeyValue::try_from_components(&components)
        .expect("valid composite primary key should construct");

    assert_eq!(key.len(), 2);
    assert!(!key.is_empty());
    assert_eq!(key.components(), components);
}

#[test]
fn composite_primary_key_value_rejects_invalid_component_counts() {
    let empty = CompositePrimaryKeyValue::try_from_components(&[])
        .expect_err("empty composite primary key should reject");
    std::assert_matches!(
        empty,
        CompositePrimaryKeyValueError::TooFewComponents { count: 0, min: 2 }
    );

    let one = CompositePrimaryKeyValue::try_from_components(&[PrimaryKeyComponent::Nat64(1)])
        .expect_err("single-component composite primary key should reject");
    std::assert_matches!(
        one,
        CompositePrimaryKeyValueError::TooFewComponents { count: 1, min: 2 }
    );

    let too_many = [PrimaryKeyComponent::Nat64(1); MAX_PRIMARY_KEY_FIELDS + 1];
    let err = CompositePrimaryKeyValue::try_from_components(&too_many)
        .expect_err("overwide composite primary key should reject");
    std::assert_matches!(
        err,
        CompositePrimaryKeyValueError::TooManyComponents { count, max }
            if count == MAX_PRIMARY_KEY_FIELDS + 1 && max == MAX_PRIMARY_KEY_FIELDS
    );
}

#[test]
fn composite_primary_key_value_rejects_unit_components() {
    let err = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(1),
        PrimaryKeyComponent::Unit,
    ])
    .expect_err("unit is scalar-only and should reject in composite keys");

    std::assert_matches!(
        err,
        CompositePrimaryKeyValueError::UnitComponent { index: 1 }
    );
}

#[test]
fn composite_primary_key_value_uses_lexicographic_component_order() {
    let left = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(1),
        PrimaryKeyComponent::Int64(10),
    ])
    .expect("left key should construct");
    let right = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(2),
        PrimaryKeyComponent::Int64(-10),
    ])
    .expect("right key should construct");

    assert!(left < right);
}

#[test]
fn compact_composite_primary_key_roundtrips_components() {
    let value = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
    ])
    .expect("valid composite primary key should construct");

    let encoded =
        EncodedPrimaryKey::encode_composite(&value).expect("composite primary key should encode");

    assert_eq!(
        encoded.kind().expect("encoded kind should decode"),
        PrimaryKeyKind::Composite,
    );
    assert_eq!(
        encoded
            .decode_composite()
            .expect("composite primary key should decode"),
        value,
    );
    assert_eq!(
        encoded
            .decode()
            .expect("composite primary key should decode"),
        PrimaryKeyValue::Composite(value),
    );
    std::assert_matches!(
        encoded.decode_component(),
        Err(CompactPrimaryKeyDecodeError::CompositeNotScalar)
    );
}

#[test]
fn compact_primary_key_value_scalar_wrapper_uses_scalar_encoding() {
    let encoded = EncodedPrimaryKey::encode(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7)))
        .expect("scalar primary-key value should encode");

    assert_eq!(
        encoded.as_bytes(),
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(7))
            .expect("component should encode")
            .as_bytes(),
    );
    assert_eq!(
        encoded
            .decode()
            .expect("scalar primary-key value should decode"),
        PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7)),
    );
}

#[test]
fn compact_primary_key_value_composite_wrapper_uses_composite_encoding() {
    let value = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(7),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(11)),
    ])
    .expect("composite primary-key value should construct");

    let encoded = EncodedPrimaryKey::encode(PrimaryKeyValue::Composite(value))
        .expect("composite primary-key value should encode");

    assert_eq!(
        encoded.kind().expect("kind should decode"),
        PrimaryKeyKind::Composite,
    );
    assert_eq!(
        encoded.decode().expect("primary-key value should decode"),
        PrimaryKeyValue::Composite(value),
    );
    std::assert_matches!(
        encoded.decode_component(),
        Err(CompactPrimaryKeyDecodeError::CompositeNotScalar)
    );
}

#[test]
fn compact_composite_primary_key_rejects_invalid_counts() {
    let count_one = [
        PrimaryKeyKind::Composite.tag(),
        1,
        PrimaryKeyKind::Nat64.tag(),
    ];
    let err = EncodedPrimaryKey {
        bytes: count_one.to_vec(),
    }
    .decode_composite()
    .expect_err("composite count one should reject");
    std::assert_matches!(
        err,
        CompactPrimaryKeyDecodeError::InvalidCompositeCount { count: 1, .. }
    );

    let overwide = [PrimaryKeyKind::Composite.tag(), 5];
    let err = EncodedPrimaryKey {
        bytes: overwide.to_vec(),
    }
    .decode_composite()
    .expect_err("overwide composite count should reject");
    std::assert_matches!(
        err,
        CompactPrimaryKeyDecodeError::InvalidCompositeCount { count: 5, .. }
    );
}

#[test]
fn compact_composite_primary_key_rejects_unit_component_payload() {
    let bytes = [
        PrimaryKeyKind::Composite.tag(),
        2,
        PrimaryKeyKind::Nat64.tag(),
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        PrimaryKeyKind::Unit.tag(),
    ];
    let err = EncodedPrimaryKey {
        bytes: bytes.to_vec(),
    }
    .decode_composite()
    .expect_err("unit component should reject");

    std::assert_matches!(
        err,
        CompactPrimaryKeyDecodeError::UnitCompositeComponent { index: 1 }
    );
}

#[test]
fn compact_composite_primary_key_byte_order_matches_component_order() {
    let left = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(1),
        PrimaryKeyComponent::Int64(-1),
    ])
    .expect("left composite key should construct");
    let right = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(1),
        PrimaryKeyComponent::Int64(1),
    ])
    .expect("right composite key should construct");
    let left_encoded =
        EncodedPrimaryKey::encode_composite(&left).expect("left composite key should encode");
    let right_encoded =
        EncodedPrimaryKey::encode_composite(&right).expect("right composite key should encode");

    assert!(left < right);
    assert!(left_encoded < right_encoded);
}

#[test]
fn compact_composite_primary_key_admits_fixed_128_bit_components() {
    let left = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat128(1),
        PrimaryKeyComponent::Int128(-1),
    ])
    .expect("left composite key should construct");
    let right = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat128(1),
        PrimaryKeyComponent::Int128(1),
    ])
    .expect("right composite key should construct");
    let left_encoded =
        EncodedPrimaryKey::encode_composite(&left).expect("left composite key should encode");
    let right_encoded =
        EncodedPrimaryKey::encode_composite(&right).expect("right composite key should encode");

    assert!(left < right);
    assert!(left_encoded < right_encoded);
    assert_eq!(
        left_encoded
            .decode()
            .expect("left composite key should decode"),
        PrimaryKeyValue::Composite(left)
    );
}

#[test]
fn compact_primary_key_roundtrip_per_key_type() {
    let values = [
        PrimaryKeyComponent::Nat64(42),
        PrimaryKeyComponent::Int64(-42),
        PrimaryKeyComponent::Nat128(u128::MAX),
        PrimaryKeyComponent::Int128(i128::MIN),
        PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-42)),
        PrimaryKeyComponent::Ulid(Ulid::from_u128(42)),
        PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
        PrimaryKeyComponent::Subaccount(Subaccount::from_array([7; 32])),
        PrimaryKeyComponent::Account(account_fixture(7)),
        PrimaryKeyComponent::Unit,
    ];

    for value in values {
        roundtrip(value);
    }
}

#[test]
fn compact_primary_key_rejects_malformed_kind_tag() {
    let err = EncodedPrimaryKey::try_from(&[0xFF][..])
        .expect_err("unknown primary-key kind tag should reject");

    std::assert_matches!(err, CompactPrimaryKeyDecodeError::UnknownKind { tag: 0xFF });
}

#[test]
fn compact_primary_key_rejects_malformed_lengths() {
    let fixed_cases = [
        PrimaryKeyKind::Nat64,
        PrimaryKeyKind::Int64,
        PrimaryKeyKind::Nat128,
        PrimaryKeyKind::Int128,
        PrimaryKeyKind::Timestamp,
        PrimaryKeyKind::Ulid,
        PrimaryKeyKind::Subaccount,
        PrimaryKeyKind::Account,
        PrimaryKeyKind::Unit,
    ];

    for kind in fixed_cases {
        let err = EncodedPrimaryKey::try_from(&[kind.tag(), 0xAA][..])
            .expect_err("fixed-width primary key should reject wrong length");
        std::assert_matches!(
            err,
            CompactPrimaryKeyDecodeError::InvalidLength {
                kind: err_kind,
                ..
            } if err_kind == kind
        );
    }
}

#[test]
fn compact_primary_key_rejects_invalid_principal_length() {
    let too_long = [
        PrimaryKeyKind::Principal.tag(),
        u8::try_from(Principal::MAX_LENGTH_IN_BYTES)
            .expect("principal max length fits in one byte")
            + 1,
    ];
    let err = EncodedPrimaryKey::try_from(&too_long[..])
        .expect_err("oversized principal length should reject");
    std::assert_matches!(err, CompactPrimaryKeyDecodeError::InvalidPrincipalLength);

    let truncated = [PrimaryKeyKind::Principal.tag(), 3, 1, 2];
    let err = EncodedPrimaryKey::try_from(&truncated[..])
        .expect_err("truncated principal payload should reject");
    std::assert_matches!(
        err,
        CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Principal,
            ..
        }
    );
}

#[test]
fn compact_primary_key_accepts_principal_max_length_and_rejects_invalid_length() {
    let max = Principal::from_slice(&[0xAB; Principal::MAX_LENGTH_IN_BYTES as usize]);
    roundtrip(PrimaryKeyComponent::Principal(max));

    let missing_length = [PrimaryKeyKind::Principal.tag()];
    let err = EncodedPrimaryKey::try_from(&missing_length[..])
        .expect_err("principal payload must contain a length byte");
    std::assert_matches!(
        err,
        CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Principal,
            ..
        }
    );
}

#[test]
fn compact_primary_key_requires_subaccount_exact_length() {
    let short = [PrimaryKeyKind::Subaccount.tag(), 0x01];
    let err = EncodedPrimaryKey::try_from(&short[..])
        .expect_err("subaccount primary key must be exactly 32 bytes");
    std::assert_matches!(
        err,
        CompactPrimaryKeyDecodeError::InvalidLength {
            kind: PrimaryKeyKind::Subaccount,
            ..
        }
    );

    roundtrip(PrimaryKeyComponent::Subaccount(Subaccount::from_array(
        [0xCC; 32],
    )));
}

#[test]
fn compact_primary_key_validates_account_payload() {
    roundtrip(PrimaryKeyComponent::Account(account_fixture(9)));

    let mut invalid = vec![PrimaryKeyKind::Account.tag()];
    invalid.extend_from_slice(&[0u8; Account::STORED_SIZE as usize]);
    invalid[1] = u8::try_from(Principal::MAX_LENGTH_IN_BYTES)
        .expect("principal max length fits in one byte")
        + 1;

    let err = EncodedPrimaryKey::try_from(&invalid[..])
        .expect_err("invalid account payload should reject");
    std::assert_matches!(err, CompactPrimaryKeyDecodeError::InvalidAccount);
}

#[test]
fn compact_primary_key_unit_is_kind_only_singleton() {
    let encoded = EncodedPrimaryKey::encode(PrimaryKeyComponent::Unit)
        .expect("unit primary key should encode");

    assert_eq!(encoded.as_bytes(), &[PrimaryKeyKind::Unit.tag()]);
    assert_eq!(
        encoded
            .decode_component()
            .expect("unit primary key should decode"),
        PrimaryKeyComponent::Unit
    );
}

#[test]
fn compact_primary_key_ordering_matches_logical_order_per_type() {
    let cases = [
        (PrimaryKeyComponent::Nat64(1), PrimaryKeyComponent::Nat64(2)),
        (
            PrimaryKeyComponent::Int64(-2),
            PrimaryKeyComponent::Int64(1),
        ),
        (
            PrimaryKeyComponent::Nat128(1),
            PrimaryKeyComponent::Nat128(u128::MAX),
        ),
        (
            PrimaryKeyComponent::Int128(i128::MIN),
            PrimaryKeyComponent::Int128(i128::MAX),
        ),
        (
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-1)),
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(1)),
        ),
        (
            PrimaryKeyComponent::Ulid(Ulid::from_u128(1)),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(2)),
        ),
        (
            PrimaryKeyComponent::Principal(Principal::from_slice(&[9])),
            PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 0])),
        ),
        (
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([1; 32])),
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([2; 32])),
        ),
        (
            PrimaryKeyComponent::Account(account_fixture(1)),
            PrimaryKeyComponent::Account(account_fixture(2)),
        ),
    ];

    for (left, right) in cases {
        assert_eq!(left.cmp(&right), std::cmp::Ordering::Less);

        let left_encoded = EncodedPrimaryKey::encode(left).expect("left primary key should encode");
        let right_encoded =
            EncodedPrimaryKey::encode(right).expect("right primary key should encode");

        assert_eq!(left_encoded.cmp(&right_encoded), left.cmp(&right));
    }
}

#[test]
fn compact_primary_key_timestamp_negative_ordering_is_biased() {
    let mut values = [
        Timestamp::from_millis(0),
        Timestamp::from_millis(i64::MIN),
        Timestamp::from_millis(-1),
        Timestamp::from_millis(1),
        Timestamp::from_millis(i64::MAX),
    ];
    values.sort();

    let mut encoded = values.map(|value| {
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Timestamp(value))
            .expect("timestamp primary key should encode")
    });
    encoded.sort();

    let decoded =
        encoded.map(
            |value| match value.decode_component().expect("timestamp should decode") {
                PrimaryKeyComponent::Timestamp(value) => value.repr(),
                other => panic!("expected timestamp primary key, got {other:?}"),
            },
        );
    let expected = values.map(|value| value.repr());

    assert_eq!(decoded, expected);
}

#[test]
fn compact_primary_key_principal_length_first_ordering_fixture() {
    let short = PrimaryKeyComponent::Principal(Principal::from_slice(&[9]));
    let long = PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 0]));

    assert_eq!(short.cmp(&long), std::cmp::Ordering::Less);

    let short_encoded = EncodedPrimaryKey::encode(short).expect("short principal should encode");
    let long_encoded = EncodedPrimaryKey::encode(long).expect("long principal should encode");

    assert_eq!(short_encoded.cmp(&long_encoded), std::cmp::Ordering::Less);
    assert_eq!(short_encoded.payload().expect("payload"), &[1, 9]);
    assert_eq!(long_encoded.payload().expect("payload"), &[2, 1, 0]);
}

#[test]
fn compact_primary_and_index_component_payload_ordering_match_for_overlapping_primitives() {
    let pairs = [
        (PrimaryKeyComponent::Nat64(7), PrimaryKeyComponent::Nat64(8)),
        (
            PrimaryKeyComponent::Int64(-7),
            PrimaryKeyComponent::Int64(8),
        ),
        (
            PrimaryKeyComponent::Nat128(7),
            PrimaryKeyComponent::Nat128(8),
        ),
        (
            PrimaryKeyComponent::Int128(-7),
            PrimaryKeyComponent::Int128(8),
        ),
        (
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-7)),
            PrimaryKeyComponent::Timestamp(Timestamp::from_millis(8)),
        ),
        (
            PrimaryKeyComponent::Ulid(Ulid::from_u128(7)),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(8)),
        ),
        (
            PrimaryKeyComponent::Principal(Principal::from_slice(&[7])),
            PrimaryKeyComponent::Principal(Principal::from_slice(&[8])),
        ),
        (
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([7; 32])),
            PrimaryKeyComponent::Subaccount(Subaccount::from_array([8; 32])),
        ),
        (
            PrimaryKeyComponent::Account(account_fixture(7)),
            PrimaryKeyComponent::Account(account_fixture(8)),
        ),
        (PrimaryKeyComponent::Unit, PrimaryKeyComponent::Unit),
    ];

    for (left, right) in pairs {
        let left_primary = EncodedPrimaryKey::encode(left).expect("left primary key should encode");
        let right_primary =
            EncodedPrimaryKey::encode(right).expect("right primary key should encode");
        let left_index = EncodedIndexComponent::encode_primary_overlap(left)
            .expect("left index component should encode");
        let right_index = EncodedIndexComponent::encode_primary_overlap(right)
            .expect("right index component should encode");

        assert_eq!(left_primary.as_bytes(), left_index.as_bytes());
        assert_eq!(
            left_primary.payload().expect("primary payload"),
            left_index.payload().expect("index payload")
        );
        assert_eq!(
            left_primary.cmp(&right_primary),
            left_index.cmp(&right_index)
        );
    }
}

#[test]
fn compact_primary_key_component_preserves_logical_values() {
    let primary_key = PrimaryKeyComponent::Timestamp(Timestamp::from_millis(-11));

    assert_eq!(
        EncodedPrimaryKey::encode(primary_key)
            .expect("primary-key component should encode")
            .decode_component()
            .expect("primary-key component should decode"),
        primary_key
    );
}

#[test]
fn key_taxonomy_wrappers_match_live_compact_data_key_cut() {
    let entity = EntityTag::new(0x159);
    let primary_key = EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(5))
        .expect("primary key should encode");
    let data_key = DataStoreKey::new(entity, primary_key.clone());
    let raw_data: RawDataStoreKey = data_key.to_raw();
    let live_data_key = DecodedDataStoreKey::new(
        entity,
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(5)),
    )
    .to_raw()
    .expect("live data key should encode");

    assert_eq!(raw_data.as_bytes().len(), size_of::<u64>() + 1 + 8);
    assert_eq!(
        live_data_key.as_bytes(),
        raw_data.as_bytes(),
        "live data-store keys should use the compact taxonomy wire shape"
    );
    assert_eq!(
        RawDataStoreKey::MAX_STORED_SIZE_BYTES,
        size_of::<u64>() as u64 + COMPOSITE_PRIMARY_KEY_MAX_SIZE as u64
    );

    let index_component =
        EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat64(3))
            .expect("index component should encode");
    let index_key = IndexStoreKey::new(IndexId::new(entity, 1), vec![index_component], primary_key);
    let raw_index: RawIndexStoreKey = index_key.to_raw().expect("raw index key should encode");
    assert!(!raw_index.as_bytes().is_empty());

    let entry = IndexEntryValue::presence_only();
    assert_eq!(
        entry.as_bytes(),
        &[0],
        "taxonomy index entry values carry only the presence witness"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "0.162 closeout evidence intentionally keeps scalar/composite footprint cases together"
)]
fn composite_primary_key_closeout_storage_footprint_is_linear_and_key_owned() {
    let entity = EntityTag::new(0x162);
    let index_id = IndexId::new(entity, 7);
    let index_component =
        EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat64(99))
            .expect("index component should encode");
    let index_entry = IndexEntryValue::presence_only();

    let two_nat64 = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Nat64(11),
        ])
        .expect("two-component composite key should build"),
    );
    let three_mixed = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Int128(i128::MIN),
            PrimaryKeyComponent::Ulid(Ulid::from_u128(11)),
        ])
        .expect("three-component composite key should build"),
    );
    let variable_mixed = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Principal(Principal::from_slice(&[1, 2, 3])),
        ])
        .expect("mixed variable-width composite key should build"),
    );

    let cases = [
        (
            "scalar_nat64",
            PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
            9,
            17,
            34,
        ),
        (
            "scalar_int128",
            PrimaryKeyValue::from(PrimaryKeyComponent::Int128(i128::MIN)),
            17,
            25,
            42,
        ),
        (
            "scalar_nat128",
            PrimaryKeyValue::from(PrimaryKeyComponent::Nat128(u128::MAX)),
            17,
            25,
            42,
        ),
        ("composite_nat64_nat64", two_nat64, 20, 28, 45),
        ("composite_nat64_int128_ulid", three_mixed, 45, 53, 70),
        ("composite_nat64_principal3", variable_mixed, 16, 24, 41),
    ];

    for (label, primary_key_value, encoded_len, data_key_len, index_key_len) in cases {
        let primary_key =
            EncodedPrimaryKey::encode(primary_key_value).expect("primary key value should encode");
        let data_key = DataStoreKey::new(entity, primary_key.clone()).to_raw();
        let index_key =
            IndexStoreKey::new(index_id, vec![index_component.clone()], primary_key.clone())
                .to_raw()
                .expect("index key should encode");

        assert_eq!(
            primary_key.as_bytes().len(),
            encoded_len,
            "{label}: encoded primary-key length"
        );
        assert_eq!(
            data_key.as_bytes().len(),
            data_key_len,
            "{label}: raw data-store key length"
        );
        assert_eq!(
            index_key.as_bytes().len(),
            index_key_len,
            "{label}: raw index-store key length with one Nat64 component"
        );
        assert_eq!(
            index_key
                .decode()
                .expect("index key should decode")
                .primary_key(),
            &primary_key,
            "{label}: row identity should roundtrip from the key suffix"
        );
        assert_eq!(
            index_entry.as_bytes().len(),
            1,
            "{label}: index entry value must remain presence-only"
        );
    }

    let fixed_128_boundary_cases = [
        (
            "int128_min",
            PrimaryKeyValue::from(PrimaryKeyComponent::Int128(i128::MIN)),
        ),
        (
            "int128_minus_one",
            PrimaryKeyValue::from(PrimaryKeyComponent::Int128(-1)),
        ),
        (
            "int128_zero",
            PrimaryKeyValue::from(PrimaryKeyComponent::Int128(0)),
        ),
        (
            "int128_one",
            PrimaryKeyValue::from(PrimaryKeyComponent::Int128(1)),
        ),
        (
            "int128_max",
            PrimaryKeyValue::from(PrimaryKeyComponent::Int128(i128::MAX)),
        ),
        (
            "nat128_zero",
            PrimaryKeyValue::from(PrimaryKeyComponent::Nat128(0)),
        ),
        (
            "nat128_one",
            PrimaryKeyValue::from(PrimaryKeyComponent::Nat128(1)),
        ),
        (
            "nat128_max",
            PrimaryKeyValue::from(PrimaryKeyComponent::Nat128(u128::MAX)),
        ),
    ];
    for (label, primary_key_value) in fixed_128_boundary_cases {
        let primary_key = EncodedPrimaryKey::encode(primary_key_value)
            .expect("128-bit primary key should encode");
        let data_key = DataStoreKey::new(entity, primary_key.clone()).to_raw();
        let index_key =
            IndexStoreKey::new(index_id, vec![index_component.clone()], primary_key.clone())
                .to_raw()
                .expect("128-bit index key should encode");

        assert_eq!(primary_key.as_bytes().len(), 17, "{label}: encoded pk");
        assert_eq!(data_key.as_bytes().len(), 25, "{label}: data key");
        assert_eq!(index_key.as_bytes().len(), 42, "{label}: index key");
    }
}

#[test]
fn raw_data_store_key_decodes_live_compact_shape() {
    let entity = EntityTag::new(0x1590);
    let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Int64(-5))
        .expect("primary key should encode");
    let raw = DataStoreKey::new(entity, primary.clone()).to_raw();

    let decoded = raw.decode().expect("raw data key should decode");

    assert_eq!(decoded.entity_tag(), entity);
    assert_eq!(decoded.primary_key(), &primary);
    assert_eq!(
        RawDataStoreKey::from_bytes(raw.as_bytes())
            .expect("validated raw data key should be retained")
            .as_bytes(),
        raw.as_bytes()
    );
}

#[test]
fn raw_data_store_key_accepts_composite_primary_key_suffix() {
    let entity = EntityTag::new(0x1620);
    let primary_value = composite_primary_key_fixture();
    let primary = EncodedPrimaryKey::encode(PrimaryKeyValue::Composite(primary_value))
        .expect("composite primary key should encode");
    let raw = DataStoreKey::new(entity, primary.clone()).to_raw();

    let decoded = raw.decode().expect("raw data key should decode");

    assert_eq!(decoded.entity_tag(), entity);
    assert_eq!(decoded.primary_key(), &primary);
    assert_eq!(
        decoded
            .primary_key()
            .decode()
            .expect("primary-key value should decode"),
        PrimaryKeyValue::Composite(primary_value),
    );
}

#[test]
fn raw_data_store_key_rejects_malformed_live_shape() {
    let short = [0u8; size_of::<u64>()];
    let err = RawDataStoreKey::from_bytes(&short[..])
        .expect_err("raw data key without primary suffix should reject");
    std::assert_matches!(err, CompactStoreKeyDecodeError::DataStoreKeyTooShort);

    let mut invalid_primary = vec![0u8; size_of::<u64>()];
    invalid_primary.push(0xFF);
    let err = RawDataStoreKey::from_bytes(&invalid_primary[..])
        .expect_err("raw data key with invalid primary suffix should reject");
    std::assert_matches!(
        err,
        CompactStoreKeyDecodeError::InvalidPrimaryKey(CompactPrimaryKeyDecodeError::UnknownKind {
            tag: 0xFF
        })
    );
}

#[test]
fn raw_data_store_key_entity_prefix_range_avoids_primary_key_sentinels() {
    let entity = EntityTag::new(0x1593);
    let range = RawDataStoreKeyRange::entity_prefix(entity);

    assert_eq!(range.lower_inclusive(), &entity.value().to_be_bytes());
    assert_eq!(
        range.upper_exclusive().expect("ordinary entity has upper"),
        &(entity.value() + 1).to_be_bytes()
    );

    let first = DataStoreKey::new(
        entity,
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(0))
            .expect("first primary key should encode"),
    )
    .to_raw();
    let last = DataStoreKey::new(
        entity,
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Unit)
            .expect("unit primary key should encode"),
    )
    .to_raw();
    let previous = DataStoreKey::new(
        EntityTag::new(entity.value() - 1),
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Unit)
            .expect("unit primary key should encode"),
    )
    .to_raw();
    let next = DataStoreKey::new(
        EntityTag::new(entity.value() + 1),
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(0))
            .expect("next primary key should encode"),
    )
    .to_raw();

    assert!(range.contains(&first));
    assert!(range.contains(&last));
    assert!(!range.contains(&previous));
    assert!(!range.contains(&next));
}

#[test]
fn raw_data_store_key_entity_prefix_range_handles_max_entity_tag() {
    let entity = EntityTag::new(u64::MAX);
    let range = RawDataStoreKeyRange::entity_prefix(entity);
    let key = DataStoreKey::new(
        entity,
        EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(1))
            .expect("primary key should encode"),
    )
    .to_raw();

    assert_eq!(range.lower_inclusive(), &u64::MAX.to_be_bytes());
    assert_eq!(range.upper_exclusive(), None);
    assert!(range.contains(&key));
}

#[test]
fn raw_index_store_key_decodes_live_compact_shape() {
    let entity = EntityTag::new(0x1591);
    let index_id = IndexId::new(entity, 7);
    let component = EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat64(99))
        .expect("index component should encode");
    let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Ulid(Ulid::from_u128(11)))
        .expect("primary key should encode");
    let raw = IndexStoreKey::new(index_id, vec![component.clone()], primary.clone())
        .to_raw()
        .expect("raw index key should encode");

    let decoded = raw.decode().expect("raw index key should decode");

    assert_eq!(decoded.key_kind(), IndexStoreKeyKind::User);
    assert_eq!(decoded.index_id(), index_id);
    assert_eq!(decoded.components(), &[component]);
    assert_eq!(decoded.primary_key(), &primary);
    assert_eq!(
        RawIndexStoreKey::from_bytes(raw.as_bytes())
            .expect("validated raw index key should be retained")
            .as_bytes(),
        raw.as_bytes()
    );
}

#[test]
fn raw_index_store_key_accepts_composite_primary_key_suffix() {
    let entity = EntityTag::new(0x1621);
    let index_id = IndexId::new(entity, 7);
    let component = EncodedIndexComponent::encode_primary_overlap(PrimaryKeyComponent::Nat64(99))
        .expect("index component should encode");
    let primary_value = composite_primary_key_fixture();
    let primary = EncodedPrimaryKey::encode(PrimaryKeyValue::Composite(primary_value))
        .expect("composite primary key should encode");
    let raw = IndexStoreKey::new(index_id, vec![component.clone()], primary.clone())
        .to_raw()
        .expect("raw index key should encode");

    let decoded = raw.decode().expect("raw index key should decode");

    assert_eq!(decoded.key_kind(), IndexStoreKeyKind::User);
    assert_eq!(decoded.index_id(), index_id);
    assert_eq!(decoded.components(), &[component]);
    assert_eq!(decoded.primary_key(), &primary);
    assert_eq!(
        decoded
            .primary_key()
            .decode()
            .expect("primary-key value should decode"),
        PrimaryKeyValue::Composite(primary_value),
    );
}

#[test]
fn raw_index_store_key_rejects_malformed_live_shape() {
    let err = RawIndexStoreKey::from_bytes(&[])
        .expect_err("empty raw index key should reject before handle open");
    std::assert_matches!(err, CompactStoreKeyDecodeError::TruncatedIndexSegment);

    let wrong_kind = [0xFF];
    let err = RawIndexStoreKey::from_bytes(&wrong_kind[..])
        .expect_err("unknown raw index key kind should reject");
    std::assert_matches!(err, CompactStoreKeyDecodeError::UnknownIndexKeyKind);

    let entity = EntityTag::new(0x1592);
    let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(1))
        .expect("primary key should encode");
    let mut truncated = IndexStoreKey::new(IndexId::new(entity, 1), Vec::new(), primary)
        .to_raw()
        .expect("raw index key should encode")
        .as_bytes()
        .to_vec();
    let _ = truncated.pop();
    let err = RawIndexStoreKey::from_bytes(&truncated[..])
        .expect_err("truncated primary-key suffix should reject");
    std::assert_matches!(err, CompactStoreKeyDecodeError::TruncatedIndexSegment);
}

#[test]
fn raw_index_store_key_rejects_empty_component_and_primary_segments() {
    let entity = EntityTag::new(0x1594);
    let index_id = IndexId::new(entity, 3);

    let mut empty_component = Vec::new();
    empty_component.push(IndexStoreKeyKind::User.tag());
    empty_component.extend_from_slice(&index_id.to_bytes());
    empty_component.push(1);
    empty_component.extend_from_slice(&0u16.to_be_bytes());
    let err = RawIndexStoreKey::from_bytes(&empty_component)
        .expect_err("empty component segment should reject");
    std::assert_matches!(err, CompactStoreKeyDecodeError::EmptyIndexSegment);

    let mut empty_primary = Vec::new();
    empty_primary.push(IndexStoreKeyKind::User.tag());
    empty_primary.extend_from_slice(&index_id.to_bytes());
    empty_primary.push(0);
    empty_primary.extend_from_slice(&0u16.to_be_bytes());
    let err = RawIndexStoreKey::from_bytes(&empty_primary)
        .expect_err("empty primary-key suffix should reject");
    std::assert_matches!(err, CompactStoreKeyDecodeError::EmptyIndexSegment);
}

#[test]
fn raw_index_store_key_taxonomy_matches_live_user_and_system_codecs() {
    let entity = EntityTag::new(0x1595);
    let index_id = IndexId::new(entity, 9);
    let component = EncodedIndexComponent::from_canonical_bytes(vec![0x20, 0xAA, 0xBB]);
    let primary = EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(77))
        .expect("primary key should encode");

    let cases = [
        (IndexStoreKeyKind::User, IndexKeyKind::User),
        (IndexStoreKeyKind::System, IndexKeyKind::System),
    ];
    for (taxonomy_kind, live_kind) in cases {
        let taxonomy_raw = IndexStoreKey::new_with_kind(
            taxonomy_kind,
            index_id,
            vec![component.clone()],
            primary.clone(),
        )
        .to_raw()
        .expect("taxonomy raw index key should encode");
        let live_raw = IndexKey::new_from_components_with_primary_key_value(
            &index_id,
            live_kind,
            &[component.as_bytes()],
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(77)),
        )
        .expect("test index key should build")
        .to_raw()
        .expect("test index key should encode");

        assert_eq!(
            taxonomy_raw.as_bytes(),
            live_raw.as_bytes(),
            "taxonomy store-key wrapper must match the live index codec for {taxonomy_kind:?}"
        );
    }
}
