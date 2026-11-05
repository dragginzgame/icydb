use super::decode_leaf_field_by_kind_bytes;
use crate::db::data::structural_field::encode_structural_field_by_accepted_kind_bytes;
use crate::{
    db::data::structural_field::{
        binary::push_binary_bytes, validate_structural_field_by_accepted_kind_bytes,
    },
    db::schema::AcceptedFieldKind,
    types::{Date, Decimal, Duration},
    value::Value,
};

#[test]
fn leaf_field_binary_roundtrips_supported_leaf_wrappers() {
    let cases = vec![
        (
            AcceptedFieldKind::Date,
            Value::Date(Date::try_new(2025, 10, 19).expect("valid date")),
        ),
        (
            AcceptedFieldKind::Decimal { scale: 2 },
            Value::Decimal(Decimal::from_i128_with_scale(12_345, 2)),
        ),
        (
            AcceptedFieldKind::Duration,
            Value::Duration(Duration::from_secs(5)),
        ),
    ];

    for (kind, value) in cases {
        let encoded = encode_structural_field_by_accepted_kind_bytes(&kind, &value)
            .expect("leaf payload should encode");
        let decoded = decode_leaf_field_by_kind_bytes(encoded.as_slice(), &kind)
            .expect("leaf payload should decode")
            .expect("leaf kind should decode through the leaf lane");

        validate_structural_field_by_accepted_kind_bytes(encoded.as_slice(), &kind)
            .expect("leaf payload should validate");

        assert_eq!(decoded, value, "leaf roundtrip mismatch for {kind:?}");
    }
}

#[test]
fn leaf_field_binary_rejects_malformed_decimal_payload() {
    let mut bytes = Vec::new();
    let mut payload = vec![0; 17];
    payload[15] = 1;
    payload[16] = u8::try_from(Decimal::max_supported_scale() + 1).unwrap();
    push_binary_bytes(&mut bytes, &payload);

    let kind = AcceptedFieldKind::Decimal { scale: 2 };

    let decode = decode_leaf_field_by_kind_bytes(bytes.as_slice(), &kind);
    let validate = validate_structural_field_by_accepted_kind_bytes(bytes.as_slice(), &kind);

    assert!(
        decode.is_err(),
        "malformed decimal payload must fail decode"
    );
    assert!(
        validate.is_err(),
        "malformed decimal payload must fail validate"
    );
}
