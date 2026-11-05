mod fixed_width;
mod traversal;

use super::*;
use crate::db::schema::MAX_ACCEPTED_RECURSIVE_DEPTH;
use crate::types::Decimal;
use crate::value::{CanonicalEnumBody, CanonicalEnumValue, EnumTypeId, EnumVariantId, ValueEnum};

fn canonical_enum(payload: Option<CanonicalValue>) -> CanonicalValue {
    let type_id = EnumTypeId::new(7).expect("test type ID should be non-zero");
    let variant_id = EnumVariantId::new(11).expect("test variant ID should be non-zero");
    let body = payload.map_or(CanonicalEnumBody::Unit, |payload| {
        CanonicalEnumBody::Payload(Box::new(payload))
    });
    CanonicalValue::Enum(ValueEnum::from_canonical(CanonicalEnumValue::new(
        type_id, variant_id, body,
    )))
}

fn nested_list_value(depth: usize) -> CanonicalValue {
    let mut value = CanonicalValue::Unit;
    for _ in 0..depth {
        value = CanonicalValue::List(vec![value]);
    }
    value
}

#[test]
fn canonical_materialization_preserves_deep_mixed_containers() {
    let mut value = CanonicalValue::List(vec![CanonicalValue::Bool(true); 128]);
    for depth in 0..60 {
        value = match depth % 3 {
            0 => CanonicalValue::List(vec![value]),
            1 => CanonicalValue::Map(vec![(CanonicalValue::Text("key".into()), value)]),
            _ => canonical_enum(Some(value)),
        };
    }
    let bytes = encode_canonical_value_storage_bytes(&value).unwrap();
    assert_eq!(decode_canonical_value_storage_bytes(&bytes).unwrap(), value);
    for end in 0..bytes.len() {
        assert!(decode_canonical_value_storage_bytes(&bytes[..end]).is_err());
    }
    let mut trailing = bytes;
    trailing.push(TAG_NULL);
    assert!(decode_canonical_value_storage_bytes(&trailing).is_err());
}

#[test]
fn canonical_value_encoding_uses_the_shared_recursive_depth_boundary() {
    let allowed = nested_list_value(MAX_ACCEPTED_RECURSIVE_DEPTH - 1);
    let excessive = nested_list_value(MAX_ACCEPTED_RECURSIVE_DEPTH);

    encode_canonical_value_storage_bytes(&allowed)
        .expect("the maximum accepted recursive depth should encode");
    encode_canonical_value_storage_bytes(&excessive)
        .expect_err("a value deeper than the accepted contract must reject");
}

#[test]
fn canonical_value_storage_round_trips_nested_id_backed_enums() {
    let value = CanonicalValue::Map(vec![(
        CanonicalValue::Text("state".to_string()),
        CanonicalValue::List(vec![
            canonical_enum(None),
            canonical_enum(Some(CanonicalValue::List(vec![
                canonical_enum(Some(CanonicalValue::Nat64(9))),
                CanonicalValue::Text("after nested enum".to_string()),
            ]))),
            CanonicalValue::Bool(true),
        ]),
    )]);

    let encoded = encode_canonical_value_storage_bytes(&value)
        .expect("canonical value storage should encode");
    assert_eq!(
        decode_canonical_value_storage_bytes(&encoded)
            .expect("canonical value storage should decode"),
        value,
    );
}

#[test]
fn canonical_value_storage_round_trips_u256_as_fixed_width_bytes() {
    for value in [crate::types::U256::ZERO, crate::types::U256::MAX] {
        let canonical = CanonicalValue::U256(value);
        let encoded =
            encode_canonical_value_storage_bytes(&canonical).expect("canonical U256 should encode");

        assert_eq!(encoded.len(), 33);
        assert_eq!(
            decode_canonical_value_storage_bytes(&encoded).expect("canonical U256 should decode"),
            canonical,
        );
    }
}

#[test]
fn canonical_value_storage_round_trips_shared_structural_binary_payloads() {
    let values = [CanonicalValue::Decimal(Decimal::from_i128_with_scale(
        12_345, 2,
    ))];

    for value in values {
        let encoded = encode_canonical_value_storage_bytes(&value)
            .expect("canonical value storage should encode");

        assert_eq!(
            decode_canonical_value_storage_bytes(&encoded)
                .expect("canonical value storage should decode"),
            value,
        );
    }
}
