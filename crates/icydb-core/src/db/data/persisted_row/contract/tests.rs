use super::*;
use crate::db::{
    codec::decode_row_payload_bytes,
    data::{encode_canonical_value_storage_bytes, encode_structural_field_by_accepted_kind_bytes},
    schema::{AcceptedFieldKind, CompositeTypeId, RowLayoutVersion},
};
use crate::value::{CanonicalEnumBody, CanonicalEnumValue, EnumTypeId, EnumVariantId, ValueEnum};

#[test]
fn named_canonical_fields_decode_and_validate_through_their_wire_owner() {
    let type_id = EnumTypeId::new(7).unwrap();
    let kind = AcceptedFieldKind::Enum { type_id };
    let value = Value::Enum(ValueEnum::from_canonical(CanonicalEnumValue::new(
        type_id,
        EnumVariantId::new(11).unwrap(),
        CanonicalEnumBody::Unit,
    )));
    for (kind, value) in [
        (kind.clone(), value.clone()),
        (
            kind.clone(),
            Value::Enum(ValueEnum::from_canonical(CanonicalEnumValue::new(
                type_id,
                EnumVariantId::new(12).unwrap(),
                CanonicalEnumBody::Payload(Box::new(Value::Nat64(5))),
            ))),
        ),
        (
            AcceptedFieldKind::Composite {
                type_id: CompositeTypeId::new(3).unwrap(),
            },
            Value::Map(vec![(Value::Text("status".into()), value.clone())]),
        ),
        (
            AcceptedFieldKind::List(Box::new(kind.clone())),
            Value::List(vec![value.clone()]),
        ),
        (
            AcceptedFieldKind::Set(Box::new(kind.clone())),
            Value::List(vec![value.clone()]),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Nat64),
                value: Box::new(kind),
            },
            Value::Map(vec![(Value::Nat64(1), value)]),
        ),
    ] {
        let wire = encode_canonical_value_storage_bytes(&value).unwrap();
        for lane in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
            let field =
                AcceptedFieldDecodeContract::new("named", &kind, true, lane, LeafCodec::Structural);
            for _ in 0..2 {
                assert_eq!(
                    decode_runtime_value_from_accepted_field_contract(field, &wire).unwrap(),
                    value
                );
                validate_non_scalar_accepted_slot_value(&wire, field).unwrap();
                assert_eq!(
                    decode_runtime_value_from_accepted_field_contract(field, &[0]).unwrap(),
                    Value::Null
                );
                validate_non_scalar_accepted_slot_value(&[0], field).unwrap();
            }
            let mut trailing = wire.clone();
            trailing.push(0);
            for malformed in [&[][..], &wire[..wire.len() - 1], &trailing, &[0xff]] {
                let decoded = decode_runtime_value_from_accepted_field_contract(field, malformed)
                    .unwrap_err();
                let validated =
                    validate_non_scalar_accepted_slot_value(malformed, field).unwrap_err();
                assert_eq!(decoded.class(), crate::error::ErrorClass::Corruption);
                assert_eq!(decoded.diagnostic_code(), validated.diagnostic_code());
            }
        }
    }
}

#[test]
fn structural_null_requires_nullable_accepted_field() {
    for kind in [
        AcceptedFieldKind::Int128,
        AcceptedFieldKind::Nat128,
        AcceptedFieldKind::Text { max_len: None },
        AcceptedFieldKind::Blob { max_len: None },
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
        AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Nat64),
            value: Box::new(AcceptedFieldKind::Bool),
        },
    ] {
        for nullable in [false, true] {
            let field = AcceptedFieldDecodeContract::new(
                "value",
                &kind,
                nullable,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            );
            let decoded = decode_runtime_value_from_accepted_field_contract(field, &[0]);
            let validated = validate_non_scalar_accepted_slot_value(&[0], field);
            if nullable {
                assert_eq!(decoded.unwrap(), Value::Null);
                validated.unwrap();
            } else {
                let decoded = decoded.unwrap_err();
                let validated = validated.unwrap_err();
                assert_eq!(decoded.class(), crate::error::ErrorClass::Corruption);
                assert_eq!(decoded.diagnostic_code(), validated.diagnostic_code());
            }
        }
    }
}

#[test]
fn nullable_by_kind_collections_use_their_accepted_codec() {
    use crate::types::{Float32, Float64};

    for (kind, value) in [
        (AcceptedFieldKind::Nat64, Value::Nat64(7)),
        (AcceptedFieldKind::Float32, Value::Float32(Float32::from(7))),
        (AcceptedFieldKind::Float64, Value::Float64(Float64::from(7))),
    ] {
        let kind = AcceptedFieldKind::List(Box::new(kind));
        let value = Value::List(vec![value]);
        let field = AcceptedFieldDecodeContract::new(
            "items",
            &kind,
            true,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        );
        let wire = encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
        assert_eq!(
            decode_runtime_value_from_accepted_field_contract(field, &wire).unwrap(),
            value
        );
        validate_non_scalar_accepted_slot_value(&wire, field).unwrap();
        assert_eq!(
            decode_runtime_value_from_accepted_field_contract(field, &[0]).unwrap(),
            Value::Null
        );
        validate_non_scalar_accepted_slot_value(&[0], field).unwrap();

        let mut trailing = wire.clone();
        trailing.push(0);
        for malformed in [&[][..], &[0, 0], &wire[..wire.len() - 1], &trailing] {
            let decoded =
                decode_runtime_value_from_accepted_field_contract(field, malformed).unwrap_err();
            let validated = validate_non_scalar_accepted_slot_value(malformed, field).unwrap_err();
            assert_eq!(decoded.class(), crate::error::ErrorClass::Corruption);
            assert_eq!(decoded.diagnostic_code(), validated.diagnostic_code());
        }
    }
}

#[test]
fn nullable_by_kind_collections_keep_accepted_depth_and_kind_boundaries() {
    use crate::db::schema::MAX_ACCEPTED_RECURSIVE_DEPTH;

    let mut kind = AcceptedFieldKind::Nat64;
    let mut value = Value::Nat64(7);
    for _ in 1..MAX_ACCEPTED_RECURSIVE_DEPTH {
        kind = AcceptedFieldKind::List(Box::new(kind));
        value = Value::List(vec![value]);
    }
    let field = AcceptedFieldDecodeContract::new(
        "items",
        &kind,
        true,
        FieldStorageDecode::ByKind,
        LeafCodec::Structural,
    );
    let wire = encode_structural_field_by_accepted_kind_bytes(&kind, &value).unwrap();
    assert_eq!(
        decode_runtime_value_from_accepted_field_contract(field, &wire).unwrap(),
        value
    );
    validate_non_scalar_accepted_slot_value(&wire, field).unwrap();

    // One extra list does not fit this accepted schema, even if a binary
    // walker can determine its framing independently of the field kind.
    let deeper = AcceptedFieldKind::List(Box::new(kind.clone()));
    let excessive =
        encode_structural_field_by_accepted_kind_bytes(&deeper, &Value::List(vec![value])).unwrap();
    for malformed in [&excessive[..], &[0xff], &[0, 0]] {
        assert!(decode_runtime_value_from_accepted_field_contract(field, malformed).is_err());
        assert!(validate_non_scalar_accepted_slot_value(malformed, field).is_err());
    }
}

#[test]
fn row_emission_preserves_collection_bytes_and_slot_offsets() {
    let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64));
    let value = Value::List((0..1_000).map(Value::Nat64).collect());
    let field = encode_structural_field_by_accepted_kind_bytes(&kind, &value)
        .expect("the accepted list should encode");
    assert_eq!(field.len(), 9_005);
    let mut fields = vec![field];
    let row = emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 1, &fields)
        .expect("one collection field should emit");
    assert_eq!(row.as_raw_row().as_bytes().len(), 9_026);

    fields.push(vec![0]);
    let row = emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 2, &fields)
        .expect("the collection and null slots should emit");
    let raw = row.as_raw_row().as_bytes();
    let decoded = decode_row_payload_bytes(raw).expect("the row envelope should decode");
    assert_eq!(decoded.layout_version(), RowLayoutVersion::INITIAL);
    let payload = decoded.into_payload();
    // Frozen count/offset/length directory: list at 0, null at 9,005.
    assert_eq!(
        &payload[..18],
        &[
            0, 2, 0, 0, 0, 0, 0, 0, 0x23, 0x2d, 0, 0, 0x23, 0x2d, 0, 0, 0, 1
        ],
    );
    assert_eq!(&payload[18..18 + 9_005], fields[0]);
    assert_eq!(&payload[18 + 9_005..], fields[1]);
    assert_eq!(
        decode_structural_field_by_accepted_kind_bytes(&payload[18..18 + 9_005], &kind)
            .expect("the emitted collection should decode"),
        value,
    );
    assert!(emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 1, &fields).is_err());
}
