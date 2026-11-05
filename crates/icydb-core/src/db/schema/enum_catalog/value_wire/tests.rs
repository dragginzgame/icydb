use super::*;

fn type_id() -> EnumTypeId {
    EnumTypeId::new(0x0102_0304).expect("test type ID should be non-zero")
}

fn variant_id() -> EnumVariantId {
    EnumVariantId::new(0x0506_0708).expect("test variant ID should be non-zero")
}

#[test]
fn canonical_enum_payload_budget_is_relative_to_its_header() {
    let value = CanonicalEnumValue::new(
        type_id(),
        variant_id(),
        CanonicalEnumBody::Payload(Box::new(())),
    );
    let prefix = [0xab; 19];
    let mut encoded = prefix.to_vec();
    push_canonical_enum_value(&mut encoded, &value, |(), out| {
        out.resize(out.len() + MAX_ENUM_PAYLOAD_BYTES, 0xcd);
        Ok(())
    })
    .expect("a maximum-size enum payload should append after a prefix");
    assert_eq!(&encoded[..prefix.len()], &prefix);
    assert_eq!(
        encoded.len(),
        prefix.len() + ENUM_VALUE_HEADER_BYTES + MAX_ENUM_PAYLOAD_BYTES,
    );
    let decoded = decode_canonical_enum_value(&encoded[prefix.len()..], |payload| {
        assert_eq!(payload.len(), MAX_ENUM_PAYLOAD_BYTES);
        assert!(payload.iter().all(|byte| *byte == 0xcd));
        Ok(())
    })
    .expect("the prefixed enum should decode with its own payload length");
    assert_eq!(decoded, value);
}

#[test]
fn canonical_unit_enum_wire_vector_is_frozen() {
    let value = CanonicalEnumValue::<u8>::new(type_id(), variant_id(), CanonicalEnumBody::Unit);
    let mut encoded = Vec::new();
    push_canonical_enum_value(&mut encoded, &value, |_payload, _encoded| {
        Err(CanonicalEnumWireError::PayloadCodec)
    })
    .expect("unit enum should encode without a payload callback");

    assert_eq!(
        encoded,
        [
            ENUM_VALUE_TAG,
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            ENUM_UNIT_BODY_TAG,
            0,
            0,
            0,
            0,
        ],
    );
    assert_eq!(
        decode_canonical_enum_value::<u8>(&encoded, |_payload| {
            Err(CanonicalEnumWireError::PayloadCodec)
        }),
        Ok(value),
    );
}

#[test]
fn canonical_payload_enum_wire_vector_is_frozen() {
    let value = CanonicalEnumValue::new(
        type_id(),
        variant_id(),
        CanonicalEnumBody::Payload(Box::new(0xaabb_u16)),
    );
    let mut encoded = Vec::new();
    push_canonical_enum_value(&mut encoded, &value, |payload, encoded| {
        encoded.extend_from_slice(&payload.to_be_bytes());
        Ok(())
    })
    .expect("payload enum should encode");

    assert_eq!(
        encoded,
        [
            ENUM_VALUE_TAG,
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            ENUM_PAYLOAD_BODY_TAG,
            0,
            0,
            0,
            2,
            0xaa,
            0xbb,
        ],
    );
    assert_eq!(
        decode_canonical_enum_value(&encoded, |payload| {
            let [first, second] = payload else {
                return Err(CanonicalEnumWireError::PayloadCodec);
            };
            Ok(u16::from_be_bytes([*first, *second]))
        }),
        Ok(value),
    );
}

#[test]
fn canonical_enum_decode_rejects_invalid_ids_body_and_framing() {
    let unit = CanonicalEnumValue::<u8>::new(type_id(), variant_id(), CanonicalEnumBody::Unit);
    let mut valid = Vec::new();
    push_canonical_enum_value(&mut valid, &unit, |_payload, _encoded| Ok(()))
        .expect("unit enum should encode");

    let cases = [
        (0, 0xff, CanonicalEnumWireError::InvalidTag),
        (9, 0xff, CanonicalEnumWireError::InvalidBodyTag),
        (13, 1, CanonicalEnumWireError::InvalidBodyLength),
    ];
    for (offset, byte, expected) in cases {
        let mut malformed = valid.clone();
        malformed[offset] = byte;
        assert_eq!(
            decode_canonical_enum_value::<u8>(&malformed, |_payload| Ok(1)),
            Err(expected),
        );
    }

    let mut zero_type = valid.clone();
    zero_type[1..5].fill(0);
    assert_eq!(
        decode_canonical_enum_value::<u8>(&zero_type, |_payload| Ok(1)),
        Err(CanonicalEnumWireError::InvalidTypeId),
    );

    let mut zero_variant = valid.clone();
    zero_variant[5..9].fill(0);
    assert_eq!(
        decode_canonical_enum_value::<u8>(&zero_variant, |_payload| Ok(1)),
        Err(CanonicalEnumWireError::InvalidVariantId),
    );

    let mut trailing = valid;
    trailing.push(0);
    assert_eq!(
        decode_canonical_enum_value::<u8>(&trailing, |_payload| Ok(1)),
        Err(CanonicalEnumWireError::InvalidBodyLength),
    );
}

#[test]
fn canonical_enum_payload_codec_is_bounded_and_fallible() {
    let payload = CanonicalEnumValue::new(
        type_id(),
        variant_id(),
        CanonicalEnumBody::Payload(Box::new(())),
    );
    assert_eq!(
        push_canonical_enum_value(&mut Vec::new(), &payload, |_payload, _encoded| Ok(())),
        Err(CanonicalEnumWireError::InvalidBodyLength),
    );
    assert_eq!(
        push_canonical_enum_value(&mut Vec::new(), &payload, |_payload, _encoded| {
            Err(CanonicalEnumWireError::PayloadCodec)
        }),
        Err(CanonicalEnumWireError::PayloadCodec),
    );
    assert_eq!(
        push_canonical_enum_value(&mut Vec::new(), &payload, |_payload, encoded| {
            encoded.resize(
                encoded
                    .len()
                    .saturating_add(MAX_ENUM_PAYLOAD_BYTES)
                    .saturating_add(1),
                0,
            );
            Ok(())
        }),
        Err(CanonicalEnumWireError::PayloadTooLarge),
    );
}
