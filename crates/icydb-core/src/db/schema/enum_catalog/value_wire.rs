//! Canonical current-format wire envelope for store-local enum values.
use crate::value::{CanonicalEnumBody, CanonicalEnumValue, EnumTypeId, EnumVariantId};

const ENUM_VALUE_TAG: u8 = 0x84;
const ENUM_UNIT_BODY_TAG: u8 = 0;
const ENUM_PAYLOAD_BODY_TAG: u8 = 1;
const ENUM_VALUE_HEADER_BYTES: usize = 14;
const MAX_ENUM_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// Typed failure from canonical enum wire encoding or decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CanonicalEnumWireError {
    InvalidTag,
    InvalidTypeId,
    InvalidVariantId,
    InvalidBodyTag,
    InvalidBodyLength,
    PayloadTooLarge,
    PayloadCodec,
}

/// Encode one canonical enum value with a caller-owned recursive payload codec.
pub(in crate::db) fn encode_canonical_enum_value<V>(
    value: &CanonicalEnumValue<V>,
    encode_payload: impl FnOnce(&V, &mut Vec<u8>) -> Result<(), CanonicalEnumWireError>,
) -> Result<Vec<u8>, CanonicalEnumWireError> {
    let mut encoded = Vec::with_capacity(ENUM_VALUE_HEADER_BYTES);
    encoded.push(ENUM_VALUE_TAG);
    encoded.extend_from_slice(&value.type_id().get().to_be_bytes());
    encoded.extend_from_slice(&value.variant_id().get().to_be_bytes());

    match value.body() {
        CanonicalEnumBody::Unit => {
            encoded.push(ENUM_UNIT_BODY_TAG);
            encoded.extend_from_slice(&0_u32.to_be_bytes());
        }
        CanonicalEnumBody::Payload(payload) => {
            let mut payload_bytes = Vec::new();
            encode_payload(payload, &mut payload_bytes)?;
            if payload_bytes.is_empty() {
                return Err(CanonicalEnumWireError::InvalidBodyLength);
            }
            if payload_bytes.len() > MAX_ENUM_PAYLOAD_BYTES {
                return Err(CanonicalEnumWireError::PayloadTooLarge);
            }
            let payload_len = u32::try_from(payload_bytes.len())
                .map_err(|_| CanonicalEnumWireError::PayloadTooLarge)?;
            encoded.push(ENUM_PAYLOAD_BODY_TAG);
            encoded.extend_from_slice(&payload_len.to_be_bytes());
            encoded.extend_from_slice(payload_bytes.as_slice());
        }
    }

    Ok(encoded)
}

/// Decode one canonical enum value with a caller-owned recursive payload codec.
pub(in crate::db) fn decode_canonical_enum_value<V>(
    encoded: &[u8],
    decode_payload: impl FnOnce(&[u8]) -> Result<V, CanonicalEnumWireError>,
) -> Result<CanonicalEnumValue<V>, CanonicalEnumWireError> {
    if encoded.len() < ENUM_VALUE_HEADER_BYTES {
        return Err(CanonicalEnumWireError::InvalidBodyLength);
    }
    if encoded[0] != ENUM_VALUE_TAG {
        return Err(CanonicalEnumWireError::InvalidTag);
    }

    let type_id =
        EnumTypeId::new(read_u32(&encoded[1..5])).ok_or(CanonicalEnumWireError::InvalidTypeId)?;
    let variant_id = EnumVariantId::new(read_u32(&encoded[5..9]))
        .ok_or(CanonicalEnumWireError::InvalidVariantId)?;
    let body_tag = encoded[9];
    let payload_len = usize::try_from(read_u32(&encoded[10..14]))
        .map_err(|_| CanonicalEnumWireError::PayloadTooLarge)?;
    if payload_len > MAX_ENUM_PAYLOAD_BYTES {
        return Err(CanonicalEnumWireError::PayloadTooLarge);
    }
    let payload = encoded
        .get(ENUM_VALUE_HEADER_BYTES..)
        .ok_or(CanonicalEnumWireError::InvalidBodyLength)?;
    if payload.len() != payload_len {
        return Err(CanonicalEnumWireError::InvalidBodyLength);
    }

    let body = match body_tag {
        ENUM_UNIT_BODY_TAG if payload.is_empty() => CanonicalEnumBody::Unit,
        ENUM_UNIT_BODY_TAG => return Err(CanonicalEnumWireError::InvalidBodyLength),
        ENUM_PAYLOAD_BODY_TAG if payload.is_empty() => {
            return Err(CanonicalEnumWireError::InvalidBodyLength);
        }
        ENUM_PAYLOAD_BODY_TAG => CanonicalEnumBody::Payload(Box::new(decode_payload(payload)?)),
        _ => return Err(CanonicalEnumWireError::InvalidBodyTag),
    };

    Ok(CanonicalEnumValue::new(type_id, variant_id, body))
}

const fn read_u32(bytes: &[u8]) -> u32 {
    let mut value = [0_u8; 4];
    value.copy_from_slice(bytes);
    u32::from_be_bytes(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn type_id() -> EnumTypeId {
        EnumTypeId::new(0x0102_0304).expect("test type ID should be non-zero")
    }

    fn variant_id() -> EnumVariantId {
        EnumVariantId::new(0x0506_0708).expect("test variant ID should be non-zero")
    }

    #[test]
    fn canonical_unit_enum_wire_vector_is_frozen() {
        let value = CanonicalEnumValue::<u8>::new(type_id(), variant_id(), CanonicalEnumBody::Unit);
        let encoded = encode_canonical_enum_value(&value, |_payload, _encoded| {
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
        let encoded = encode_canonical_enum_value(&value, |payload, encoded| {
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
        let valid = encode_canonical_enum_value(&unit, |_payload, _encoded| Ok(()))
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
            encode_canonical_enum_value(&payload, |_payload, _encoded| Ok(())),
            Err(CanonicalEnumWireError::InvalidBodyLength),
        );
        assert_eq!(
            encode_canonical_enum_value(&payload, |_payload, _encoded| {
                Err(CanonicalEnumWireError::PayloadCodec)
            }),
            Err(CanonicalEnumWireError::PayloadCodec),
        );
        assert_eq!(
            encode_canonical_enum_value(&payload, |_payload, encoded| {
                encoded.resize(MAX_ENUM_PAYLOAD_BYTES.saturating_add(1), 0);
                Ok(())
            }),
            Err(CanonicalEnumWireError::PayloadTooLarge),
        );
    }
}
