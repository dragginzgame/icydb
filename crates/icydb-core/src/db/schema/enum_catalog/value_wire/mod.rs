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

/// Append one canonical enum using the caller's recursive destination.
/// The payload callback must append bytes; failures invalidate this destination.
pub(in crate::db) fn push_canonical_enum_value<V>(
    encoded: &mut Vec<u8>,
    value: &CanonicalEnumValue<V>,
    encode_payload: impl FnOnce(&V, &mut Vec<u8>) -> Result<(), CanonicalEnumWireError>,
) -> Result<(), CanonicalEnumWireError> {
    encoded.push(ENUM_VALUE_TAG);
    encoded.extend_from_slice(&value.type_id().get().to_be_bytes());
    encoded.extend_from_slice(&value.variant_id().get().to_be_bytes());
    match value.body() {
        CanonicalEnumBody::Unit => {
            encoded.push(ENUM_UNIT_BODY_TAG);
            encoded.extend_from_slice(&0_u32.to_be_bytes());
        }
        CanonicalEnumBody::Payload(payload) => {
            encoded.push(ENUM_PAYLOAD_BODY_TAG);
            let length_offset = encoded.len();
            encoded.extend_from_slice(&0_u32.to_be_bytes());
            let payload_start = encoded.len();
            encode_payload(payload, encoded)?;
            let payload_len = encoded
                .len()
                .checked_sub(payload_start)
                .ok_or(CanonicalEnumWireError::InvalidBodyLength)?;
            if payload_len == 0 {
                return Err(CanonicalEnumWireError::InvalidBodyLength);
            }
            if payload_len > MAX_ENUM_PAYLOAD_BYTES {
                return Err(CanonicalEnumWireError::PayloadTooLarge);
            }
            let payload_len =
                u32::try_from(payload_len).map_err(|_| CanonicalEnumWireError::PayloadTooLarge)?;
            // Length is relative to this enum, including when nested after a prefix.
            encoded
                .get_mut(length_offset..payload_start)
                .ok_or(CanonicalEnumWireError::InvalidBodyLength)?
                .copy_from_slice(&payload_len.to_be_bytes());
        }
    }
    Ok(())
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
mod tests;
