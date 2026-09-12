//! Shared Serde integer ingress and Candid-compatible binary byte bodies.
//!
//! Atom values choose native small integers or tagged wide bodies. Transport and
//! accepted-field owners retain their own byte budgets; this module owns no
//! additional numeric-domain limit or persisted database format.

use std::{fmt, marker::PhantomData, str::FromStr};

use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};

/// Atom-specific conversion after the transport has supplied one scalar frame.
pub(crate) trait IntegerWire: FromStr {
    fn from_signed(value: i64) -> Option<Self>;
    fn from_unsigned(value: u64) -> Self;
    fn from_wire_bytes(value: &[u8]) -> Option<Self>;
}

struct IntegerVisitor<T>(PhantomData<T>);

impl<T: IntegerWire> Visitor<'_> for IntegerVisitor<T> {
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an integer or canonical tagged integer bytes")
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<T, E> {
        T::from_signed(value).ok_or_else(|| E::invalid_value(de::Unexpected::Signed(value), &self))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<T, E> {
        Ok(T::from_unsigned(value))
    }

    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<T, E> {
        T::from_wire_bytes(value)
            .ok_or_else(|| E::invalid_value(de::Unexpected::Other("invalid integer bytes"), &self))
    }

    fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<T, E> {
        self.visit_bytes(&value)
    }
}

/// Keep human text distinct from the native/binary visitor used by Candid.
pub(crate) fn deserialize_integer<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: IntegerWire,
    T::Err: fmt::Display,
{
    if deserializer.is_human_readable() {
        return String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom);
    }
    deserializer.deserialize_any(IntegerVisitor(PhantomData))
}

/// Validate the marker and minimal unsigned little-endian body before conversion.
pub(crate) fn unsigned_body(bytes: &[u8]) -> Option<&[u8]> {
    let (&marker, body) = bytes.split_first()?;
    if marker != 1 || body.is_empty() || (body.len() > 1 && body.last() == Some(&0)) {
        return None;
    }
    Some(body)
}

/// Validate minimal two's-complement sign extension before bigint construction.
pub(crate) fn signed_body(bytes: &[u8]) -> Option<&[u8]> {
    let (&marker, body) = bytes.split_first()?;
    if marker != 0 || body.is_empty() {
        return None;
    }
    if body.len() > 1 {
        let high = body[body.len() - 1];
        let next = body[body.len() - 2];
        if (high == 0 && next & 0x80 == 0) || (high == 0xff && next & 0x80 != 0) {
            return None;
        }
    }
    Some(body)
}

/// Append borrowed limbs directly into the sole tagged unsigned output buffer.
pub(crate) fn unsigned_bytes(limbs: impl Iterator<Item = u32>) -> Vec<u8> {
    let mut bytes = vec![1];
    for limb in limbs {
        bytes.extend_from_slice(&limb.to_le_bytes());
    }
    if bytes.len() == 1 {
        bytes.push(0);
    }
    while bytes.len() > 2 && bytes.last() == Some(&0) {
        bytes.pop();
    }
    bytes
}

/// Form signed bytes from borrowed magnitude limbs without another payload Vec.
pub(crate) fn signed_bytes(negative: bool, limbs: impl Iterator<Item = u32>) -> Vec<u8> {
    let mut bytes = vec![0];
    let mut carry = negative;
    for magnitude in limbs {
        let limb = if negative {
            let (limb, overflow) = (!magnitude).overflowing_add(u32::from(carry));
            carry = overflow;
            limb
        } else {
            magnitude
        };
        bytes.extend_from_slice(&limb.to_le_bytes());
    }
    // Explicit extension supplies the sign even for a full high limb.
    bytes.push(if negative { 0xff } else { 0 });
    while bytes.len() > 2 {
        let high = bytes[bytes.len() - 1];
        let next = bytes[bytes.len() - 2];
        if (high == 0 && next & 0x80 == 0) || (high == 0xff && next & 0x80 != 0) {
            bytes.pop();
        } else {
            break;
        }
    }
    bytes
}
