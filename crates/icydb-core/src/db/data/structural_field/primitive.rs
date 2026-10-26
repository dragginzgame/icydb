//! Module: data::structural_field::primitive
//! Responsibility: fixed-width primitive conversion and accepted integer bounds shared by structural codecs.
//! Does not own: outer Structural Binary framing, runtime `Value` materialization, or row policy.
//! Boundary: sibling lanes share canonical primitive payload bytes and declared integer ranges.

use crate::db::{data::structural_field::FieldDecodeError, schema::AcceptedFieldKind};

// Keep fixed-width primitive payload bytes under one owner so structural
// sibling lanes can share the same raw byte rules while keeping their own
// outer framing contracts.

// Accepted integer widths are shared by value and relation-key decoding.
pub(in crate::db::data::structural_field) const fn fixed_int_kind_accepts_value(
    kind: &AcceptedFieldKind,
    value: i64,
) -> bool {
    match kind {
        AcceptedFieldKind::Int64 => true,
        AcceptedFieldKind::Int8 => value >= i8::MIN as i64 && value <= i8::MAX as i64,
        AcceptedFieldKind::Int16 => value >= i16::MIN as i64 && value <= i16::MAX as i64,
        AcceptedFieldKind::Int32 => value >= i32::MIN as i64 && value <= i32::MAX as i64,
        _ => false,
    }
}

pub(in crate::db::data::structural_field) const fn fixed_nat_kind_accepts_value(
    kind: &AcceptedFieldKind,
    value: u64,
) -> bool {
    match kind {
        AcceptedFieldKind::Nat64 => true,
        AcceptedFieldKind::Nat8 => value <= u8::MAX as u64,
        AcceptedFieldKind::Nat16 => value <= u16::MAX as u64,
        AcceptedFieldKind::Nat32 => value <= u32::MAX as u64,
        _ => false,
    }
}

// Decode one fixed-width primitive payload into its exact byte array.
fn decode_fixed_width_bytes<const N: usize>(bytes: &[u8]) -> Result<[u8; N], FieldDecodeError> {
    bytes.try_into().map_err(|_| FieldDecodeError::new())
}

// Encode one fixed-width signed integer payload into canonical big-endian
// bytes shared by structural sibling lanes.
pub(in crate::db::data::structural_field) const fn encode_i64_payload_bytes(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}

// Decode one fixed-width signed integer payload from canonical big-endian
// bytes shared by structural sibling lanes.
pub(in crate::db::data::structural_field) fn decode_i64_payload_bytes(
    bytes: &[u8],
) -> Result<i64, FieldDecodeError> {
    Ok(i64::from_be_bytes(decode_fixed_width_bytes(bytes)?))
}

// Encode one fixed-width unsigned integer payload into canonical big-endian
// bytes shared by structural sibling lanes.
pub(in crate::db::data::structural_field) const fn encode_u64_payload_bytes(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

// Decode one fixed-width unsigned integer payload from canonical big-endian
// bytes shared by structural sibling lanes.
pub(in crate::db::data::structural_field) fn decode_u64_payload_bytes(
    bytes: &[u8],
) -> Result<u64, FieldDecodeError> {
    Ok(u64::from_be_bytes(decode_fixed_width_bytes(bytes)?))
}

// Encode one fixed-width float32 payload into canonical IEEE-754 big-endian
// bytes shared by structural sibling lanes.
pub(in crate::db::data::structural_field) const fn encode_f32_payload_bytes(value: f32) -> [u8; 4] {
    value.to_bits().to_be_bytes()
}

// Encode one fixed-width float64 payload into canonical IEEE-754 big-endian
// bytes shared by structural sibling lanes.
pub(in crate::db::data::structural_field) const fn encode_f64_payload_bytes(value: f64) -> [u8; 8] {
    value.to_bits().to_be_bytes()
}
