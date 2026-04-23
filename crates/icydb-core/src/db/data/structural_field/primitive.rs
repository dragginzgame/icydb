use crate::db::data::structural_field::FieldDecodeError;

// Keep fixed-width primitive payload bytes under one owner so structural
// sibling lanes can share the same raw byte rules while keeping their own
// outer framing contracts.

// Decode one fixed-width primitive payload into its exact byte array.
fn decode_fixed_width_bytes<const N: usize>(
    bytes: &[u8],
    label: &'static str,
) -> Result<[u8; N], FieldDecodeError> {
    bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new(format!("structural binary: invalid {label} payload")))
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
    label: &'static str,
) -> Result<i64, FieldDecodeError> {
    Ok(i64::from_be_bytes(decode_fixed_width_bytes(bytes, label)?))
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
    label: &'static str,
) -> Result<u64, FieldDecodeError> {
    Ok(u64::from_be_bytes(decode_fixed_width_bytes(bytes, label)?))
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
