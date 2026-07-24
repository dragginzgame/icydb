//! Module: data::structural_field::typed
//! Responsibility: typed wrapper payload conversion shared by structural codecs.
//! Does not own: outer Structural Binary framing, storage-key routing, or row reconstruction.
//! Boundary: converts between domain wrapper types and bounded owner-local payload bytes.

use crate::{
    error::InternalError,
    types::{
        Account, AccountStorageCodec, Date, Decimal, Duration, Float32, Float64, Principal,
        Subaccount, Timestamp, Ulid,
    },
};

use super::{
    FieldDecodeError,
    primitive::{encode_f32_payload_bytes, encode_f64_payload_bytes},
};

// Keep the typed leaf payload rules in one owner so sibling structural lanes
// can share semantic conversions without copying payload validation.

// Encode one account payload into the canonical raw bytes used by structural
// sibling lanes before they add their own outer framing.
pub(in crate::db::data::structural_field) fn encode_account_payload_bytes(
    value: Account,
) -> Result<Vec<u8>, InternalError> {
    value
        .to_bytes()
        .map_err(InternalError::persisted_row_encode_failed)
}

// Decode one account payload from the canonical raw bytes shared by structural
// sibling lanes.
pub(in crate::db::data::structural_field) fn decode_account_payload_bytes(
    bytes: &[u8],
) -> Result<Account, FieldDecodeError> {
    Account::try_from_bytes(bytes).map_err(|_| FieldDecodeError::new())
}

// Encode one principal payload into its canonical raw byte form.
pub(in crate::db::data::structural_field) fn encode_principal_payload_bytes(
    value: Principal,
) -> Result<Vec<u8>, InternalError> {
    value
        .stored_bytes()
        .map(<[u8]>::to_vec)
        .map_err(InternalError::persisted_row_encode_failed)
}

// Decode one principal payload from its canonical raw byte form.
pub(in crate::db::data::structural_field) fn decode_principal_payload_bytes(
    bytes: &[u8],
) -> Result<Principal, FieldDecodeError> {
    Principal::try_from_bytes(bytes).map_err(|_| FieldDecodeError::new())
}

// Encode one subaccount payload into its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) const fn encode_subaccount_payload_bytes(
    value: Subaccount,
) -> [u8; 32] {
    value.to_array()
}

// Decode one subaccount payload from its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) fn decode_subaccount_payload_bytes(
    bytes: &[u8],
) -> Result<Subaccount, FieldDecodeError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| FieldDecodeError::new())?;

    Ok(Subaccount::from_array(bytes))
}

// Encode one timestamp payload into canonical millis.
pub(in crate::db::data::structural_field) const fn encode_timestamp_payload_millis(
    value: Timestamp,
) -> i64 {
    value.as_millis()
}

// Decode one timestamp payload from canonical millis.
pub(in crate::db::data::structural_field) const fn decode_timestamp_payload_millis(
    millis: i64,
) -> Timestamp {
    Timestamp::from_millis(millis)
}

// Encode one ULID payload into its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) const fn encode_ulid_payload_bytes(
    value: Ulid,
) -> [u8; 16] {
    value.to_bytes()
}

// Decode one ULID payload from its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) fn decode_ulid_payload_bytes(
    bytes: &[u8],
) -> Result<Ulid, FieldDecodeError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| FieldDecodeError::new())?;

    Ok(Ulid::from_bytes(bytes))
}

// Encode one float32 payload into its canonical byte form.
pub(in crate::db::data::structural_field) const fn encode_float32_payload_bytes(
    value: Float32,
) -> [u8; 4] {
    encode_f32_payload_bytes(value.get())
}

// Decode one float32 payload from its canonical byte form.
pub(in crate::db::data::structural_field) fn decode_float32_payload_bytes(
    bytes: &[u8],
) -> Result<Float32, FieldDecodeError> {
    Float32::try_from_bytes(bytes).map_err(|_| FieldDecodeError::new())
}

// Encode one float64 payload into its canonical byte form.
pub(in crate::db::data::structural_field) const fn encode_float64_payload_bytes(
    value: Float64,
) -> [u8; 8] {
    encode_f64_payload_bytes(value.get())
}

// Decode one float64 payload from its canonical byte form.
pub(in crate::db::data::structural_field) fn decode_float64_payload_bytes(
    bytes: &[u8],
) -> Result<Float64, FieldDecodeError> {
    Float64::try_from_bytes(bytes).map_err(|_| FieldDecodeError::new())
}

// Encode one int128 payload into its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) const fn encode_int128_payload_bytes(
    value: i128,
) -> [u8; 16] {
    value.to_be_bytes()
}

// Decode one int128 payload from its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) fn decode_int128_payload_bytes(
    bytes: &[u8],
) -> Result<i128, FieldDecodeError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| FieldDecodeError::new())?;

    Ok(i128::from_be_bytes(bytes))
}

// Encode one nat128 payload into its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) const fn encode_nat128_payload_bytes(
    value: u128,
) -> [u8; 16] {
    value.to_be_bytes()
}

// Decode one nat128 payload from its canonical fixed-width byte form.
pub(in crate::db::data::structural_field) fn decode_nat128_payload_bytes(
    bytes: &[u8],
) -> Result<u128, FieldDecodeError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| FieldDecodeError::new())?;

    Ok(u128::from_be_bytes(bytes))
}

// Encode one date payload into canonical signed day-count form.
pub(in crate::db::data::structural_field) fn encode_date_payload_days(value: Date) -> i64 {
    i64::from(value.as_days_since_epoch())
}

// Decode one date payload from canonical signed day-count form.
pub(in crate::db::data::structural_field) fn decode_date_payload_days(
    days: i64,
) -> Result<Date, FieldDecodeError> {
    Date::try_from_i64(days).ok_or_else(FieldDecodeError::new)
}

// Encode one duration payload into canonical millis.
pub(in crate::db::data::structural_field) const fn encode_duration_payload_millis(
    value: Duration,
) -> u64 {
    value.as_millis()
}

// Decode one duration payload from canonical millis.
pub(in crate::db::data::structural_field) const fn decode_duration_payload_millis(
    millis: u64,
) -> Duration {
    Duration::from_millis(millis)
}

// Split one decimal into the canonical `(mantissa, scale)` pair shared by the
// structural sibling lanes.
pub(in crate::db::data::structural_field) const fn decimal_payload_mantissa_and_scale(
    value: Decimal,
) -> (i128, u32) {
    let decimal_parts = value.parts();
    (decimal_parts.mantissa(), decimal_parts.scale())
}

// Apply Decimal's mantissa/scale validation locally so all structural lanes
// share one normalization rule instead of drifting independently.
pub(in crate::db::data::structural_field) fn decode_decimal_payload_mantissa_and_scale(
    mantissa: i128,
    scale: u32,
) -> Result<Decimal, FieldDecodeError> {
    if scale <= Decimal::max_supported_scale() {
        return Decimal::try_from_i128_with_scale(mantissa, scale)
            .ok_or_else(FieldDecodeError::new);
    }

    let mut value = mantissa;
    let mut normalized_scale = scale;
    while normalized_scale > Decimal::max_supported_scale() {
        if value == 0 {
            return Decimal::try_from_i128_with_scale(0, Decimal::max_supported_scale())
                .ok_or_else(FieldDecodeError::new);
        }
        if value % 10 != 0 {
            return Err(FieldDecodeError::new());
        }
        value /= 10;
        normalized_scale -= 1;
    }

    Decimal::try_from_i128_with_scale(value, normalized_scale).ok_or_else(FieldDecodeError::new)
}
