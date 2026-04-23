use crate::{
    error::InternalError,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int128, Nat128, Principal, Subaccount,
        Timestamp, Ulid,
    },
};

use super::FieldDecodeError;

// Keep the typed leaf payload rules in one owner so sibling structural lanes
// can share semantic conversions without copying payload validation.

// Encode one account payload into the canonical raw bytes used by structural
// sibling lanes before they add their own outer framing.
pub(in crate::db) fn encode_account_payload_bytes(
    value: Account,
) -> Result<Vec<u8>, InternalError> {
    value
        .to_bytes()
        .map_err(InternalError::persisted_row_encode_failed)
}

// Decode one account payload from the canonical raw bytes shared by structural
// sibling lanes.
pub(in crate::db) fn decode_account_payload_bytes(
    bytes: &[u8],
) -> Result<Account, FieldDecodeError> {
    Account::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

// Encode one principal payload into its canonical raw byte form.
pub(in crate::db) fn encode_principal_payload_bytes(
    value: Principal,
) -> Result<Vec<u8>, InternalError> {
    value
        .stored_bytes()
        .map(<[u8]>::to_vec)
        .map_err(InternalError::persisted_row_encode_failed)
}

// Decode one principal payload from its canonical raw byte form.
pub(in crate::db) fn decode_principal_payload_bytes(
    bytes: &[u8],
) -> Result<Principal, FieldDecodeError> {
    Principal::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

// Encode one subaccount payload into its canonical fixed-width byte form.
pub(in crate::db) const fn encode_subaccount_payload_bytes(value: Subaccount) -> [u8; 32] {
    value.to_array()
}

// Decode one subaccount payload from its canonical fixed-width byte form.
pub(in crate::db) fn decode_subaccount_payload_bytes(
    bytes: &[u8],
) -> Result<Subaccount, FieldDecodeError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid subaccount payload"))?;

    Ok(Subaccount::from_array(bytes))
}

// Encode one timestamp payload into canonical millis.
pub(in crate::db) const fn encode_timestamp_payload_millis(value: Timestamp) -> i64 {
    value.as_millis()
}

// Decode one timestamp payload from canonical millis.
pub(in crate::db) const fn decode_timestamp_payload_millis(millis: i64) -> Timestamp {
    Timestamp::from_millis(millis)
}

// Encode one ULID payload into its canonical fixed-width byte form.
pub(in crate::db) fn encode_ulid_payload_bytes(value: Ulid) -> [u8; 16] {
    value.to_bytes()
}

// Decode one ULID payload from its canonical fixed-width byte form.
pub(in crate::db) fn decode_ulid_payload_bytes(bytes: &[u8]) -> Result<Ulid, FieldDecodeError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid ulid length"))?;

    Ok(Ulid::from_bytes(bytes))
}

// Encode one float32 payload into its canonical byte form.
pub(in crate::db) const fn encode_float32_payload_bytes(value: Float32) -> [u8; 4] {
    value.to_be_bytes()
}

// Decode one float32 payload from its canonical byte form.
pub(in crate::db) fn decode_float32_payload_bytes(
    bytes: &[u8],
) -> Result<Float32, FieldDecodeError> {
    Float32::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

// Encode one float64 payload into its canonical byte form.
pub(in crate::db) const fn encode_float64_payload_bytes(value: Float64) -> [u8; 8] {
    value.to_be_bytes()
}

// Decode one float64 payload from its canonical byte form.
pub(in crate::db) fn decode_float64_payload_bytes(
    bytes: &[u8],
) -> Result<Float64, FieldDecodeError> {
    Float64::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("structural binary: {err}")))
}

// Encode one int128 payload into its canonical fixed-width byte form.
pub(in crate::db) const fn encode_int128_payload_bytes(value: Int128) -> [u8; 16] {
    value.get().to_be_bytes()
}

// Decode one int128 payload from its canonical fixed-width byte form.
pub(in crate::db) fn decode_int128_payload_bytes(bytes: &[u8]) -> Result<Int128, FieldDecodeError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid int128 length"))?;

    Ok(Int128::from(i128::from_be_bytes(bytes)))
}

// Encode one nat128 payload into its canonical fixed-width byte form.
pub(in crate::db) const fn encode_nat128_payload_bytes(value: Nat128) -> [u8; 16] {
    value.get().to_be_bytes()
}

// Decode one nat128 payload from its canonical fixed-width byte form.
pub(in crate::db) fn decode_nat128_payload_bytes(bytes: &[u8]) -> Result<Nat128, FieldDecodeError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid uint128 length"))?;

    Ok(Nat128::from(u128::from_be_bytes(bytes)))
}

// Encode one date payload into canonical signed day-count form.
pub(in crate::db) fn encode_date_payload_days(value: Date) -> i64 {
    i64::from(value.as_days_since_epoch())
}

// Decode one date payload from canonical signed day-count form.
pub(in crate::db) fn decode_date_payload_days(days: i64) -> Result<Date, FieldDecodeError> {
    Date::try_from_i64(days)
        .ok_or_else(|| FieldDecodeError::new("structural binary: date day count out of range"))
}

// Encode one duration payload into canonical millis.
pub(in crate::db) const fn encode_duration_payload_millis(value: Duration) -> u64 {
    value.as_millis()
}

// Decode one duration payload from canonical millis.
pub(in crate::db) const fn decode_duration_payload_millis(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

// Split one decimal into the canonical `(mantissa, scale)` pair shared by the
// structural sibling lanes.
pub(in crate::db) const fn encode_decimal_payload_parts(value: Decimal) -> (i128, u32) {
    let parts = value.parts();
    (parts.mantissa(), parts.scale())
}

// Apply Decimal's mantissa/scale validation locally so all structural lanes
// share one normalization rule instead of drifting independently.
pub(in crate::db) fn decode_decimal_payload_parts(
    mantissa: i128,
    scale: u32,
) -> Result<Decimal, FieldDecodeError> {
    if scale <= Decimal::max_supported_scale() {
        return Ok(Decimal::from_i128_with_scale(mantissa, scale));
    }

    let mut value = mantissa;
    let mut normalized_scale = scale;
    while normalized_scale > Decimal::max_supported_scale() {
        if value == 0 {
            return Ok(Decimal::from_i128_with_scale(
                0,
                Decimal::max_supported_scale(),
            ));
        }
        if value % 10 != 0 {
            return Err(FieldDecodeError::new(
                "structural binary: invalid decimal payload",
            ));
        }
        value /= 10;
        normalized_scale -= 1;
    }

    Ok(Decimal::from_i128_with_scale(value, normalized_scale))
}
