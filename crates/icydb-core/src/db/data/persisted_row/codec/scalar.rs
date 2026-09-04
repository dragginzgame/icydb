//! Module: db::data::persisted_row::codec::scalar
//! Responsibility: canonical scalar-slot encoding, decoding, and materialization.
//! Does not own: accepted-field policy or structural value decoding.
//! Boundary: persisted-row readers and writers use this scalar codec.

use crate::{
    db::{key_taxonomy::PrimaryKeyComponent, schema::ScalarCodec},
    error::InternalError,
    types::{Date, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256, Ulid},
    value::Value,
};
use std::str;

const SCALAR_SLOT_PREFIX: u8 = 0xFF;
const SCALAR_SLOT_TAG_NULL: u8 = 0;
const SCALAR_SLOT_TAG_VALUE: u8 = 1;

const SCALAR_BOOL_FALSE_TAG: u8 = 0;
const SCALAR_BOOL_TRUE_TAG: u8 = 1;

///
/// ScalarValueRef
///
/// ScalarValueRef is the borrowed-or-copy scalar payload view returned by the
/// slot-reader fast path.
/// It preserves cheap references for text/blob payloads while keeping fixed
/// width scalar wrappers as copy values.
///

#[derive(Clone, Copy, Debug)]
pub(crate) enum ScalarValueRef<'a> {
    Blob(&'a [u8]),
    Bool(bool),
    Date(Date),
    Duration(Duration),
    Float32(Float32),
    Float64(Float64),
    Int(i64),
    Principal(Principal),
    Subaccount(Subaccount),
    Text(&'a str),
    Timestamp(Timestamp),
    Nat(u64),
    Ulid(Ulid),
    Unit,
    U256(U256),
}

impl ScalarValueRef<'_> {
    /// Convert this scalar view into the canonical primary-key representation
    /// when its scalar family is primary-key compatible.
    #[must_use]
    pub(crate) const fn into_primary_key_component(self) -> Option<PrimaryKeyComponent> {
        match self {
            Self::Int(value) => Some(PrimaryKeyComponent::Int64(value)),
            Self::Principal(value) => Some(PrimaryKeyComponent::Principal(value)),
            Self::Subaccount(value) => Some(PrimaryKeyComponent::Subaccount(value)),
            Self::Timestamp(value) => Some(PrimaryKeyComponent::Timestamp(value)),
            Self::Nat(value) => Some(PrimaryKeyComponent::Nat64(value)),
            Self::Ulid(value) => Some(PrimaryKeyComponent::Ulid(value)),
            Self::Unit => Some(PrimaryKeyComponent::Unit),
            Self::U256(value) => Some(PrimaryKeyComponent::U256(value)),
            Self::Blob(_)
            | Self::Bool(_)
            | Self::Date(_)
            | Self::Duration(_)
            | Self::Float32(_)
            | Self::Float64(_)
            | Self::Text(_) => None,
        }
    }

    /// Materialize this scalar view into the runtime `Value` enum.
    #[must_use]
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::Blob(value) => Value::Blob(value.to_vec()),
            Self::Bool(value) => Value::Bool(value),
            Self::Date(value) => Value::Date(value),
            Self::Duration(value) => Value::Duration(value),
            Self::Float32(value) => Value::Float32(value),
            Self::Float64(value) => Value::Float64(value),
            Self::Int(value) => Value::Int64(value),
            Self::Principal(value) => Value::Principal(value),
            Self::Subaccount(value) => Value::Subaccount(value),
            Self::Text(value) => Value::Text(value.to_owned()),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Nat(value) => Value::Nat64(value),
            Self::Ulid(value) => Value::Ulid(value),
            Self::Unit => Value::Unit,
            Self::U256(value) => Value::U256(value),
        }
    }
}

///
/// ScalarSlotValueRef
///
/// ScalarSlotValueRef preserves the distinction between a missing slot and an
/// explicitly persisted `NULL` scalar payload.
/// The outer `Option` from `SlotReader::get_scalar` therefore still means
/// "slot absent".
///

#[derive(Clone, Copy, Debug)]
pub(crate) enum ScalarSlotValueRef<'a> {
    Null,
    Value(ScalarValueRef<'a>),
}

impl ScalarSlotValueRef<'_> {
    /// Materialize this nullable scalar-slot view into the runtime `Value` enum.
    #[must_use]
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Value(value) => value.into_value(),
        }
    }
}

// Copy a fixed-width scalar payload into an array while preserving the exact
// field/codec-specific length error used by each scalar owner.
fn decode_fixed<const N: usize>(bytes: &[u8], field_name: &str) -> Result<[u8; N], InternalError> {
    bytes
        .try_into()
        .map_err(|_| InternalError::persisted_row_field_payload_exact_len_required(field_name))
}

// Decode the one-byte boolean scalar payload shared by raw scalar slots and
// generated scalar-field owners.
fn decode_bool_scalar_payload(bytes: &[u8], field_name: &str) -> Result<bool, InternalError> {
    let [value] = bytes else {
        return Err(InternalError::persisted_row_field_payload_exact_len_required(field_name));
    };

    match *value {
        SCALAR_BOOL_FALSE_TAG => Ok(false),
        SCALAR_BOOL_TRUE_TAG => Ok(true),
        _ => Err(InternalError::persisted_row_field_payload_invalid_byte(
            field_name,
        )),
    }
}

// Decode the empty unit scalar payload shared by `()` and the public `Unit`
// wrapper without giving either owner its own copy of the same guard.
fn decode_unit_scalar_payload(bytes: &[u8], field_name: &str) -> Result<(), InternalError> {
    if !bytes.is_empty() {
        return Err(InternalError::persisted_row_field_payload_must_be_empty(
            field_name,
        ));
    }

    Ok(())
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_i32_payload(bytes: &[u8], field_name: &str) -> Result<i32, InternalError> {
    Ok(i32::from_le_bytes(decode_fixed(bytes, field_name)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_i64_payload(bytes: &[u8], field_name: &str) -> Result<i64, InternalError> {
    Ok(i64::from_le_bytes(decode_fixed(bytes, field_name)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_u32_payload(bytes: &[u8], field_name: &str) -> Result<u32, InternalError> {
    Ok(u32::from_le_bytes(decode_fixed(bytes, field_name)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_u64_payload(bytes: &[u8], field_name: &str) -> Result<u64, InternalError> {
    Ok(u64::from_le_bytes(decode_fixed(bytes, field_name)?))
}

// Write the two-byte scalar slot envelope prefix shared by generic scalar
// encoding and the hot direct scalar slot writer.
fn write_scalar_envelope_prefix(out: &mut Vec<u8>, is_null: bool) {
    out.push(SCALAR_SLOT_PREFIX);
    out.push(if is_null {
        SCALAR_SLOT_TAG_NULL
    } else {
        SCALAR_SLOT_TAG_VALUE
    });
}

// Encode the scalar-lane null sentinel while keeping the scalar envelope bytes
// owned by the scalar subsystem instead of the strategy root.
pub(in crate::db::data::persisted_row::codec) fn encode_null_slot_payload() -> Vec<u8> {
    let mut encoded = Vec::with_capacity(2);
    write_scalar_envelope_prefix(&mut encoded, true);

    encoded
}

// Compute the encoded scalar payload size before writing the slot envelope so
// the hot scalar writer can reserve exactly once for fixed-width values.
const fn scalar_value_payload_len(value: ScalarValueRef<'_>) -> usize {
    match value {
        ScalarValueRef::Blob(bytes) => bytes.len(),
        ScalarValueRef::Bool(_) => 1,
        ScalarValueRef::Date(_) | ScalarValueRef::Float32(_) => 4,
        ScalarValueRef::Duration(_)
        | ScalarValueRef::Float64(_)
        | ScalarValueRef::Int(_)
        | ScalarValueRef::Timestamp(_)
        | ScalarValueRef::Nat(_) => 8,
        ScalarValueRef::Principal(value) => value.as_slice().len(),
        ScalarValueRef::Subaccount(_) | ScalarValueRef::U256(_) => 32,
        ScalarValueRef::Text(value) => value.len(),
        ScalarValueRef::Ulid(_) => 16,
        ScalarValueRef::Unit => 0,
    }
}

// Encode one scalar slot value into the canonical prefixed scalar envelope.
pub(in crate::db::data::persisted_row) fn encode_scalar_slot_value(
    value: ScalarSlotValueRef<'_>,
) -> Vec<u8> {
    match value {
        ScalarSlotValueRef::Null => encode_null_slot_payload(),
        ScalarSlotValueRef::Value(value) => {
            let mut encoded = Vec::with_capacity(2 + scalar_value_payload_len(value));
            write_scalar_envelope_prefix(&mut encoded, false);

            match value {
                ScalarValueRef::Blob(bytes) => encoded.extend_from_slice(bytes),
                ScalarValueRef::Bool(value) => encoded.push(u8::from(value)),
                ScalarValueRef::Date(value) => {
                    encoded.extend_from_slice(&value.as_days_since_epoch().to_le_bytes());
                }
                ScalarValueRef::Duration(value) => {
                    encoded.extend_from_slice(&value.as_millis().to_le_bytes());
                }
                ScalarValueRef::Float32(value) => {
                    encoded.extend_from_slice(&value.get().to_bits().to_le_bytes());
                }
                ScalarValueRef::Float64(value) => {
                    encoded.extend_from_slice(&value.get().to_bits().to_le_bytes());
                }
                ScalarValueRef::Int(value) => encoded.extend_from_slice(&value.to_le_bytes()),
                ScalarValueRef::Principal(value) => encoded.extend_from_slice(value.as_slice()),
                ScalarValueRef::Subaccount(value) => encoded.extend_from_slice(&value.to_bytes()),
                ScalarValueRef::Text(value) => encoded.extend_from_slice(value.as_bytes()),
                ScalarValueRef::Timestamp(value) => {
                    encoded.extend_from_slice(&value.as_millis().to_le_bytes());
                }
                ScalarValueRef::Nat(value) => encoded.extend_from_slice(&value.to_le_bytes()),
                ScalarValueRef::Ulid(value) => encoded.extend_from_slice(&value.to_bytes()),
                ScalarValueRef::Unit => {}
                ScalarValueRef::U256(value) => {
                    encoded.extend_from_slice(&value.to_be_bytes());
                }
            }

            encoded
        }
    }
}

// Split one scalar slot envelope into `NULL` vs payload bytes.
fn decode_scalar_slot_payload_body<'a>(
    bytes: &'a [u8],
    field_name: &str,
) -> Result<Option<&'a [u8]>, InternalError> {
    let Some((&prefix, rest)) = bytes.split_first() else {
        return Err(InternalError::persisted_row_field_decode_corruption(
            field_name,
        ));
    };
    if prefix != SCALAR_SLOT_PREFIX {
        return Err(InternalError::persisted_row_field_decode_corruption(
            field_name,
        ));
    }
    let Some((&tag, payload)) = rest.split_first() else {
        return Err(InternalError::persisted_row_field_decode_corruption(
            field_name,
        ));
    };

    match tag {
        SCALAR_SLOT_TAG_NULL => {
            if !payload.is_empty() {
                return Err(InternalError::persisted_row_field_decode_corruption(
                    field_name,
                ));
            }

            Ok(None)
        }
        SCALAR_SLOT_TAG_VALUE => Ok(Some(payload)),
        _ => Err(InternalError::persisted_row_field_decode_corruption(
            field_name,
        )),
    }
}

// Decode one scalar slot view using the field-declared scalar codec.
pub(in crate::db::data::persisted_row) fn decode_scalar_slot_value<'a>(
    bytes: &'a [u8],
    codec: ScalarCodec,
    field_name: &str,
) -> Result<ScalarSlotValueRef<'a>, InternalError> {
    let Some(payload) = decode_scalar_slot_payload_body(bytes, field_name)? else {
        return Ok(ScalarSlotValueRef::Null);
    };

    let value = match codec {
        ScalarCodec::Blob => ScalarValueRef::Blob(payload),
        ScalarCodec::Bool => ScalarValueRef::Bool(decode_bool_scalar_payload(payload, field_name)?),
        ScalarCodec::Date => {
            let days = decode_i32_payload(payload, field_name)?;
            ScalarValueRef::Date(
                Date::try_from_days_since_epoch(days).ok_or_else(|| {
                    InternalError::persisted_row_field_decode_corruption(field_name)
                })?,
            )
        }
        ScalarCodec::Duration => {
            let millis = decode_u64_payload(payload, field_name)?;
            ScalarValueRef::Duration(Duration::from_millis(millis))
        }
        ScalarCodec::Float32 => {
            let value = f32::from_bits(decode_u32_payload(payload, field_name)?);
            let value = Float32::try_new(value)
                .ok_or_else(|| InternalError::persisted_row_field_payload_non_finite(field_name))?;
            ScalarValueRef::Float32(value)
        }
        ScalarCodec::Float64 => {
            let value = f64::from_bits(decode_u64_payload(payload, field_name)?);
            let value = Float64::try_new(value)
                .ok_or_else(|| InternalError::persisted_row_field_payload_non_finite(field_name))?;
            ScalarValueRef::Float64(value)
        }
        ScalarCodec::Int64 => ScalarValueRef::Int(decode_i64_payload(payload, field_name)?),
        ScalarCodec::Principal => ScalarValueRef::Principal(
            Principal::try_from_bytes(payload)
                .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?,
        ),
        ScalarCodec::Subaccount => {
            let bytes = decode_fixed(payload, field_name)?;
            ScalarValueRef::Subaccount(Subaccount::from_array(bytes))
        }
        ScalarCodec::Text => {
            let value = str::from_utf8(payload).map_err(|_| {
                InternalError::persisted_row_field_text_payload_invalid_utf8(field_name)
            })?;
            ScalarValueRef::Text(value)
        }
        ScalarCodec::Timestamp => {
            let millis = decode_i64_payload(payload, field_name)?;
            ScalarValueRef::Timestamp(Timestamp::from_millis(millis))
        }
        ScalarCodec::Nat64 => ScalarValueRef::Nat(decode_u64_payload(payload, field_name)?),
        ScalarCodec::Ulid => {
            let bytes = decode_fixed(payload, field_name)?;
            ScalarValueRef::Ulid(Ulid::from_bytes(bytes))
        }
        ScalarCodec::Unit => {
            decode_unit_scalar_payload(payload, field_name)?;
            ScalarValueRef::Unit
        }
        ScalarCodec::U256 => {
            ScalarValueRef::U256(U256::from_be_bytes(decode_fixed(payload, field_name)?))
        }
    };

    Ok(ScalarSlotValueRef::Value(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_date_slot(days: i32) -> Vec<u8> {
        let mut encoded = vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE];
        encoded.extend_from_slice(&days.to_le_bytes());
        encoded
    }

    #[test]
    fn date_slot_decode_rejects_days_outside_bounded_calendar() {
        let valid = encoded_date_slot(Date::MAX.as_days_since_epoch());
        let invalid = encoded_date_slot(Date::MAX.as_days_since_epoch() + 1);

        assert!(matches!(
            decode_scalar_slot_value(&valid, ScalarCodec::Date, "created_on"),
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Date(Date::MAX))),
        ));
        assert!(decode_scalar_slot_value(&invalid, ScalarCodec::Date, "created_on").is_err());
    }

    #[test]
    fn scalar_slot_materialization_preserves_null_and_payload_values() {
        assert_eq!(ScalarSlotValueRef::Null.into_value(), Value::Null);
        assert_eq!(
            ScalarSlotValueRef::Value(ScalarValueRef::Text("current")).into_value(),
            Value::Text("current".to_string()),
        );
        assert_eq!(
            ScalarSlotValueRef::Value(ScalarValueRef::Nat(42)).into_value(),
            Value::Nat64(42),
        );
    }

    #[test]
    fn scalar_primary_key_conversion_keeps_one_supported_family_authority() {
        assert_eq!(
            ScalarValueRef::Int(-7).into_primary_key_component(),
            Some(PrimaryKeyComponent::Int64(-7)),
        );
        assert_eq!(
            ScalarValueRef::Nat(42).into_primary_key_component(),
            Some(PrimaryKeyComponent::Nat64(42)),
        );
        assert_eq!(
            ScalarValueRef::Unit.into_primary_key_component(),
            Some(PrimaryKeyComponent::Unit),
        );
        assert_eq!(
            ScalarValueRef::U256(U256::ONE).into_primary_key_component(),
            Some(PrimaryKeyComponent::U256(U256::ONE)),
        );
        assert_eq!(
            ScalarValueRef::Text("not-a-key").into_primary_key_component(),
            None,
        );
    }

    #[test]
    fn time_scalar_slots_roundtrip_exact_primitive_payloads() {
        let duration = Duration::from_millis(u64::MAX);
        let encoded_duration = encode_scalar_slot_value(ScalarSlotValueRef::Value(
            ScalarValueRef::Duration(duration),
        ));
        assert_eq!(&encoded_duration[2..], &duration.as_millis().to_le_bytes());
        assert!(matches!(
            decode_scalar_slot_value(&encoded_duration, ScalarCodec::Duration, "elapsed"),
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Duration(decoded))) if decoded == duration,
        ));

        let timestamp = Timestamp::from_millis(i64::MIN);
        let encoded_timestamp = encode_scalar_slot_value(ScalarSlotValueRef::Value(
            ScalarValueRef::Timestamp(timestamp),
        ));
        assert_eq!(
            &encoded_timestamp[2..],
            &timestamp.as_millis().to_le_bytes()
        );
        assert!(matches!(
            decode_scalar_slot_value(&encoded_timestamp, ScalarCodec::Timestamp, "created_at"),
            Ok(ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(decoded))) if decoded == timestamp,
        ));
    }

    #[test]
    fn u256_scalar_slot_roundtrips_exact_fixed_width_payload() {
        for value in [U256::ZERO, U256::ONE, U256::MAX] {
            let encoded =
                encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::U256(value)));

            assert_eq!(encoded.len(), 34);
            assert_eq!(&encoded[..2], &[SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE]);
            assert_eq!(&encoded[2..], value.to_be_bytes());
            assert!(matches!(
                decode_scalar_slot_value(&encoded, ScalarCodec::U256, "amount"),
                Ok(ScalarSlotValueRef::Value(ScalarValueRef::U256(decoded))) if decoded == value,
            ));
        }
    }

    #[test]
    fn u256_scalar_slot_rejects_non_exact_payload_lengths() {
        for payload_len in [31, 33] {
            let mut encoded = vec![SCALAR_SLOT_PREFIX, SCALAR_SLOT_TAG_VALUE];
            encoded.resize(2 + payload_len, 0);

            assert!(decode_scalar_slot_value(&encoded, ScalarCodec::U256, "amount").is_err());
        }
    }
}
