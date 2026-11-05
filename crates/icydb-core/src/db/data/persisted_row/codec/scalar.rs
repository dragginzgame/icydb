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

// Copy a fixed-width scalar payload only when its length matches the codec.
fn decode_fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], InternalError> {
    bytes
        .try_into()
        .map_err(|_| InternalError::persisted_row_decode_corruption())
}

// Decode the one-byte boolean scalar payload shared by raw scalar slots and
// generated scalar-field owners.
fn decode_bool_scalar_payload(bytes: &[u8]) -> Result<bool, InternalError> {
    let [value] = bytes else {
        return Err(InternalError::persisted_row_decode_corruption());
    };

    match *value {
        SCALAR_BOOL_FALSE_TAG => Ok(false),
        SCALAR_BOOL_TRUE_TAG => Ok(true),
        _ => Err(InternalError::persisted_row_decode_corruption()),
    }
}

// Decode the empty unit scalar payload shared by `()` and the public `Unit`
// wrapper without giving either owner its own copy of the same guard.
fn decode_unit_scalar_payload(bytes: &[u8]) -> Result<(), InternalError> {
    if !bytes.is_empty() {
        return Err(InternalError::persisted_row_decode_corruption());
    }

    Ok(())
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_i32_payload(bytes: &[u8]) -> Result<i32, InternalError> {
    Ok(i32::from_le_bytes(decode_fixed(bytes)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_i64_payload(bytes: &[u8]) -> Result<i64, InternalError> {
    Ok(i64::from_le_bytes(decode_fixed(bytes)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_u32_payload(bytes: &[u8]) -> Result<u32, InternalError> {
    Ok(u32::from_le_bytes(decode_fixed(bytes)?))
}

// Decode common little-endian scalar words through one fixed-width path.
fn decode_u64_payload(bytes: &[u8]) -> Result<u64, InternalError> {
    Ok(u64::from_le_bytes(decode_fixed(bytes)?))
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
fn scalar_value_payload_len(value: ScalarValueRef<'_>, codec: ScalarCodec) -> Option<usize> {
    Some(match value {
        ScalarValueRef::Blob(bytes) => bytes.len(),
        ScalarValueRef::Bool(_) => 1,
        ScalarValueRef::Date(_) | ScalarValueRef::Float32(_) => 4,
        ScalarValueRef::Duration(_) | ScalarValueRef::Float64(_) | ScalarValueRef::Timestamp(_) => {
            8
        }
        ScalarValueRef::Int(value) => match codec {
            ScalarCodec::Int8 => {
                i8::try_from(value).ok()?;
                1
            }
            ScalarCodec::Int16 => {
                i16::try_from(value).ok()?;
                2
            }
            ScalarCodec::Int32 => {
                i32::try_from(value).ok()?;
                4
            }
            ScalarCodec::Int64 => 8,
            _ => return None,
        },
        ScalarValueRef::Nat(value) => match codec {
            ScalarCodec::Nat8 => {
                u8::try_from(value).ok()?;
                1
            }
            ScalarCodec::Nat16 => {
                u16::try_from(value).ok()?;
                2
            }
            ScalarCodec::Nat32 => {
                u32::try_from(value).ok()?;
                4
            }
            ScalarCodec::Nat64 => 8,
            _ => return None,
        },
        ScalarValueRef::Principal(value) => value.as_slice().len(),
        ScalarValueRef::Subaccount(_) | ScalarValueRef::U256(_) => 32,
        ScalarValueRef::Text(value) => value.len(),
        ScalarValueRef::Ulid(_) => 16,
        ScalarValueRef::Unit => 0,
    })
}

// Encode one scalar slot value into the canonical prefixed scalar envelope.
pub(in crate::db::data::persisted_row) fn encode_scalar_slot_value(
    value: ScalarSlotValueRef<'_>,
    codec: ScalarCodec,
) -> Result<Vec<u8>, InternalError> {
    match value {
        ScalarSlotValueRef::Null => Ok(encode_null_slot_payload()),
        ScalarSlotValueRef::Value(value) => {
            // Validate the accepted integer range before taking its low little-endian
            // bytes. Reads sign/zero extend back into the existing runtime value family.
            let payload_len = scalar_value_payload_len(value, codec)
                .ok_or_else(InternalError::persisted_row_encode_internal)?;
            let mut encoded = Vec::with_capacity(2 + payload_len);
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
                ScalarValueRef::Int(value) => {
                    encoded.extend_from_slice(&value.to_le_bytes()[..payload_len]);
                }
                ScalarValueRef::Principal(value) => encoded.extend_from_slice(value.as_slice()),
                ScalarValueRef::Subaccount(value) => encoded.extend_from_slice(&value.to_bytes()),
                ScalarValueRef::Text(value) => encoded.extend_from_slice(value.as_bytes()),
                ScalarValueRef::Timestamp(value) => {
                    encoded.extend_from_slice(&value.as_millis().to_le_bytes());
                }
                ScalarValueRef::Nat(value) => {
                    encoded.extend_from_slice(&value.to_le_bytes()[..payload_len]);
                }
                ScalarValueRef::Ulid(value) => encoded.extend_from_slice(&value.to_bytes()),
                ScalarValueRef::Unit => {}
                ScalarValueRef::U256(value) => {
                    encoded.extend_from_slice(&value.to_be_bytes());
                }
            }

            Ok(encoded)
        }
    }
}

// Split one scalar slot envelope into `NULL` vs payload bytes.
fn decode_scalar_slot_payload_body(bytes: &[u8]) -> Result<Option<&[u8]>, InternalError> {
    let Some((&prefix, rest)) = bytes.split_first() else {
        return Err(InternalError::persisted_row_decode_corruption());
    };
    if prefix != SCALAR_SLOT_PREFIX {
        return Err(InternalError::persisted_row_decode_corruption());
    }
    let Some((&tag, payload)) = rest.split_first() else {
        return Err(InternalError::persisted_row_decode_corruption());
    };

    match tag {
        SCALAR_SLOT_TAG_NULL => {
            if !payload.is_empty() {
                return Err(InternalError::persisted_row_decode_corruption());
            }

            Ok(None)
        }
        SCALAR_SLOT_TAG_VALUE => Ok(Some(payload)),
        _ => Err(InternalError::persisted_row_decode_corruption()),
    }
}

// Decode one scalar slot view using the field-declared scalar codec.
pub(in crate::db::data::persisted_row) fn decode_scalar_slot_value(
    bytes: &[u8],
    codec: ScalarCodec,
) -> Result<ScalarSlotValueRef<'_>, InternalError> {
    let Some(payload) = decode_scalar_slot_payload_body(bytes)? else {
        return Ok(ScalarSlotValueRef::Null);
    };

    let value = match codec {
        ScalarCodec::Blob => ScalarValueRef::Blob(payload),
        ScalarCodec::Bool => ScalarValueRef::Bool(decode_bool_scalar_payload(payload)?),
        ScalarCodec::Date => {
            let days = decode_i32_payload(payload)?;
            ScalarValueRef::Date(
                Date::try_from_days_since_epoch(days)
                    .ok_or_else(InternalError::persisted_row_decode_corruption)?,
            )
        }
        ScalarCodec::Duration => {
            let millis = decode_u64_payload(payload)?;
            ScalarValueRef::Duration(Duration::from_millis(millis))
        }
        ScalarCodec::Float32 => {
            let value = f32::from_bits(decode_u32_payload(payload)?);
            let value = Float32::try_new(value)
                .ok_or_else(InternalError::persisted_row_decode_corruption)?;
            ScalarValueRef::Float32(value)
        }
        ScalarCodec::Float64 => {
            let value = f64::from_bits(decode_u64_payload(payload)?);
            let value = Float64::try_new(value)
                .ok_or_else(InternalError::persisted_row_decode_corruption)?;
            ScalarValueRef::Float64(value)
        }
        ScalarCodec::Int8 => {
            ScalarValueRef::Int(i64::from(i8::from_le_bytes(decode_fixed(payload)?)))
        }
        ScalarCodec::Int16 => {
            ScalarValueRef::Int(i64::from(i16::from_le_bytes(decode_fixed(payload)?)))
        }
        ScalarCodec::Int32 => {
            ScalarValueRef::Int(i64::from(i32::from_le_bytes(decode_fixed(payload)?)))
        }
        ScalarCodec::Int64 => ScalarValueRef::Int(decode_i64_payload(payload)?),
        ScalarCodec::Principal => ScalarValueRef::Principal(
            Principal::try_from_bytes(payload)
                .map_err(|_| InternalError::persisted_row_decode_corruption())?,
        ),
        ScalarCodec::Subaccount => {
            let bytes = decode_fixed(payload)?;
            ScalarValueRef::Subaccount(Subaccount::from_array(bytes))
        }
        ScalarCodec::Text => {
            let value = str::from_utf8(payload)
                .map_err(|_| InternalError::persisted_row_decode_corruption())?;
            ScalarValueRef::Text(value)
        }
        ScalarCodec::Timestamp => {
            let millis = decode_i64_payload(payload)?;
            ScalarValueRef::Timestamp(Timestamp::from_millis(millis))
        }
        ScalarCodec::Nat8 => {
            ScalarValueRef::Nat(u64::from(u8::from_le_bytes(decode_fixed(payload)?)))
        }
        ScalarCodec::Nat16 => {
            ScalarValueRef::Nat(u64::from(u16::from_le_bytes(decode_fixed(payload)?)))
        }
        ScalarCodec::Nat32 => {
            ScalarValueRef::Nat(u64::from(u32::from_le_bytes(decode_fixed(payload)?)))
        }
        ScalarCodec::Nat64 => ScalarValueRef::Nat(decode_u64_payload(payload)?),
        ScalarCodec::Ulid => {
            let bytes = decode_fixed(payload)?;
            ScalarValueRef::Ulid(Ulid::from_bytes(bytes))
        }
        ScalarCodec::Unit => {
            decode_unit_scalar_payload(payload)?;
            ScalarValueRef::Unit
        }
        ScalarCodec::U256 => ScalarValueRef::U256(U256::from_be_bytes(decode_fixed(payload)?)),
    };

    Ok(ScalarSlotValueRef::Value(value))
}

#[cfg(test)]
mod tests;
