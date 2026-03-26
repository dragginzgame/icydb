//! Module: data::structural_field::leaf
//! Responsibility: typed wrapper and structured leaf decoding that still has fixed payload semantics.
//! Does not own: raw CBOR walking, scalar primitive fast paths, or `Value` storage envelopes.
//! Boundary: sibling modules use this file for typed payloads like account, timestamp, bigint, decimal, and subaccount.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::cbor::{
    cbor_text_literal_eq, decode_cbor_integer, decode_text_scalar_bytes, parse_tagged_cbor_head,
    payload_bytes, skip_cbor_value, walk_cbor_map_entries,
};
use crate::db::data::structural_field::storage_key::decode_unit_storage_key_bytes;
use crate::{
    types::{Account, Date, Decimal, Duration, Int, Nat, Timestamp},
    value::Value,
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};

/// Decode one non-recursive `ByKind` field payload.
///
/// Leaf decoders never recurse back into the structural-field root. Composite
/// kinds stay in the composite lane so recursive re-entry has one owner.
pub(super) fn decode_leaf_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: crate::model::field::FieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    let value = match kind {
        crate::model::field::FieldKind::Account => decode_account_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Blob
        | crate::model::field::FieldKind::Bool
        | crate::model::field::FieldKind::Float32
        | crate::model::field::FieldKind::Float64
        | crate::model::field::FieldKind::Int
        | crate::model::field::FieldKind::Int128
        | crate::model::field::FieldKind::Text
        | crate::model::field::FieldKind::Uint
        | crate::model::field::FieldKind::Uint128
        | crate::model::field::FieldKind::Ulid => {
            return Err(FieldDecodeError::new(
                "scalar field unexpectedly bypassed byte-level fast path",
            ));
        }
        crate::model::field::FieldKind::Date => decode_date_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Decimal { .. } => decode_decimal_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Duration => decode_duration_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::IntBig => decode_int_big_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Principal => decode_principal_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Structured { .. } => Value::Null,
        crate::model::field::FieldKind::Subaccount => decode_subaccount_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Timestamp => decode_timestamp_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::UintBig => decode_uint_big_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Unit => decode_unit_value_bytes(raw_bytes)?,
        crate::model::field::FieldKind::Enum { .. }
        | crate::model::field::FieldKind::List(_)
        | crate::model::field::FieldKind::Map { .. }
        | crate::model::field::FieldKind::Relation { .. }
        | crate::model::field::FieldKind::Set(_) => return Ok(None),
    };

    Ok(Some(value))
}

// Carry the partially decoded account payload while the shared map walker
// visits account fields.
type AccountDecodeState = (
    Option<crate::types::Principal>,
    Option<crate::types::Subaccount>,
);

// Push one decoded account field into the running account payload state.
//
// Safety:
// `context` must be a valid `AccountDecodeState`.
fn push_account_field(
    key_bytes: &[u8],
    value_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<AccountDecodeState>() };
    if cbor_text_literal_eq(key_bytes, b"owner")? {
        state.0 = Some(decode_principal_payload(value_bytes)?);
    } else if cbor_text_literal_eq(key_bytes, b"subaccount")? {
        state.1 = decode_optional_subaccount_value(value_bytes)?;
    }

    Ok(())
}

// Decode one date payload from its persisted CBOR text form.
pub(super) fn decode_date_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let text = decode_required_text_payload(raw_bytes, "date")?;

    Date::parse(text).map(Value::Date).ok_or_else(|| {
        FieldDecodeError::new(format!("typed CBOR decode failed: invalid date: {text}"))
    })
}

// Decode one account payload from its persisted CBOR struct form.
pub(super) fn decode_account_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_account_payload(raw_bytes).map(Value::Account)
}

// Decode one account payload from its persisted CBOR struct form.
pub(super) fn decode_account_payload(raw_bytes: &[u8]) -> Result<Account, FieldDecodeError> {
    let mut state: AccountDecodeState = (None, None);
    walk_cbor_map_entries(
        raw_bytes,
        "expected CBOR map for account payload",
        "typed CBOR decode failed: trailing bytes after account payload",
        (&raw mut state).cast(),
        push_account_field,
    )?;

    let owner = state.0.ok_or_else(|| {
        FieldDecodeError::new("typed CBOR decode failed: missing account owner field")
    })?;

    Ok(Account::from_parts(owner, state.1))
}

// Decode one decimal payload from its persisted binary-or-text CBOR form.
pub(super) fn decode_decimal_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let (major, _, _) = parse_complete_top_level_payload(raw_bytes, "decimal")?;

    let value = match major {
        3 => decode_required_text_payload(raw_bytes, "decimal")?
            .parse::<Decimal>()
            .map_err(|err| FieldDecodeError::new(format!("typed CBOR decode failed: {err}")))?,
        4 => decode_decimal_binary_payload(raw_bytes)?,
        _ => {
            return Err(FieldDecodeError::new(
                "typed CBOR decode failed: expected decimal text or binary tuple",
            ));
        }
    };

    Ok(Value::Decimal(value))
}

// Decode one duration payload from its persisted integer-or-string CBOR form.
pub(super) fn decode_duration_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let (major, argument, payload_start) = parse_complete_top_level_payload(raw_bytes, "duration")?;

    let value = match major {
        0 => Duration::from_millis(argument),
        3 => Duration::parse_flexible(decode_text_scalar_bytes(
            raw_bytes,
            argument,
            payload_start,
        )?)
        .map_err(|err| FieldDecodeError::new(format!("typed CBOR decode failed: {err}")))?,
        _ => {
            return Err(FieldDecodeError::new(
                "typed CBOR decode failed: expected duration millis or string",
            ));
        }
    };

    Ok(Value::Duration(value))
}

// Decode one timestamp payload from its persisted integer-or-string CBOR form.
pub(super) fn decode_timestamp_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_timestamp_payload(raw_bytes).map(Value::Timestamp)
}

// Decode one timestamp payload from its persisted integer-or-string CBOR form.
pub(super) fn decode_timestamp_payload(raw_bytes: &[u8]) -> Result<Timestamp, FieldDecodeError> {
    let (major, argument, payload_start) =
        parse_complete_top_level_payload(raw_bytes, "timestamp")?;

    let value = match major {
        0 | 1 => {
            let millis = i64::try_from(decode_cbor_integer(major, argument)?).map_err(|_| {
                FieldDecodeError::new("typed CBOR decode failed: timestamp out of i64 range")
            })?;
            Timestamp::from_millis(millis)
        }
        3 => Timestamp::parse_flexible(decode_text_scalar_bytes(
            raw_bytes,
            argument,
            payload_start,
        )?)
        .map_err(|err| FieldDecodeError::new(format!("typed CBOR decode failed: {err}")))?,
        _ => {
            return Err(FieldDecodeError::new(
                "typed CBOR decode failed: expected unix millis or RFC3339 string",
            ));
        }
    };

    Ok(value)
}

// Decode one arbitrary-precision signed integer payload from its persisted
// CBOR `(sign, limbs)` tuple.
pub(super) fn decode_int_big_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let (sign, magnitude) = decode_bigint_tuple_payload(raw_bytes)?;
    let wrapped = WrappedInt::from(BigInt::from_biguint(sign, magnitude));

    Ok(Value::IntBig(Int::from(wrapped)))
}

// Decode one arbitrary-precision unsigned integer payload from its persisted
// CBOR limb sequence.
pub(super) fn decode_uint_big_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    let wrapped = WrappedNat::from(decode_biguint_payload(raw_bytes)?);

    Ok(Value::UintBig(Nat::from(wrapped)))
}

// Decode one principal payload from its persisted CBOR byte-string form.
pub(super) fn decode_principal_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_principal_payload(raw_bytes).map(Value::Principal)
}

// Decode one principal payload from its persisted CBOR byte-string form.
pub(super) fn decode_principal_payload(
    raw_bytes: &[u8],
) -> Result<crate::types::Principal, FieldDecodeError> {
    let bytes = decode_required_bytes_payload(raw_bytes, "principal")?;
    crate::types::Principal::try_from_bytes(bytes)
        .map_err(|err| FieldDecodeError::new(format!("typed CBOR decode failed: {err}")))
}

// Decode one subaccount payload from its persisted CBOR sequence or byte-string
// form.
pub(super) fn decode_subaccount_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_subaccount_payload(raw_bytes).map(Value::Subaccount)
}

// Decode one subaccount payload from its persisted CBOR sequence or byte-string
// form.
pub(super) fn decode_subaccount_payload(
    raw_bytes: &[u8],
) -> Result<crate::types::Subaccount, FieldDecodeError> {
    let bytes = decode_subaccount_payload_bytes(raw_bytes)?;

    Ok(crate::types::Subaccount::from_array(bytes))
}

// Decode one optional subaccount field, treating explicit null as absence.
fn decode_optional_subaccount_value(
    raw_bytes: &[u8],
) -> Result<Option<crate::types::Subaccount>, FieldDecodeError> {
    let (major, argument, _) = parse_complete_top_level_payload(raw_bytes, "subaccount")?;
    if major == 7 && argument == 22 {
        return Ok(None);
    }

    decode_subaccount_payload(raw_bytes).map(Some)
}

// Decode one decimal binary payload tuple `(mantissa_bytes, scale)`.
fn decode_decimal_binary_payload(raw_bytes: &[u8]) -> Result<Decimal, FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated decimal payload",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: expected decimal binary tuple",
        ));
    }

    let mantissa_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    let scale_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after decimal payload",
        ));
    }

    let mantissa_bytes: [u8; 16] =
        decode_required_bytes_payload(&raw_bytes[mantissa_start..scale_start], "decimal mantissa")?
            .try_into()
            .map_err(|_| {
                FieldDecodeError::new(
                    "typed CBOR decode failed: invalid decimal mantissa length: 16 bytes expected",
                )
            })?;
    let scale = decode_required_u32_payload(&raw_bytes[scale_start..cursor], "decimal scale")?;

    decode_decimal_mantissa_scale(i128::from_be_bytes(mantissa_bytes), scale)
}

// Decode one `(sign, magnitude)` tuple into a `BigInt` construction pair.
fn decode_bigint_tuple_payload(
    raw_bytes: &[u8],
) -> Result<(BigIntSign, BigUint), FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated bigint payload",
        ));
    };
    if major != 4 || argument != 2 {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: expected bigint sign/magnitude tuple",
        ));
    }

    let sign_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    let magnitude_start = cursor;
    cursor = skip_cbor_value(raw_bytes, cursor)?;
    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after bigint payload",
        ));
    }

    let sign = decode_bigint_sign_payload(&raw_bytes[sign_start..magnitude_start])?;
    let magnitude = decode_biguint_payload(&raw_bytes[magnitude_start..cursor])?;

    Ok((sign, magnitude))
}

// Decode one bigint sign payload serialized as -1, 0, or 1.
fn decode_bigint_sign_payload(raw_bytes: &[u8]) -> Result<BigIntSign, FieldDecodeError> {
    let (major, argument, _) = parse_complete_top_level_payload(raw_bytes, "bigint sign")?;

    match decode_cbor_integer(major, argument)? {
        -1 => Ok(BigIntSign::Minus),
        0 => Ok(BigIntSign::NoSign),
        1 => Ok(BigIntSign::Plus),
        other => Err(FieldDecodeError::new(format!(
            "typed CBOR decode failed: invalid bigint sign {other}"
        ))),
    }
}

// Decode one biguint payload serialized as a sequence of base-2^32 limbs.
fn decode_biguint_payload(raw_bytes: &[u8]) -> Result<BigUint, FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated biguint payload",
        ));
    };
    if major != 4 {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: expected biguint limb sequence",
        ));
    }

    let limb_count = usize::try_from(argument)
        .map_err(|_| FieldDecodeError::new("expected bounded CBOR array length"))?;
    let mut limbs = Vec::with_capacity(limb_count);

    for _ in 0..limb_count {
        let limb_start = cursor;
        cursor = skip_cbor_value(raw_bytes, cursor)?;
        limbs.push(decode_required_u32_payload(
            &raw_bytes[limb_start..cursor],
            "biguint limb",
        )?);
    }

    if cursor != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: trailing bytes after biguint payload",
        ));
    }

    Ok(BigUint::new(limbs))
}

// Decode one unit payload from its persisted CBOR null form.
pub(super) fn decode_unit_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_unit_storage_key_bytes(raw_bytes)?;

    Ok(Value::Unit)
}

// Decode one null payload from its persisted CBOR null form.
pub(super) fn decode_null_value_bytes(raw_bytes: &[u8]) -> Result<Value, FieldDecodeError> {
    decode_unit_storage_key_bytes(raw_bytes)?;

    Ok(Value::Null)
}

// Decode one required top-level text payload and enforce full-byte consumption.
fn decode_required_text_payload<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a str, FieldDecodeError> {
    let (major, argument, payload_start) = parse_complete_top_level_payload(raw_bytes, label)?;
    if major != 3 {
        return Err(FieldDecodeError::new(format!(
            "typed CBOR decode failed: expected a text string for {label}"
        )));
    }

    decode_text_scalar_bytes(raw_bytes, argument, payload_start)
}

// Decode one required top-level byte-string payload and enforce full-byte
// consumption.
fn decode_required_bytes_payload<'a>(
    raw_bytes: &'a [u8],
    label: &'static str,
) -> Result<&'a [u8], FieldDecodeError> {
    let (major, argument, payload_start) = parse_complete_top_level_payload(raw_bytes, label)?;
    if major != 2 {
        return Err(FieldDecodeError::new(format!(
            "typed CBOR decode failed: expected a byte string for {label}"
        )));
    }

    payload_bytes(raw_bytes, argument, payload_start, "byte string")
}

// Decode one required top-level unsigned-32 payload and enforce full-byte
// consumption.
fn decode_required_u32_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<u32, FieldDecodeError> {
    let (major, argument, _) = parse_complete_top_level_payload(raw_bytes, label)?;
    if major != 0 {
        return Err(FieldDecodeError::new(format!(
            "typed CBOR decode failed: expected unsigned integer for {label}"
        )));
    }

    u32::try_from(argument).map_err(|_| {
        FieldDecodeError::new(format!(
            "typed CBOR decode failed: {label} out of u32 range"
        ))
    })
}

// Parse one top-level CBOR payload and enforce that it consumes the whole
// provided byte slice.
fn parse_complete_top_level_payload(
    raw_bytes: &[u8],
    label: &'static str,
) -> Result<(u8, u64, usize), FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(format!(
            "typed CBOR decode failed: truncated {label} payload"
        )));
    };

    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(format!(
            "typed CBOR decode failed: trailing bytes after {label} payload"
        )));
    }

    Ok((major, argument, payload_start))
}

// Apply Decimal's binary mantissa/scale validation without routing through
// serde.
fn decode_decimal_mantissa_scale(mantissa: i128, scale: u32) -> Result<Decimal, FieldDecodeError> {
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
                "typed CBOR decode failed: invalid decimal binary payload",
            ));
        }
        value /= 10;
        normalized_scale -= 1;
    }

    Ok(Decimal::from_i128_with_scale(value, normalized_scale))
}

// Decode one subaccount payload as either the derived 32-item byte array shape
// or an equivalent raw byte string.
fn decode_subaccount_payload_bytes(raw_bytes: &[u8]) -> Result<[u8; 32], FieldDecodeError> {
    let Some((major, argument, mut cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "typed CBOR decode failed: truncated subaccount payload",
        ));
    };

    match major {
        2 => decode_required_bytes_payload(raw_bytes, "subaccount")?
            .try_into()
            .map_err(|_| {
                FieldDecodeError::new(
                    "typed CBOR decode failed: expected 32 bytes for subaccount payload",
                )
            }),
        4 => {
            if argument != 32 {
                return Err(FieldDecodeError::new(
                    "typed CBOR decode failed: expected 32-byte array for subaccount payload",
                ));
            }

            let mut bytes = [0u8; 32];
            for byte in &mut bytes {
                let item_start = cursor;
                cursor = skip_cbor_value(raw_bytes, cursor)?;
                let Some((item_major, item_argument, _)) =
                    parse_tagged_cbor_head(&raw_bytes[item_start..cursor], 0)?
                else {
                    return Err(FieldDecodeError::new(
                        "typed CBOR decode failed: truncated subaccount item",
                    ));
                };
                if item_major != 0 {
                    return Err(FieldDecodeError::new(
                        "typed CBOR decode failed: expected unsigned byte in subaccount payload",
                    ));
                }
                *byte = u8::try_from(item_argument).map_err(|_| {
                    FieldDecodeError::new("typed CBOR decode failed: subaccount byte out of range")
                })?;
            }

            if cursor != raw_bytes.len() {
                return Err(FieldDecodeError::new(
                    "typed CBOR decode failed: trailing bytes after subaccount payload",
                ));
            }

            Ok(bytes)
        }
        _ => Err(FieldDecodeError::new(
            "typed CBOR decode failed: expected byte string or byte array for subaccount payload",
        )),
    }
}
