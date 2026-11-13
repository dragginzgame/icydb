//! Accepted persisted-row field encoding and decode adapters.
//!
//! Production writes and test fixtures enter through accepted field contracts.

use crate::{
    db::schema::{AcceptedFieldKind, FieldStorageDecode, LeafCodec},
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, StructuralRowContract,
            decode_structural_field_by_accepted_kind_bytes, decode_structural_value_storage_bytes,
            validate_structural_field_by_accepted_kind_bytes,
            validate_structural_value_storage_bytes, value_storage_bytes_are_null,
        },
        schema::AcceptedFieldDecodeContract,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

use crate::db::data::persisted_row::codec::{
    ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value,
};

/// Decode one slot payload through an accepted-schema field contract.
///
/// It keeps accepted `AcceptedFieldKind` metadata intact for recursive
/// payloads.
pub(in crate::db) fn decode_runtime_value_from_accepted_field_contract(
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    match field.leaf_codec() {
        LeafCodec::Scalar(_) => Ok(decode_scalar_slot_value_from_accepted_field_contract(
            field, raw_value,
        )?
        .into_value()),
        LeafCodec::Structural => decode_non_scalar_accepted_slot_value(raw_value, field),
    }
}

/// Decode and validate one slot payload through the accepted row contract.
///
/// This is the row-contract authority boundary for decode sites that know the
/// physical slot. Canonical admission and by-kind codec validation are owned
/// here; callers materializing a value must not prevalidate the payload again.
pub(in crate::db) fn decode_runtime_value_from_row_contract(
    contract: &StructuralRowContract,
    slot: usize,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    let accepted_field = contract.required_accepted_field_decode_contract(slot)?;

    if accepted_field.uses_canonical_value_wire() {
        let persistence = contract.required_accepted_field_persistence_contract(slot)?;
        let admitted = super::canonical::decode_admitted_value_from_accepted_field_contract(
            &persistence,
            raw_value,
        )?;
        return Ok(admitted.into_value());
    }

    decode_runtime_value_from_accepted_field_contract(accepted_field, raw_value)
}

/// Decode one scalar slot payload through accepted row metadata.
pub(in crate::db) fn decode_scalar_slot_value_from_row_contract<'raw>(
    contract: &StructuralRowContract,
    slot: usize,
    raw_value: &'raw [u8],
) -> Result<ScalarSlotValueRef<'raw>, InternalError> {
    let accepted_field = contract.required_accepted_field_decode_contract(slot)?;

    decode_scalar_slot_value_from_accepted_field_contract(accepted_field, raw_value)
}

/// Admit a borrowed scalar against its accepted field without materializing it.
pub(in crate::db::data::persisted_row) fn decode_scalar_slot_value_from_accepted_field_contract<
    'raw,
>(
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &'raw [u8],
) -> Result<ScalarSlotValueRef<'raw>, InternalError> {
    let LeafCodec::Scalar(codec) = field.leaf_codec() else {
        return Err(InternalError::persisted_row_decode_corruption());
    };

    let value = decode_scalar_slot_value(raw_value, codec)?;
    let admitted = match (field.kind(), value) {
        (_, ScalarSlotValueRef::Null) => field.nullable(),
        (
            AcceptedFieldKind::Blob { max_len },
            ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)),
        ) => max_len.is_none_or(|limit| bytes.len() <= limit as usize),
        (
            AcceptedFieldKind::Text { max_len },
            ScalarSlotValueRef::Value(ScalarValueRef::Text(text)),
        ) => max_len.is_none_or(|limit| text.chars().count() <= limit as usize),
        _ => true,
    };
    if !admitted {
        return Err(InternalError::persisted_row_decode_corruption());
    }
    Ok(value)
}

// Build one dense slot image by running one caller-supplied encode step per
// declared slot. This keeps the canonical row-emission loops on one shared
// shape while callers still decide whether they start from raw payload bytes or
// from already decoded runtime values.
fn dense_slot_image_from_source<F>(
    slot_count: usize,
    mut encode_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Vec<u8>, InternalError>,
{
    let mut slot_payloads = Vec::with_capacity(slot_count);

    for slot in 0..slot_count {
        slot_payloads.push(encode_slot(slot)?);
    }

    Ok(slot_payloads)
}

// Build one dense canonical slot image through accepted field metadata.
fn dense_canonical_slot_image_from_runtime_value_source_with_accepted_contract<'a, F>(
    contract: &StructuralRowContract,
    mut value_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    dense_slot_image_from_source(contract.field_count(), |slot| {
        let value = value_for_slot(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;

        super::canonical::encode_canonical_value_for_accepted_field_contract(
            &encoding,
            value.as_ref(),
        )
    })
}

// Build and emit one canonical row from runtime values through accepted field
// contracts.
pub(in crate::db) fn canonical_row_from_runtime_value_source_with_accepted_contract<'a, F>(
    contract: &StructuralRowContract,
    value_for_slot: F,
) -> Result<CanonicalRow, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    let slot_payloads =
        dense_canonical_slot_image_from_runtime_value_source_with_accepted_contract(
            contract,
            value_for_slot,
        )?;

    emit_raw_row_from_slot_payloads(
        contract.current_layout_version(),
        contract.field_count(),
        slot_payloads.as_slice(),
    )
}

// Emit the directory and admitted field images directly into the bounded row.
pub(in crate::db) fn emit_raw_row_from_slot_payloads(
    layout_version: crate::db::schema::RowLayoutVersion,
    expected_slot_count: usize,
    slot_payloads: &[Vec<u8>],
) -> Result<CanonicalRow, InternalError> {
    if slot_payloads.len() != expected_slot_count {
        return Err(InternalError::persisted_row_encode_internal());
    }
    let field_count = u16::try_from(slot_payloads.len())
        .map_err(|_| InternalError::persisted_row_encode_internal())?;
    let directory_len = 2 + usize::from(field_count) * 8;
    let payload_len = slot_payloads
        .iter()
        .try_fold(directory_len, |len, payload| len.checked_add(payload.len()))
        .ok_or_else(InternalError::persisted_row_encode_internal)?;
    let encoded = serialize_row_payload(layout_version, payload_len, |encoded| {
        encoded.extend_from_slice(&field_count.to_be_bytes());
        let mut start = 0_u32;
        for payload in slot_payloads {
            let len = u32::try_from(payload.len())
                .map_err(|_| InternalError::persisted_row_encode_internal())?;
            encoded.extend_from_slice(&start.to_be_bytes());
            encoded.extend_from_slice(&len.to_be_bytes());
            start = start
                .checked_add(len)
                .ok_or_else(InternalError::persisted_row_encode_internal)?;
        }
        for payload in slot_payloads {
            encoded.extend_from_slice(payload);
        }
        Ok(())
    })?;
    let raw_row = RawRow::from_untrusted_bytes(encoded)?;
    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

// Decode one non-scalar slot through the accepted persisted schema contract.
fn decode_non_scalar_accepted_slot_value(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<Value, InternalError> {
    if nullable_accepted_slot_payload_is_structural_null(raw_value, field)? {
        return Ok(Value::Null);
    }

    // Named values keep canonical wire bytes even under ByKind proposal metadata.
    // Reuse schema's classifier instead of treating that label as a second authority.
    if field.uses_canonical_value_wire() {
        decode_structural_value_storage_bytes(raw_value)
            .map_err(|_| InternalError::persisted_row_decode_corruption())
    } else {
        decode_structural_field_by_accepted_kind_bytes(raw_value, field.kind())
            .map_err(|_| InternalError::persisted_row_decode_corruption())
    }
}

/// Validate one non-scalar slot through an accepted-schema field contract.
/// Schema selects its wire grammar; row contracts retain named-value admission.
pub(in crate::db) fn validate_non_scalar_accepted_slot_value(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<(), InternalError> {
    if nullable_accepted_slot_payload_is_structural_null(raw_value, field)? {
        return Ok(());
    }

    if field.uses_canonical_value_wire() {
        validate_structural_value_storage_bytes(raw_value)
            .map_err(|_| InternalError::persisted_row_decode_corruption())
    } else {
        validate_structural_field_by_accepted_kind_bytes(raw_value, field.kind())
            .map_err(|_| InternalError::persisted_row_decode_corruption())
    }
}

/// Validate one non-scalar slot through the accepted row contract.
///
pub(in crate::db) fn validate_non_scalar_slot_value_with_row_contract(
    contract: &StructuralRowContract,
    slot: usize,
    raw_value: &[u8],
) -> Result<(), InternalError> {
    let accepted_field = contract.required_accepted_field_decode_contract(slot)?;
    if accepted_field.uses_canonical_value_wire() {
        let persistence = contract.required_accepted_field_persistence_contract(slot)?;
        super::canonical::decode_admitted_value_from_accepted_field_contract(
            &persistence,
            raw_value,
        )?;
        return Ok(());
    }

    validate_non_scalar_accepted_slot_value(raw_value, accepted_field)
}

// Every nullable structural field uses the exact structural-null sentinel;
// relation collection membership does not change the field's nullability.
// Scalar codecs retain their separate slot framing. Non-null bytes still use
// the accepted codec rather than a preliminary walk with another grammar.
fn nullable_accepted_slot_payload_is_structural_null(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<bool, InternalError> {
    let is_null = matches!(field.storage_decode(), FieldStorageDecode::ByKind)
        && value_storage_bytes_are_null(raw_value);
    if is_null && !field.nullable() {
        return Err(InternalError::persisted_row_decode_corruption());
    }
    Ok(is_null)
}

#[cfg(test)]
mod tests;
