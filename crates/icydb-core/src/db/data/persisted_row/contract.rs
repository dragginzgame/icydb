//! Accepted persisted-row field encoding and decode adapters.
//!
//! Production writes and test fixtures enter through accepted field contracts.

use crate::{
    db::schema::{FieldStorageDecode, LeafCodec},
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, StructuralRowContract,
            accepted_kind_supports_primary_key_component_binary,
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

use crate::db::data::persisted_row::codec::{ScalarSlotValueRef, decode_scalar_slot_value};

pub(in crate::db::data::persisted_row) const RETIRED_SLOT_PLACEHOLDER_PAYLOAD: &[u8] = &[0];

/// Decode one slot payload through an accepted-schema field contract.
///
/// It keeps accepted `AcceptedFieldKind` metadata intact for recursive
/// payloads.
pub(in crate::db) fn decode_runtime_value_from_accepted_field_contract(
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    match field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            Ok(decode_scalar_slot_value(raw_value, codec, field.field_name())?.into_value())
        }
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
            persistence,
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

    let LeafCodec::Scalar(codec) = accepted_field.leaf_codec() else {
        return Err(InternalError::persisted_row_decode_corruption());
    };

    decode_scalar_slot_value(raw_value, codec, accepted_field.field_name())
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
        if !contract.has_active_field_slot(slot) {
            return Ok(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
        }

        let value = value_for_slot(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;

        super::canonical::encode_canonical_value_for_accepted_field_contract(
            encoding,
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
    let raw_row = RawRow::from_untrusted_bytes(encoded).map_err(InternalError::from)?;
    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

// Decode one non-scalar slot through the accepted persisted schema contract.
fn decode_non_scalar_accepted_slot_value(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<Value, InternalError> {
    if nullable_non_primary_key_component_accepted_slot_payload_is_structural_null(
        raw_value, field,
    )? {
        return Ok(Value::Null);
    }

    match field.storage_decode() {
        FieldStorageDecode::ByKind => {
            decode_structural_field_by_accepted_kind_bytes(raw_value, field.kind()).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.field_name(),
                    field.kind(),
                    err,
                )
            })
        }
        FieldStorageDecode::CatalogValue => decode_structural_value_storage_bytes(raw_value)
            .map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.field_name(),
                    field.kind(),
                    err,
                )
            }),
    }
}

/// Validate one non-scalar slot through an accepted-schema field contract.
/// Recursive payload validation stays on accepted `AcceptedFieldKind` metadata.
pub(in crate::db) fn validate_non_scalar_accepted_slot_value(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<(), InternalError> {
    if nullable_non_primary_key_component_accepted_slot_payload_is_structural_null(
        raw_value, field,
    )? {
        return Ok(());
    }

    match field.storage_decode() {
        FieldStorageDecode::ByKind => {
            validate_structural_field_by_accepted_kind_bytes(raw_value, field.kind()).map_err(
                |err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                },
            )
        }
        FieldStorageDecode::CatalogValue => validate_structural_value_storage_bytes(raw_value)
            .map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.field_name(),
                    field.kind(),
                    err,
                )
            }),
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
            persistence,
            raw_value,
        )?;
        return Ok(());
    }

    validate_non_scalar_accepted_slot_value(raw_value, accepted_field)
}

// Accepted-schema equivalent of the generated-field nullable structural-null
// check. Storage-key-compatible accepted kinds keep their own null encoding
// lane, so only non-storage-key by-kind payloads use the structural null
// sentinel here.
fn nullable_non_primary_key_component_accepted_slot_payload_is_structural_null(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<bool, InternalError> {
    if !field.nullable()
        || !matches!(field.storage_decode(), FieldStorageDecode::ByKind)
        || accepted_kind_supports_primary_key_component_binary(field.kind())
    {
        return Ok(false);
    }

    value_storage_bytes_are_null(raw_value).map_err(|err| {
        InternalError::persisted_row_field_kind_decode_failed(field.field_name(), field.kind(), err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        codec::decode_row_payload_bytes,
        data::encode_structural_field_by_accepted_kind_bytes,
        schema::{AcceptedFieldKind, RowLayoutVersion},
    };

    #[test]
    fn row_emission_preserves_collection_bytes_and_slot_offsets() {
        let kind = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64));
        let value = Value::List((0..1_000).map(Value::Nat64).collect());
        let field = encode_structural_field_by_accepted_kind_bytes(&kind, &value, "numbers")
            .expect("the accepted list should encode");
        assert_eq!(field.len(), 9_005);
        let mut fields = vec![field];
        let row = emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 1, &fields)
            .expect("one collection field should emit");
        assert_eq!(row.as_raw_row().as_bytes().len(), 9_026);

        fields.push(vec![0]);
        let row = emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 2, &fields)
            .expect("the collection and null slots should emit");
        let raw = row.as_raw_row().as_bytes();
        let decoded = decode_row_payload_bytes(raw).expect("the row envelope should decode");
        assert_eq!(decoded.layout_version(), RowLayoutVersion::INITIAL);
        let payload = decoded.into_payload();
        // Frozen count/offset/length directory: list at 0, null at 9,005.
        assert_eq!(
            &payload[..18],
            &[
                0, 2, 0, 0, 0, 0, 0, 0, 0x23, 0x2d, 0, 0, 0x23, 0x2d, 0, 0, 0, 1
            ],
        );
        assert_eq!(&payload[18..18 + 9_005], fields[0]);
        assert_eq!(&payload[18 + 9_005..], fields[1]);
        assert_eq!(
            decode_structural_field_by_accepted_kind_bytes(&payload[18..18 + 9_005], &kind)
                .expect("the emitted collection should decode"),
            value,
        );
        assert!(emit_raw_row_from_slot_payloads(RowLayoutVersion::INITIAL, 1, &fields).is_err());
    }
}
