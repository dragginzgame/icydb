//! Accepted persisted-row field encoding and decode adapters.
//!
//! Production writes and test fixtures enter through accepted field contracts.

use crate::{
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
    model::field::{FieldStorageDecode, LeafCodec},
    value::Value,
};
#[cfg(test)]
use crate::{model::entity::EntityModel, value::InputValue};
use std::borrow::Cow;

use crate::db::data::persisted_row::codec::{ScalarSlotValueRef, decode_scalar_slot_value};

pub(in crate::db::data::persisted_row) const RETIRED_SLOT_PLACEHOLDER_PAYLOAD: &[u8] = &[0];

/// Decode one structural slot payload into a runtime boundary `Value`.
///
/// Test models are first projected into accepted row authority, matching the
/// production decode path.
#[cfg(test)]
pub(in crate::db) fn decode_slot_into_runtime_value(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    let contract = StructuralRowContract::from_model_proposal_for_test(model);

    decode_runtime_value_from_row_contract(&contract, slot, raw_value)
}

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
            match decode_scalar_slot_value(raw_value, codec, field.field_name())? {
                ScalarSlotValueRef::Null => Ok(Value::Null),
                ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
            }
        }
        LeafCodec::StructuralFallback => decode_non_scalar_accepted_slot_value(raw_value, field),
    }
}

/// Decode one slot payload through the accepted row contract.
///
/// This is the row-contract authority boundary for decode sites that know the
/// physical slot.
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
        return Ok(admitted.value().clone());
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

/// Normalize and encode one test value through accepted model-proposal authority.
///
/// This fixture adapter converts the runtime value back into authored input;
/// production persistence consumes admitted or strictly validated values.
#[cfg(test)]
pub(in crate::db) fn encode_value_with_model_proposal_for_test(
    model: &'static EntityModel,
    slot: usize,
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let contract = StructuralRowContract::from_model_proposal_for_test(model);
    let encoding = contract.required_accepted_field_persistence_contract(slot)?;
    let input = InputValue::try_from_runtime_non_enum(value)
        .ok_or_else(InternalError::persisted_row_encode_internal)?;
    let mut budget = crate::db::schema::enum_catalog::ValueAdmissionBudget::standard();
    super::canonical::encode_input_value_for_accepted_field_contract(encoding, input, &mut budget)
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

// Encode one fixed-width slot table plus concatenated slot payload bytes into
// the canonical row payload container.
pub(in crate::db::data::persisted_row) fn encode_slot_payload_from_table_and_bytes(
    slot_count: usize,
    slot_table: &[(u32, u32)],
    payload_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let field_count =
        u16::try_from(slot_count).map_err(|_| InternalError::persisted_row_encode_internal())?;
    let mut encoded = Vec::with_capacity(
        usize::from(field_count) * (u32::BITS as usize / 4) + 2 + payload_bytes.len(),
    );
    encoded.extend_from_slice(&field_count.to_be_bytes());
    for (start, len) in slot_table {
        encoded.extend_from_slice(&start.to_be_bytes());
        encoded.extend_from_slice(&len.to_be_bytes());
    }
    encoded.extend_from_slice(payload_bytes);

    Ok(encoded)
}

// Flatten one dense slot payload image into the canonical slot container while
// letting the caller keep ownership of slot-local overflow error wording.
fn encode_slot_payload_from_dense_slot_image<FS, FL>(
    slot_payloads: &[Vec<u8>],
    mut start_error: FS,
    mut len_error: FL,
) -> Result<Vec<u8>, InternalError>
where
    FS: FnMut(usize) -> InternalError,
    FL: FnMut(usize) -> InternalError,
{
    let payload_capacity = slot_payloads
        .iter()
        .try_fold(0usize, |len, payload| len.checked_add(payload.len()))
        .ok_or_else(InternalError::persisted_row_encode_internal)?;
    let mut payload_bytes = Vec::with_capacity(payload_capacity);
    let mut slot_table = Vec::with_capacity(slot_payloads.len());

    for (slot, payload) in slot_payloads.iter().enumerate() {
        let start = u32::try_from(payload_bytes.len()).map_err(|_| start_error(slot))?;
        let len = u32::try_from(payload.len()).map_err(|_| len_error(slot))?;
        payload_bytes.extend_from_slice(payload.as_slice());
        slot_table.push((start, len));
    }

    encode_slot_payload_from_table_and_bytes(
        slot_payloads.len(),
        slot_table.as_slice(),
        &payload_bytes,
    )
}

// Build and emit one canonical row from runtime values through accepted field
// contracts.
pub(in crate::db::data::persisted_row) fn canonical_row_from_runtime_value_source_with_accepted_contract<
    'a,
    F,
>(
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

    emit_raw_row_from_slot_payloads(contract.field_count(), slot_payloads.as_slice())
}

// Wrap one already-encoded canonical slot payload container in the shared row
// envelope so callers that already own a dense slot payload image do not have
// to rebuild the row wrapper choreography themselves.
fn canonical_row_from_slot_payload_bytes(
    row_payload: Vec<u8>,
) -> Result<CanonicalRow, InternalError> {
    let encoded = serialize_row_payload(row_payload)?;
    let raw_row = RawRow::from_untrusted_bytes(encoded).map_err(InternalError::from)?;

    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

// Emit one raw row from a dense canonical slot image.
pub(in crate::db::data::persisted_row) fn emit_raw_row_from_slot_payloads(
    expected_slot_count: usize,
    slot_payloads: &[Vec<u8>],
) -> Result<CanonicalRow, InternalError> {
    if slot_payloads.len() != expected_slot_count {
        return Err(InternalError::persisted_row_encode_internal());
    }

    // Phase 1: flatten the already canonicalized dense slot image directly so
    // row re-emission does not clone each slot payload back through the
    // mutable slot-writer staging buffer first.
    let row_payload = encode_slot_payload_from_dense_slot_image(
        slot_payloads,
        |_| InternalError::persisted_row_encode_internal(),
        |_| InternalError::persisted_row_encode_internal(),
    )?;

    // Phase 2: wrap the canonical slot container in the shared row envelope.
    canonical_row_from_slot_payload_bytes(row_payload)
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
        FieldStorageDecode::Value => {
            decode_structural_value_storage_bytes(raw_value).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.field_name(),
                    field.kind(),
                    err,
                )
            })
        }
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
        FieldStorageDecode::Value => {
            validate_structural_value_storage_bytes(raw_value).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.field_name(),
                    field.kind(),
                    err,
                )
            })
        }
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
