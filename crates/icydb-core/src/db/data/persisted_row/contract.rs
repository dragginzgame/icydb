//! Runtime boundary adapters between typed persisted-row slots and `Value`.
//!
//! This module is not the persisted-field contract. It is the runtime boundary
//! adapter that converts `Value` -> bytes using schema `FieldModel` validation.
//! It intentionally does not use Rust field types because runtime write, query,
//! projection, and patch paths naturally carry dynamic `Value` payloads at the
//! outer row boundary.
//!
//! Persistence contracts remain type-owned in codecs. `Value` must stay
//! runtime-only and must never implement persisted-field codec traits.

use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, decode_storage_key_binary_value_bytes,
            decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
            encode_storage_key_binary_value_bytes, encode_structural_field_by_kind_bytes,
            encode_structural_value_storage_bytes, encode_structural_value_storage_null_bytes,
            supports_storage_key_binary_kind, validate_storage_key_binary_value_bytes,
            validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
            value_storage_bytes_are_null,
        },
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
    },
    value::Value,
};
use std::borrow::Cow;

use crate::db::data::persisted_row::{
    codec::{
        ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value, encode_scalar_slot_value,
    },
    types::field_model_for_slot,
};

/// Decode one structural slot payload into a runtime boundary `Value`.
///
/// This adapter is for runtime row consumers only. It uses the owning
/// `FieldModel` contract to select the exact storage lane before materializing
/// a dynamic value for query/projection code.
pub(in crate::db) fn decode_slot_into_runtime_value(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    let field = field_model_for_slot(model, slot)?;

    decode_field_slot_into_runtime_value(field, raw_value)
}

// Decode one runtime-boundary slot payload once the owning field contract has
// already been resolved. Callers inside persisted-row readers use this to avoid
// repeating field-model lookup while still sharing the same adapter policy.
pub(in crate::db::data::persisted_row) fn decode_field_slot_into_runtime_value(
    field: &FieldModel,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    match field.leaf_codec() {
        LeafCodec::Scalar(codec) => match decode_scalar_slot_value(raw_value, codec, field.name())?
        {
            ScalarSlotValueRef::Null => Ok(Value::Null),
            ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
        },
        LeafCodec::StructuralFallback => decode_non_scalar_slot_value(raw_value, field),
    }
}

/// Encode one runtime boundary `Value` into a persisted slot payload.
///
/// This adapter converts `Value` -> bytes through schema `FieldModel`
/// validation. It is a boundary contract, not permission to persist `Value` as
/// a field type; persisted Rust fields remain governed by type-owned codecs.
pub(in crate::db::data::persisted_row) fn encode_runtime_value_into_slot(
    model: &'static EntityModel,
    slot: usize,
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let field = field_model_for_slot(model, slot)?;
    let value = field
        .normalize_runtime_value_for_storage(value)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field.name(), err))?;
    let value = value.as_ref();

    field
        .validate_runtime_value_for_storage(value)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field.name(), err))?;
    if matches!(value, Value::Null) {
        return encode_null_slot_value_for_field(field);
    }

    match field.storage_decode() {
        FieldStorageDecode::Value => encode_structural_value_storage_bytes(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field.name(), err)),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(codec) => {
                let scalar =
                    scalar_slot_value_ref_from_runtime_value(value, codec).ok_or_else(|| {
                        InternalError::persisted_row_field_encode_failed(
                            field.name(),
                            format!(
                                "field kind {:?} requires a scalar runtime value, found {value:?}",
                                field.kind()
                            ),
                        )
                    })?;

                Ok(encode_scalar_slot_value(scalar))
            }
            LeafCodec::StructuralFallback => {
                if supports_storage_key_binary_kind(field.kind()) {
                    encode_storage_key_binary_value_bytes(field.kind(), value, field.name())?
                        .ok_or_else(|| {
                            InternalError::persisted_row_field_encode_failed(
                                field.name(),
                                "storage-key binary lane rejected a supported field kind",
                            )
                        })
                } else {
                    encode_structural_field_by_kind_bytes(field.kind(), value, field.name())
                }
            }
        },
    }
}

// Encode an explicit nullable `NULL` through the same slot lane the field uses
// for non-null values. Storage-key-compatible fields keep their dedicated lane
// because relation nulls already have storage-key-specific shape.
fn encode_null_slot_value_for_field(field: &FieldModel) -> Result<Vec<u8>, InternalError> {
    match field.storage_decode() {
        FieldStorageDecode::Value => Ok(encode_structural_value_storage_null_bytes()),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(_) => Ok(encode_scalar_slot_value(ScalarSlotValueRef::Null)),
            LeafCodec::StructuralFallback if supports_storage_key_binary_kind(field.kind()) => {
                encode_storage_key_binary_value_bytes(field.kind(), &Value::Null, field.name())?
                    .ok_or_else(|| {
                        InternalError::persisted_row_field_encode_failed(
                            field.name(),
                            "storage-key binary lane rejected a supported field kind",
                        )
                    })
            }
            LeafCodec::StructuralFallback => Ok(encode_structural_value_storage_null_bytes()),
        },
    }
}

// Convert one runtime scalar value into the borrowed scalar-slot view expected
// by the persisted-row scalar codec. Field compatibility has already been
// checked by the model field contract before this storage encoder runs.
const fn scalar_slot_value_ref_from_runtime_value(
    value: &Value,
    codec: ScalarCodec,
) -> Option<ScalarSlotValueRef<'_>> {
    let scalar = match (codec, value) {
        (ScalarCodec::Blob, Value::Blob(value)) => ScalarValueRef::Blob(value.as_slice()),
        (ScalarCodec::Bool, Value::Bool(value)) => ScalarValueRef::Bool(*value),
        (ScalarCodec::Date, Value::Date(value)) => ScalarValueRef::Date(*value),
        (ScalarCodec::Duration, Value::Duration(value)) => ScalarValueRef::Duration(*value),
        (ScalarCodec::Float32, Value::Float32(value)) => ScalarValueRef::Float32(*value),
        (ScalarCodec::Float64, Value::Float64(value)) => ScalarValueRef::Float64(*value),
        (ScalarCodec::Int64, Value::Int(value)) => ScalarValueRef::Int(*value),
        (ScalarCodec::Principal, Value::Principal(value)) => ScalarValueRef::Principal(*value),
        (ScalarCodec::Subaccount, Value::Subaccount(value)) => ScalarValueRef::Subaccount(*value),
        (ScalarCodec::Text, Value::Text(value)) => ScalarValueRef::Text(value.as_str()),
        (ScalarCodec::Timestamp, Value::Timestamp(value)) => ScalarValueRef::Timestamp(*value),
        (ScalarCodec::Uint64, Value::Uint(value)) => ScalarValueRef::Uint(*value),
        (ScalarCodec::Ulid, Value::Ulid(value)) => ScalarValueRef::Ulid(*value),
        (ScalarCodec::Unit, Value::Unit) => ScalarValueRef::Unit,
        _ => return None,
    };

    Some(ScalarSlotValueRef::Value(scalar))
}

// Decode one slot payload and immediately re-encode it through the current
// field contract so every row-emission path normalizes bytes at the boundary.
fn canonicalize_slot_payload(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let value = decode_slot_into_runtime_value(model, slot, raw_value)?;

    encode_runtime_value_into_slot(model, slot, &value)
}

// Build one dense slot image by running one caller-supplied encode step per
// declared slot. This keeps the canonical row-emission loops on one shared
// shape while callers still decide whether they start from raw payload bytes or
// from already decoded runtime values.
fn dense_slot_image_from_source<F>(
    model: &'static EntityModel,
    mut encode_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Vec<u8>, InternalError>,
{
    let mut slot_payloads = Vec::with_capacity(model.fields().len());

    for slot in 0..model.fields().len() {
        slot_payloads.push(encode_slot(slot)?);
    }

    Ok(slot_payloads)
}

// Build one dense canonical slot image from any slot-addressable payload source.
// Callers keep ownership of missing-slot policy while this helper centralizes
// the slot-by-slot canonicalization loop.
fn dense_canonical_slot_image_from_payload_source<'a, F>(
    model: &'static EntityModel,
    mut payload_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<&'a [u8], InternalError>,
{
    dense_slot_image_from_source(model, |slot| {
        let payload = payload_for_slot(slot)?;
        canonicalize_slot_payload(model, slot, payload)
    })
}

// Build one dense canonical slot image from already-decoded runtime values.
// This keeps row-emission paths from re-decoding raw slot bytes when a caller
// already owns the validated structural value cache.
fn dense_canonical_slot_image_from_runtime_value_source<'a, F>(
    model: &'static EntityModel,
    mut value_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    dense_slot_image_from_source(model, |slot| {
        let value = value_for_slot(slot)?;
        encode_runtime_value_into_slot(model, slot, value.as_ref())
    })
}

// Encode one fixed-width slot table plus concatenated slot payload bytes into
// the canonical row payload container.
pub(in crate::db::data::persisted_row) fn encode_slot_payload_from_parts(
    slot_count: usize,
    slot_table: &[(u32, u32)],
    payload_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let field_count = u16::try_from(slot_count).map_err(|_| {
        InternalError::persisted_row_encode_failed(format!(
            "field count {slot_count} exceeds u16 slot table capacity",
        ))
    })?;
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
        .ok_or_else(|| {
            InternalError::persisted_row_encode_failed(
                "canonical slot image payload length overflow",
            )
        })?;
    let mut payload_bytes = Vec::with_capacity(payload_capacity);
    let mut slot_table = Vec::with_capacity(slot_payloads.len());

    for (slot, payload) in slot_payloads.iter().enumerate() {
        let start = u32::try_from(payload_bytes.len()).map_err(|_| start_error(slot))?;
        let len = u32::try_from(payload.len()).map_err(|_| len_error(slot))?;
        payload_bytes.extend_from_slice(payload.as_slice());
        slot_table.push((start, len));
    }

    encode_slot_payload_from_parts(slot_payloads.len(), slot_table.as_slice(), &payload_bytes)
}

// Build and emit one canonical row from any slot-addressable payload source so
// patch replay and row rebuild call sites do not have to stage the dense slot
// image and row emission as two separate owner-local steps.
pub(in crate::db::data::persisted_row) fn canonical_row_from_payload_source<'a, F>(
    model: &'static EntityModel,
    payload_for_slot: F,
) -> Result<CanonicalRow, InternalError>
where
    F: FnMut(usize) -> Result<&'a [u8], InternalError>,
{
    let slot_payloads = dense_canonical_slot_image_from_payload_source(model, payload_for_slot)?;

    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}

// Build and emit one canonical row from already-decoded runtime values so
// callers that already own the structural value cache can reuse the same
// row-emission owner without staging the dense slot image themselves.
pub(in crate::db::data::persisted_row) fn canonical_row_from_runtime_value_source<'a, F>(
    model: &'static EntityModel,
    value_for_slot: F,
) -> Result<CanonicalRow, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    let slot_payloads =
        dense_canonical_slot_image_from_runtime_value_source(model, value_for_slot)?;

    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
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
fn emit_raw_row_from_slot_payloads(
    model: &'static EntityModel,
    slot_payloads: &[Vec<u8>],
) -> Result<CanonicalRow, InternalError> {
    if slot_payloads.len() != model.fields().len() {
        return Err(InternalError::persisted_row_encode_failed(format!(
            "canonical slot image expected {} slots for entity '{}', found {}",
            model.fields().len(),
            model.path(),
            slot_payloads.len()
        )));
    }

    // Phase 1: flatten the already canonicalized dense slot image directly so
    // row re-emission does not clone each slot payload back through the
    // mutable slot-writer staging buffer first.
    let row_payload = encode_slot_payload_from_dense_slot_image(
        slot_payloads,
        |slot| {
            InternalError::persisted_row_encode_failed(format!(
                "canonical slot payload start exceeds u32 range: slot={slot}",
            ))
        },
        |slot| {
            InternalError::persisted_row_encode_failed(format!(
                "canonical slot payload length exceeds u32 range: slot={slot}",
            ))
        },
    )?;

    // Phase 2: wrap the canonical slot container in the shared row envelope.
    canonical_row_from_slot_payload_bytes(row_payload)
}

// Decode one non-scalar slot through the exact persisted contract declared by
// the field model.
fn decode_non_scalar_slot_value(
    raw_value: &[u8],
    field: &FieldModel,
) -> Result<Value, InternalError> {
    if nullable_non_storage_key_by_kind_slot_payload_is_structural_null(raw_value, field)? {
        return Ok(Value::Null);
    }

    match field.storage_decode() {
        crate::model::field::FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::StructuralFallback if supports_storage_key_binary_kind(field.kind()) => {
                match decode_storage_key_binary_value_bytes(raw_value, field.kind()) {
                    Ok(Some(value)) => Ok(value),
                    Ok(None) => {
                        unreachable!("storage-key binary lane must decode supported field kinds")
                    }
                    Err(err) => Err(InternalError::persisted_row_field_kind_decode_failed(
                        field.name(),
                        field.kind(),
                        err,
                    )),
                }
            }
            _ => decode_structural_field_by_kind_bytes(raw_value, field.kind()).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.name(),
                    field.kind(),
                    err,
                )
            }),
        },
        crate::model::field::FieldStorageDecode::Value => {
            decode_structural_value_storage_bytes(raw_value).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.name(),
                    field.kind(),
                    err,
                )
            })
        }
    }
}

// Validate one non-scalar slot through the exact persisted contract declared
// by the field model without eagerly building the final runtime `Value`.
pub(in crate::db::data::persisted_row) fn validate_non_scalar_slot_value(
    raw_value: &[u8],
    field: &FieldModel,
) -> Result<(), InternalError> {
    if nullable_non_storage_key_by_kind_slot_payload_is_structural_null(raw_value, field)? {
        return Ok(());
    }

    match field.storage_decode() {
        crate::model::field::FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::StructuralFallback if supports_storage_key_binary_kind(field.kind()) => {
                match validate_storage_key_binary_value_bytes(raw_value, field.kind()) {
                    Ok(true) => Ok(()),
                    Ok(false) => {
                        unreachable!("storage-key binary lane must validate supported field kinds")
                    }
                    Err(err) => Err(InternalError::persisted_row_field_kind_decode_failed(
                        field.name(),
                        field.kind(),
                        err,
                    )),
                }
            }
            _ => validate_structural_field_by_kind_bytes(raw_value, field.kind()).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.name(),
                    field.kind(),
                    err,
                )
            }),
        },
        crate::model::field::FieldStorageDecode::Value => {
            validate_structural_value_storage_bytes(raw_value).map_err(|err| {
                InternalError::persisted_row_field_kind_decode_failed(
                    field.name(),
                    field.kind(),
                    err,
                )
            })
        }
    }
}

// Nullable non-storage-key by-kind leaves share the structural null sentinel,
// but their concrete leaf decoders are intentionally strict about non-null
// field kinds. Detect the sentinel before dispatching to those leaf decoders.
fn nullable_non_storage_key_by_kind_slot_payload_is_structural_null(
    raw_value: &[u8],
    field: &FieldModel,
) -> Result<bool, InternalError> {
    if !field.nullable()
        || !matches!(field.storage_decode(), FieldStorageDecode::ByKind)
        || supports_storage_key_binary_kind(field.kind())
    {
        return Ok(false);
    }

    value_storage_bytes_are_null(raw_value).map_err(|err| {
        InternalError::persisted_row_field_kind_decode_failed(field.name(), field.kind(), err)
    })
}
