use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, decode_storage_key_binary_value_bytes,
            decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
            encode_storage_key_binary_value_bytes, encode_structural_field_by_kind_bytes,
            encode_structural_value_storage_bytes, supports_storage_key_binary_kind,
            validate_storage_key_binary_value_bytes, validate_structural_field_by_kind_bytes,
            validate_structural_value_storage_bytes,
        },
        scalar_expr::compile_scalar_literal_expr_value,
        schema::{field_type_from_model_kind, literal_matches_type},
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
    },
    value::{StorageKey, Value},
};
use std::{borrow::Cow, cmp::Ordering};

use crate::db::data::persisted_row::{
    codec::{
        ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value, encode_scalar_slot_value,
    },
    types::SerializedUpdatePatch,
};

/// Decode one slot value through the declared field contract without routing
/// through `SlotReader::get_value`.
#[cfg(test)]
pub(in crate::db::data::persisted_row) fn decode_slot_value_by_contract(
    slots: &dyn crate::db::data::persisted_row::SlotReader,
    slot: usize,
) -> Result<Option<Value>, InternalError> {
    let Some(raw_value) = slots.get_bytes(slot) else {
        return Ok(None);
    };

    decode_slot_value_from_bytes(slots.model(), slot, raw_value).map(Some)
}

/// Decode one structural slot payload using the owning model field contract.
///
/// This is the canonical field-level decode boundary for persisted-row bytes.
/// Higher-level row readers may still cache decoded values, but they should not
/// rebuild scalar-vs-structural field dispatch themselves.
pub(in crate::db) fn decode_slot_value_from_bytes(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Value, InternalError> {
    let field = field_model_for_slot(model, slot)?;

    decode_slot_value_for_field(field, raw_value)
}

// Decode one structural slot payload once the owning field contract has
// already been resolved.
pub(in crate::db::data::persisted_row) fn decode_slot_value_for_field(
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

/// Encode one structural slot value using the owning model field contract.
///
/// This is the initial `0.64` write-side field-codec boundary. It currently
/// covers:
/// - scalar leaf slots
/// - `FieldStorageDecode::Value` slots
///
/// Composite `ByKind` field encoding remains a follow-up slice so the runtime
/// can add one structural encoder owner instead of quietly rebuilding typed
/// per-field branches.
pub(in crate::db::data::persisted_row) fn encode_slot_value_from_value(
    model: &'static EntityModel,
    slot: usize,
    value: &Value,
) -> Result<Vec<u8>, InternalError> {
    let field = field_model_for_slot(model, slot)?;
    ensure_slot_value_matches_field_contract(field, value)?;

    match field.storage_decode() {
        FieldStorageDecode::Value => encode_structural_value_storage_bytes(value)
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field.name(), err)),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(_) => {
                let scalar = compile_scalar_literal_expr_value(value).ok_or_else(|| {
                    InternalError::persisted_row_field_encode_failed(
                        field.name(),
                        format!(
                            "field kind {:?} requires a scalar runtime value, found {value:?}",
                            field.kind()
                        ),
                    )
                })?;

                Ok(encode_scalar_slot_value(scalar.as_slot_value_ref()))
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

// Decode one slot payload and immediately re-encode it through the current
// field contract so every row-emission path normalizes bytes at the boundary.
fn canonicalize_slot_payload(
    model: &'static EntityModel,
    slot: usize,
    raw_value: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let value = decode_slot_value_from_bytes(model, slot, raw_value)?;

    encode_slot_value_from_value(model, slot, &value)
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
fn dense_canonical_slot_image_from_value_source<'a, F>(
    model: &'static EntityModel,
    mut value_for_slot: F,
) -> Result<Vec<Vec<u8>>, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    dense_slot_image_from_source(model, |slot| {
        let value = value_for_slot(slot)?;
        encode_slot_value_from_value(model, slot, value.as_ref())
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
pub(in crate::db::data::persisted_row) fn encode_slot_payload_from_dense_slot_image<FS, FL>(
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
pub(in crate::db::data::persisted_row) fn canonical_row_from_value_source<'a, F>(
    model: &'static EntityModel,
    value_for_slot: F,
) -> Result<CanonicalRow, InternalError>
where
    F: FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
{
    let slot_payloads = dense_canonical_slot_image_from_value_source(model, value_for_slot)?;

    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}

// Wrap one already-encoded canonical slot payload container in the shared row
// envelope so callers that already own a dense slot payload image do not have
// to rebuild the row wrapper choreography themselves.
pub(in crate::db::data::persisted_row) fn canonical_row_from_slot_payload_bytes(
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

// Validate one runtime value against the persisted field contract before field-
// level structural encoding writes bytes into a row slot.
fn ensure_slot_value_matches_field_contract(
    field: &FieldModel,
    value: &Value,
) -> Result<(), InternalError> {
    if matches!(value, Value::Null) {
        if field.nullable() {
            return Ok(());
        }

        return Err(InternalError::persisted_row_field_encode_failed(
            field.name(),
            "required field cannot store null",
        ));
    }

    // `FieldStorageDecode::Value` fields persist the generic `Value` envelope
    // directly, so storage-side validation must accept structured leaves nested
    // under collection contracts instead of reusing the predicate literal gate.
    if matches!(field.storage_decode(), FieldStorageDecode::Value) {
        if !field.kind().accepts_value(value) {
            return Err(InternalError::persisted_row_field_encode_failed(
                field.name(),
                format!(
                    "field kind {:?} does not accept runtime value {value:?}",
                    field.kind()
                ),
            ));
        }

        ensure_decimal_scale_matches(field.name(), field.kind(), value)?;

        return ensure_value_is_deterministic_for_storage(field.name(), field.kind(), value);
    }

    let field_type = field_type_from_model_kind(&field.kind());
    if !literal_matches_type(value, &field_type) {
        return Err(InternalError::persisted_row_field_encode_failed(
            field.name(),
            format!(
                "field kind {:?} does not accept runtime value {value:?}",
                field.kind()
            ),
        ));
    }

    ensure_decimal_scale_matches(field.name(), field.kind(), value)?;
    ensure_value_is_deterministic_for_storage(field.name(), field.kind(), value)
}

// Enforce fixed decimal scales through nested collection/map shapes before a
// field-level patch value is persisted.
fn ensure_decimal_scale_matches(
    field_name: &str,
    kind: FieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    if matches!(value, Value::Null) {
        return Ok(());
    }

    match (kind, value) {
        (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
            if decimal.scale() != scale {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!(
                        "decimal scale mismatch: expected {scale}, found {}",
                        decimal.scale()
                    ),
                ));
            }

            Ok(())
        }
        (FieldKind::Relation { key_kind, .. }, value) => {
            ensure_decimal_scale_matches(field_name, *key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            for item in items {
                ensure_decimal_scale_matches(field_name, *inner, item)?;
            }

            Ok(())
        }
        (
            FieldKind::Map {
                key,
                value: map_value,
            },
            Value::Map(entries),
        ) => {
            for (entry_key, entry_value) in entries {
                ensure_decimal_scale_matches(field_name, *key, entry_key)?;
                ensure_decimal_scale_matches(field_name, *map_value, entry_value)?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Enforce the canonical persisted ordering rules for set/map shapes before one
// field-level patch value becomes row bytes.
fn ensure_value_is_deterministic_for_storage(
    field_name: &str,
    kind: FieldKind,
    value: &Value,
) -> Result<(), InternalError> {
    match (kind, value) {
        (FieldKind::Set(_), Value::List(items)) => {
            for pair in items.windows(2) {
                let [left, right] = pair else {
                    continue;
                };
                if Value::canonical_cmp(left, right) != Ordering::Less {
                    return Err(InternalError::persisted_row_field_encode_failed(
                        field_name,
                        "set payload must already be canonical and deduplicated",
                    ));
                }
            }

            Ok(())
        }
        (FieldKind::Map { .. }, Value::Map(entries)) => {
            Value::validate_map_entries(entries.as_slice())
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?;

            if !Value::map_entries_are_strictly_canonical(entries.as_slice()) {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    "map payload must already be canonical and deduplicated",
                ));
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Materialize the last-write-wins serialized patch view indexed by stable slot.
pub(in crate::db::data::persisted_row) fn serialized_patch_payload_by_slot<'a>(
    model: &'static EntityModel,
    patch: &'a SerializedUpdatePatch,
) -> Result<Vec<Option<&'a [u8]>>, InternalError> {
    let mut payloads = vec![None; model.fields().len()];

    for entry in patch.entries() {
        let slot = entry.slot().index();
        field_model_for_slot(model, slot)?;
        payloads[slot] = Some(entry.payload());
    }

    Ok(payloads)
}

// Resolve one field model entry by stable slot index.
pub(in crate::db::data::persisted_row) fn field_model_for_slot(
    model: &'static EntityModel,
    slot: usize,
) -> Result<&'static FieldModel, InternalError> {
    model
        .fields()
        .get(slot)
        .ok_or_else(|| InternalError::persisted_row_slot_lookup_out_of_bounds(model.path(), slot))
}

// Convert one scalar slot fast-path value into its storage-key form when the
// field kind is storage-key-compatible.
pub(in crate::db::data::persisted_row) const fn storage_key_from_scalar_ref(
    value: ScalarValueRef<'_>,
) -> Option<StorageKey> {
    match value {
        ScalarValueRef::Int(value) => Some(StorageKey::Int(value)),
        ScalarValueRef::Principal(value) => Some(StorageKey::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(StorageKey::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(StorageKey::Timestamp(value)),
        ScalarValueRef::Uint(value) => Some(StorageKey::Uint(value)),
        ScalarValueRef::Ulid(value) => Some(StorageKey::Ulid(value)),
        ScalarValueRef::Unit => Some(StorageKey::Unit),
        _ => None,
    }
}
