use crate::{
    db::data::{CanonicalRow, RawRow, StructuralRowDecodeError, StructuralRowFieldBytes},
    error::InternalError,
    model::entity::EntityModel,
};
use std::borrow::Cow;

use crate::db::data::persisted_row::{
    contract::{
        dense_canonical_slot_image_from_payload_source,
        dense_canonical_slot_image_from_value_source, emit_raw_row_from_slot_payloads,
        encode_slot_value_from_value, serialized_patch_payload_by_slot,
    },
    reader::StructuralSlotReader,
    types::{PersistedRow, SerializedFieldUpdate, SerializedUpdatePatch, UpdatePatch},
    writer::{SerializedPatchWriter, SlotBufferWriter},
};

// Build one dense canonical slot image from a serialized patch, failing closed
// when any declared slot is missing or any payload is non-canonical.
fn dense_canonical_slot_image_from_serialized_patch(
    model: &'static EntityModel,
    patch: &SerializedUpdatePatch,
) -> Result<Vec<Vec<u8>>, InternalError> {
    let patch_payloads = serialized_patch_payload_by_slot(model, patch)?;

    dense_canonical_slot_image_from_payload_source(model, |slot| {
        patch_payloads[slot].ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "serialized patch did not emit slot {slot} for entity '{}'",
                model.path()
            ))
        })
    })
}

/// Build one canonical row from one serialized structural patch that already
/// describes a full logical row image.
pub(in crate::db) fn canonical_row_from_serialized_update_patch(
    model: &'static EntityModel,
    patch: &SerializedUpdatePatch,
) -> Result<CanonicalRow, InternalError> {
    let slot_payloads = dense_canonical_slot_image_from_serialized_patch(model, patch)?;

    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}

/// Build one canonical row directly from one typed entity slot writer.
pub(in crate::db) fn canonical_row_from_entity<E>(entity: &E) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let mut writer = SlotBufferWriter::for_model(E::MODEL);

    // Phase 1: let the derive-owned slot writer emit the complete typed row image.
    entity.write_slots(&mut writer)?;

    // Phase 2: wrap the canonical slot container in the shared row envelope.
    let encoded = crate::db::codec::serialize_row_payload(writer.finish()?)?;
    let raw_row = RawRow::from_untrusted_bytes(encoded).map_err(InternalError::from)?;

    Ok(CanonicalRow::from_canonical_raw_row(raw_row))
}

/// Build one canonical row from one already-decoded structural slot reader.
pub(in crate::db) fn canonical_row_from_structural_slot_reader(
    row_fields: &StructuralSlotReader<'_>,
) -> Result<CanonicalRow, InternalError> {
    // Phase 1: re-encode every declared slot from the already-decoded cache so
    // commit preparation does not re-enter raw field-byte decode after the
    // structural reader has already validated the row.
    let slot_payloads = dense_canonical_slot_image_from_value_source(row_fields.model(), |slot| {
        row_fields
            .required_cached_value(slot)
            .map(Cow::Borrowed)
            .map_err(|_| {
                InternalError::persisted_row_encode_failed(format!(
                    "slot {slot} is missing from the structural value cache for entity '{}'",
                    row_fields.model().path()
                ))
            })
    })?;

    // Phase 2: re-emit the full image through the single row-emission owner.
    emit_raw_row_from_slot_payloads(row_fields.model(), slot_payloads.as_slice())
}

// Rebuild one full canonical row image from an existing raw row before it
// crosses a storage write boundary.
pub(in crate::db) fn canonical_row_from_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    // Phase 1: canonicalize every declared slot from the existing row image.
    let slot_payloads = dense_canonical_slot_image_from_payload_source(model, |slot| {
        field_bytes.field(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is missing from the baseline row for entity '{}'",
                model.path()
            ))
        })
    })?;

    // Phase 2: re-emit the full image through the single row-emission owner.
    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}

// Rewrap one row already loaded from storage as a canonical write token.
pub(in crate::db) const fn canonical_row_from_stored_raw_row(raw_row: RawRow) -> CanonicalRow {
    CanonicalRow::from_canonical_raw_row(raw_row)
}

/// Apply one ordered structural patch to one raw row using the current
/// persisted-row field codec authority.
pub(in crate::db) fn apply_update_patch_to_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &UpdatePatch,
) -> Result<CanonicalRow, InternalError> {
    let serialized_patch = serialize_update_patch_fields(model, patch)?;

    apply_serialized_update_patch_to_raw_row(model, raw_row, &serialized_patch)
}

/// Serialize one ordered structural patch into canonical slot payload bytes.
///
/// This is the phase-1 partial-serialization seam for `0.64`: later mutation
/// stages can stage or replay one field patch without rebuilding the runtime
/// value-to-bytes contract per consumer.
pub(in crate::db) fn serialize_update_patch_fields(
    model: &'static EntityModel,
    patch: &UpdatePatch,
) -> Result<SerializedUpdatePatch, InternalError> {
    if patch.is_empty() {
        return Ok(SerializedUpdatePatch::default());
    }

    let mut entries = Vec::with_capacity(patch.entries().len());

    // Phase 1: validate and encode each ordered field update through the
    // canonical slot codec owner.
    for entry in patch.entries() {
        let slot = entry.slot();
        let payload = encode_slot_value_from_value(model, slot.index(), entry.value())?;
        entries.push(SerializedFieldUpdate::new(slot, payload));
    }

    Ok(SerializedUpdatePatch::new(entries))
}

/// Serialize one full typed entity image into the canonical serialized patch
/// artifact used by row-boundary patch replay.
///
/// This keeps typed save/update APIs on the existing surface while moving the
/// actual after-image staging onto the structural slot-patch boundary.
pub(in crate::db) fn serialize_entity_slots_as_update_patch<E>(
    entity: &E,
) -> Result<SerializedUpdatePatch, InternalError>
where
    E: PersistedRow,
{
    let mut writer = SerializedPatchWriter::for_model(E::MODEL);

    // Phase 1: let the derive-owned persisted-row writer emit the complete
    // structural slot image for this entity.
    entity.write_slots(&mut writer)?;

    // Phase 2: require a dense slot image so save/update replay remains
    // equivalent to the existing full-row write semantics.
    writer.finish_complete()
}

/// Apply one serialized structural patch to one raw row.
///
/// This mechanical replay step no longer owns any `Value -> bytes` dispatch.
/// It only replays already encoded slot payloads over the current row layout.
pub(in crate::db) fn apply_serialized_update_patch_to_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &SerializedUpdatePatch,
) -> Result<CanonicalRow, InternalError> {
    if patch.is_empty() {
        return canonical_row_from_raw_row(model, raw_row);
    }

    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;
    let patch_payloads = serialized_patch_payload_by_slot(model, patch)?;

    // Phase 1: replay the current row layout slot-by-slot.
    // Both patch and baseline bytes are normalized through the field contract
    // so no opaque payload can cross into the emitted row image.
    let slot_payloads = dense_canonical_slot_image_from_payload_source(model, |slot| {
        if let Some(payload) = patch_payloads[slot] {
            Ok(payload)
        } else {
            field_bytes.field(slot).ok_or_else(|| {
                InternalError::persisted_row_encode_failed(format!(
                    "slot {slot} is missing from the baseline row for entity '{}'",
                    model.path()
                ))
            })
        }
    })?;

    // Phase 2: emit the rebuilt row through the single row-construction owner.
    emit_raw_row_from_slot_payloads(model, slot_payloads.as_slice())
}
