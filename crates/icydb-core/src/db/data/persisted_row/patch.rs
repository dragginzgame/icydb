use crate::{
    db::data::{CanonicalRow, RawRow, StructuralRowDecodeError, StructuralRowFieldBytes},
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};
use std::borrow::Cow;

use crate::db::data::persisted_row::{
    codec::ScalarSlotValueRef,
    contract::{
        canonical_row_from_payload_source, canonical_row_from_slot_payload_bytes,
        canonical_row_from_value_source, decode_slot_value_from_bytes,
        encode_slot_value_from_value, field_model_for_slot, serialized_patch_payload_by_slot,
    },
    reader::StructuralSlotReader,
    types::{PersistedRow, SerializedFieldUpdate, SerializedUpdatePatch, SlotReader, UpdatePatch},
    writer::{CompleteSerializedPatchWriter, SlotBufferWriter},
};

///
/// SerializedPatchSlotReader
///
/// Adapts a sparse serialized structural patch to the slot-reader contract so
/// typed materialization can apply derive-owned missing-slot semantics before
/// any dense row image is emitted.
///
struct SerializedPatchSlotReader<'a> {
    model: &'static EntityModel,
    payloads: Vec<Option<&'a [u8]>>,
    decoded: Vec<Option<Value>>,
}

impl<'a> SerializedPatchSlotReader<'a> {
    // Build one sparse patch-backed slot reader for one entity model.
    fn new(
        model: &'static EntityModel,
        patch: &'a SerializedUpdatePatch,
    ) -> Result<Self, InternalError> {
        let payloads = serialized_patch_payload_by_slot(model, patch)?;
        let decoded = vec![None; model.fields().len()];

        Ok(Self {
            model,
            payloads,
            decoded,
        })
    }
}

impl SlotReader for SerializedPatchSlotReader<'_> {
    fn model(&self) -> &'static EntityModel {
        self.model
    }

    fn has(&self, slot: usize) -> bool {
        self.payloads.get(slot).is_some_and(Option::is_some)
    }

    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.payloads.get(slot).copied().flatten()
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let Some(raw_value) = self.get_bytes(slot) else {
            return Ok(None);
        };
        let field = field_model_for_slot(self.model, slot)?;
        let crate::model::field::LeafCodec::Scalar(codec) = field.leaf_codec() else {
            return Ok(None);
        };

        crate::db::data::persisted_row::codec::decode_scalar_slot_value(
            raw_value,
            codec,
            field.name(),
        )
        .map(Some)
    }

    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        if slot >= self.decoded.len() {
            return Ok(None);
        }

        if self.decoded[slot].is_none()
            && let Some(raw_value) = self.get_bytes(slot)
        {
            self.decoded[slot] = Some(decode_slot_value_from_bytes(self.model, slot, raw_value)?);
        }

        Ok(self.decoded[slot].clone())
    }
}

// Materialize one typed entity directly from a sparse serialized structural
// patch so derive-owned missing-slot semantics run before final row emission.
pub(in crate::db) fn materialize_entity_from_serialized_update_patch<E>(
    patch: &SerializedUpdatePatch,
) -> Result<E, InternalError>
where
    E: PersistedRow,
{
    let mut slots = SerializedPatchSlotReader::new(E::MODEL, patch)?;

    E::materialize_from_slots(&mut slots)
}

/// Build one canonical row from one complete serialized slot image.
///
/// This helper is intentionally dense-image-only. Sparse structural insert and
/// replace materialization now routes through typed preflight first.
pub(in crate::db) fn canonical_row_from_complete_serialized_update_patch(
    model: &'static EntityModel,
    patch: &SerializedUpdatePatch,
) -> Result<CanonicalRow, InternalError> {
    let patch_payloads = serialized_patch_payload_by_slot(model, patch)?;

    canonical_row_from_payload_source(model, |slot| {
        patch_payloads[slot].ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "serialized patch did not emit slot {slot} for entity '{}'",
                model.path()
            ))
        })
    })
}

/// Build one canonical row directly from one typed entity slot writer.
pub(in crate::db) fn canonical_row_from_entity<E>(entity: &E) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let mut writer = SlotBufferWriter::for_model(E::MODEL);

    // Phase 1: let the derive-owned slot writer emit the complete typed row image.
    entity.write_slots(&mut writer)?;

    // Phase 2: re-emit the dense slot payload image through the shared
    // contract-side row-envelope owner.
    canonical_row_from_slot_payload_bytes(writer.finish()?)
}

/// Build one canonical row from one already-decoded structural slot reader.
pub(in crate::db) fn canonical_row_from_structural_slot_reader(
    row_fields: &StructuralSlotReader<'_>,
) -> Result<CanonicalRow, InternalError> {
    canonical_row_from_value_source(row_fields.model(), |slot| {
        row_fields
            .required_cached_value(slot)
            .map(Cow::Borrowed)
            .map_err(|_| {
                InternalError::persisted_row_encode_failed(format!(
                    "slot {slot} is missing from the structural value cache for entity '{}'",
                    row_fields.model().path()
                ))
            })
    })
}

// Rebuild one full canonical row image from an existing raw row before it
// crosses a storage write boundary.
pub(in crate::db) fn canonical_row_from_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    canonical_row_from_payload_source(model, |slot| {
        field_bytes.field(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is missing from the baseline row for entity '{}'",
                model.path()
            ))
        })
    })
}

// Rewrap one row already loaded from storage as a canonical write token.
pub(in crate::db) const fn canonical_row_from_stored_raw_row(raw_row: RawRow) -> CanonicalRow {
    CanonicalRow::from_canonical_raw_row(raw_row)
}

/// Apply one ordered structural patch to one raw row using the current
/// persisted-row field codec authority.
#[cfg(test)]
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

/// Serialize one full typed entity image into one complete serialized slot
/// image used by the typed save bridge.
///
/// This keeps typed save/update APIs on the existing surface while making it
/// explicit that the typed lane is staging a complete after-image, not a sparse
/// structural update patch.
pub(in crate::db) fn serialize_entity_slots_as_complete_serialized_patch<E>(
    entity: &E,
) -> Result<SerializedUpdatePatch, InternalError>
where
    E: PersistedRow,
{
    let mut writer = CompleteSerializedPatchWriter::for_model(E::MODEL);

    // Phase 1: let the derive-owned persisted-row writer emit the complete
    // structural slot image for this entity.
    entity.write_slots(&mut writer)?;

    // Phase 2: require a dense slot image so save/update replay remains
    // equivalent to the existing full-row write semantics.
    writer.finish_dense_slot_image()
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

    canonical_row_from_payload_source(model, |slot| {
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
    })
}
