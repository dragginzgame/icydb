use crate::{
    db::data::{
        CanonicalRow, RawRow, StructuralRowContract, StructuralRowDecodeError,
        StructuralRowFieldBytes,
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
    value::Value,
};
use std::borrow::Cow;

use crate::db::data::persisted_row::{
    codec::ScalarSlotValueRef,
    contract::{
        canonical_row_from_payload_source, canonical_row_from_runtime_value_source,
        decode_field_slot_into_runtime_value, encode_runtime_value_into_slot,
    },
    reader::StructuralSlotReader,
    types::{
        PersistedRow, SerializedStructuralFieldUpdate, SerializedStructuralPatch, SlotReader,
        StructuralPatch,
    },
    writer::CompleteSerializedPatchWriter,
};

///
/// SerializedPatchPayloads
///
/// SerializedPatchPayloads owns the slot-indexed view of one serialized
/// structural patch.
/// It centralizes duplicate-slot last-write-wins handling and the difference
/// between complete after-image payloads and sparse baseline-overlay replay.
///

struct SerializedPatchPayloads<'a> {
    contract: StructuralRowContract,
    payloads: Vec<Option<&'a [u8]>>,
}

impl<'a> SerializedPatchPayloads<'a> {
    // Materialize the last-write-wins serialized patch view indexed by stable
    // slot so later replay paths do not each rebuild that policy locally.
    fn new(
        model: &'static EntityModel,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let contract = StructuralRowContract::from_model(model);
        let mut payloads = vec![None; contract.field_count()];

        for entry in patch.entries() {
            let slot = entry.slot().index();
            Self::field_contract_for(contract, slot)?;
            payloads[slot] = Some(entry.payload());
        }

        Ok(Self { contract, payloads })
    }

    // Resolve one field contract by stable slot index so patch-backed readers
    // use the same metadata seam as raw-row structural readers.
    fn field_contract(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        Self::field_contract_for(self.contract, slot)
    }

    // Resolve one field contract from a projected structural row contract.
    fn field_contract_for(
        contract: StructuralRowContract,
        slot: usize,
    ) -> Result<&'static FieldModel, InternalError> {
        contract.fields().get(slot).ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(contract.entity_path(), slot)
        })
    }

    // Return whether this patch after-image currently carries a payload for
    // the requested slot.
    fn has(&self, slot: usize) -> bool {
        self.payloads.get(slot).is_some_and(Option::is_some)
    }

    // Borrow one patch payload by stable slot index.
    fn get(&self, slot: usize) -> Option<&[u8]> {
        self.payloads.get(slot).copied().flatten()
    }

    // Borrow one complete after-image payload, rejecting sparse patches at the
    // fresh-row emission boundary where every declared slot must be present.
    fn required_complete_payload(&self, slot: usize) -> Result<&[u8], InternalError> {
        self.get(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "serialized patch did not emit slot {slot} for entity '{}'",
                self.contract.entity_path()
            ))
        })
    }

    // Borrow either the sparse patch payload or the baseline row payload for
    // one slot, keeping update overlay policy out of the final row-emission
    // closure.
    fn overlay_payload<'b>(
        &'b self,
        baseline: &'b StructuralRowFieldBytes<'_>,
        slot: usize,
    ) -> Result<&'b [u8], InternalError> {
        if let Some(payload) = self.get(slot) {
            return Ok(payload);
        }

        baseline.field(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is missing from the baseline row for entity '{}'",
                self.contract.entity_path()
            ))
        })
    }
}

///
/// SerializedPatchSlotReader
///
/// Adapts a sparse serialized structural patch to the slot-reader contract so
/// typed materialization can apply derive-owned missing-slot semantics before
/// any dense row image is emitted.
///
struct SerializedPatchSlotReader<'a> {
    payloads: SerializedPatchPayloads<'a>,
    decoded: Vec<Option<Value>>,
}

impl<'a> SerializedPatchSlotReader<'a> {
    // Build one sparse patch-backed slot reader for one entity model.
    fn new(
        model: &'static EntityModel,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let payloads = SerializedPatchPayloads::new(model, patch)?;
        let decoded = vec![None; payloads.contract.field_count()];

        Ok(Self { payloads, decoded })
    }
}

impl SlotReader for SerializedPatchSlotReader<'_> {
    fn field_contract(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        self.payloads.field_contract(slot)
    }

    fn has(&self, slot: usize) -> bool {
        self.payloads.has(slot)
    }

    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.payloads.get(slot)
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let Some(raw_value) = self.get_bytes(slot) else {
            return Ok(None);
        };
        let field = self.field_contract(slot)?;
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
            let field = self.field_contract(slot)?;
            self.decoded[slot] = Some(decode_field_slot_into_runtime_value(field, raw_value)?);
        }

        Ok(self.decoded[slot].clone())
    }
}

// Materialize one typed entity directly from a sparse serialized structural
// patch so derive-owned missing-slot semantics run before final row emission.
pub(in crate::db) fn materialize_entity_from_serialized_structural_patch<E>(
    patch: &SerializedStructuralPatch,
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
pub(in crate::db) fn canonical_row_from_complete_serialized_structural_patch(
    model: &'static EntityModel,
    patch: &SerializedStructuralPatch,
) -> Result<CanonicalRow, InternalError> {
    let patch_payloads = SerializedPatchPayloads::new(model, patch)?;

    canonical_row_from_payload_source(model, |slot| patch_payloads.required_complete_payload(slot))
}

/// Build one canonical row directly from one typed entity slot writer.
pub(in crate::db) fn canonical_row_from_entity<E>(entity: &E) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let serialized_slots = serialize_entity_slots_as_complete_serialized_patch(entity)?;

    canonical_row_from_complete_serialized_structural_patch(E::MODEL, &serialized_slots)
}

/// Build one canonical row from one already-decoded structural slot reader.
pub(in crate::db) fn canonical_row_from_structural_slot_reader(
    model: &'static EntityModel,
    row_fields: &StructuralSlotReader<'_>,
) -> Result<CanonicalRow, InternalError> {
    canonical_row_from_runtime_value_source(model, |slot| {
        row_fields
            .required_cached_value(slot)
            .map(Cow::Borrowed)
            .map_err(|_| {
                InternalError::persisted_row_encode_failed(format!(
                    "slot {slot} is missing from the structural value cache for entity '{}'",
                    model.path()
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

/// Serialize one ordered structural patch into canonical slot payload bytes.
///
/// This is the phase-1 partial-serialization seam for `0.64`: later mutation
/// stages can stage or replay one field patch without rebuilding the runtime
/// value-to-bytes contract per consumer.
pub(in crate::db) fn serialize_structural_patch_fields(
    model: &'static EntityModel,
    patch: &StructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    if patch.is_empty() {
        return Ok(SerializedStructuralPatch::default());
    }

    let mut entries = Vec::with_capacity(patch.entries().len());

    // Phase 1: validate and encode each ordered field update through the
    // canonical slot codec owner.
    for entry in patch.entries() {
        let slot = entry.slot();
        let payload = encode_runtime_value_into_slot(model, slot.index(), entry.value())?;
        entries.push(SerializedStructuralFieldUpdate::new(slot, payload));
    }

    Ok(SerializedStructuralPatch::new(entries))
}

/// Serialize one full typed entity image into one complete serialized slot
/// image used by the typed save bridge.
///
/// This keeps typed save/update APIs on the existing surface while making it
/// explicit that the typed lane is staging a complete after-image, not a sparse
/// structural update patch.
pub(in crate::db) fn serialize_entity_slots_as_complete_serialized_patch<E>(
    entity: &E,
) -> Result<SerializedStructuralPatch, InternalError>
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
pub(in crate::db) fn apply_serialized_structural_patch_to_raw_row(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &SerializedStructuralPatch,
) -> Result<CanonicalRow, InternalError> {
    if patch.is_empty() {
        return canonical_row_from_raw_row(model, raw_row);
    }

    let field_bytes = StructuralRowFieldBytes::from_raw_row(raw_row, model)
        .map_err(StructuralRowDecodeError::into_internal_error)?;
    let patch_payloads = SerializedPatchPayloads::new(model, patch)?;

    canonical_row_from_payload_source(model, |slot| {
        patch_payloads.overlay_payload(&field_bytes, slot)
    })
}
