use crate::{
    db::data::persisted_row::types::{
        FieldSlot, SerializedStructuralFieldUpdate, SerializedStructuralPatch, SlotWriter,
    },
    error::InternalError,
    model::entity::EntityModel,
};

// Resolve one staged slot cell by layout index before writer-specific payload handling.
fn slot_cell_mut<T>(slots: &mut [T], slot: usize) -> Result<&mut T, InternalError> {
    slots
        .get_mut(slot)
        .ok_or_else(InternalError::persisted_row_encode_internal)
}

// Reject slot clears at the canonical slot-image staging boundary.
fn required_slot_payload_bytes(payload: Option<&[u8]>) -> Result<&[u8], InternalError> {
    payload.ok_or_else(InternalError::persisted_row_encode_internal)
}

// Materialize one complete dense slot image from writer-owned staged slots.
fn required_dense_slot_payloads(
    slots: Vec<StagedSlotPayload>,
) -> Result<Vec<Vec<u8>>, InternalError> {
    let mut slot_payloads = Vec::with_capacity(slots.len());

    for slot_payload in slots {
        match slot_payload {
            StagedSlotPayload::Set(bytes) => slot_payloads.push(bytes),
            StagedSlotPayload::Missing => {
                return Err(InternalError::persisted_row_encode_internal());
            }
        }
    }

    Ok(slot_payloads)
}

///
/// StagedSlotPayload
///
/// StagedSlotPayload tracks whether one dense slot-image writer has emitted a
/// payload for one declared slot or failed to visit it at all.
/// `CompleteSerializedPatchWriter` uses this staged state to enforce one
/// complete canonical slot image before later contract-side row emission.
///
#[derive(Clone, Debug, Eq, PartialEq)]
enum StagedSlotPayload {
    Missing,
    Set(Vec<u8>),
}

///
/// CompleteSerializedPatchWriter
///
/// CompleteSerializedPatchWriter captures a dense typed entity slot image into
/// the serialized slot artifact used by typed save staging.
/// It preserves slot-level ownership so later stages can emit the final
/// complete row image through the structural row boundary.
///

pub(super) struct CompleteSerializedPatchWriter {
    model: &'static EntityModel,
    slots: Vec<StagedSlotPayload>,
}

impl CompleteSerializedPatchWriter {
    /// Build one empty serialized patch writer for one entity model.
    pub(super) fn for_generated_model_for_test(model: &'static EntityModel) -> Self {
        Self {
            model,
            slots: vec![StagedSlotPayload::Missing; model.fields().len()],
        }
    }

    /// Materialize one dense serialized slot image, erroring if the writer
    /// failed to emit any declared slot.
    pub(super) fn finish_dense_slot_image(
        self,
    ) -> Result<SerializedStructuralPatch, InternalError> {
        let slot_payloads = required_dense_slot_payloads(self.slots)?;
        let mut entries = Vec::with_capacity(slot_payloads.len());

        // Phase 1: require a complete slot image so typed save/update staging
        // stays equivalent to the existing full-row encoder.
        for (slot, payload) in slot_payloads.into_iter().enumerate() {
            let field_slot = FieldSlot::from_index(self.model, slot)?;
            entries.push(SerializedStructuralFieldUpdate::new(field_slot, payload));
        }

        Ok(SerializedStructuralPatch::new(entries))
    }
}

impl SlotWriter for CompleteSerializedPatchWriter {
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
        let entry = slot_cell_mut(self.slots.as_mut_slice(), slot)?;
        let payload = required_slot_payload_bytes(payload)?;
        *entry = StagedSlotPayload::Set(payload.to_vec());

        Ok(())
    }
}
