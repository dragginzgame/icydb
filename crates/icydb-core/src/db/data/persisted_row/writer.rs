use crate::{
    db::data::persisted_row::{
        contract::encode_slot_payload_from_parts,
        types::{FieldSlot, SerializedFieldUpdate, SerializedUpdatePatch, SlotWriter},
    },
    error::InternalError,
    model::entity::EntityModel,
};

// Resolve one staged slot cell by layout index before writer-specific payload handling.
fn slot_cell_mut<T>(slots: &mut [T], slot: usize) -> Result<&mut T, InternalError> {
    slots.get_mut(slot).ok_or_else(|| {
        InternalError::persisted_row_encode_failed(
            format!("slot {slot} is outside the row layout",),
        )
    })
}

// Reject slot clears at the canonical slot-image staging boundary while keeping
// writer-specific error wording at the call site.
fn required_slot_payload_bytes<'a>(
    model: &'static EntityModel,
    writer_label: &str,
    slot: usize,
    payload: Option<&'a [u8]>,
) -> Result<&'a [u8], InternalError> {
    payload.ok_or_else(|| {
        InternalError::persisted_row_encode_failed(format!(
            "{writer_label} cannot clear slot {slot} for entity '{}'",
            model.path()
        ))
    })
}

///
/// SlotBufferWriter
///
/// SlotBufferWriter captures one dense canonical row worth of slot payloads
/// before they are encoded into the canonical slot container.
///

pub(in crate::db) struct SlotBufferWriter {
    model: &'static EntityModel,
    slots: Vec<SlotBufferSlot>,
}

impl SlotBufferWriter {
    /// Build one empty slot buffer for one entity model.
    pub(in crate::db) fn for_model(model: &'static EntityModel) -> Self {
        Self {
            model,
            slots: vec![SlotBufferSlot::Missing; model.fields().len()],
        }
    }

    /// Encode the buffered slots into the canonical row payload.
    pub(in crate::db) fn finish(self) -> Result<Vec<u8>, InternalError> {
        let slot_count = self.slots.len();
        let mut payload_bytes = Vec::new();
        let mut slot_table = Vec::with_capacity(slot_count);

        // Phase 1: require one payload for every declared slot before the row
        // can cross the canonical persisted-row boundary.
        for (slot, slot_payload) in self.slots.into_iter().enumerate() {
            match slot_payload {
                SlotBufferSlot::Set(bytes) => {
                    let start = u32::try_from(payload_bytes.len()).map_err(|_| {
                        InternalError::persisted_row_encode_failed(
                            "slot payload start exceeds u32 range",
                        )
                    })?;
                    let len = u32::try_from(bytes.len()).map_err(|_| {
                        InternalError::persisted_row_encode_failed(
                            "slot payload length exceeds u32 range",
                        )
                    })?;
                    payload_bytes.extend_from_slice(&bytes);
                    slot_table.push((start, len));
                }
                SlotBufferSlot::Missing => {
                    return Err(InternalError::persisted_row_encode_failed(format!(
                        "slot buffer writer did not emit slot {slot} for entity '{}'",
                        self.model.path()
                    )));
                }
            }
        }

        // Phase 2: flatten the slot table plus payload bytes into the canonical row image.
        encode_slot_payload_from_parts(slot_count, slot_table.as_slice(), payload_bytes.as_slice())
    }
}

impl SlotWriter for SlotBufferWriter {
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
        let entry = slot_cell_mut(self.slots.as_mut_slice(), slot)?;
        let payload = required_slot_payload_bytes(self.model, "slot buffer writer", slot, payload)?;
        *entry = SlotBufferSlot::Set(payload.to_vec());

        Ok(())
    }
}

///
/// SlotBufferSlot
///
/// SlotBufferSlot tracks whether one canonical row encoder has emitted a
/// payload for every declared slot before flattening the row payload.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum SlotBufferSlot {
    Missing,
    Set(Vec<u8>),
}

///
/// SerializedPatchWriter
///
/// SerializedPatchWriter
///
/// SerializedPatchWriter captures a dense typed entity slot image into the
/// serialized patch artifact used by `0.64` mutation staging.
/// Unlike `SlotBufferWriter`, this writer does not flatten into one row payload;
/// it preserves slot-level ownership so later stages can replay the row through
/// the structural patch boundary.
///

pub(in crate::db::data::persisted_row) struct SerializedPatchWriter {
    model: &'static EntityModel,
    slots: Vec<PatchWriterSlot>,
}

impl SerializedPatchWriter {
    /// Build one empty serialized patch writer for one entity model.
    pub(in crate::db::data::persisted_row) fn for_model(model: &'static EntityModel) -> Self {
        Self {
            model,
            slots: vec![PatchWriterSlot::Missing; model.fields().len()],
        }
    }

    /// Materialize one dense serialized patch, erroring if the writer failed
    /// to emit any declared slot.
    pub(in crate::db::data::persisted_row) fn finish_complete(
        self,
    ) -> Result<SerializedUpdatePatch, InternalError> {
        let mut entries = Vec::with_capacity(self.slots.len());

        // Phase 1: require a complete slot image so typed save/update staging
        // stays equivalent to the existing full-row encoder.
        for (slot, payload) in self.slots.into_iter().enumerate() {
            let field_slot = FieldSlot::from_index(self.model, slot)?;
            let serialized = match payload {
                PatchWriterSlot::Set(payload) => SerializedFieldUpdate::new(field_slot, payload),
                PatchWriterSlot::Missing => {
                    return Err(InternalError::persisted_row_encode_failed(format!(
                        "serialized patch writer did not emit slot {slot} for entity '{}'",
                        self.model.path()
                    )));
                }
            };
            entries.push(serialized);
        }

        Ok(SerializedUpdatePatch::new(entries))
    }
}

impl SlotWriter for SerializedPatchWriter {
    fn write_slot(&mut self, slot: usize, payload: Option<&[u8]>) -> Result<(), InternalError> {
        let entry = slot_cell_mut(self.slots.as_mut_slice(), slot)?;
        let payload =
            required_slot_payload_bytes(self.model, "serialized patch writer", slot, payload)?;
        *entry = PatchWriterSlot::Set(payload.to_vec());

        Ok(())
    }
}

///
/// PatchWriterSlot
///
/// PatchWriterSlot
///
/// PatchWriterSlot tracks whether one dense slot-image writer has emitted a
/// payload or failed to visit the slot at all.
/// That lets the typed save/update bridge reject incomplete writers instead of
/// silently leaving stale bytes in the baseline row.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum PatchWriterSlot {
    Missing,
    Set(Vec<u8>),
}
