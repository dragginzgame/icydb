#[cfg(test)]
use crate::db::data::persisted_row::{
    contract::canonical_row_from_payload_source, writer::CompleteSerializedPatchWriter,
};
use crate::{
    db::{
        data::{
            CanonicalRow, RawRow, StructuralRowContract,
            persisted_row::{
                codec::ScalarSlotValueRef,
                contract::{
                    canonical_row_from_runtime_value_source_with_accepted_contract,
                    canonical_row_from_runtime_value_source_with_generated_contract,
                    decode_runtime_value_from_row_contract,
                    decode_scalar_slot_value_from_row_contract,
                    encode_runtime_value_for_accepted_field_contract,
                },
                reader::StructuralSlotReader,
                types::{
                    FieldSlot, PersistedRow, SerializedStructuralFieldUpdate,
                    SerializedStructuralPatch, SlotReader, StructuralPatch,
                },
            },
        },
        schema::AcceptedRowDecodeContract,
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
    traits::EntityValue,
    value::Value,
};
use std::borrow::Cow;

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
    #[cfg(test)]
    fn new(
        model: &'static EntityModel,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        Self::from_contract(StructuralRowContract::from_model(model), patch)
    }

    // Materialize one patch payload view over an accepted schema row contract
    // while retaining the generated bridge needed by typed entity materializers.
    fn new_with_accepted_contract(
        model: &'static EntityModel,
        accepted_decode_contract: AcceptedRowDecodeContract,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        Self::from_contract(
            StructuralRowContract::from_model_with_accepted_decode_contract(
                model,
                accepted_decode_contract,
            ),
            patch,
        )
    }

    // Materialize the slot-indexed payload view from an already selected row
    // contract so generated and accepted materialization lanes share duplicate
    // slot handling without sharing field-codec authority.
    fn from_contract(
        contract: StructuralRowContract,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let mut payloads = vec![None; contract.field_count()];

        for entry in patch.entries() {
            let slot = entry.slot().index();
            Self::generated_compatible_field_model_for(&contract, slot)?;
            payloads[slot] = Some(entry.payload());
        }

        Ok(Self { contract, payloads })
    }

    // Resolve one generated-compatible field model by stable slot index for
    // typed materialization compatibility surfaces.
    fn generated_compatible_field_model(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        Self::generated_compatible_field_model_for(&self.contract, slot)
    }

    // Resolve one generated-compatible field model from a projected structural
    // row contract.
    fn generated_compatible_field_model_for(
        contract: &StructuralRowContract,
        slot: usize,
    ) -> Result<&'static FieldModel, InternalError> {
        contract.generated_compatible_field_model(slot)
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
    #[cfg(test)]
    fn required_complete_payload(&self, slot: usize) -> Result<&[u8], InternalError> {
        self.get(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "serialized patch did not emit slot {slot} for entity '{}'",
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
    #[cfg(test)]
    fn new(
        model: &'static EntityModel,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let payloads = SerializedPatchPayloads::new(model, patch)?;
        let decoded = vec![None; payloads.contract.field_count()];

        Ok(Self { payloads, decoded })
    }

    // Build one patch-backed slot reader over the accepted row contract used by
    // production structural insert/replace staging. The accepted contract owns
    // scalar/value decode while the generated bridge remains only for typed
    // field materialization callbacks.
    fn new_with_accepted_contract(
        model: &'static EntityModel,
        accepted_decode_contract: AcceptedRowDecodeContract,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let payloads = SerializedPatchPayloads::new_with_accepted_contract(
            model,
            accepted_decode_contract,
            patch,
        )?;
        let decoded = vec![None; payloads.contract.field_count()];

        Ok(Self { payloads, decoded })
    }
}

impl SlotReader for SerializedPatchSlotReader<'_> {
    fn generated_compatible_field_model(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        self.payloads.generated_compatible_field_model(slot)
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
        let crate::model::field::LeafCodec::Scalar(_) =
            self.payloads.contract.field_leaf_codec(slot)?
        else {
            return Ok(None);
        };

        decode_scalar_slot_value_from_row_contract(
            &self.payloads.contract,
            slot,
            raw_value,
            "accepted serialized structural patch scalar read reached non-scalar slot",
            "generated serialized structural patch scalar read reached non-scalar slot",
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
            self.decoded[slot] = Some(decode_runtime_value_from_row_contract(
                &self.payloads.contract,
                slot,
                raw_value,
            )?);
        }

        Ok(self.decoded[slot].clone())
    }
}

// Materialize one typed entity directly from a sparse serialized structural
// patch so derive-owned missing-slot semantics run before final row emission.
#[cfg(test)]
pub(in crate::db) fn materialize_entity_from_serialized_structural_patch<E>(
    patch: &SerializedStructuralPatch,
) -> Result<E, InternalError>
where
    E: PersistedRow,
{
    let mut slots = SerializedPatchSlotReader::new(E::MODEL, patch)?;

    E::materialize_from_slots(&mut slots)
}

// Materialize one typed entity from a serialized structural after-image using
// accepted persisted schema as the decode authority. This is the production
// insert/replace validation bridge after SQL/session has already selected and
// serialized a complete accepted patch image.
pub(in crate::db) fn materialize_entity_from_serialized_structural_patch_with_accepted_contract<E>(
    patch: &SerializedStructuralPatch,
    accepted_decode_contract: AcceptedRowDecodeContract,
) -> Result<E, InternalError>
where
    E: PersistedRow,
{
    let mut slots = SerializedPatchSlotReader::new_with_accepted_contract(
        E::MODEL,
        accepted_decode_contract,
        patch,
    )?;

    E::materialize_from_slots(&mut slots)
}

/// Build one canonical row from one complete serialized slot image.
///
/// This helper is intentionally dense-image-only. Sparse structural insert and
/// replace materialization now routes through typed preflight first.
#[cfg(test)]
pub(in crate::db) fn canonical_row_from_complete_serialized_structural_patch(
    model: &'static EntityModel,
    patch: &SerializedStructuralPatch,
) -> Result<CanonicalRow, InternalError> {
    let patch_payloads = SerializedPatchPayloads::new(model, patch)?;

    canonical_row_from_payload_source(model, |slot| patch_payloads.required_complete_payload(slot))
}

/// Build one canonical row directly from one typed entity slot writer.
#[cfg(test)]
pub(in crate::db) fn canonical_row_from_entity<E>(entity: &E) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let serialized_slots = serialize_entity_slots_as_complete_serialized_patch(entity)?;

    canonical_row_from_complete_serialized_structural_patch(E::MODEL, &serialized_slots)
}

/// Build one canonical row from one typed entity through accepted field contracts.
///
/// This is the production save boundary for typed after-images. The concrete
/// entity still supplies runtime values by stable slot, but the accepted schema
/// contract owns the persisted field encoding policy for the final row bytes.
pub(in crate::db) fn canonical_row_from_entity_with_accepted_contract<E>(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    entity: &E,
) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);

    canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        entity
            .get_value_by_index(slot)
            .map(Cow::Owned)
            .ok_or_else(|| {
                InternalError::persisted_row_encode_failed(format!(
                    "accepted entity row emission missing slot {slot} for entity '{}'",
                    contract.entity_path()
                ))
            })
    })
}

/// Build one canonical row from one generated-contract structural slot reader.
fn canonical_row_from_structural_slot_reader_with_generated_contract(
    row_fields: &StructuralSlotReader<'_>,
) -> Result<CanonicalRow, InternalError> {
    canonical_row_from_runtime_value_source_with_generated_contract(row_fields.contract(), |slot| {
        structural_slot_reader_value(row_fields, slot)
    })
}

/// Build one canonical row from one accepted-contract structural slot reader.
pub(in crate::db) fn canonical_row_from_structural_slot_reader_with_accepted_contract(
    row_fields: &StructuralSlotReader<'_>,
) -> Result<CanonicalRow, InternalError> {
    canonical_row_from_runtime_value_source_with_accepted_contract(row_fields.contract(), |slot| {
        structural_slot_reader_value(row_fields, slot)
    })
}

/// Build one canonical row from raw bytes using one structural row contract.
///
/// This is the accepted-schema counterpart to generated-only raw-row
/// canonicalization. Callers pass the already-selected row contract, and the
/// data layer owns the exact sequence of structural decode, slot validation,
/// and dense row emission.
pub(in crate::db) fn canonical_row_from_raw_row_with_structural_contract(
    raw_row: &RawRow,
    contract: StructuralRowContract,
) -> Result<CanonicalRow, InternalError> {
    let row_fields = StructuralSlotReader::from_raw_row_with_validated_contract(raw_row, contract)?;

    if row_fields.has_accepted_decode_contract() {
        return canonical_row_from_structural_slot_reader_with_accepted_contract(&row_fields);
    }

    canonical_row_from_structural_slot_reader_with_generated_contract(&row_fields)
}

/// Build one canonical row from raw bytes using an accepted row-decode contract.
///
/// This is the accepted-schema boundary used by save paths that need to
/// normalize old before-images into generated-compatible dense row bytes before
/// commit preflight. The data layer owns accepted row-contract projection so
/// callers do not rebuild that plumbing locally.
pub(in crate::db) fn canonical_row_from_raw_row_with_accepted_decode_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);

    canonical_row_from_raw_row_with_structural_contract(raw_row, contract)
}

// Rewrap one row already loaded from storage as a canonical write token.
pub(in crate::db) const fn canonical_row_from_stored_raw_row(raw_row: RawRow) -> CanonicalRow {
    CanonicalRow::from_canonical_raw_row(raw_row)
}

/// Serialize one structural patch through an accepted row-decode contract.
///
/// This is the accepted-schema counterpart to the generated-only serializer
/// above. It keeps write target-slot admission and value-to-bytes encoding on
/// the selected persisted row contract, with no generated field fallback inside
/// the accepted write lane.
pub(in crate::db) fn serialize_structural_patch_fields_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    patch: &StructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);

    serialize_structural_patch_fields_for_accepted_contract(&contract, patch)
}

/// Serialize one structural insert/replace after-image through an accepted
/// row-decode contract.
///
/// Unlike sparse update serialization, this fills omitted accepted slots using
/// the schema-owned missing-slot policy before typed materialization. That
/// keeps insert/replace omissions on accepted database defaults instead of
/// falling through to generated Rust construction defaults.
pub(in crate::db) fn serialize_complete_structural_patch_fields_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    patch: &StructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);

    serialize_complete_structural_patch_fields_for_accepted_contract(&contract, patch)
}

// Serialize accepted-schema structural patch entries through accepted field
// contracts only. Missing accepted contracts are rejected as slot-boundary
// errors instead of falling back to generated field metadata.
fn serialize_structural_patch_fields_for_accepted_contract(
    contract: &StructuralRowContract,
    patch: &StructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    if patch.is_empty() {
        return Ok(SerializedStructuralPatch::default());
    }

    let mut entries = Vec::with_capacity(patch.entries().len());

    // Phase 1: validate and encode each ordered field update through the
    // accepted field contract selected by the database schema snapshot.
    for entry in patch.entries() {
        let slot = entry.slot();
        let Some(field) = contract.accepted_field_decode_contract(slot.index()) else {
            return Err(InternalError::persisted_row_slot_lookup_out_of_bounds(
                contract.entity_path(),
                slot.index(),
            ));
        };
        let payload = encode_runtime_value_for_accepted_field_contract(field, entry.value())?;
        entries.push(SerializedStructuralFieldUpdate::new(slot, payload));
    }

    Ok(SerializedStructuralPatch::new(entries))
}

// Serialize one sparse structural patch as a complete after-image by applying
// accepted-schema default/null policy for every omitted slot. This is only used
// at insert/replace staging, where the next materialization step expects a
// dense logical row image rather than update-style sparse intent.
fn serialize_complete_structural_patch_fields_for_accepted_contract(
    contract: &StructuralRowContract,
    patch: &StructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    let mut payloads = vec![None; contract.field_count()];

    // Phase 1: encode explicit user-provided assignments with last-write-wins
    // semantics per physical slot.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let Some(field) = contract.accepted_field_decode_contract(slot) else {
            return Err(InternalError::persisted_row_slot_lookup_out_of_bounds(
                contract.entity_path(),
                slot,
            ));
        };
        let payload = encode_runtime_value_for_accepted_field_contract(field, entry.value())?;
        payloads[slot] = Some(payload);
    }

    // Phase 2: fill every omitted accepted slot using schema-owned absence
    // policy. Required fields still fail closed here.
    for (slot, payload) in payloads.iter_mut().enumerate() {
        if payload.is_some() {
            continue;
        }
        let Some(field) = contract.accepted_field_decode_contract(slot) else {
            return Err(InternalError::persisted_row_slot_lookup_out_of_bounds(
                contract.entity_path(),
                slot,
            ));
        };
        let value = contract.missing_slot_value(slot)?;
        *payload = Some(encode_runtime_value_for_accepted_field_contract(
            field, &value,
        )?);
    }

    let entries = payloads
        .into_iter()
        .enumerate()
        .map(|(slot, payload)| {
            let payload = payload.ok_or_else(|| {
                InternalError::persisted_row_slot_lookup_out_of_bounds(contract.entity_path(), slot)
            })?;

            Ok(SerializedStructuralFieldUpdate::new(
                FieldSlot::from_validated_index(slot),
                payload,
            ))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    Ok(SerializedStructuralPatch::new(entries))
}

/// Serialize one full typed entity image into one complete serialized slot
/// image used by the typed save bridge.
///
/// This keeps typed save/update APIs on the existing surface while making it
/// explicit that the typed lane is staging a complete after-image, not a sparse
/// structural update patch.
#[cfg(test)]
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

/// Apply one serialized structural patch through an accepted row-decode contract.
///
/// This is the schema-transition counterpart to the generated-only replay
/// helper above. It materializes the old row through the accepted contract first
/// so missing append-only nullable slots become ordinary `NULL` values, then
/// overlays sparse current-layout patch payloads through accepted field decode
/// contracts before final accepted-contract row emission.
pub(in crate::db) fn apply_serialized_structural_patch_to_raw_row_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    raw_row: &RawRow,
    patch: &SerializedStructuralPatch,
) -> Result<CanonicalRow, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);
    let row_fields =
        StructuralSlotReader::from_raw_row_with_validated_contract(raw_row, contract.clone())?;
    let mut values = Vec::with_capacity(contract.field_count());

    // Phase 1: materialize the accepted baseline into current generated slot
    // order, including any nullable appended slots that are absent on disk.
    for slot in 0..contract.field_count() {
        values.push(row_fields.required_cached_value(slot)?.clone());
    }

    // Phase 2: overlay the sparse current-layout patch. Payloads are already
    // encoded bytes, so accepted field decode can materialize them directly
    // before final canonical row emission.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let value = values.get_mut(slot).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is outside the accepted structural after-image for entity '{}'",
                contract.entity_path()
            ))
        })?;
        *value = decode_runtime_value_from_row_contract(&contract, slot, entry.payload())?;
    }

    canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        values.get(slot).map(Cow::Borrowed).ok_or_else(|| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is missing from accepted structural after-image for entity '{}'",
                contract.entity_path()
            ))
        })
    })
}

// Borrow one decoded structural value by slot for canonical row emission. Both
// accepted and generated row-emission lanes use the same cache lookup and error
// wording; only the downstream field-codec authority differs.
fn structural_slot_reader_value<'a>(
    row_fields: &'a StructuralSlotReader<'_>,
    slot: usize,
) -> Result<Cow<'a, Value>, InternalError> {
    row_fields
        .required_cached_value(slot)
        .map(Cow::Borrowed)
        .map_err(|_| {
            InternalError::persisted_row_encode_failed(format!(
                "slot {slot} is missing from the structural value cache for entity '{}'",
                row_fields.contract().entity_path()
            ))
        })
}
