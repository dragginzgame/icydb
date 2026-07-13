#[cfg(test)]
use crate::model::entity::EntityModel;
use crate::{
    db::{
        data::{
            CanonicalRow, RawRow, StructuralRowContract,
            encode_canonical_value_for_accepted_field_contract,
            encode_input_value_for_accepted_field_contract,
            persisted_row::{
                codec::ScalarSlotValueRef,
                contract::{
                    RETIRED_SLOT_PLACEHOLDER_PAYLOAD,
                    canonical_row_from_runtime_value_source_with_accepted_contract,
                    decode_runtime_value_from_row_contract,
                    decode_scalar_slot_value_from_row_contract, emit_raw_row_from_slot_payloads,
                },
                reader::StructuralSlotReader,
                types::{
                    AuthoredStructuralPatch, FieldSlot, PersistedRow,
                    SerializedStructuralFieldUpdate, SerializedStructuralPatch, SlotReader,
                },
            },
        },
        schema::{
            AcceptedFieldPersistenceContract, AcceptedRowDecodeContract,
            authored_projection::AcceptedAuthoredFieldProjection,
            enum_catalog::ValueAdmissionBudget,
        },
    },
    error::InternalError,
    value::{InputValue, Value},
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
    fn new_for_model_proposal_for_test(
        model: &'static EntityModel,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        Self::from_contract(
            StructuralRowContract::from_model_proposal_for_test(model),
            patch,
        )
    }

    // Materialize one patch payload view over an accepted schema row contract.
    fn new_with_accepted_contract(
        entity_path: &'static str,
        accepted_decode_contract: AcceptedRowDecodeContract,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        Self::from_contract(
            StructuralRowContract::from_accepted_decode_contract(
                entity_path,
                accepted_decode_contract,
            ),
            patch,
        )
    }

    // Materialize the slot-indexed payload view from an already selected row
    // contract so every materialization boundary shares duplicate-slot policy.
    fn from_contract(
        contract: StructuralRowContract,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let mut payloads = vec![None; contract.field_count()];

        for entry in patch.entries() {
            let slot = entry.slot().index();
            let _ = contract.required_accepted_field_decode_contract(slot)?;
            payloads[slot] = Some(entry.payload());
        }

        Ok(Self { contract, payloads })
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
        self.get(slot)
            .ok_or_else(InternalError::persisted_row_encode_internal)
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
    // Build one sparse patch-backed reader after projecting a model proposal
    // into an accepted test contract.
    #[cfg(test)]
    fn new_for_model_proposal_for_test(
        model: &'static EntityModel,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let payloads = SerializedPatchPayloads::new_for_model_proposal_for_test(model, patch)?;
        let decoded = vec![None; payloads.contract.field_count()];

        Ok(Self { payloads, decoded })
    }

    // Build one patch-backed slot reader over the accepted row contract used by
    // production structural insert/replace staging.
    fn new_with_accepted_contract(
        entity_path: &'static str,
        accepted_decode_contract: AcceptedRowDecodeContract,
        patch: &'a SerializedStructuralPatch,
    ) -> Result<Self, InternalError> {
        let payloads = SerializedPatchPayloads::new_with_accepted_contract(
            entity_path,
            accepted_decode_contract,
            patch,
        )?;
        let decoded = vec![None; payloads.contract.field_count()];

        Ok(Self { payloads, decoded })
    }
}

impl SlotReader for SerializedPatchSlotReader<'_> {
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

        decode_scalar_slot_value_from_row_contract(&self.payloads.contract, slot, raw_value)
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

    fn runtime_enum_context(&self) -> Option<&dyn crate::value::RuntimeEnumContext> {
        Some(
            self.payloads
                .contract
                .accepted_enum_catalog_handle()
                .catalog() as &dyn crate::value::RuntimeEnumContext,
        )
    }
}

// Materialize one typed entity directly from a sparse serialized structural
// patch so derive-owned missing-slot semantics run before final row emission.
#[cfg(test)]
pub(in crate::db) fn materialize_entity_from_serialized_structural_patch_for_model_proposal_for_test<
    E,
>(
    patch: &SerializedStructuralPatch,
) -> Result<E, InternalError>
where
    E: PersistedRow,
{
    let mut slots = SerializedPatchSlotReader::new_for_model_proposal_for_test(E::MODEL, patch)?;

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
        E::MODEL.path(),
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
pub(in crate::db) fn canonical_row_from_complete_serialized_structural_patch_for_model_proposal_for_test(
    model: &'static EntityModel,
    patch: &SerializedStructuralPatch,
) -> Result<CanonicalRow, InternalError> {
    let patch_payloads = SerializedPatchPayloads::new_for_model_proposal_for_test(model, patch)?;
    let slot_payloads = (0..patch_payloads.contract.field_count())
        .map(|slot| {
            patch_payloads
                .required_complete_payload(slot)
                .map(Vec::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let staged = emit_raw_row_from_slot_payloads(
        patch_payloads.contract.field_count(),
        slot_payloads.as_slice(),
    )?
    .into_raw_row();

    canonical_row_from_raw_row_with_structural_contract(&staged, &patch_payloads.contract)
}

/// Build one canonical row from a model proposal through accepted field contracts.
#[cfg(test)]
pub(in crate::db) fn canonical_row_from_entity_for_model_proposal_for_test<E>(
    entity: &E,
) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let accepted_decode_contract =
        AcceptedRowDecodeContract::from_model_proposal_for_test(E::MODEL);

    canonical_row_from_entity_with_accepted_contract(
        E::MODEL.path(),
        accepted_decode_contract,
        entity,
    )
}

/// Build one canonical row from one typed entity through accepted field contracts.
///
/// This is the production save boundary for typed after-images. The concrete
/// entity supplies authored inputs by stable slot, and the accepted schema
/// contract owns admission and persisted encoding for the final row bytes.
pub(in crate::db) fn canonical_row_from_entity_with_accepted_contract<E>(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    entity: &E,
) -> Result<CanonicalRow, InternalError>
where
    E: PersistedRow,
{
    let authored = AcceptedAuthoredFieldProjection::new(&accepted_decode_contract);
    let contract = StructuralRowContract::from_accepted_decode_contract(
        entity_path,
        accepted_decode_contract.clone(),
    );
    let mut slot_payloads = Vec::with_capacity(contract.field_count());

    for slot in 0..contract.field_count() {
        if !contract.has_active_field_slot(slot) {
            slot_payloads.push(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
            continue;
        }
        let field = accepted_decode_contract
            .field_for_slot(slot)
            .ok_or_else(InternalError::persisted_row_encode_internal)?;
        if field.generated() {
            let mut budget = ValueAdmissionBudget::standard();
            slot_payloads.push(
                authored
                    .encode_field(entity, slot, &mut budget)
                    .map_err(|_| InternalError::persisted_row_encode_internal())?,
            );
            continue;
        }

        let value = contract.missing_slot_value(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        slot_payloads.push(encode_canonical_value_for_accepted_field_contract(
            encoding, &value,
        )?);
    }

    emit_raw_row_from_slot_payloads(contract.field_count(), slot_payloads.as_slice())
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
/// Callers must pass an accepted-schema row contract.
pub(in crate::db) fn canonical_row_from_raw_row_with_structural_contract(
    raw_row: &RawRow,
    contract: &StructuralRowContract,
) -> Result<CanonicalRow, InternalError> {
    let row_fields =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(raw_row, contract)?;

    canonical_row_from_structural_slot_reader_with_accepted_contract(&row_fields)
}

/// Build one canonical row from raw bytes using an accepted row-decode contract.
///
/// This is the accepted-schema boundary used by save paths that need to
/// normalize current-format before-images into accepted dense row bytes before
/// commit preflight. The data layer owns accepted row-contract projection so
/// callers do not rebuild that plumbing locally.
pub(in crate::db) fn canonical_row_from_raw_row_with_accepted_decode_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);

    canonical_row_from_raw_row_with_structural_contract(raw_row, &contract)
}

// Rewrap one row already loaded from storage as a canonical write token.
pub(in crate::db) const fn canonical_row_from_stored_raw_row(raw_row: RawRow) -> CanonicalRow {
    CanonicalRow::from_canonical_raw_row(raw_row)
}

// Admit every authored value before selecting its accepted storage codec.
fn encode_authored_value_for_accepted_field_contract(
    encoding: AcceptedFieldPersistenceContract<'_>,
    input: InputValue,
) -> Result<Vec<u8>, InternalError> {
    let field = encoding.field();
    let mut budget = ValueAdmissionBudget::standard();
    encode_input_value_for_accepted_field_contract(encoding, input, &mut budget)
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))
}

/// Serialize one structural patch through an accepted row-decode contract.
///
/// Write target-slot admission and value-to-bytes encoding remain on the
/// selected accepted row contract.
pub(in crate::db) fn serialize_structural_patch_fields_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    patch: &AuthoredStructuralPatch,
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
/// falling through to generated Rust `Default` behavior.
pub(in crate::db) fn serialize_complete_structural_patch_fields_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    patch: &AuthoredStructuralPatch,
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
    patch: &AuthoredStructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    if patch.is_empty() {
        return Ok(SerializedStructuralPatch::default());
    }

    let mut entries = Vec::with_capacity(patch.entries().len());

    // Phase 1: validate and encode each ordered field update through the
    // accepted field contract selected by the database schema snapshot.
    for entry in patch.entries() {
        let slot = entry.slot();
        let encoding = contract.required_accepted_field_persistence_contract(slot.index())?;
        let payload =
            encode_authored_value_for_accepted_field_contract(encoding, entry.value().clone())?;
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
    patch: &AuthoredStructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    let mut payloads = vec![None; contract.field_count()];

    // Phase 1: encode explicit user-provided assignments with last-write-wins
    // semantics per physical slot.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        let payload =
            encode_authored_value_for_accepted_field_contract(encoding, entry.value().clone())?;
        payloads[slot] = Some(payload);
    }

    // Phase 2: fill every omitted accepted slot using schema-owned absence
    // policy. Required fields still fail closed here.
    for (slot, payload) in payloads.iter_mut().enumerate() {
        if payload.is_some() {
            continue;
        }
        if !contract.has_active_field_slot(slot) {
            *payload = Some(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
            continue;
        }
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        let value = contract.missing_slot_value(slot)?;
        *payload = Some(encode_authored_value_for_accepted_field_contract(
            encoding,
            InputValue::try_from_runtime_non_enum(&value)
                .ok_or_else(InternalError::persisted_row_encode_internal)?,
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

/// Apply one serialized structural patch through an accepted row-decode contract.
///
/// It materializes the old row through the accepted contract first
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
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(raw_row, &contract)?;
    let mut values = Vec::with_capacity(contract.field_count());

    // Phase 1: materialize the accepted baseline into current generated slot
    // order, including any nullable appended slots that are absent on disk.
    for slot in 0..contract.field_count() {
        if contract.has_active_field_slot(slot) {
            values.push(row_fields.required_cached_value(slot)?.clone());
        } else {
            values.push(Value::Null);
        }
    }

    // Phase 2: overlay the sparse current-layout patch. Payloads are already
    // encoded bytes, so accepted field decode can materialize them directly
    // before final canonical row emission.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let value = values
            .get_mut(slot)
            .ok_or_else(InternalError::persisted_row_encode_internal)?;
        *value = decode_runtime_value_from_row_contract(&contract, slot, entry.payload())?;
    }

    canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
        values
            .get(slot)
            .map(Cow::Borrowed)
            .ok_or_else(InternalError::persisted_row_encode_internal)
    })
}

// Borrow one decoded structural value by slot for canonical row emission.
fn structural_slot_reader_value<'a>(
    row_fields: &'a StructuralSlotReader<'_>,
    slot: usize,
) -> Result<Cow<'a, Value>, InternalError> {
    row_fields
        .required_cached_value(slot)
        .map(Cow::Borrowed)
        .map_err(|_| InternalError::persisted_row_encode_internal())
}
