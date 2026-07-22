#[cfg(feature = "sql")]
use crate::db::codec::{
    finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32, write_hash_u32,
};
#[cfg(feature = "sql")]
use crate::db::data::CanonicalSlotReader;
#[cfg(test)]
use crate::db::data::PersistedRow;
#[cfg(feature = "sql")]
use crate::db::data::persisted_row::types::FieldSlot;
use crate::{
    db::{
        data::{
            CanonicalRow, RawRow, StructuralRowContract,
            encode_accepted_value_ref_for_accepted_field_contract,
            encode_canonical_value_for_accepted_field_contract,
            persisted_row::{
                contract::{
                    RETIRED_SLOT_PLACEHOLDER_PAYLOAD,
                    canonical_row_from_runtime_value_source_with_accepted_contract,
                    emit_raw_row_from_slot_payloads,
                },
                reader::StructuralSlotReader,
                types::{
                    AcceptedInsertPolicyRequest, AcceptedMutationFieldWriteIntent,
                    AcceptedMutationIntentPatch, SlotReader,
                },
            },
        },
        schema::{
            AcceptedFieldPersistenceContract, AcceptedInsertOmissionPolicy,
            AcceptedRowDecodeContract,
            authored_projection::{AcceptedAuthoredFieldProjection, AuthoredFieldAdmissionError},
            enum_catalog::{ValueAdmissionBudget, ValueAdmissionError},
        },
    },
    error::InternalError,
    model::field::{FieldInsertGeneration, FieldWriteManagement},
    sanitize::SanitizeWriteContext,
    traits::AuthoredFieldProjection,
    types::Ulid,
    value::{InputValue, Value},
};
#[cfg(feature = "sql")]
use sha2::Digest;
use std::borrow::Cow;

#[cfg(feature = "sql")]
const ACCEPTED_FIXED_UPDATE_PATCH_FINGERPRINT_DOMAIN: &[u8] =
    b"icydb.accepted-fixed-update-patch.v1";

/// Provenance of one resolved accepted field in a mutation after-image.
///
/// Sanitizer validation uses this transient fact to distinguish caller-authored
/// values from canonical database values that application code may not alter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedFieldWriteProvenance {
    /// Exact caller-authored field input.
    Authored,
    /// Accepted default selected for one exact insertion-policy request.
    ResolvedDefault(AcceptedInsertPolicyRequest),
    /// Accepted nullable policy materialized as canonical `NULL`.
    ResolvedNull(AcceptedInsertPolicyRequest),
    /// Accepted insert generator evaluated exactly once for this after-image.
    InsertGenerated(AcceptedInsertPolicyRequest),
    /// Accepted insert-management policy evaluated for this after-image.
    InsertManaged(AcceptedInsertPolicyRequest),
    /// Accepted update-management policy evaluated for this after-image.
    UpdateManaged,
    /// Existing logical value preserved by an unassigned update field.
    Preserved,
    /// Database-owned primary-key value preserved by keyed replacement.
    PreservedReplacementIdentity,
    /// Frozen historical fill materialized from a legitimately shorter row.
    HistoricalFill,
}

impl AcceptedFieldWriteProvenance {
    /// Return whether an application sanitizer may transform this field.
    #[must_use]
    pub(in crate::db) const fn sanitizer_may_transform(self) -> bool {
        matches!(self, Self::Authored)
    }
}

/// Complete canonical accepted row paired with per-slot write provenance.
///
/// Construction resolves every active slot before typed materialization, so a
/// caller cannot separate canonical database values from the provenance proof
/// required at the sanitizer boundary.
pub(in crate::db) struct ResolvedAcceptedMutationRow {
    row: CanonicalRow,
    provenance: Vec<Option<AcceptedFieldWriteProvenance>>,
}

/// Canonical accepted target for one fixed resumable-update assignment.
///
/// The payload has already crossed accepted value admission and storage
/// encoding, so later resumable pages cannot reinterpret the authored literal
/// or an explicit `DEFAULT` request.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
pub(in crate::db) struct AcceptedFixedUpdateField {
    slot: FieldSlot,
    payload: Vec<u8>,
}

#[cfg(feature = "sql")]
impl AcceptedFixedUpdateField {
    /// Return the accepted physical field slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> FieldSlot {
        self.slot
    }
}

/// Canonical fixed patch admitted for resumable convergence.
///
/// Duplicate SQL assignments have already collapsed through the ordinary
/// last-write-wins patch rule. Entries are stored in physical-slot order and
/// carry one deterministic fingerprint over the exact accepted payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
pub(in crate::db) struct AcceptedFixedUpdatePatch {
    fields: Vec<AcceptedFixedUpdateField>,
    fingerprint: [u8; 32],
}

#[cfg(feature = "sql")]
impl AcceptedFixedUpdatePatch {
    /// Resolve one update intent into fixed accepted payloads.
    pub(in crate::db) fn from_update_intent(
        entity_path: &'static str,
        accepted_decode_contract: AcceptedRowDecodeContract,
        patch: &AcceptedMutationIntentPatch,
    ) -> Result<Self, InternalError> {
        let contract = StructuralRowContract::from_accepted_decode_contract(
            entity_path,
            accepted_decode_contract,
        );
        let mut intents = vec![None; contract.field_count()];

        for entry in patch.entries() {
            let slot = entry.slot().index();
            let _ = contract.required_accepted_field_contract(slot)?;
            intents[slot] = Some(entry.intent().clone());
        }

        let mut fields = Vec::with_capacity(patch.entries().len());
        for (slot, intent) in intents.into_iter().enumerate() {
            let Some(intent) = intent else {
                continue;
            };
            let payload = match intent {
                AcceptedMutationFieldWriteIntent::Authored(input) => {
                    encode_authored_value_for_accepted_field_contract(
                        contract.required_accepted_field_persistence_contract(slot)?,
                        input,
                    )?
                }
                AcceptedMutationFieldWriteIntent::Resolve(
                    AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
                ) => resolve_explicit_update_default(&contract, slot)?.0,
                AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(_)
                | AcceptedMutationFieldWriteIntent::Resolve(
                    AcceptedInsertPolicyRequest::OmittedInsert
                    | AcceptedInsertPolicyRequest::ExplicitInsertDefault,
                ) => return Err(InternalError::executor_invariant()),
            };
            fields.push(AcceptedFixedUpdateField {
                slot: FieldSlot::from_validated_index(slot),
                payload,
            });
        }

        if fields.is_empty() {
            return Err(InternalError::executor_invariant());
        }

        let mut hasher = new_hash_sha256_prefixed(ACCEPTED_FIXED_UPDATE_PATCH_FINGERPRINT_DOMAIN);
        write_hash_len_u32(&mut hasher, fields.len());
        for field in &fields {
            write_hash_u32(
                &mut hasher,
                u32::try_from(field.slot.index())
                    .map_err(|_| InternalError::executor_invariant())?,
            );
            write_hash_len_u32(&mut hasher, field.payload.len());
            hasher.update(field.payload.as_slice());
        }

        Ok(Self {
            fields,
            fingerprint: finalize_hash_sha256(hasher),
        })
    }

    /// Borrow fixed accepted targets in physical-slot order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[AcceptedFixedUpdateField] {
        self.fields.as_slice()
    }

    /// Return whether every fixed authored target already matches one accepted row.
    pub(in crate::db) fn is_satisfied_by(
        &self,
        row: &dyn CanonicalSlotReader,
    ) -> Result<bool, InternalError> {
        for field in &self.fields {
            if row.required_bytes(field.slot.index())? != field.payload.as_slice() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Return the deterministic accepted patch fingerprint.
    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

impl ResolvedAcceptedMutationRow {
    /// Build one resolved row after canonical slot emission succeeds.
    #[must_use]
    const fn new(row: CanonicalRow, provenance: Vec<Option<AcceptedFieldWriteProvenance>>) -> Self {
        Self { row, provenance }
    }

    /// Consume the invariant-bearing row into its paired artifacts.
    #[must_use]
    pub(in crate::db) fn into_parts(
        self,
    ) -> (CanonicalRow, Vec<Option<AcceptedFieldWriteProvenance>>) {
        (self.row, self.provenance)
    }
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

/// Build a test-only canonical row from typed fields and accepted insert policy.
///
/// Production mutation paths already carry a resolved complete after-image and
/// use `canonical_row_from_resolved_entity_with_accepted_contract` instead.
#[cfg(test)]
pub(in crate::db) fn canonical_row_from_entity_with_accepted_contract<E>(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    entity: &E,
) -> Result<CanonicalRow, InternalError>
where
    E: AuthoredFieldProjection,
{
    canonical_row_from_entity_with_optional_resolved_row(
        entity_path,
        accepted_decode_contract,
        entity,
        None,
    )
}

/// Re-emit one sanitizer-normalized entity while preserving resolved DDL-owned slots.
///
/// Generated fields come from the normalized entity. Fields absent from the
/// generated Rust type come from the already-resolved complete after-image, so
/// updates never reinterpret preserved values through current insert policy.
pub(in crate::db) fn canonical_row_from_resolved_entity_with_accepted_contract<E>(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    entity: &E,
    resolved_row: &RawRow,
) -> Result<CanonicalRow, InternalError>
where
    E: AuthoredFieldProjection,
{
    canonical_row_from_entity_with_optional_resolved_row(
        entity_path,
        accepted_decode_contract,
        entity,
        Some(resolved_row),
    )
}

fn canonical_row_from_entity_with_optional_resolved_row<E>(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    entity: &E,
    resolved_row: Option<&RawRow>,
) -> Result<CanonicalRow, InternalError>
where
    E: AuthoredFieldProjection,
{
    let authored = AcceptedAuthoredFieldProjection::new(&accepted_decode_contract);
    let contract = StructuralRowContract::from_accepted_decode_contract(
        entity_path,
        accepted_decode_contract.clone(),
    );
    let resolved = resolved_row
        .map(|row| {
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, &contract)
        })
        .transpose()?;
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
                    .map_err(authored_field_admission_error)?,
            );
            continue;
        }

        if let Some(resolved) = resolved.as_ref() {
            let payload = resolved
                .get_bytes(slot)
                .ok_or_else(InternalError::persisted_row_encode_internal)?;
            slot_payloads.push(payload.to_vec());
            continue;
        }

        let value = contract.insert_omission_value(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        slot_payloads.push(encode_canonical_value_for_accepted_field_contract(
            encoding, &value,
        )?);
    }

    emit_raw_row_from_slot_payloads(
        contract.current_layout_version(),
        contract.field_count(),
        slot_payloads.as_slice(),
    )
}

// Preserve authored-input rejection as caller-unsupported while classifying
// accepted/generated authority drift as an invariant failure. Encoding begins
// only after exact admission succeeds, so codec failures retain serialize
// ownership.
fn authored_field_admission_error(error: AuthoredFieldAdmissionError) -> InternalError {
    match error {
        AuthoredFieldAdmissionError::Admission(error) => value_admission_error(error),
        AuthoredFieldAdmissionError::FieldNotGenerated { .. }
        | AuthoredFieldAdmissionError::MissingAuthoredValue { .. }
        | AuthoredFieldAdmissionError::MissingFieldContract { .. } => {
            InternalError::executor_invariant()
        }
        AuthoredFieldAdmissionError::PersistenceEncoding { .. } => {
            InternalError::persisted_row_encode_internal()
        }
    }
}

fn value_admission_error(error: ValueAdmissionError) -> InternalError {
    match error {
        ValueAdmissionError::InvalidAcceptedContract
        | ValueAdmissionError::MissingSchemaRevision
        | ValueAdmissionError::UnknownCompositeType => InternalError::executor_invariant(),
        ValueAdmissionError::DepthExceeded
        | ValueAdmissionError::SizeExceeded
        | ValueAdmissionError::TypeMismatch
        | ValueAdmissionError::ScalarConstraint
        | ValueAdmissionError::EnumPathMismatch
        | ValueAdmissionError::EnumTypeMismatch
        | ValueAdmissionError::UnknownEnumType
        | ValueAdmissionError::UnknownEnumVariant
        | ValueAdmissionError::EnumBodyMismatch
        | ValueAdmissionError::CompositeShapeMismatch
        | ValueAdmissionError::CompositeFieldMismatch
        | ValueAdmissionError::DuplicateSetItem
        | ValueAdmissionError::DuplicateMapKey => InternalError::executor_unsupported(),
    }
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
    encoding
        .admission_contract()
        .with_normalized(input, &mut budget, |accepted| {
            encode_accepted_value_ref_for_accepted_field_contract(field, &accepted)
        })
        .map_err(value_admission_error)?
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))
}

// Resolve one active insert slot while keeping field-policy branching inside
// the accepted row boundary. The caller owns only dense slot assembly.
fn resolve_insert_active_slot(
    entity_path: &'static str,
    contract: &StructuralRowContract,
    slot: usize,
    intent: Option<AcceptedMutationFieldWriteIntent>,
    write_context: SanitizeWriteContext,
) -> Result<(Vec<u8>, AcceptedFieldWriteProvenance), InternalError> {
    let field = contract.required_accepted_field_contract(slot)?;
    let write_policy = field.write_policy();
    let request = match intent {
        Some(AcceptedMutationFieldWriteIntent::Authored(input)) => {
            if write_policy.insert_generation().is_some()
                || write_policy.write_management().is_some()
            {
                return Err(InternalError::mutation_database_owned_field_explicit(
                    entity_path,
                    field.field_name(),
                ));
            }
            let encoding = contract.required_accepted_field_persistence_contract(slot)?;
            let payload = encode_authored_value_for_accepted_field_contract(encoding, input)?;

            return Ok((payload, AcceptedFieldWriteProvenance::Authored));
        }
        Some(AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(input)) => {
            if !contract.primary_key_slot_indices().contains(&slot)
                || (write_policy.insert_generation().is_none()
                    && write_policy.write_management().is_none())
            {
                return Err(InternalError::executor_invariant());
            }
            let encoding = contract.required_accepted_field_persistence_contract(slot)?;
            let payload = encode_authored_value_for_accepted_field_contract(encoding, input)?;

            return Ok((
                payload,
                AcceptedFieldWriteProvenance::PreservedReplacementIdentity,
            ));
        }
        Some(AcceptedMutationFieldWriteIntent::Resolve(
            AcceptedInsertPolicyRequest::ExplicitInsertDefault,
        )) => AcceptedInsertPolicyRequest::ExplicitInsertDefault,
        Some(AcceptedMutationFieldWriteIntent::Resolve(
            AcceptedInsertPolicyRequest::OmittedInsert
            | AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
        )) => return Err(InternalError::executor_invariant()),
        None => AcceptedInsertPolicyRequest::OmittedInsert,
    };

    if let Some(generation) = write_policy.insert_generation() {
        let value = accepted_insert_generated_value(generation, write_context);
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        let payload = encode_canonical_value_for_accepted_field_contract(encoding, &value)?;

        return Ok((
            payload,
            AcceptedFieldWriteProvenance::InsertGenerated(request),
        ));
    }
    if let Some(management) = write_policy.write_management() {
        let value = accepted_insert_managed_value(management, write_context);
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        let payload = encode_canonical_value_for_accepted_field_contract(encoding, &value)?;

        return Ok((
            payload,
            AcceptedFieldWriteProvenance::InsertManaged(request),
        ));
    }

    let provenance = match field.insert_omission_policy() {
        AcceptedInsertOmissionPolicy::NullIfMissing => {
            AcceptedFieldWriteProvenance::ResolvedNull(request)
        }
        AcceptedInsertOmissionPolicy::DefaultIfMissing => {
            AcceptedFieldWriteProvenance::ResolvedDefault(request)
        }
        AcceptedInsertOmissionPolicy::Required => {
            if matches!(request, AcceptedInsertPolicyRequest::ExplicitInsertDefault) {
                return Err(InternalError::query_sql_write_boundary(
                    icydb_diagnostic_code::SqlWriteBoundaryCode::InsertDefaultRequiredField,
                ));
            }
            return Err(InternalError::mutation_required_field_missing(
                entity_path,
                field.field_name(),
            ));
        }
    };

    Ok((contract.insert_omission_payload(slot)?, provenance))
}

/// Resolve one sparse insert patch through accepted insertion authority.
///
/// Authored inputs remain distinct from omission, while accepted generation,
/// management, default, and nullable policies produce canonical protected
/// values before any typed entity or sanitizer can observe the after-image.
pub(in crate::db) fn resolve_insert_structural_patch_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    patch: &AcceptedMutationIntentPatch,
    write_context: SanitizeWriteContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);
    let mut payloads = vec![None; contract.field_count()];
    let mut provenance = vec![None; contract.field_count()];
    let mut intents = vec![None; contract.field_count()];

    // Phase 1: retain exact last-write-wins request intent without evaluating
    // policy or encoding a value that a later assignment replaces.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let _ = contract.required_accepted_field_contract(slot)?;
        intents[slot] = Some(entry.intent().clone());
    }

    // Phase 2: resolve every exact authored/default/omitted request from the
    // accepted field policy selected for this operation.
    // Retired slots retain their canonical placeholder but carry no field
    // provenance because no logical field exists at that slot.
    for slot in 0..contract.field_count() {
        if !contract.has_active_field_slot(slot) {
            payloads[slot] = Some(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
            continue;
        }

        let (payload, source) = resolve_insert_active_slot(
            entity_path,
            &contract,
            slot,
            intents[slot].take(),
            write_context,
        )?;
        payloads[slot] = Some(payload);
        provenance[slot] = Some(source);
    }

    let slot_payloads = payloads
        .into_iter()
        .map(|payload| payload.ok_or_else(InternalError::persisted_row_encode_internal))
        .collect::<Result<Vec<_>, _>>()?;
    let row = emit_raw_row_from_slot_payloads(
        contract.current_layout_version(),
        contract.field_count(),
        slot_payloads.as_slice(),
    )?;

    Ok(ResolvedAcceptedMutationRow::new(row, provenance))
}

/// Resolve one sparse update patch over an accepted logical before-image.
///
/// Unassigned fields preserve their accepted logical values, including frozen
/// historical fills, while update-managed fields resolve from the operation's
/// stable write context before sanitizer execution.
pub(in crate::db) fn resolve_update_structural_patch_with_accepted_contract(
    entity_path: &'static str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    raw_row: &RawRow,
    patch: &AcceptedMutationIntentPatch,
    write_context: SanitizeWriteContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let contract =
        StructuralRowContract::from_accepted_decode_contract(entity_path, accepted_decode_contract);
    let baseline =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(raw_row, &contract)?;
    let mut payloads = vec![None; contract.field_count()];
    let mut provenance = vec![None; contract.field_count()];
    let mut intents = vec![None; contract.field_count()];

    // Phase 1: retain exact last-write-wins assignment provenance.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let _ = contract.required_accepted_field_contract(slot)?;
        intents[slot] = Some(entry.intent().clone());
    }

    // Phase 2: resolve updated-at policy and preserve every other unassigned
    // logical value. Historical absence is classified before canonical current
    // layout emission so its frozen provenance is not lost.
    for slot in 0..contract.field_count() {
        if payloads[slot].is_some() {
            continue;
        }
        if !contract.has_active_field_slot(slot) {
            payloads[slot] = Some(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
            continue;
        }

        let field = contract.required_accepted_field_contract(slot)?;
        let write_policy = field.write_policy();
        match intents[slot].take() {
            Some(AcceptedMutationFieldWriteIntent::Authored(input)) => {
                if write_policy.insert_generation().is_some()
                    || write_policy.write_management().is_some()
                {
                    return Err(InternalError::mutation_database_owned_field_explicit(
                        entity_path,
                        field.field_name(),
                    ));
                }
                let encoding = contract.required_accepted_field_persistence_contract(slot)?;
                payloads[slot] = Some(encode_authored_value_for_accepted_field_contract(
                    encoding, input,
                )?);
                provenance[slot] = Some(AcceptedFieldWriteProvenance::Authored);
                continue;
            }
            Some(AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(_)) => {
                return Err(InternalError::executor_invariant());
            }
            Some(AcceptedMutationFieldWriteIntent::Resolve(
                AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
            )) => {
                let (payload, resolved_provenance) =
                    resolve_explicit_update_default(&contract, slot)?;
                payloads[slot] = Some(payload);
                provenance[slot] = Some(resolved_provenance);
                continue;
            }
            Some(AcceptedMutationFieldWriteIntent::Resolve(
                AcceptedInsertPolicyRequest::OmittedInsert
                | AcceptedInsertPolicyRequest::ExplicitInsertDefault,
            )) => return Err(InternalError::executor_invariant()),
            None => {}
        }
        if matches!(
            write_policy.write_management(),
            Some(FieldWriteManagement::UpdatedAt)
        ) {
            let value = Value::Timestamp(write_context.now());
            let encoding = contract.required_accepted_field_persistence_contract(slot)?;
            payloads[slot] = Some(encode_canonical_value_for_accepted_field_contract(
                encoding, &value,
            )?);
            provenance[slot] = Some(AcceptedFieldWriteProvenance::UpdateManaged);
            continue;
        }

        let value = baseline.required_cached_value(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        payloads[slot] = Some(encode_canonical_value_for_accepted_field_contract(
            encoding, value,
        )?);
        provenance[slot] = Some(if baseline.get_bytes(slot).is_some() {
            AcceptedFieldWriteProvenance::Preserved
        } else {
            AcceptedFieldWriteProvenance::HistoricalFill
        });
    }

    let slot_payloads = payloads
        .into_iter()
        .map(|payload| payload.ok_or_else(InternalError::persisted_row_encode_internal))
        .collect::<Result<Vec<_>, _>>()?;
    let row = emit_raw_row_from_slot_payloads(
        contract.current_layout_version(),
        contract.field_count(),
        slot_payloads.as_slice(),
    )?;

    Ok(ResolvedAcceptedMutationRow::new(row, provenance))
}

// Resolve one update-default request through the ordinary accepted insertion
// policy without permitting generation or management to become update owners.
fn resolve_explicit_update_default(
    contract: &StructuralRowContract,
    slot: usize,
) -> Result<(Vec<u8>, AcceptedFieldWriteProvenance), InternalError> {
    let field = contract.required_accepted_field_contract(slot)?;
    let write_policy = field.write_policy();
    if write_policy.insert_generation().is_some() || write_policy.write_management().is_some() {
        return Err(InternalError::query_sql_write_boundary(
            icydb_diagnostic_code::SqlWriteBoundaryCode::UpdateDefaultDatabaseOwnedField,
        ));
    }
    let provenance = match field.insert_omission_policy() {
        AcceptedInsertOmissionPolicy::NullIfMissing => AcceptedFieldWriteProvenance::ResolvedNull(
            AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
        ),
        AcceptedInsertOmissionPolicy::DefaultIfMissing => {
            AcceptedFieldWriteProvenance::ResolvedDefault(
                AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
            )
        }
        AcceptedInsertOmissionPolicy::Required => {
            return Err(InternalError::query_sql_write_boundary(
                icydb_diagnostic_code::SqlWriteBoundaryCode::UpdateDefaultRequiredField,
            ));
        }
    };

    Ok((contract.insert_omission_payload(slot)?, provenance))
}

fn accepted_insert_generated_value(
    generation: FieldInsertGeneration,
    write_context: SanitizeWriteContext,
) -> Value {
    match generation {
        FieldInsertGeneration::Ulid => Value::Ulid(Ulid::generate()),
        FieldInsertGeneration::Timestamp => Value::Timestamp(write_context.now()),
    }
}

const fn accepted_insert_managed_value(
    management: FieldWriteManagement,
    write_context: SanitizeWriteContext,
) -> Value {
    match management {
        FieldWriteManagement::CreatedAt | FieldWriteManagement::UpdatedAt => {
            Value::Timestamp(write_context.now())
        }
    }
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
