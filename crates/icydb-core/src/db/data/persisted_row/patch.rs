#[cfg(feature = "sql")]
use crate::db::codec::{
    finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32, write_hash_u32,
};
#[cfg(feature = "sql")]
use crate::db::data::CanonicalSlotReader;
#[cfg(feature = "sql")]
use crate::db::data::persisted_row::types::FieldSlot;
use crate::{
    db::schema::{FieldInsertGeneration, FieldWriteManagement},
    db::{
        commit::CommitSchemaFingerprint,
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
            AcceptedFieldPersistenceContract, AcceptedIdentityAllocation,
            AcceptedInsertOmissionPolicy, AcceptedRowDecodeContract,
            CompiledAcceptedRowConstraints, accepted_row_constraint_write_error,
            enum_catalog::{ValueAdmissionBudget, ValueAdmissionError},
        },
        write_context::AcceptedWriteContext,
    },
    error::InternalError,
    types::{GenerateKey, Ulid},
    value::{InputValue, Value},
};
#[cfg(feature = "sql")]
use sha2::Digest;
use std::borrow::Cow;

#[cfg(feature = "sql")]
const ACCEPTED_FIXED_UPDATE_PATCH_FINGERPRINT_DOMAIN: &[u8] =
    b"icydb.accepted-fixed-update-patch.v1";

#[derive(Clone, Copy)]
struct AcceptedRowConstraintWriteContext<'a> {
    entity_path: &'a str,
    fingerprint: CommitSchemaFingerprint,
    constraints: &'a CompiledAcceptedRowConstraints,
}

impl<'a> AcceptedRowConstraintWriteContext<'a> {
    const fn new(
        entity_path: &'a str,
        fingerprint: CommitSchemaFingerprint,
        constraints: &'a CompiledAcceptedRowConstraints,
    ) -> Self {
        Self {
            entity_path,
            fingerprint,
            constraints,
        }
    }
}

/// Provenance of one resolved accepted field in a mutation after-image.
///
/// Accepted constraint and activation gates use this transient fact to
/// distinguish authored inputs from values resolved by database policy.
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

/// Complete canonical accepted row paired with per-slot write provenance.
///
/// Construction resolves every active slot before typed materialization, so a
/// caller cannot separate canonical database values from the provenance proof
/// required by accepted constraint and activation gates.
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
        entity_path: &str,
        accepted_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
        constraints: &CompiledAcceptedRowConstraints,
        patch: &AcceptedMutationIntentPatch,
    ) -> Result<Self, InternalError> {
        let constraint_context = AcceptedRowConstraintWriteContext::new(
            entity_path,
            accepted_schema_fingerprint,
            constraints,
        );
        let contract = StructuralRowContract::from_owned_accepted_decode_contract(
            entity_path.to_string(),
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
                        constraint_context,
                        slot,
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
    entity_path: &str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    let contract = StructuralRowContract::from_owned_accepted_decode_contract(
        entity_path.to_string(),
        accepted_decode_contract,
    );

    canonical_row_from_raw_row_with_structural_contract(raw_row, &contract)
}

// Rewrap one row already loaded from storage as a canonical write token.
pub(in crate::db) const fn canonical_row_from_stored_raw_row(raw_row: RawRow) -> CanonicalRow {
    CanonicalRow::from_canonical_raw_row(raw_row)
}

// Admit every authored value before selecting its accepted storage codec.
fn encode_authored_value_for_accepted_field_contract(
    constraint_context: AcceptedRowConstraintWriteContext<'_>,
    slot: usize,
    encoding: AcceptedFieldPersistenceContract<'_>,
    input: InputValue,
) -> Result<Vec<u8>, InternalError> {
    if matches!(input, InputValue::Null) {
        constraint_context
            .constraints
            .evaluate_accepted_not_null_before_encoding(constraint_context.fingerprint, slot)
            .map_err(|error| {
                accepted_row_constraint_write_error(constraint_context.entity_path, None, error)
            })?;
    }
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
    constraint_context: AcceptedRowConstraintWriteContext<'_>,
    contract: &StructuralRowContract,
    slot: usize,
    intent: Option<AcceptedMutationFieldWriteIntent>,
    write_context: AcceptedWriteContext,
    identity_allocation: Option<&AcceptedIdentityAllocation>,
) -> Result<(Vec<u8>, AcceptedFieldWriteProvenance), InternalError> {
    let field = contract.required_accepted_field_contract(slot)?;
    let write_policy = field.write_policy();
    let request = match intent {
        Some(AcceptedMutationFieldWriteIntent::Authored(input)) => {
            if write_policy.insert_generation().is_some()
                || write_policy.write_management().is_some()
            {
                return Err(InternalError::mutation_database_owned_field_explicit(
                    constraint_context.entity_path,
                    field.field_name(),
                ));
            }
            let encoding = contract.required_accepted_field_persistence_contract(slot)?;
            let payload = encode_authored_value_for_accepted_field_contract(
                constraint_context,
                slot,
                encoding,
                input,
            )?;

            return Ok((payload, AcceptedFieldWriteProvenance::Authored));
        }
        Some(AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(input)) => {
            if !contract.primary_key_slot_indices().contains(&slot) {
                return Err(InternalError::executor_invariant());
            }
            let encoding = contract.required_accepted_field_persistence_contract(slot)?;
            let payload = encode_authored_value_for_accepted_field_contract(
                constraint_context,
                slot,
                encoding,
                input,
            )?;

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
        let value =
            accepted_insert_generated_value(generation, slot, write_context, identity_allocation)?;
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
                constraint_context.entity_path,
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
/// values before any typed entity projection can observe the after-image.
pub(in crate::db) fn resolve_insert_structural_patch_with_accepted_contract(
    entity_path: &str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
    identity_allocation: Option<&AcceptedIdentityAllocation>,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let constraint_context = AcceptedRowConstraintWriteContext::new(
        entity_path,
        accepted_schema_fingerprint,
        constraints,
    );
    let contract = StructuralRowContract::from_owned_accepted_decode_contract(
        entity_path.to_string(),
        accepted_decode_contract,
    );
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
            constraint_context,
            &contract,
            slot,
            intents[slot].take(),
            write_context,
            identity_allocation,
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
/// stable write context before typed materialization.
#[expect(
    clippy::too_many_lines,
    reason = "the phased resolver keeps provenance, no-op detection, and managed-time ownership in one accepted-contract boundary"
)]
pub(in crate::db) fn resolve_update_structural_patch_with_accepted_contract(
    entity_path: &str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
    raw_row: &RawRow,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let constraint_context = AcceptedRowConstraintWriteContext::new(
        entity_path,
        accepted_schema_fingerprint,
        constraints,
    );
    let contract = StructuralRowContract::from_owned_accepted_decode_contract(
        entity_path.to_string(),
        accepted_decode_contract,
    );
    let baseline =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(raw_row, &contract)?;
    let mut payloads = vec![None; contract.field_count()];
    let mut provenance = vec![None; contract.field_count()];
    let mut intents = vec![None; contract.field_count()];
    let mut updated_at_slot = None;

    // Phase 1: retain exact last-write-wins assignment provenance.
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let _ = contract.required_accepted_field_contract(slot)?;
        intents[slot] = Some(entry.intent().clone());
    }

    // Phase 2: resolve authored/default intent while initially preserving every
    // managed timestamp. `UpdatedAt` is deliberately deferred until the full
    // logical candidate can be compared with the accepted before-image.
    for (slot, payload) in payloads.iter_mut().enumerate() {
        if payload.is_some() {
            continue;
        }
        if !contract.has_active_field_slot(slot) {
            *payload = Some(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
            continue;
        }

        let field = contract.required_accepted_field_contract(slot)?;
        let write_policy = field.write_policy();
        if matches!(
            write_policy.write_management(),
            Some(FieldWriteManagement::UpdatedAt)
        ) && updated_at_slot.replace(slot).is_some()
        {
            return Err(InternalError::accepted_row_constraint_program_corrupt());
        }
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
                *payload = Some(encode_authored_value_for_accepted_field_contract(
                    constraint_context,
                    slot,
                    encoding,
                    input,
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
                let (resolved_payload, resolved_provenance) =
                    resolve_explicit_update_default(&contract, slot)?;
                *payload = Some(resolved_payload);
                provenance[slot] = Some(resolved_provenance);
                continue;
            }
            Some(AcceptedMutationFieldWriteIntent::Resolve(
                AcceptedInsertPolicyRequest::OmittedInsert
                | AcceptedInsertPolicyRequest::ExplicitInsertDefault,
            )) => return Err(InternalError::executor_invariant()),
            None => {}
        }
        let value = baseline.required_cached_value(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        *payload = Some(encode_canonical_value_for_accepted_field_contract(
            encoding, value,
        )?);
        provenance[slot] = Some(if baseline.get_bytes(slot).is_some() {
            AcceptedFieldWriteProvenance::Preserved
        } else {
            AcceptedFieldWriteProvenance::HistoricalFill
        });
    }

    // Phase 3: compare canonical logical values without letting the managed
    // timestamp manufacture a change. Historical values are encoded through
    // their accepted current contract before comparison.
    let mut logical_changed = false;
    for (slot, payload) in payloads.iter().enumerate() {
        if !contract.has_active_field_slot(slot) || updated_at_slot == Some(slot) {
            continue;
        }
        let before = baseline.required_cached_value(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        let before = encode_canonical_value_for_accepted_field_contract(encoding, before)?;
        if payload.as_deref() != Some(before.as_slice()) {
            logical_changed = true;
            break;
        }
    }

    // Phase 4: refresh `UpdatedAt` only for a real logical row change. The
    // accepted clock contract is fail-closed: restored future timestamps are
    // preserved and block a write that would move managed time backward.
    if logical_changed && let Some(slot) = updated_at_slot {
        validate_managed_timestamp_progression(
            &contract,
            &baseline,
            write_context.operation_timestamp(),
        )?;
        let value = Value::Timestamp(write_context.operation_timestamp());
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        payloads[slot] = Some(encode_canonical_value_for_accepted_field_contract(
            encoding, &value,
        )?);
        provenance[slot] = Some(AcceptedFieldWriteProvenance::UpdateManaged);
    } else {
        validate_existing_managed_timestamp_order(&contract, &baseline)?;
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

/// Resolve one replacement over an existing accepted row.
///
/// Ordinary omitted fields use current insert policy, while `CreatedAt`
/// remains immutable and `UpdatedAt` is refreshed only when the resulting
/// logical candidate differs from the accepted before-image.
pub(in crate::db) fn resolve_existing_replace_structural_patch_with_accepted_contract(
    entity_path: &str,
    accepted_decode_contract: AcceptedRowDecodeContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
    raw_row: &RawRow,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let inserted = resolve_insert_structural_patch_with_accepted_contract(
        entity_path,
        accepted_decode_contract.clone(),
        accepted_schema_fingerprint,
        constraints,
        patch,
        write_context,
        None,
    )?;
    let (inserted, mut provenance) = inserted.into_parts();
    let contract = StructuralRowContract::from_owned_accepted_decode_contract(
        entity_path.to_string(),
        accepted_decode_contract,
    );
    let baseline =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(raw_row, &contract)?;
    let candidate = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
        inserted.as_raw_row(),
        &contract,
    )?;
    let mut payloads = vec![None; contract.field_count()];
    let mut updated_at_slot = None;

    for (slot, payload) in payloads.iter_mut().enumerate() {
        if !contract.has_active_field_slot(slot) {
            *payload = Some(RETIRED_SLOT_PLACEHOLDER_PAYLOAD.to_vec());
            continue;
        }

        let field = contract.required_accepted_field_contract(slot)?;
        let source = match field.write_policy().write_management() {
            Some(FieldWriteManagement::CreatedAt) => {
                provenance[slot] = Some(if baseline.get_bytes(slot).is_some() {
                    AcceptedFieldWriteProvenance::Preserved
                } else {
                    AcceptedFieldWriteProvenance::HistoricalFill
                });
                baseline.required_cached_value(slot)?
            }
            Some(FieldWriteManagement::UpdatedAt) => {
                if updated_at_slot.replace(slot).is_some() {
                    return Err(InternalError::accepted_row_constraint_program_corrupt());
                }
                provenance[slot] = Some(if baseline.get_bytes(slot).is_some() {
                    AcceptedFieldWriteProvenance::Preserved
                } else {
                    AcceptedFieldWriteProvenance::HistoricalFill
                });
                baseline.required_cached_value(slot)?
            }
            None => candidate.required_cached_value(slot)?,
        };
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        *payload = Some(encode_canonical_value_for_accepted_field_contract(
            encoding, source,
        )?);
    }

    let mut logical_changed = false;
    for (slot, payload) in payloads.iter().enumerate() {
        if !contract.has_active_field_slot(slot) || updated_at_slot == Some(slot) {
            continue;
        }
        let before = baseline.required_cached_value(slot)?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        let before = encode_canonical_value_for_accepted_field_contract(encoding, before)?;
        if payload.as_deref() != Some(before.as_slice()) {
            logical_changed = true;
            break;
        }
    }

    if logical_changed && let Some(slot) = updated_at_slot {
        validate_managed_timestamp_progression(
            &contract,
            &baseline,
            write_context.operation_timestamp(),
        )?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        payloads[slot] = Some(encode_canonical_value_for_accepted_field_contract(
            encoding,
            &Value::Timestamp(write_context.operation_timestamp()),
        )?);
        provenance[slot] = Some(AcceptedFieldWriteProvenance::UpdateManaged);
    } else {
        validate_existing_managed_timestamp_order(&contract, &baseline)?;
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

fn validate_managed_timestamp_progression(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
    operation_timestamp: crate::types::Timestamp,
) -> Result<(), InternalError> {
    let (created_at, updated_at) = managed_timestamp_values(contract, baseline)?;
    if created_at.is_some_and(|created_at| operation_timestamp < created_at)
        || updated_at.is_some_and(|updated_at| operation_timestamp < updated_at)
    {
        return Err(InternalError::mutation_managed_timestamp_regression());
    }

    Ok(())
}

fn validate_existing_managed_timestamp_order(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
) -> Result<(), InternalError> {
    let (created_at, updated_at) = managed_timestamp_values(contract, baseline)?;
    if matches!((created_at, updated_at), (Some(created), Some(updated)) if created > updated) {
        return Err(InternalError::mutation_managed_timestamp_regression());
    }

    Ok(())
}

fn managed_timestamp_values(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
) -> Result<
    (
        Option<crate::types::Timestamp>,
        Option<crate::types::Timestamp>,
    ),
    InternalError,
> {
    let mut created_at = None;
    let mut updated_at = None;

    for slot in 0..contract.field_count() {
        if !contract.has_active_field_slot(slot) {
            continue;
        }
        let field = contract.required_accepted_field_contract(slot)?;
        let target = match field.write_policy().write_management() {
            Some(FieldWriteManagement::CreatedAt) => &mut created_at,
            Some(FieldWriteManagement::UpdatedAt) => &mut updated_at,
            None => continue,
        };
        if target.is_some() {
            return Err(InternalError::accepted_row_constraint_program_corrupt());
        }
        let Value::Timestamp(value) = baseline.required_cached_value(slot)? else {
            return Err(InternalError::accepted_row_constraint_program_corrupt());
        };
        *target = Some(*value);
    }

    Ok((created_at, updated_at))
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
    field_slot: usize,
    write_context: AcceptedWriteContext,
    identity_allocation: Option<&AcceptedIdentityAllocation>,
) -> Result<Value, InternalError> {
    Ok(match generation {
        FieldInsertGeneration::Identity => {
            let allocation = identity_allocation
                .filter(|allocation| allocation.field_slot() == field_slot)
                .ok_or_else(InternalError::executor_invariant)?;
            allocation.value().clone()
        }
        FieldInsertGeneration::Ulid => Value::Ulid(Ulid::generate()?),
        FieldInsertGeneration::Timestamp => Value::Timestamp(write_context.operation_timestamp()),
    })
}

const fn accepted_insert_managed_value(
    management: FieldWriteManagement,
    write_context: AcceptedWriteContext,
) -> Value {
    match management {
        FieldWriteManagement::CreatedAt | FieldWriteManagement::UpdatedAt => {
            Value::Timestamp(write_context.operation_timestamp())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::{
        AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedRowLayoutRuntimeContract,
        AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
        FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
        ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        empty_accepted_enum_catalog_for_tests,
    };
    use crate::error::ConstraintDiagnosticKind;

    #[test]
    fn accepted_not_null_pre_encoding_failure_preserves_constraint_identity() {
        let field = PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            crate::db::schema::AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        );
        let accepted = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::User".to_string(),
            "User".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
            vec![field],
        ))
        .expect("test not-null snapshot should close");
        let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let fingerprint = [7; 16];
        let constraints =
            CompiledAcceptedRowConstraints::compile(&accepted, &value_catalog, fingerprint)
                .expect("accepted not-null program should compile");
        let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
            .expect("accepted row layout should build");
        let patch = AcceptedMutationIntentPatch::new().set_authored(
            crate::db::data::FieldSlot::from_validated_index(0),
            InputValue::Null,
        );
        let Err(error) = resolve_insert_structural_patch_with_accepted_contract(
            accepted.entity_path(),
            row_layout.row_decode_contract(value_catalog),
            fingerprint,
            &constraints,
            &patch,
            AcceptedWriteContext::new(crate::types::Timestamp::from_millis(1)),
            None,
        ) else {
            panic!("explicit null should violate accepted not-null");
        };
        let diagnostic = error
            .constraint_diagnostic()
            .expect("not-null violation should retain accepted diagnostic");
        let accepted_identity = accepted
            .persisted_snapshot()
            .constraints()
            .iter()
            .find(|constraint| {
                matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::NotNull { field_id }
                        if *field_id == FieldId::new(1)
                )
            })
            .expect("accepted not-null identity should exist");

        assert_eq!(diagnostic.constraint_id(), accepted_identity.id().get());
        assert_eq!(diagnostic.constraint_name(), accepted_identity.name());
        assert_eq!(
            diagnostic.constraint_kind(),
            ConstraintDiagnosticKind::NotNull,
        );
        assert_eq!(diagnostic.entity(), "tests::User");
        assert_eq!(diagnostic.field_paths(), &["id".to_string()]);
    }
}
