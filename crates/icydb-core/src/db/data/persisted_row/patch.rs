#[cfg(feature = "sql")]
use crate::db::codec::{
    finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32, write_hash_u32,
};
#[cfg(test)]
use crate::db::data::RawRow;
#[cfg(feature = "sql")]
use crate::db::{data::persisted_row::types::FieldSlot, schema::AcceptedRowDecodeContract};
use crate::{
    db::schema::{FieldInsertGeneration, FieldWriteManagement},
    db::{
        commit::CommitSchemaFingerprint,
        data::{
            CanonicalRow, StructuralRowContract,
            encode_accepted_value_ref_for_accepted_field_contract,
            encode_canonical_value_for_accepted_field_contract,
            persisted_row::{
                contract::{
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
            AcceptedInsertOmissionPolicy, CompiledAcceptedRowConstraints,
            OwnedAcceptedFieldDecodeContract, accepted_row_constraint_write_error,
            enum_catalog::{ValueAdmissionBudget, ValueAdmissionError},
        },
        write_context::AcceptedWriteContext,
    },
    error::{InternalError, MutationDiagnosticContext},
    types::generate_ulid_at,
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
    fingerprint: CommitSchemaFingerprint,
    entity_tag: u64,
    mutation: Option<MutationDiagnosticContext>,
    constraints: &'a CompiledAcceptedRowConstraints,
}

impl<'a> AcceptedRowConstraintWriteContext<'a> {
    const fn new(
        fingerprint: CommitSchemaFingerprint,
        entity_tag: u64,
        mutation: Option<MutationDiagnosticContext>,
        constraints: &'a CompiledAcceptedRowConstraints,
    ) -> Self {
        Self {
            fingerprint,
            entity_tag,
            mutation,
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

    /// Borrow the already-admitted canonical persisted field payload.
    #[must_use]
    pub(in crate::db) const fn payload(&self) -> &[u8] {
        self.payload.as_slice()
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
        entity_tag: u64,
        accepted_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
        constraints: &CompiledAcceptedRowConstraints,
        patch: &AcceptedMutationIntentPatch,
    ) -> Result<Self, InternalError> {
        let constraint_context = AcceptedRowConstraintWriteContext::new(
            accepted_schema_fingerprint,
            entity_tag,
            Some(MutationDiagnosticContext::operation_only(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                accepted_schema_fingerprint,
                entity_tag,
                icydb_diagnostic_code::DiagnosticMutationOperation::Update,
            )),
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
                | AcceptedMutationFieldWriteIntent::CanonicalPayload(_)
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

        Ok(Self {
            fingerprint: accepted_fixed_update_patch_fingerprint(fields.as_slice())?,
            fields,
        })
    }

    /// Rebuild one current canonical fixed patch from persisted slot payloads.
    ///
    /// The caller must already have checked the persisted intent's accepted
    /// schema authority. Slots are required to be strictly increasing so one
    /// current wire has exactly one semantic representation.
    pub(in crate::db) fn from_canonical_fields(
        fields: Vec<(FieldSlot, Vec<u8>)>,
    ) -> Result<Self, InternalError> {
        if fields.is_empty()
            || fields
                .windows(2)
                .any(|pair| pair[0].0.index() >= pair[1].0.index())
        {
            return Err(InternalError::executor_invariant());
        }
        let fields = fields
            .into_iter()
            .map(|(slot, payload)| AcceptedFixedUpdateField { slot, payload })
            .collect::<Vec<_>>();
        Ok(Self {
            fingerprint: accepted_fixed_update_patch_fingerprint(fields.as_slice())?,
            fields,
        })
    }

    /// Borrow fixed accepted targets in physical-slot order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[AcceptedFixedUpdateField] {
        self.fields.as_slice()
    }

    /// Validate retained field encodings once against the current row contract.
    /// Restored bytes need codec validation; row constraints remain at preflight.
    pub(in crate::db) fn validate_for_row_contract(
        &self,
        contract: &StructuralRowContract,
    ) -> Result<(), InternalError> {
        for field in &self.fields {
            let slot = field.slot.index();
            let policy = contract
                .required_accepted_field_contract(slot)?
                .write_policy();
            if policy.insert_generation().is_some() || policy.write_management().is_some() {
                return Err(InternalError::executor_invariant());
            }
            let _ = crate::db::data::decode_runtime_value_from_row_contract(
                contract,
                slot,
                field.payload.as_slice(),
            )?;
        }

        Ok(())
    }

    /// Bind the private update intent to the accepted contract used for row execution.
    /// Callers must keep that same authority for every row consuming this patch.
    pub(in crate::db) fn to_update_intent(
        &self,
        contract: &StructuralRowContract,
    ) -> Result<AcceptedMutationIntentPatch, InternalError> {
        self.validate_for_row_contract(contract)?;

        Ok(self
            .fields
            .iter()
            .fold(AcceptedMutationIntentPatch::new(), |patch, field| {
                patch.set_canonical_payload(field.slot, field.payload.clone())
            }))
    }

    /// Return whether every fixed authored target already matches one accepted row.
    pub(in crate::db) fn is_satisfied_by(
        &self,
        row: &StructuralSlotReader<'_>,
    ) -> Result<bool, InternalError> {
        for field in &self.fields {
            let slot = field.slot.index();
            let payload = match row.get_bytes(slot) {
                Some(bytes) => Cow::Borrowed(bytes),
                // Metadata-only field additions leave older rows physically
                // shorter. Compare their frozen accepted fill in the same
                // canonical encoding as the retained assignment.
                None => Cow::Owned(encode_canonical_value_for_accepted_field_contract(
                    row.contract()
                        .required_accepted_field_persistence_contract(slot)?,
                    row.required_cached_value(slot)?,
                )?),
            };
            if payload.as_ref() != field.payload.as_slice() {
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

#[cfg(feature = "sql")]
fn accepted_fixed_update_patch_fingerprint(
    fields: &[AcceptedFixedUpdateField],
) -> Result<[u8; 32], InternalError> {
    let mut hasher = new_hash_sha256_prefixed(ACCEPTED_FIXED_UPDATE_PATCH_FINGERPRINT_DOMAIN);
    write_hash_len_u32(&mut hasher, fields.len());
    for field in fields {
        write_hash_u32(
            &mut hasher,
            u32::try_from(field.slot.index()).map_err(|_| InternalError::executor_invariant())?,
        );
        write_hash_len_u32(&mut hasher, field.payload.len());
        hasher.update(field.payload.as_slice());
    }
    Ok(finalize_hash_sha256(hasher))
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

// Rewrap one row already loaded from storage as a canonical write token.
#[cfg(test)]
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
    if input.is_null() {
        constraint_context
            .constraints
            .evaluate_accepted_not_null_before_encoding(constraint_context.fingerprint, slot)
            .map_err(|error| {
                accepted_row_constraint_write_error(
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    constraint_context.fingerprint,
                    constraint_context.entity_tag,
                    constraint_context.mutation,
                    error,
                )
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
        .map_err(|_| InternalError::persisted_row_encode_internal())
}

// Resolve one active insert slot while keeping field-policy branching inside
// the accepted row boundary. The caller owns only dense slot assembly.
fn resolve_insert_active_slot(
    constraint_context: AcceptedRowConstraintWriteContext<'_>,
    contract: &StructuralRowContract,
    slot: usize,
    intent: Option<AcceptedMutationFieldWriteIntent>,
    write_context: AcceptedWriteContext,
    mutation_context: MutationDiagnosticContext,
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
                    mutation_context,
                    field.field_id().get(),
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
        Some(AcceptedMutationFieldWriteIntent::CanonicalPayload(_)) => {
            return Err(InternalError::executor_invariant());
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
                mutation_context,
                field.field_id().get(),
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
    contract: &StructuralRowContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
    mutation_context: MutationDiagnosticContext,
    identity_allocation: Option<&AcceptedIdentityAllocation>,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let constraint_context = AcceptedRowConstraintWriteContext::new(
        accepted_schema_fingerprint,
        mutation_context.entity_tag(),
        Some(mutation_context),
        constraints,
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
    for slot in 0..contract.field_count() {
        let (payload, source) = resolve_insert_active_slot(
            constraint_context,
            contract,
            slot,
            intents[slot].take(),
            write_context,
            mutation_context,
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
pub(in crate::db) fn resolve_update_structural_patch_with_accepted_contract(
    contract: &StructuralRowContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
    baseline: &StructuralSlotReader<'_>,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
    mutation_context: MutationDiagnosticContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let constraint_context = AcceptedRowConstraintWriteContext::new(
        accepted_schema_fingerprint,
        mutation_context.entity_tag(),
        Some(mutation_context),
        constraints,
    );
    finish_existing_row_patch(
        contract,
        baseline,
        patch,
        write_context,
        mutation_context,
        |slot, field, intent| {
            let write_policy = field.write_policy();
            match intent {
                Some(AcceptedMutationFieldWriteIntent::Authored(input)) => {
                    if write_policy.insert_generation().is_some()
                        || write_policy.write_management().is_some()
                    {
                        return Err(InternalError::mutation_database_owned_field_explicit(
                            mutation_context,
                            field.field_id().get(),
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
                Some(AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(_)) => {
                    return Err(InternalError::executor_invariant());
                }
                Some(AcceptedMutationFieldWriteIntent::CanonicalPayload(payload)) => {
                    if write_policy.insert_generation().is_some()
                        || write_policy.write_management().is_some()
                    {
                        return Err(InternalError::executor_invariant());
                    }
                    // Fixed-patch bytes were admitted once; row constraints
                    // remain at commit preflight.
                    return Ok((payload, AcceptedFieldWriteProvenance::Authored));
                }
                Some(AcceptedMutationFieldWriteIntent::Resolve(
                    AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
                )) => return resolve_explicit_update_default(contract, slot),
                Some(AcceptedMutationFieldWriteIntent::Resolve(
                    AcceptedInsertPolicyRequest::OmittedInsert
                    | AcceptedInsertPolicyRequest::ExplicitInsertDefault,
                )) => return Err(InternalError::executor_invariant()),
                None => {}
            }
            let value = baseline.required_cached_value(slot)?;
            let encoding = contract.required_accepted_field_persistence_contract(slot)?;
            let payload = encode_canonical_value_for_accepted_field_contract(encoding, value)?;
            let provenance = if baseline.get_bytes(slot).is_some() {
                AcceptedFieldWriteProvenance::Preserved
            } else {
                AcceptedFieldWriteProvenance::HistoricalFill
            };
            Ok((payload, provenance))
        },
        |provenance| {
            // Unassigned fields already preserve the exact accepted logical value,
            // including frozen historical fills; compare only assigned updates.
            !matches!(
                provenance,
                Some(
                    AcceptedFieldWriteProvenance::Preserved
                        | AcceptedFieldWriteProvenance::HistoricalFill
                )
            )
        },
    )
}

// Updates and replacements resolve fields differently, but share last-write-wins
// intents, managed-time ownership, logical comparison and canonical row emission.
// The comparison selector preserves each existing policy without a new mode.
fn finish_existing_row_patch(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
    mutation_context: MutationDiagnosticContext,
    mut resolve_slot: impl FnMut(
        usize,
        &OwnedAcceptedFieldDecodeContract,
        Option<AcceptedMutationFieldWriteIntent>,
    ) -> Result<(Vec<u8>, AcceptedFieldWriteProvenance), InternalError>,
    should_compare: impl Fn(Option<AcceptedFieldWriteProvenance>) -> bool,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let mut payloads = vec![None; contract.field_count()];
    let mut provenance = vec![None; contract.field_count()];
    let mut intents = vec![None; contract.field_count()];
    let mut updated_at_slot = None;

    for entry in patch.entries() {
        let slot = entry.slot().index();
        let _ = contract.required_accepted_field_contract(slot)?;
        intents[slot] = Some(entry.intent().clone());
    }

    // Preserve managed values while resolving every slot. UpdatedAt is deferred
    // until the complete logical candidate can be compared with the before-image.
    for (slot, payload) in payloads.iter_mut().enumerate() {
        let field = contract.required_accepted_field_contract(slot)?;
        if field.write_policy().write_management() == Some(FieldWriteManagement::UpdatedAt)
            && updated_at_slot.replace(slot).is_some()
        {
            return Err(InternalError::accepted_row_constraint_program_corrupt());
        }
        let (resolved, source) = resolve_slot(slot, field, intents[slot].take())?;
        *payload = Some(resolved);
        provenance[slot] = Some(source);
    }

    let mut logical_changed = false;
    for (slot, payload) in payloads.iter().enumerate() {
        if updated_at_slot == Some(slot) || !should_compare(provenance[slot]) {
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

    // Fail closed on real changes that would move managed time backwards; no-op
    // writes retain the stored time but must still validate its internal order.
    if logical_changed && let Some(slot) = updated_at_slot {
        validate_managed_timestamp_progression(
            contract,
            baseline,
            write_context.operation_timestamp(),
            mutation_context,
        )?;
        let encoding = contract.required_accepted_field_persistence_contract(slot)?;
        payloads[slot] = Some(encode_canonical_value_for_accepted_field_contract(
            encoding,
            &Value::Timestamp(write_context.operation_timestamp()),
        )?);
        provenance[slot] = Some(AcceptedFieldWriteProvenance::UpdateManaged);
    } else {
        validate_existing_managed_timestamp_order(contract, baseline, mutation_context)?;
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

// Existing database-owned values come from the before-image, not insert
// synthesis. Ordinary replacement fields still share insertion/default policy.
fn resolve_existing_replace_active_slot(
    constraint_context: AcceptedRowConstraintWriteContext<'_>,
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
    slot: usize,
    intent: Option<AcceptedMutationFieldWriteIntent>,
    write_context: AcceptedWriteContext,
    mutation_context: MutationDiagnosticContext,
) -> Result<(Vec<u8>, AcceptedFieldWriteProvenance), InternalError> {
    let field = contract.required_accepted_field_contract(slot)?;
    let policy = field.write_policy();
    if policy.insert_generation().is_none() && policy.write_management().is_none() {
        return resolve_insert_active_slot(
            constraint_context,
            contract,
            slot,
            intent,
            write_context,
            mutation_context,
            None,
        );
    }

    let encoding = contract.required_accepted_field_persistence_contract(slot)?;
    let payload = encode_canonical_value_for_accepted_field_contract(
        encoding,
        baseline.required_cached_value(slot)?,
    )?;
    match intent {
        Some(intent @ AcceptedMutationFieldWriteIntent::PreservedReplacementIdentity(_)) => {
            // Retain the existing selected-key admission and prove it matches
            // the value being preserved, without evaluating any generator.
            let (selected, _) = resolve_insert_active_slot(
                constraint_context,
                contract,
                slot,
                Some(intent),
                write_context,
                mutation_context,
                None,
            )?;
            if selected != payload {
                return Err(InternalError::executor_invariant());
            }
        }
        Some(AcceptedMutationFieldWriteIntent::Authored(_)) => {
            return Err(InternalError::mutation_database_owned_field_explicit(
                mutation_context,
                field.field_id().get(),
            ));
        }
        None
        | Some(AcceptedMutationFieldWriteIntent::Resolve(
            AcceptedInsertPolicyRequest::ExplicitInsertDefault,
        )) => {}
        Some(
            AcceptedMutationFieldWriteIntent::CanonicalPayload(_)
            | AcceptedMutationFieldWriteIntent::Resolve(
                AcceptedInsertPolicyRequest::OmittedInsert
                | AcceptedInsertPolicyRequest::ExplicitUpdateDefault,
            ),
        ) => return Err(InternalError::executor_invariant()),
    }
    let provenance = if baseline.get_bytes(slot).is_none() {
        AcceptedFieldWriteProvenance::HistoricalFill
    } else if policy.insert_generation().is_some()
        && contract.primary_key_slot_indices().contains(&slot)
    {
        AcceptedFieldWriteProvenance::PreservedReplacementIdentity
    } else {
        AcceptedFieldWriteProvenance::Preserved
    };
    Ok((payload, provenance))
}

/// Resolve one replacement over an existing accepted row.
///
/// Ordinary omitted fields use current insert policy. Insert-generated fields
/// and `CreatedAt` remain immutable; `UpdatedAt` is refreshed only when the
/// resulting logical candidate differs from the accepted before-image.
pub(in crate::db) fn resolve_existing_replace_structural_patch_with_accepted_contract(
    contract: &StructuralRowContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
    baseline: &StructuralSlotReader<'_>,
    patch: &AcceptedMutationIntentPatch,
    write_context: AcceptedWriteContext,
    mutation_context: MutationDiagnosticContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError> {
    let constraint_context = AcceptedRowConstraintWriteContext::new(
        accepted_schema_fingerprint,
        mutation_context.entity_tag(),
        Some(mutation_context),
        constraints,
    );
    finish_existing_row_patch(
        contract,
        baseline,
        patch,
        write_context,
        mutation_context,
        |slot, _field, intent| {
            resolve_existing_replace_active_slot(
                constraint_context,
                contract,
                baseline,
                slot,
                intent,
                write_context,
                mutation_context,
            )
        },
        |_| true,
    )
}

fn validate_managed_timestamp_progression(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
    operation_timestamp: crate::types::Timestamp,
    mutation_context: MutationDiagnosticContext,
) -> Result<(), InternalError> {
    if managed_timestamp_progression_regresses(contract, baseline, operation_timestamp)? {
        return Err(InternalError::mutation_managed_timestamp_regression(
            mutation_context,
        ));
    }

    Ok(())
}

/// Report whether one accepted managed-write timestamp would move an existing
/// row's database-owned time backward.
///
/// Durable mutation-job preparation shares this writer-owned comparison so a
/// deterministic regression can become terminal before any row is staged.
pub(in crate::db) fn managed_timestamp_progression_regresses(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
    operation_timestamp: crate::types::Timestamp,
) -> Result<bool, InternalError> {
    let (created_at, updated_at) = managed_timestamp_values(contract, baseline)?;
    Ok(managed_timestamp_values_regress(
        created_at,
        updated_at,
        operation_timestamp,
    ))
}

const fn managed_timestamp_values_regress(
    created_at: Option<crate::types::Timestamp>,
    updated_at: Option<crate::types::Timestamp>,
    operation_timestamp: crate::types::Timestamp,
) -> bool {
    matches!(created_at, Some(created_at) if operation_timestamp.as_millis() < created_at.as_millis())
        || matches!(updated_at, Some(updated_at) if operation_timestamp.as_millis() < updated_at.as_millis())
}

fn validate_existing_managed_timestamp_order(
    contract: &StructuralRowContract,
    baseline: &StructuralSlotReader<'_>,
    mutation_context: MutationDiagnosticContext,
) -> Result<(), InternalError> {
    let (created_at, updated_at) = managed_timestamp_values(contract, baseline)?;
    if matches!((created_at, updated_at), (Some(created), Some(updated)) if created > updated) {
        return Err(InternalError::mutation_managed_timestamp_regression(
            mutation_context,
        ));
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
        FieldInsertGeneration::Ulid => {
            let timestamp_ms = u64::try_from(write_context.operation_timestamp().as_millis())
                .map_err(|_| InternalError::executor_invariant())?;
            Value::Ulid(generate_ulid_at(timestamp_ms)?)
        }
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
    use crate::db::data::{CanonicalSlotReader, FieldSlot};
    use crate::db::schema::{
        AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedFieldKind,
        AcceptedRowLayoutRuntimeContract, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
        AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
        PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
    };
    use crate::error::MutationDiagnosticContext;
    use crate::types::Ulid;
    use icydb_diagnostic_code::DiagnosticMutationOperation;

    type ExistingRowResolver = fn(
        &StructuralRowContract,
        CommitSchemaFingerprint,
        &CompiledAcceptedRowConstraints,
        &StructuralSlotReader<'_>,
        &AcceptedMutationIntentPatch,
        AcceptedWriteContext,
        MutationDiagnosticContext,
    ) -> Result<ResolvedAcceptedMutationRow, InternalError>;

    const EXISTING_ROW_RESOLVERS: [(DiagnosticMutationOperation, ExistingRowResolver); 2] = [
        (
            DiagnosticMutationOperation::Update,
            resolve_update_structural_patch_with_accepted_contract,
        ),
        (
            DiagnosticMutationOperation::Replace,
            resolve_existing_replace_structural_patch_with_accepted_contract,
        ),
    ];

    fn replacement_owned_snapshot() -> AcceptedSchemaSnapshot {
        use crate::db::schema::SchemaFieldWritePolicy;

        let fields = [
            ("id", AcceptedFieldKind::Nat64, None, None),
            ("payload", AcceptedFieldKind::Nat64, None, None),
            (
                "token",
                AcceptedFieldKind::Ulid,
                Some(FieldInsertGeneration::Ulid),
                None,
            ),
            (
                "stamp",
                AcceptedFieldKind::Timestamp,
                Some(FieldInsertGeneration::Timestamp),
                None,
            ),
            (
                "created_at",
                AcceptedFieldKind::Timestamp,
                None,
                Some(FieldWriteManagement::CreatedAt),
            ),
            (
                "updated_at",
                AcceptedFieldKind::Timestamp,
                None,
                Some(FieldWriteManagement::UpdatedAt),
            ),
            ("optional", AcceptedFieldKind::Nat64, None, None),
        ]
        .into_iter()
        .enumerate()
        .map(|(slot, (name, kind, generation, management))| {
            let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(u32::try_from(slot).unwrap() + 1),
                name.into(),
                SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
                kind,
                vec![],
                slot == 6,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(generation, management),
                FieldStorageDecode::ByKind,
                codec,
            )
        })
        .collect::<Vec<_>>();
        AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::ReplacementOwned".into(),
            "ReplacementOwned".into(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        ))
        .unwrap()
    }

    fn replacement_owned_contract(
        accepted: &AcceptedSchemaSnapshot,
    ) -> (StructuralRowContract, CompiledAcceptedRowConstraints) {
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let constraints = CompiledAcceptedRowConstraints::compile(
            accepted,
            &catalog,
            [7; 16],
            &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
        )
        .unwrap();
        let layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(accepted).unwrap();
        let contract = StructuralRowContract::from_accepted_decode_contract(
            accepted.entity_path(),
            layout.row_decode_contract(catalog),
        );
        (contract, constraints)
    }

    fn replacement_owned_values() -> [Value; 7] {
        use crate::types::Timestamp;

        [
            Value::Nat64(1),
            Value::Nat64(10),
            Value::Ulid(Ulid::from_u128(7)),
            Value::Timestamp(Timestamp::from_millis(7)),
            Value::Timestamp(Timestamp::from_millis(7)),
            Value::Timestamp(Timestamp::from_millis(7)),
            Value::Null,
        ]
    }

    #[cfg(feature = "sql")]
    #[test]
    fn fixed_update_binding_preserves_payload_bytes_across_rows_and_noops() {
        use crate::types::Timestamp;
        use icydb_diagnostic_code::DiagnosticMutationOperation;

        let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
        let payload = encode_canonical_value_for_accepted_field_contract(
            contract
                .required_accepted_field_persistence_contract(1)
                .unwrap(),
            &Value::Nat64(77),
        )
        .unwrap();
        let null = encode_canonical_value_for_accepted_field_contract(
            contract
                .required_accepted_field_persistence_contract(6)
                .unwrap(),
            &Value::Null,
        )
        .unwrap();
        let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![
            (FieldSlot::from_validated_index(1), payload.clone()),
            (FieldSlot::from_validated_index(6), null.clone()),
        ])
        .unwrap();
        let patch = fixed.to_update_intent(&contract).unwrap();
        for (id, before_value) in [(1, 10), (2, 20), (3, 77)] {
            let mut values = replacement_owned_values();
            values[0] = Value::Nat64(id);
            values[1] = Value::Nat64(before_value);
            let before =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&values[slot]))
                })
                .unwrap()
                .into_raw_row();
            let baseline = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &before, &contract,
            )
            .unwrap();
            let after = resolve_update_structural_patch_with_accepted_contract(
                &contract,
                [7; 16],
                &constraints,
                &baseline,
                &patch,
                AcceptedWriteContext::new(Timestamp::from_millis(8)),
                MutationDiagnosticContext::new(
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    [7; 16],
                    1,
                    DiagnosticMutationOperation::Update,
                    0,
                ),
            )
            .unwrap()
            .into_parts()
            .0
            .into_raw_row();
            let reader =
                StructuralSlotReader::from_raw_row_with_borrowed_contract(&after, &contract)
                    .unwrap();
            assert_eq!(reader.required_bytes(1).unwrap(), payload);
            assert_eq!(reader.required_bytes(6).unwrap(), null);
            assert_eq!(reader.required_cached_value(0).unwrap(), &Value::Nat64(id));
            let timestamp = if before_value == 77 { 7 } else { 8 };
            assert_eq!(
                reader.required_cached_value(5).unwrap(),
                &Value::Timestamp(Timestamp::from_millis(timestamp))
            );
            if before_value == 77 {
                assert_eq!(after.as_bytes(), before.as_bytes());
            }
        }
    }

    #[cfg(feature = "sql")]
    #[test]
    fn fixed_update_binding_rejects_malformed_wrong_contract_and_managed_payloads() {
        let (contract, _) = replacement_owned_contract(&replacement_owned_snapshot());
        let payload = encode_canonical_value_for_accepted_field_contract(
            contract
                .required_accepted_field_persistence_contract(1)
                .unwrap(),
            &Value::Nat64(77),
        )
        .unwrap();
        for (slot, bytes) in [(1, Vec::new()), (1, vec![0xff]), (99, payload.clone())] {
            let expected =
                crate::db::data::decode_runtime_value_from_row_contract(&contract, slot, &bytes)
                    .unwrap_err();
            let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
                FieldSlot::from_validated_index(slot),
                bytes,
            )])
            .unwrap();
            let error = fixed.to_update_intent(&contract).unwrap_err();
            assert_eq!(error.diagnostic(), expected.diagnostic());
            assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
        }
        for slot in 2..=5 {
            let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
                FieldSlot::from_validated_index(slot),
                vec![0xff],
            )])
            .unwrap();
            let error = fixed.to_update_intent(&contract).unwrap_err();
            assert_eq!(
                error.diagnostic(),
                InternalError::executor_invariant().diagnostic()
            );
        }
        let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
            FieldSlot::from_validated_index(1),
            payload.clone(),
        )])
        .unwrap();
        let (other_contract, _) =
            replacement_owned_contract(&narrow_integer_snapshot(AcceptedFieldKind::Nat16, 2));
        let expected =
            crate::db::data::decode_runtime_value_from_row_contract(&other_contract, 1, &payload)
                .unwrap_err();
        let error = fixed.to_update_intent(&other_contract).unwrap_err();
        assert_eq!(error.diagnostic(), expected.diagnostic());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }

    #[test]
    fn insert_generated_values_share_the_frozen_operation_timestamp() {
        use crate::types::Timestamp;
        use icydb_diagnostic_code::DiagnosticMutationOperation;

        let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
        let mut previous = None;
        for (index, millis) in [1000, 1000, 1001].into_iter().enumerate() {
            let timestamp = Timestamp::from_millis(millis);
            let patch = AcceptedMutationIntentPatch::new()
                .set_authored(
                    FieldSlot::from_validated_index(0),
                    InputValue::nat64(u64::try_from(index).unwrap() + 1),
                )
                .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(10));
            let row = resolve_insert_structural_patch_with_accepted_contract(
                &contract,
                [7; 16],
                &constraints,
                &patch,
                AcceptedWriteContext::new(timestamp),
                MutationDiagnosticContext::new(
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    [7; 16],
                    1,
                    DiagnosticMutationOperation::Insert,
                    u32::try_from(index).unwrap(),
                ),
                None,
            )
            .unwrap()
            .into_parts()
            .0
            .into_raw_row();
            let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &row, &contract,
            )
            .unwrap();
            let Value::Ulid(token) = reader.required_cached_value(2).unwrap() else {
                panic!("generated token must be a ULID");
            };
            assert_eq!(
                ulid::Ulid::from_bytes(token.to_bytes()).timestamp_ms(),
                u64::try_from(millis).unwrap()
            );
            if let Some(previous) = previous {
                assert!(previous < *token);
            }
            previous = Some(*token);
            for slot in 3..=5 {
                assert_eq!(
                    reader.required_cached_value(slot).unwrap(),
                    &Value::Timestamp(timestamp)
                );
            }
        }
    }

    #[test]
    fn existing_mutations_preserve_generated_values_and_real_change_timestamps() {
        use crate::types::Timestamp;
        use icydb_diagnostic_code::ErrorCode;

        for (operation, resolve) in EXISTING_ROW_RESOLVERS {
            let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
            let values = replacement_owned_values();
            let baseline =
                canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                    Ok(Cow::Borrowed(&values[slot]))
                })
                .unwrap()
                .into_raw_row();
            let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &baseline, &contract,
            )
            .unwrap();
            let context = MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                [7; 16],
                1,
                operation,
                0,
            );
            let patch = |payload| {
                AcceptedMutationIntentPatch::new()
                    .set_authored(FieldSlot::from_validated_index(0), InputValue::nat64(1))
                    .set_authored(
                        FieldSlot::from_validated_index(1),
                        InputValue::text("ignored".into()),
                    )
                    .set_authored(
                        FieldSlot::from_validated_index(1),
                        InputValue::nat64(payload),
                    )
            };
            for (payload, now) in [(10, 6), (10, 20), (11, 20)] {
                let (row, provenance) = resolve(
                    &contract,
                    [7; 16],
                    &constraints,
                    &reader,
                    &patch(payload),
                    AcceptedWriteContext::new(Timestamp::from_millis(now)),
                    context,
                )
                .unwrap()
                .into_parts();
                let row = row.into_raw_row();
                let after = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    &row, &contract,
                )
                .unwrap();
                for slot in 2..=4 {
                    assert_eq!(after.required_cached_value(slot).unwrap(), &values[slot]);
                    assert_eq!(
                        provenance[slot],
                        Some(AcceptedFieldWriteProvenance::Preserved)
                    );
                }
                assert_eq!(after.required_cached_value(6).unwrap(), &Value::Null);
                assert_eq!(
                    after.required_cached_value(5).unwrap(),
                    &Value::Timestamp(Timestamp::from_millis(if payload == 10 { 7 } else { now }))
                );
                if payload == 10 {
                    assert_eq!(row.as_bytes(), baseline.as_bytes());
                }
            }
            let error = resolve(
                &contract,
                [7; 16],
                &constraints,
                &reader,
                &patch(11),
                AcceptedWriteContext::new(Timestamp::from_millis(6)),
                context,
            )
            .err()
            .expect("real changes must still reject managed time regression");
            assert_eq!(
                error.diagnostic().error_code(),
                ErrorCode::RUNTIME_BOUNDARY_MUTATION_MANAGED_TIMESTAMP_REGRESSION
            );
        }
    }

    #[test]
    fn existing_noops_reject_inverted_stored_timestamps() {
        use crate::types::Timestamp;
        use icydb_diagnostic_code::ErrorCode;

        let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
        let mut values = replacement_owned_values();
        values[4] = Value::Timestamp(Timestamp::from_millis(9));
        let baseline =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            &baseline, &contract,
        )
        .unwrap();
        let patch = AcceptedMutationIntentPatch::new()
            .set_authored(FieldSlot::from_validated_index(0), InputValue::nat64(1))
            .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(10));
        for (operation, resolve) in EXISTING_ROW_RESOLVERS {
            let context = MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                [7; 16],
                1,
                operation,
                0,
            );
            let error = resolve(
                &contract,
                [7; 16],
                &constraints,
                &reader,
                &patch,
                AcceptedWriteContext::new(Timestamp::from_millis(20)),
                context,
            )
            .err()
            .expect("unchanged writes must reject inverted stored timestamps");
            assert_eq!(
                error.diagnostic().error_code(),
                ErrorCode::RUNTIME_BOUNDARY_MUTATION_MANAGED_TIMESTAMP_REGRESSION
            );
        }
    }

    #[test]
    fn replacement_preserves_generated_historical_fill() {
        use crate::{
            db::schema::{
                PersistedFieldOrigin, RowLayoutVersion, SchemaFieldWritePolicy,
                SchemaHistoricalFill,
            },
            types::Timestamp,
        };
        use icydb_diagnostic_code::DiagnosticMutationOperation;

        let initial = replacement_owned_snapshot();
        let (old_contract, _) = replacement_owned_contract(&initial);
        let values = replacement_owned_values();
        let baseline =
            canonical_row_from_runtime_value_source_with_accepted_contract(&old_contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let fill = Value::Timestamp(Timestamp::from_millis(42));
        let payload = encode_canonical_value_for_accepted_field_contract(
            old_contract
                .required_accepted_field_persistence_contract(3)
                .unwrap(),
            &fill,
        )
        .unwrap();
        let layout = RowLayoutVersion::INITIAL.checked_next().unwrap();
        let mut fields = initial.persisted_snapshot().fields().to_vec();
        fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
            FieldId::new(8),
            "historical_stamp".into(),
            SchemaFieldSlot::new(7),
            AcceptedFieldKind::Timestamp,
            vec![],
            false,
            layout,
            SchemaInsertDefault::None,
            SchemaHistoricalFill::SlotPayload(payload),
            SchemaFieldWritePolicy::from_model_policies(
                Some(FieldInsertGeneration::Timestamp),
                None,
            ),
            PersistedFieldOrigin::Generated,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Timestamp),
        ));
        let accepted = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            initial.entity_path().into(),
            "ReplacementOwned".into(),
            FieldId::new(1),
            SchemaRowLayout::new(
                layout,
                RowLayoutVersion::INITIAL,
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        ))
        .unwrap();
        let (contract, constraints) = replacement_owned_contract(&accepted);
        let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            &baseline, &contract,
        )
        .unwrap();
        assert!(reader.get_bytes(7).is_none());
        let patch = AcceptedMutationIntentPatch::new()
            .set_authored(FieldSlot::from_validated_index(0), InputValue::nat64(1))
            .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(10));
        let (row, provenance) = resolve_existing_replace_structural_patch_with_accepted_contract(
            &contract,
            [7; 16],
            &constraints,
            &reader,
            &patch,
            AcceptedWriteContext::new(Timestamp::from_millis(20)),
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                [7; 16],
                1,
                DiagnosticMutationOperation::Replace,
                0,
            ),
        )
        .unwrap()
        .into_parts();
        let raw = row.into_raw_row();
        let after =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&raw, &contract)
                .unwrap();
        assert_eq!(after.required_cached_value(7).unwrap(), &fill);
        assert_eq!(
            provenance[7],
            Some(AcceptedFieldWriteProvenance::HistoricalFill)
        );
        assert_eq!(after.required_cached_value(5).unwrap(), &values[5]);
    }

    fn narrow_integer_snapshot(kind: AcceptedFieldKind, width: usize) -> AcceptedSchemaSnapshot {
        let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
        let mut default = vec![0xff, 1, 7];
        default.resize(2 + width, 0);
        let fields = vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                kind.clone(),
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                codec,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "amount".to_string(),
                SchemaFieldSlot::new(1),
                kind,
                Vec::new(),
                true,
                SchemaInsertDefault::SlotPayload(default),
                FieldStorageDecode::ByKind,
                codec,
            ),
        ];
        AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::NarrowWrite".to_string(),
            "NarrowWrite".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(fields.iter().map(|f| (f.id(), f.slot())).collect()),
            fields,
        ))
        .unwrap()
    }

    #[test]
    fn narrow_integer_writes_preserve_defaults_updates_and_primary_keys() {
        for (kind, width, initial, boundary, invalid) in [
            (
                AcceptedFieldKind::Int8,
                1,
                Value::Int64(7),
                Value::Int64(-128),
                Value::Int64(128),
            ),
            (
                AcceptedFieldKind::Int16,
                2,
                Value::Int64(7),
                Value::Int64(-32768),
                Value::Int64(32768),
            ),
            (
                AcceptedFieldKind::Int32,
                4,
                Value::Int64(7),
                Value::Int64(i64::from(i32::MIN)),
                Value::Int64(i64::from(i32::MAX) + 1),
            ),
            (
                AcceptedFieldKind::Nat8,
                1,
                Value::Nat64(7),
                Value::Nat64(255),
                Value::Nat64(256),
            ),
            (
                AcceptedFieldKind::Nat16,
                2,
                Value::Nat64(7),
                Value::Nat64(65535),
                Value::Nat64(65536),
            ),
            (
                AcceptedFieldKind::Nat32,
                4,
                Value::Nat64(7),
                Value::Nat64(u64::from(u32::MAX)),
                Value::Nat64(u64::from(u32::MAX) + 1),
            ),
        ] {
            assert_narrow_integer_write(kind, width, initial, boundary, invalid);
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one width matrix shares insert/default, unchanged-update, boundary rejection and primary-key assertions"
    )]
    fn assert_narrow_integer_write(
        kind: AcceptedFieldKind,
        width: usize,
        initial: Value,
        boundary: Value,
        invalid: Value,
    ) {
        use crate::db::key_taxonomy::PrimaryKeyComponent;
        use icydb_diagnostic_code::DiagnosticMutationOperation;

        let accepted = narrow_integer_snapshot(kind, width);
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let fingerprint = [7; 16];
        let constraints = CompiledAcceptedRowConstraints::compile(
            &accepted,
            &catalog,
            fingerprint,
            &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
        )
        .unwrap();
        let layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
        let contract = StructuralRowContract::from_accepted_decode_contract(
            accepted.entity_path(),
            layout.row_decode_contract(catalog),
        );
        let input = |value: &Value| match value {
            Value::Int64(value) => InputValue::int64(*value),
            Value::Nat64(value) => InputValue::nat64(*value),
            Value::Null => InputValue::null(),
            _ => panic!("integer fixture"),
        };
        let patch = AcceptedMutationIntentPatch::new()
            .set_authored(FieldSlot::from_validated_index(0), input(&initial));
        let write = AcceptedWriteContext::new(crate::types::Timestamp::from_millis(1));
        let row = resolve_insert_structural_patch_with_accepted_contract(
            &contract,
            fingerprint,
            &constraints,
            &patch,
            write,
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                fingerprint,
                1,
                DiagnosticMutationOperation::Insert,
                0,
            ),
            None,
        )
        .unwrap()
        .into_parts()
        .0
        .into_raw_row();
        let reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap();
        assert_eq!(reader.required_cached_value(1).unwrap(), &initial);
        assert_eq!(reader.required_bytes(1).unwrap().len(), 2 + width);
        let key = PrimaryKeyComponent::from_runtime_value(&initial)
            .unwrap()
            .into();
        reader.validate_primary_key_value(&key).unwrap();
        let replaced = resolve_existing_replace_structural_patch_with_accepted_contract(
            &contract,
            fingerprint,
            &constraints,
            &reader,
            &patch,
            write,
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                fingerprint,
                1,
                DiagnosticMutationOperation::Replace,
                0,
            ),
        )
        .unwrap()
        .into_parts()
        .0
        .into_raw_row();
        assert_eq!(
            replaced.as_bytes(),
            row.as_bytes(),
            "replacement retains the same accepted default"
        );
        for value in [
            None,
            Some(initial.clone()),
            Some(boundary),
            Some(Value::Null),
            Some(invalid.clone()),
        ] {
            let patch = value
                .as_ref()
                .map_or_else(AcceptedMutationIntentPatch::new, |value| {
                    AcceptedMutationIntentPatch::new()
                        .set_authored(FieldSlot::from_validated_index(1), input(value))
                });
            let value = value.unwrap_or_else(|| initial.clone());
            let result = resolve_update_structural_patch_with_accepted_contract(
                &contract,
                fingerprint,
                &constraints,
                &StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    &row, &contract,
                )
                .unwrap(),
                &patch,
                write,
                MutationDiagnosticContext::new(
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    fingerprint,
                    1,
                    DiagnosticMutationOperation::Update,
                    0,
                ),
            );
            if value == invalid {
                assert!(result.is_err());
                continue;
            }
            let updated = result.unwrap().into_parts().0.into_raw_row();
            if value == initial {
                assert_eq!(updated.as_bytes(), row.as_bytes());
            }
            let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &updated, &contract,
            )
            .unwrap();
            assert_eq!(reader.required_cached_value(1).unwrap(), &value);
            assert_eq!(
                reader.required_bytes(1).unwrap().len(),
                if value == Value::Null { 2 } else { 2 + width }
            );
            reader.validate_primary_key_value(&key).unwrap();
        }
    }

    #[test]
    fn managed_timestamp_progression_uses_the_current_managed_write_time() {
        let existing_created_at = crate::types::Timestamp::from_millis(10);
        let existing_updated_at = crate::types::Timestamp::from_millis(20);

        assert!(managed_timestamp_values_regress(
            Some(existing_created_at),
            Some(existing_updated_at),
            crate::types::Timestamp::from_millis(19),
        ));
        assert!(!managed_timestamp_values_regress(
            Some(existing_created_at),
            Some(existing_updated_at),
            crate::types::Timestamp::from_millis(20),
        ));
        assert!(!managed_timestamp_values_regress(
            Some(existing_created_at),
            Some(existing_updated_at),
            crate::types::Timestamp::from_millis(30),
        ));
    }

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
        let constraints = CompiledAcceptedRowConstraints::compile(
            &accepted,
            &value_catalog,
            fingerprint,
            &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
        )
        .expect("accepted not-null program should compile");
        let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
            .expect("accepted row layout should build");
        let patch = AcceptedMutationIntentPatch::new().set_authored(
            crate::db::data::FieldSlot::from_validated_index(0),
            InputValue::null(),
        );
        let contract = StructuralRowContract::from_accepted_decode_contract(
            accepted.entity_path(),
            row_layout.row_decode_contract(value_catalog),
        );
        let Err(error) = resolve_insert_structural_patch_with_accepted_contract(
            &contract,
            fingerprint,
            &constraints,
            &patch,
            AcceptedWriteContext::new(crate::types::Timestamp::from_millis(1)),
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                fingerprint,
                1,
                icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
                0,
            ),
            None,
        ) else {
            panic!("explicit null should violate accepted not-null");
        };
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

        let facts = error.diagnostic_facts();
        assert!(facts.contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintId,
            u64::from(accepted_identity.id().get()),
        )));
        assert!(facts.contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::NotNull.raw(),
        )));
        assert!(facts.contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
            icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
        )));
    }
}
