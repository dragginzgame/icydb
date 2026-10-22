//! Sole structural mutation coordinator: accepted after-images, identity,
//! staging admission and atomic publication. Frontends supply lowered intent
//! and final result/progress preparation; no session or SQL contracts enter here.

use crate::{
    db::{
        commit::{CommitRowOp, database_incarnation_id},
        data::{
            AcceptedMutationIntentPatch, AcceptedPreKeyInsert, DecodedDataStoreKey, FieldSlot,
            RawRow, StructuralRowContract, StructuralSlotReader,
            canonical_row_from_structural_slot_reader_with_accepted_contract,
            resolve_existing_replace_structural_patch_with_accepted_contract,
            resolve_insert_structural_patch_with_accepted_contract,
            resolve_update_structural_patch_with_accepted_contract,
        },
        executor::{
            AcceptedMutationConstraintContext, AcceptedMutationConstraintScheduler,
            AcceptedSchemaCatalogContext, commit_structural_row_ops_with_mutation_progress,
            commit_structural_row_ops_with_window, mutation_key_exists_error,
        },
        integrity::MutationProgressRecordOp,
        resource::budget::finish_current_execution_instruction_watermark,
        schema::{
            AcceptedFieldKind, AcceptedIdentityAllocation, AcceptedRowLayoutRuntimeContract,
            FieldId, FieldInsertGeneration, IdentityStatementCursor,
        },
        write_context::{AcceptedWriteContext, MutationMode},
    },
    error::{InternalError, MutationDiagnosticContext},
    metrics::EntityMetricsSpan,
    traits::CanisterKind,
    types::Timestamp,
    value::{InputValue, Value},
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedIdentityInsertField {
    field_id: FieldId,
    field_slot: usize,
    accepted_kind: AcceptedFieldKind,
}

pub(in crate::db) struct AcceptedStructuralMutationCommitOptions {
    capture_output_values: bool,
    packing: AcceptedStructuralMutationPacking,
}

impl AcceptedStructuralMutationCommitOptions {
    pub(in crate::db) const fn standard(capture_output_values: bool) -> Self {
        Self {
            capture_output_values,
            packing: AcceptedStructuralMutationPacking::Complete,
        }
    }

    #[cfg(test)]
    pub(in crate::db) const fn with_mutation_progress() -> Self {
        Self {
            capture_output_values: false,
            packing: AcceptedStructuralMutationPacking::Complete,
        }
    }

    pub(in crate::db) const fn bounded_prefix() -> Self {
        Self {
            capture_output_values: false,
            packing: AcceptedStructuralMutationPacking::BoundedPrefix,
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::db) enum AcceptedStructuralMutationPacking {
    Complete,
    BoundedPrefix,
}

pub(in crate::db) enum AcceptedStructuralMutationCommitDirective {
    Standard,
    WithMutationProgress(MutationProgressRecordOp),
    Skip,
}

/// Accepted row identity carried by a structural mutation after frontend
/// lowering but before the canonical after-image exists.
pub(in crate::db) enum AcceptedStructuralMutationTarget {
    ResolveFromAfterImage,
    Expected(Box<DecodedDataStoreKey>),
    ExpectedLoaded(AcceptedLoadedStructuralRow),
}

/// One retained row whose accepted key relationship was validated by the
/// synchronous operation that loaded it.
pub(in crate::db) struct AcceptedLoadedStructuralRow {
    key: Box<DecodedDataStoreKey>,
    row: RawRow,
}

impl AcceptedLoadedStructuralRow {
    pub(in crate::db) fn from_validated_parts(key: DecodedDataStoreKey, row: RawRow) -> Self {
        Self {
            key: Box::new(key),
            row,
        }
    }

    fn into_parts(self) -> (DecodedDataStoreKey, RawRow) {
        (*self.key, self.row)
    }
}

impl AcceptedStructuralMutationTarget {
    pub(in crate::db) fn expected(key: DecodedDataStoreKey) -> Self {
        Self::Expected(Box::new(key))
    }

    /// Retain a row loaded by the same synchronous operation so mutation
    /// materialization does not perform a duplicate backend point read.
    pub(in crate::db) const fn expected_loaded(row: AcceptedLoadedStructuralRow) -> Self {
        Self::ExpectedLoaded(row)
    }
}

/// One accepted structural mutation intent ready for shared batch
/// materialization.
pub(in crate::db) enum AcceptedStructuralMutation {
    Save {
        mode: MutationMode,
        target: AcceptedStructuralMutationTarget,
        patch: AcceptedMutationIntentPatch,
    },
    Delete {
        key: Box<DecodedDataStoreKey>,
    },
}

impl AcceptedStructuralMutation {
    pub(in crate::db) const fn save(
        mode: MutationMode,
        target: AcceptedStructuralMutationTarget,
        patch: AcceptedMutationIntentPatch,
    ) -> Self {
        Self::Save {
            mode,
            target,
            patch,
        }
    }

    pub(in crate::db) fn delete(key: DecodedDataStoreKey) -> Self {
        Self::Delete { key: Box::new(key) }
    }
}

pub(in crate::db) const MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS: usize = 4_096;
pub(in crate::db) const MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES: usize = 64;
pub(in crate::db) const STRUCTURAL_MUTATION_BATCH_STAGED_BYTES_POLICY: u32 = 16 * 1024 * 1024;
pub(in crate::db) const MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES: usize =
    STRUCTURAL_MUTATION_BATCH_STAGED_BYTES_POLICY as usize;
pub(in crate::db) const MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES: usize = 1024 * 1024;

pub(in crate::db) struct AcceptedStructuralMutationBatchItem {
    pub(in crate::db) catalog: AcceptedSchemaCatalogContext,
    pub(in crate::db) mutation: AcceptedStructuralMutation,
}

struct AcceptedStructuralMutationEntityState {
    entity_tag: crate::types::EntityTag,
    output_field_slots: Vec<usize>,
    identity_field: Option<AcceptedIdentityInsertField>,
    identity_incarnation: Option<crate::db::integrity::DatabaseIncarnationId>,
    identity_cursor: Option<IdentityStatementCursor>,
    identity_insert_ordinal: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedStructuralMutationPackingReport {
    admitted_mutations: usize,
    staged_bytes: usize,
    stopped_before_candidate: bool,
    candidate_exceeds_batch_policy: bool,
}

impl AcceptedStructuralMutationPackingReport {
    #[must_use]
    pub(in crate::db) const fn admitted_mutations(self) -> usize {
        self.admitted_mutations
    }

    #[must_use]
    pub(in crate::db) const fn stopped_before_candidate(self) -> bool {
        self.stopped_before_candidate
    }

    #[must_use]
    pub(in crate::db) const fn candidate_exceeds_batch_policy(self) -> bool {
        self.candidate_exceeds_batch_policy
    }
}

pub(in crate::db) fn structural_mutation_staged_charge(
    lengths: impl IntoIterator<Item = usize>,
) -> Result<usize, InternalError> {
    lengths.into_iter().try_fold(0_usize, |total, length| {
        total.checked_add(length).ok_or_else(|| {
            InternalError::mutation_batch_staged_bytes_exceeded(
                None,
                MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES,
            )
        })
    })
}

pub(in crate::db) fn add_structural_mutation_staged_bytes(
    total: &mut usize,
    lengths: impl IntoIterator<Item = usize>,
) -> Result<(), InternalError> {
    let charge = structural_mutation_staged_charge(lengths)?;
    *total = total.checked_add(charge).ok_or_else(|| {
        InternalError::mutation_batch_staged_bytes_exceeded(
            None,
            MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES,
        )
    })?;
    if *total > MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES {
        return Err(InternalError::mutation_batch_staged_bytes_exceeded(
            Some(*total),
            MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES,
        ));
    }
    Ok(())
}

pub(in crate::db) fn admit_structural_mutation_staged_charge(
    total: &mut usize,
    lengths: impl IntoIterator<Item = usize>,
    packing: AcceptedStructuralMutationPacking,
) -> Result<AcceptedStructuralMutationStagedAdmission, InternalError> {
    if matches!(packing, AcceptedStructuralMutationPacking::Complete) {
        add_structural_mutation_staged_bytes(total, lengths)?;
        return Ok(AcceptedStructuralMutationStagedAdmission::Admitted);
    }

    let charge = structural_mutation_staged_charge(lengths)?;
    if charge > MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES {
        return Ok(AcceptedStructuralMutationStagedAdmission::CandidateExceedsPolicy);
    }
    let Some(next_total) = total.checked_add(charge) else {
        return Ok(AcceptedStructuralMutationStagedAdmission::PageFull);
    };
    if next_total > MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES {
        return Ok(AcceptedStructuralMutationStagedAdmission::PageFull);
    }
    *total = next_total;
    Ok(AcceptedStructuralMutationStagedAdmission::Admitted)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedStructuralMutationStagedAdmission {
    Admitted,
    PageFull,
    CandidateExceedsPolicy,
}

pub(in crate::db) fn validate_structural_mutation_result_bytes(
    encoded_bytes: usize,
) -> Result<(), InternalError> {
    if encoded_bytes > MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES {
        return Err(InternalError::mutation_batch_result_bytes_exceeded(
            encoded_bytes,
            MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES,
        ));
    }
    Ok(())
}

/// One canonical row produced by structural mutation materialization.
pub(in crate::db) struct AcceptedStructuralMutationRow {
    pub(in crate::db) values: Vec<Value>,
    pub(in crate::db) logical_changed: bool,
}

impl AcceptedStructuralMutationRow {
    #[cfg(any(feature = "sql", test))]
    pub(in crate::db) fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub(in crate::db) const fn logical_changed(&self) -> bool {
        self.logical_changed
    }
}

pub(in crate::db) fn mutation_diagnostic_context(
    catalog: &AcceptedSchemaCatalogContext,
    mode: MutationMode,
    batch_position: u32,
) -> MutationDiagnosticContext {
    MutationDiagnosticContext::new(
        catalog.fingerprint_method_version(),
        catalog.fingerprint(),
        catalog.identity().entity_tag().value(),
        mode.diagnostic_operation(),
        batch_position,
    )
}

pub(in crate::db) const fn dynamic_write_context(
    operation_timestamp: Timestamp,
) -> AcceptedWriteContext {
    AcceptedWriteContext::new(operation_timestamp)
}

pub(in crate::db) fn insert_key_exists_after_generation(identity_generated: bool) -> InternalError {
    if identity_generated {
        InternalError::identity_state_corruption()
    } else {
        mutation_key_exists_error()
    }
}

// Carry the selected key into the replacement after-image. Row existence and
// database-owned key policy are admitted together by the mutation coordinator.
fn preserve_dynamic_replacement_identity(
    key: &DecodedDataStoreKey,
    contract: &StructuralRowContract,
    mut patch: AcceptedMutationIntentPatch,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    let primary_key_slots = contract.primary_key_slot_indices();
    let runtime_key = key.primary_key_runtime_value();
    let components = match runtime_key {
        Value::List(values) if primary_key_slots.len() > 1 => values,
        value if primary_key_slots.len() == 1 => vec![value],
        _ => return Err(InternalError::executor_invariant()),
    };
    if components.len() != primary_key_slots.len() {
        return Err(InternalError::executor_invariant());
    }

    for (slot, value) in primary_key_slots.iter().copied().zip(components) {
        let _ = contract
            .required_accepted_field_contract(slot)
            .map_err(|_| InternalError::executor_invariant())?;
        let has_explicit_intent = patch
            .entries()
            .iter()
            .any(|entry| entry.slot().index() == slot);
        if has_explicit_intent {
            continue;
        }
        let value = InputValue::try_from_runtime_non_enum(&value)
            .ok_or_else(InternalError::executor_invariant)?;
        patch =
            patch.set_preserved_replacement_identity(FieldSlot::from_validated_index(slot), value);
    }

    Ok(patch)
}

fn accepted_identity_insert_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<Option<AcceptedIdentityInsertField>, InternalError> {
    let mut identity = None;
    for field in descriptor.fields() {
        if field.write_policy().insert_generation() != Some(FieldInsertGeneration::Identity) {
            continue;
        }
        let field_slot = usize::from(field.slot().get());
        if identity
            .replace(AcceptedIdentityInsertField {
                field_id: field.field_id(),
                field_slot,
                accepted_kind: field.kind().clone(),
            })
            .is_some()
            || descriptor.primary_key_slot_indices() != [field_slot]
        {
            return Err(InternalError::identity_corruption());
        }
    }
    Ok(identity)
}

pub(in crate::db) fn checked_pre_key_candidate_count(count: usize) -> Result<u32, InternalError> {
    u32::try_from(count).map_err(|_| InternalError::identity_candidate_count_exhausted())
}

fn validate_identity_materialization(
    entity_tag: crate::types::EntityTag,
    identity_field: &AcceptedIdentityInsertField,
    candidate: &AcceptedPreKeyInsert,
    allocation: &AcceptedIdentityAllocation,
    data_key: &DecodedDataStoreKey,
    reader: &StructuralSlotReader<'_>,
) -> Result<(), InternalError> {
    let owner = allocation.owner();
    let slot_value = reader.required_cached_value(identity_field.field_slot)?;
    if candidate.entity_tag() != entity_tag
        || candidate.input_ordinal() != allocation.input_ordinal()
        || owner.entity_tag() != entity_tag
        || owner.field_id() != identity_field.field_id
        || allocation.field_slot() != identity_field.field_slot
        || slot_value != allocation.value()
        || data_key.primary_key_runtime_value() != *allocation.value()
    {
        return Err(InternalError::identity_corruption());
    }
    Ok(())
}

// The write owner validates the whole after-image before selecting its key.
// Borrow that reader and retain cached components for subsequent Identity checks.
pub(in crate::db) fn data_key_from_validated_reader(
    entity_tag: crate::types::EntityTag,
    reader: &StructuralSlotReader<'_>,
) -> Result<DecodedDataStoreKey, InternalError> {
    let values = reader
        .contract()
        .primary_key_slot_indices()
        .iter()
        .map(|slot| reader.required_cached_value(*slot).cloned())
        .collect::<Result<Vec<_>, _>>()?;

    DecodedDataStoreKey::try_from_structural_key_values(entity_tag, &values)
}

fn load_existing_row(
    store: crate::db::registry::StoreHandle,
    data_key: &DecodedDataStoreKey,
) -> Result<Option<RawRow>, InternalError> {
    let raw_key = data_key.to_raw()?;
    Ok(store.with_data(|data| data.get(&raw_key)))
}

// This is the reader's last use, after whole-row and Identity validation.
// Result columns follow accepted field order, not the physical slot layout.
pub(in crate::db) fn into_mutation_output_values(
    mut reader: StructuralSlotReader<'_>,
    field_slots: &[usize],
) -> Result<Vec<Value>, InternalError> {
    let mut values = Vec::with_capacity(field_slots.len());
    for slot in field_slots {
        values.push(reader.take_required_value(*slot)?);
    }
    Ok(values)
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one phased owner keeps accepted authority, mutation context, precommit preparation, output capture, and commit staging inseparable"
)]
pub(in crate::db) fn execute_accepted_structural_mutation_batch<C: CanisterKind, T>(
    db: &crate::db::Db<C>,
    anchor_catalog: &AcceptedSchemaCatalogContext,
    mutation_capacity: usize,
    identity_candidate_count: usize,
    mut next_mutation: impl FnMut()
        -> Result<Option<AcceptedStructuralMutationBatchItem>, InternalError>,
    operation_timestamp: Timestamp,
    options: AcceptedStructuralMutationCommitOptions,
    precommit_preparation: impl FnOnce(
        Vec<AcceptedStructuralMutationRow>,
        AcceptedStructuralMutationPackingReport,
    ) -> Result<
        (T, AcceptedStructuralMutationCommitDirective),
        InternalError,
    >,
) -> Result<T, InternalError> {
    let AcceptedStructuralMutationCommitOptions {
        capture_output_values,
        packing,
    } = options;
    let anchor_identity = anchor_catalog.identity();
    let accepted_root_identity = anchor_catalog.runtime_root_identity();
    let store_path = anchor_identity.store_path();
    let store = db.recovered_store(store_path)?;
    let write_context = dynamic_write_context(operation_timestamp);
    if mutation_capacity > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
        return Err(InternalError::mutation_batch_too_many_items(
            mutation_capacity,
            MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
        ));
    }
    let _ = checked_pre_key_candidate_count(identity_candidate_count)?;
    let mut entity_states: Vec<AcceptedStructuralMutationEntityState> = Vec::new();
    let mut scheduler = AcceptedMutationConstraintScheduler::new(mutation_capacity);
    let mut output = Vec::with_capacity(mutation_capacity);
    let mut staged_bytes = 0_usize;
    let mut stopped_before_candidate = false;
    let mut candidate_exceeds_batch_policy = false;
    let mut input_index = 0_usize;

    while let Some(item) = next_mutation()? {
        if input_index >= mutation_capacity {
            return Err(InternalError::mutation_batch_too_many_items(
                input_index.saturating_add(1),
                mutation_capacity,
            ));
        }
        let batch_input_ordinal = u32::try_from(input_index).map_err(|_| {
            InternalError::mutation_batch_too_many_items(
                mutation_capacity,
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
            )
        })?;
        input_index = input_index.saturating_add(1);
        let catalog = &item.catalog;
        let identity = catalog.identity();
        if catalog.runtime_root_identity() != accepted_root_identity
            || identity.store_path() != store_path
        {
            return Err(InternalError::query_executor_invariant());
        }
        let entity_path = identity.entity_path();
        let _metrics_span = EntityMetricsSpan::new(entity_path);
        // The captured root already owns current/historical decode authority.
        // Repeated items borrow it rather than rebuilding or deep-cloning it.
        let row_contract = catalog.inspection_plan().row_contract();
        let entity_state_index = entity_states
            .iter()
            .position(|state| state.entity_tag == identity.entity_tag());
        let entity_state_index = if let Some(index) = entity_state_index {
            index
        } else {
            if entity_states.len() >= MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES {
                return Err(InternalError::mutation_batch_too_many_entities(
                    entity_states.len().saturating_add(1),
                    MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES,
                ));
            }
            let descriptor =
                AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
            let identity_field = accepted_identity_insert_field(&descriptor)?;
            let identity_incarnation = identity_field
                .as_ref()
                .map(|_| database_incarnation_id())
                .transpose()?;
            entity_states.push(AcceptedStructuralMutationEntityState {
                entity_tag: identity.entity_tag(),
                // Field order belongs to this same immutable root. Retain only
                // the output projection, once per entity, when requested.
                output_field_slots: if capture_output_values {
                    descriptor
                        .fields()
                        .iter()
                        .map(|field| usize::from(field.slot().get()))
                        .collect()
                } else {
                    Vec::new()
                },
                identity_field,
                identity_incarnation,
                identity_cursor: None,
                identity_insert_ordinal: 0,
            });
            entity_states.len().saturating_sub(1)
        };
        let identity_field = entity_states[entity_state_index].identity_field.clone();
        let identity_insert_ordinal = entity_states[entity_state_index].identity_insert_ordinal;
        let mutation = item.mutation;
        let AcceptedStructuralMutation::Save {
            mode,
            target,
            patch: authored_patch,
        } = mutation
        else {
            let AcceptedStructuralMutation::Delete { key } = mutation else {
                return Err(InternalError::executor_invariant());
            };
            let before = load_existing_row(store, &key)?
                .ok_or_else(|| InternalError::store_not_found(&key))?;
            let raw_key = key.to_raw()?;
            let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &before,
                row_contract,
            )?;
            reader.validate_primary_key(&key)?;
            let canonical_before =
                canonical_row_from_structural_slot_reader_with_accepted_contract(&reader)?;
            let admission = admit_structural_mutation_staged_charge(
                &mut staged_bytes,
                [
                    raw_key.as_bytes().len(),
                    canonical_before.as_raw_row().as_bytes().len(),
                ],
                packing,
            )?;
            match admission {
                AcceptedStructuralMutationStagedAdmission::Admitted => {}
                AcceptedStructuralMutationStagedAdmission::PageFull => {
                    stopped_before_candidate = true;
                    break;
                }
                AcceptedStructuralMutationStagedAdmission::CandidateExceedsPolicy => {
                    stopped_before_candidate = true;
                    candidate_exceeds_batch_policy = true;
                    break;
                }
            }
            scheduler.schedule_delete(
                entity_path,
                identity.entity_tag(),
                catalog.fingerprint(),
                CommitRowOp::new(
                    entity_path,
                    raw_key,
                    Some(canonical_before),
                    None,
                    catalog.fingerprint(),
                ),
                batch_input_ordinal,
            )?;
            let values = if capture_output_values {
                into_mutation_output_values(
                    reader,
                    &entity_states[entity_state_index].output_field_slots,
                )?
            } else {
                Vec::new()
            };
            output.push(AcceptedStructuralMutationRow {
                values,
                logical_changed: true,
            });
            continue;
        };
        let mutation_context = mutation_diagnostic_context(catalog, mode, batch_input_ordinal);
        let (expected_key, preloaded_before, pre_key_insert, mut keyed_patch) = match target {
            AcceptedStructuralMutationTarget::ResolveFromAfterImage => {
                let candidate_ordinal =
                    if identity_field.is_some() && matches!(mode, MutationMode::Insert) {
                        identity_insert_ordinal
                    } else {
                        batch_input_ordinal
                    };
                (
                    None,
                    None,
                    Some(AcceptedPreKeyInsert::new(
                        identity.entity_tag(),
                        authored_patch,
                        candidate_ordinal,
                    )),
                    None,
                )
            }
            AcceptedStructuralMutationTarget::Expected(key) => {
                (Some(*key), None, None, Some(authored_patch))
            }
            AcceptedStructuralMutationTarget::ExpectedLoaded(loaded) => {
                let (key, row) = loaded.into_parts();
                (Some(key), Some(row), None, Some(authored_patch))
            }
        };
        if matches!(mode, MutationMode::Replace)
            && let Some(key) = expected_key.as_ref()
        {
            let patch = keyed_patch
                .take()
                .ok_or_else(InternalError::executor_invariant)?;
            keyed_patch = Some(preserve_dynamic_replacement_identity(
                key,
                row_contract,
                patch,
            )?);
        }
        let patch = pre_key_insert
            .as_ref()
            .map(AcceptedPreKeyInsert::fields)
            .or(keyed_patch.as_ref())
            .ok_or_else(InternalError::executor_invariant)?;
        let before = match (expected_key.as_ref(), preloaded_before) {
            (Some(_), Some(row)) => Some(row),
            (Some(key), None) => load_existing_row(store, key)?,
            (None, None) => None,
            (None, Some(_)) => return Err(InternalError::executor_invariant()),
        };
        let before_reader = before
            .as_ref()
            .map(|row| {
                let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    row,
                    row_contract,
                )?;
                reader.validate_primary_key(
                    expected_key
                        .as_ref()
                        .ok_or_else(InternalError::executor_invariant)?,
                )?;
                Ok::<_, InternalError>(reader)
            })
            .transpose()?;
        match mode {
            MutationMode::Insert if before.is_some() => {
                return Err(mutation_key_exists_error());
            }
            MutationMode::Update if before.is_none() => {
                let key = expected_key
                    .as_ref()
                    .ok_or_else(InternalError::executor_invariant)?;
                return Err(InternalError::store_not_found(key));
            }
            MutationMode::Replace if before.is_none() => {
                // Without a before-image, preserving a generated/managed key
                // would let the caller author a database-owned insert field.
                for slot in row_contract.primary_key_slot_indices() {
                    let field = row_contract.required_accepted_field_contract(*slot)?;
                    let policy = field.write_policy();
                    if policy.insert_generation().is_some() || policy.write_management().is_some() {
                        return Err(InternalError::mutation_database_owned_field_explicit(
                            mutation_context,
                            field.field_id().get(),
                        ));
                    }
                }
            }
            MutationMode::Insert | MutationMode::Replace | MutationMode::Update => {}
        }

        let identity_allocation = if let Some(identity_field) = identity_field.as_ref()
            && matches!(mode, MutationMode::Insert)
            && before.is_none()
        {
            let candidate = pre_key_insert.as_ref().ok_or_else(|| {
                InternalError::mutation_database_owned_field_explicit(
                    mutation_context,
                    identity_field.field_id.get(),
                )
            })?;
            if entity_states[entity_state_index].identity_cursor.is_none() {
                let incarnation = entity_states[entity_state_index]
                    .identity_incarnation
                    .ok_or_else(InternalError::identity_state_corruption)?;
                entity_states[entity_state_index].identity_cursor =
                    Some(store.with_schema(|schema_store| {
                        schema_store.identity_statement_cursor(
                            incarnation,
                            identity.entity_tag(),
                            identity_field.field_id,
                            &identity_field.accepted_kind,
                        )
                    })?);
            }
            let allocation = entity_states[entity_state_index]
                .identity_cursor
                .as_mut()
                .ok_or_else(InternalError::identity_state_corruption)?
                .allocate(identity_field.field_slot, candidate.input_ordinal())?;
            entity_states[entity_state_index].identity_insert_ordinal = identity_insert_ordinal
                .checked_add(1)
                .ok_or_else(InternalError::identity_candidate_count_exhausted)?;
            Some(allocation)
        } else {
            None
        };

        let resolved = match (mode, before_reader.as_ref()) {
            (MutationMode::Insert | MutationMode::Replace, None) => {
                resolve_insert_structural_patch_with_accepted_contract(
                    row_contract,
                    catalog.fingerprint(),
                    catalog.accepted_row_constraints(),
                    patch,
                    write_context,
                    mutation_context,
                    identity_allocation.as_ref(),
                )?
            }
            (MutationMode::Update, Some(before)) => {
                resolve_update_structural_patch_with_accepted_contract(
                    row_contract,
                    catalog.fingerprint(),
                    catalog.accepted_row_constraints(),
                    before,
                    patch,
                    write_context,
                    mutation_context,
                )?
            }
            (MutationMode::Replace, Some(before)) => {
                resolve_existing_replace_structural_patch_with_accepted_contract(
                    row_contract,
                    catalog.fingerprint(),
                    catalog.accepted_row_constraints(),
                    before,
                    patch,
                    write_context,
                    mutation_context,
                )?
            }
            (MutationMode::Insert, Some(_)) | (MutationMode::Update, None) => {
                return Err(InternalError::executor_invariant());
            }
        };
        let (after, provenance) = resolved.into_parts();
        let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(
            after.as_raw_row(),
            row_contract,
        )?;
        let data_key = match expected_key {
            Some(key) => {
                reader.validate_primary_key(&key)?;
                key
            }
            None => data_key_from_validated_reader(identity.entity_tag(), &reader)?,
        };
        if let Some(allocation) = identity_allocation.as_ref() {
            validate_identity_materialization(
                identity.entity_tag(),
                identity_field
                    .as_ref()
                    .ok_or_else(InternalError::identity_corruption)?,
                pre_key_insert
                    .as_ref()
                    .ok_or_else(InternalError::identity_corruption)?,
                allocation,
                &data_key,
                &reader,
            )?;
        }
        if matches!(mode, MutationMode::Insert)
            && let Some(existing) = load_existing_row(store, &data_key)?
        {
            // Preserve fail-closed admission at the retained store boundary even
            // when this generated key collision will reject the write.
            let existing_reader =
                StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    &existing,
                    row_contract,
                )?;
            existing_reader.validate_primary_key(&data_key)?;
            return Err(insert_key_exists_after_generation(
                identity_allocation.is_some(),
            ));
        }
        let raw_key = data_key.to_raw()?;
        let canonical_before = before_reader
            .as_ref()
            .map(canonical_row_from_structural_slot_reader_with_accepted_contract)
            .transpose()?;
        let logical_changed = canonical_before
            .as_ref()
            .is_none_or(|before| before.as_raw_row().as_bytes() != after.as_raw_row().as_bytes());
        let physical_changed = before
            .as_ref()
            .is_none_or(|before| before.as_bytes() != after.as_raw_row().as_bytes());
        let admission = admit_structural_mutation_staged_charge(
            &mut staged_bytes,
            [
                raw_key.as_bytes().len(),
                canonical_before
                    .as_ref()
                    .map_or(0, |before| before.as_raw_row().as_bytes().len()),
                after.as_raw_row().as_bytes().len(),
            ],
            packing,
        )?;
        match admission {
            AcceptedStructuralMutationStagedAdmission::Admitted => {}
            AcceptedStructuralMutationStagedAdmission::PageFull => {
                stopped_before_candidate = true;
                break;
            }
            AcceptedStructuralMutationStagedAdmission::CandidateExceedsPolicy => {
                stopped_before_candidate = true;
                candidate_exceeds_batch_policy = true;
                break;
            }
        }
        let row_op = physical_changed.then(|| {
            CommitRowOp::new(
                entity_path,
                raw_key.clone(),
                canonical_before,
                Some(after.clone()),
                catalog.fingerprint(),
            )
        });
        scheduler.schedule_save_after_image(
            AcceptedMutationConstraintContext {
                entity_tag: identity.entity_tag(),
                row_contract,
                schema_fingerprint: catalog.fingerprint(),
                fingerprint_method: catalog.fingerprint_method_version(),
                row_constraints: catalog.accepted_row_constraints(),
            },
            mode,
            &data_key,
            &reader,
            provenance.as_slice(),
            row_op,
            batch_input_ordinal,
        )?;
        let values = if capture_output_values {
            into_mutation_output_values(
                reader,
                &entity_states[entity_state_index].output_field_slots,
            )?
        } else {
            Vec::new()
        };
        output.push(AcceptedStructuralMutationRow {
            values,
            logical_changed,
        });
    }

    let report = AcceptedStructuralMutationPackingReport {
        admitted_mutations: output.len(),
        staged_bytes,
        stopped_before_candidate,
        candidate_exceeds_batch_policy,
    };
    let batch = scheduler.finish();
    let (prepared, commit_directive) = precommit_preparation(output, report)?;
    finish_current_execution_instruction_watermark()?;
    let mut identity_ranges = Vec::with_capacity(entity_states.len());
    for state in entity_states {
        if let Some(range) = state
            .identity_cursor
            .map(IdentityStatementCursor::into_range_advance)
            .transpose()?
            .flatten()
        {
            identity_ranges.push(range);
        }
    }
    if !matches!(
        commit_directive,
        AcceptedStructuralMutationCommitDirective::Skip
    ) && batch.is_empty()
        && !identity_ranges.is_empty()
    {
        return Err(InternalError::identity_corruption());
    }
    match commit_directive {
        AcceptedStructuralMutationCommitDirective::Skip => {}
        AcceptedStructuralMutationCommitDirective::Standard if batch.is_empty() => {}
        AcceptedStructuralMutationCommitDirective::Standard => {
            commit_structural_row_ops_with_window(db, batch, identity_ranges, operation_timestamp)?;
        }
        AcceptedStructuralMutationCommitDirective::WithMutationProgress(operation)
            if batch.is_empty() =>
        {
            let _ = operation;
            return Err(InternalError::executor_invariant());
        }
        AcceptedStructuralMutationCommitDirective::WithMutationProgress(operation) => {
            commit_structural_row_ops_with_mutation_progress(
                db,
                batch,
                identity_ranges,
                operation,
                operation_timestamp,
            )?;
        }
    }
    Ok(prepared)
}

/// Pack a checkpoint-aware update prefix using the writer's exact staging
/// charge, then apply the caller's atomic commit decision.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_accepted_structural_update_bounded_prefix<C: CanisterKind, T>(
    db: &crate::db::Db<C>,
    catalog: &AcceptedSchemaCatalogContext,
    mutation_capacity: usize,
    mut next_mutation: impl FnMut() -> Result<Option<AcceptedStructuralMutation>, InternalError>,
    operation_timestamp: Timestamp,
    precommit_preparation: impl FnOnce(
        AcceptedStructuralMutationPackingReport,
    ) -> Result<
        (T, AcceptedStructuralMutationCommitDirective),
        InternalError,
    >,
) -> Result<T, InternalError> {
    execute_accepted_structural_mutation_batch(
        db,
        catalog,
        mutation_capacity,
        0,
        || {
            next_mutation().map(|mutation| {
                mutation.map(|mutation| AcceptedStructuralMutationBatchItem {
                    catalog: catalog.clone(),
                    mutation,
                })
            })
        },
        operation_timestamp,
        AcceptedStructuralMutationCommitOptions::bounded_prefix(),
        |rows, report| {
            if rows.len() != report.admitted_mutations() {
                return Err(InternalError::executor_invariant());
            }
            precommit_preparation(report)
        },
    )
}
