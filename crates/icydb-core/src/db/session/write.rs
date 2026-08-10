//! Module: db::session::write
//! Responsibility: session-owned typed write APIs for insert, replace, update,
//! and structural mutation entrypoints over the shared save pipeline.
//! Does not own: commit staging, mutation execution, or persistence encoding.
//! Boundary: keeps public session write semantics above the executor save surface.

use super::AcceptedSchemaCatalogContext;
use crate::{
    db::{
        DbSession, DynamicMutation, DynamicMutationResult, DynamicStructuralPatch,
        DynamicTypedBindingError, DynamicTypedEntityBinding, DynamicTypedFieldBindingRequest,
        DynamicTypedFieldType, DynamicTypedMutation, DynamicTypedStructuralPatch, DynamicWriteCell,
        commit::{CommitRowOp, database_incarnation_id},
        data::{
            AcceptedMutationIntentPatch, AcceptedPreKeyInsert, DecodedDataStoreKey, FieldSlot,
            RawRow, StructuralRowContract, StructuralSlotReader,
            canonical_row_from_raw_row_with_accepted_decode_contract,
            resolve_existing_replace_structural_patch_with_accepted_contract,
            resolve_insert_structural_patch_with_accepted_contract,
            resolve_update_structural_patch_with_accepted_contract,
        },
        executor::{
            AcceptedMutationConstraintScheduler,
            commit_structural_row_ops_with_mutation_progress_for_path,
            commit_structural_row_ops_with_window_for_path, mutation_key_exists_error,
        },
        integrity::MutationProgressRecordOp,
        schema::{
            AcceptedFieldKind, AcceptedIdentityAllocation, AcceptedRowLayoutRuntimeContract,
            FieldId, FieldInsertGeneration, IdentityStatementCursor, lower_field_type,
            output_value_from_runtime,
        },
        write_context::{AcceptedWriteContext, MutationMode},
    },
    error::{InternalError, MutationDiagnosticContext},
    metrics::sink::{MetricsEvent, SaveMutationKind, record},
    traits::CanisterKind,
    types::{CurrentTimestamp, Timestamp},
    value::{InputValue, Value},
};
use icydb_schema::{EntitySourceKey, FieldSourceKey, FieldType, TypeSourceKey};

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedIdentityInsertField {
    field_id: FieldId,
    field_slot: usize,
    accepted_kind: AcceptedFieldKind,
}

struct AcceptedStructuralMutationCommitOptions {
    largest_journaled_prefix: bool,
    mutation_progress: Option<MutationProgressRecordOp>,
}

impl AcceptedStructuralMutationCommitOptions {
    const fn standard() -> Self {
        Self {
            largest_journaled_prefix: false,
            mutation_progress: None,
        }
    }

    const fn largest_journaled_prefix() -> Self {
        Self {
            largest_journaled_prefix: true,
            mutation_progress: None,
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "durable Forward execution consumes the Patch 4 commit option"
        )
    )]
    const fn with_mutation_progress(mutation_progress: MutationProgressRecordOp) -> Self {
        Self {
            largest_journaled_prefix: false,
            mutation_progress: Some(mutation_progress),
        }
    }
}

/// Accepted row identity carried by a structural mutation after frontend
/// lowering but before the canonical after-image exists.
pub(in crate::db::session) enum AcceptedStructuralMutationTarget {
    ResolveFromAfterImage,
    Expected(Box<DecodedDataStoreKey>),
}

impl AcceptedStructuralMutationTarget {
    pub(in crate::db::session) fn expected(key: DecodedDataStoreKey) -> Self {
        Self::Expected(Box::new(key))
    }
}

/// One accepted structural mutation intent ready for shared batch
/// materialization.
pub(in crate::db::session) enum AcceptedStructuralMutation {
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
    pub(in crate::db::session) const fn save(
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

    pub(in crate::db::session) fn delete(key: DecodedDataStoreKey) -> Self {
        Self::Delete { key: Box::new(key) }
    }
}

const MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS: usize = 4_096;
const MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES: usize = 16 * 1024 * 1024;
const MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES: usize = 1024 * 1024;

fn add_structural_mutation_staged_bytes(
    total: &mut usize,
    lengths: impl IntoIterator<Item = usize>,
) -> Result<(), InternalError> {
    for length in lengths {
        *total = total.checked_add(length).ok_or_else(|| {
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
    }
    Ok(())
}

fn validate_structural_mutation_result_bytes(encoded_bytes: usize) -> Result<(), InternalError> {
    if encoded_bytes > MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES {
        return Err(InternalError::mutation_batch_result_bytes_exceeded(
            encoded_bytes,
            MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES,
        ));
    }
    Ok(())
}

/// One canonical row produced by structural mutation materialization.
pub(in crate::db::session) struct AcceptedStructuralMutationRow {
    values: Vec<Value>,
    logical_changed: bool,
}

impl AcceptedStructuralMutationRow {
    #[cfg(feature = "sql")]
    pub(in crate::db::session) fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub(in crate::db::session) const fn logical_changed(&self) -> bool {
        self.logical_changed
    }
}

const fn dynamic_mutation_mode(request: &DynamicMutation) -> Option<MutationMode> {
    match request {
        DynamicMutation::Insert { .. } => Some(MutationMode::Insert),
        DynamicMutation::Update { .. } => Some(MutationMode::Update),
        DynamicMutation::Replace { .. } => Some(MutationMode::Replace),
        DynamicMutation::Delete { .. } => None,
    }
}

const fn dynamic_typed_mutation_mode(request: &DynamicTypedMutation) -> MutationMode {
    match request {
        DynamicTypedMutation::Insert { .. } => MutationMode::Insert,
        DynamicTypedMutation::Update { .. } => MutationMode::Update,
        DynamicTypedMutation::Replace { .. } => MutationMode::Replace,
    }
}

const fn diagnostic_mutation_operation(
    mode: MutationMode,
) -> icydb_diagnostic_code::DiagnosticMutationOperation {
    match mode {
        MutationMode::Insert => icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
        MutationMode::Replace => icydb_diagnostic_code::DiagnosticMutationOperation::Replace,
        MutationMode::Update => icydb_diagnostic_code::DiagnosticMutationOperation::Update,
    }
}

const fn mutation_diagnostic_context(
    entity_tag: crate::types::EntityTag,
    mode: MutationMode,
    batch_position: u32,
) -> MutationDiagnosticContext {
    MutationDiagnosticContext::new(
        entity_tag.value(),
        diagnostic_mutation_operation(mode),
        batch_position,
    )
}

const fn dynamic_write_context(operation_timestamp: Timestamp) -> AcceptedWriteContext {
    AcceptedWriteContext::new(operation_timestamp)
}

fn insert_key_exists_after_generation(identity_generated: bool) -> InternalError {
    if identity_generated {
        InternalError::identity_state_corruption()
    } else {
        mutation_key_exists_error()
    }
}

fn dynamic_key(
    entity_tag: crate::types::EntityTag,
    key: &InputValue,
) -> Result<DecodedDataStoreKey, InternalError> {
    let value = key
        .clone()
        .try_into_runtime_non_enum()
        .ok_or_else(InternalError::executor_unsupported)?;
    DecodedDataStoreKey::try_from_structural_key(entity_tag, &value)
}

fn lower_dynamic_patch(
    entity_path: &str,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: &DynamicStructuralPatch,
    mode: MutationMode,
    mutation_context: MutationDiagnosticContext,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    let mut lowered = AcceptedMutationIntentPatch::new();
    for (field_name, cell) in patch.fields() {
        let slot = descriptor
            .field_slot_index_by_name(field_name)
            .ok_or_else(|| {
                InternalError::mutation_structural_field_unknown(entity_path, field_name)
            })?;
        let field = descriptor
            .field_for_slot_index(slot)
            .ok_or_else(InternalError::executor_invariant)?;
        if !matches!(cell, DynamicWriteCell::Omitted)
            && (field.write_policy().insert_generation().is_some()
                || field.write_policy().write_management().is_some())
        {
            return Err(InternalError::mutation_database_owned_field_explicit(
                mutation_context,
                field.field_id().get(),
            ));
        }
        let slot = FieldSlot::from_validated_index(slot);
        lowered = match cell {
            DynamicWriteCell::Omitted => lowered,
            DynamicWriteCell::Default => match mode {
                MutationMode::Insert | MutationMode::Replace => {
                    lowered.set_explicit_insert_default(slot)
                }
                MutationMode::Update => lowered.set_explicit_update_default(slot),
            },
            DynamicWriteCell::Null => lowered.set_authored(slot, InputValue::Null),
            DynamicWriteCell::Value(value) => lowered.set_authored(slot, value.clone()),
        };
    }
    Ok(lowered)
}

fn lower_dynamic_mutation_intent(
    entity_tag: crate::types::EntityTag,
    entity_path: &str,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    request: &DynamicMutation,
    batch_position: u32,
) -> Result<(AcceptedStructuralMutation, Option<SaveMutationKind>), InternalError> {
    match request {
        DynamicMutation::Insert { patch, .. } => {
            let mode = MutationMode::Insert;
            Ok((
                AcceptedStructuralMutation::save(
                    mode,
                    AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                    lower_dynamic_patch(
                        entity_path,
                        descriptor,
                        patch,
                        mode,
                        mutation_diagnostic_context(entity_tag, mode, batch_position),
                    )?,
                ),
                Some(SaveMutationKind::Insert),
            ))
        }
        DynamicMutation::Update { key, patch, .. }
        | DynamicMutation::Replace { key, patch, .. } => {
            let mode =
                dynamic_mutation_mode(request).ok_or_else(InternalError::executor_invariant)?;
            let kind = match mode {
                MutationMode::Insert => SaveMutationKind::Insert,
                MutationMode::Replace => SaveMutationKind::Replace,
                MutationMode::Update => SaveMutationKind::Update,
            };
            Ok((
                AcceptedStructuralMutation::save(
                    mode,
                    AcceptedStructuralMutationTarget::expected(dynamic_key(entity_tag, key)?),
                    lower_dynamic_patch(
                        entity_path,
                        descriptor,
                        patch,
                        mode,
                        mutation_diagnostic_context(entity_tag, mode, batch_position),
                    )?,
                ),
                Some(kind),
            ))
        }
        DynamicMutation::Delete { key, .. } => Ok((
            AcceptedStructuralMutation::delete(dynamic_key(entity_tag, key)?),
            None,
        )),
    }
}

fn lower_typed_patch(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: &DynamicTypedStructuralPatch,
    mode: MutationMode,
    mutation_context: MutationDiagnosticContext,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    let mut lowered = AcceptedMutationIntentPatch::new();
    for (field_id, slot, cell) in patch.fields() {
        let slot_index = usize::from(*slot);
        let field = descriptor
            .field_for_slot_index(slot_index)
            .ok_or_else(InternalError::store_invariant)?;
        if field.field_id().get() != *field_id {
            return Err(InternalError::store_invariant());
        }
        if !matches!(cell, DynamicWriteCell::Omitted)
            && (field.write_policy().insert_generation().is_some()
                || field.write_policy().write_management().is_some())
        {
            return Err(InternalError::mutation_database_owned_field_explicit(
                mutation_context,
                field.field_id().get(),
            ));
        }
        let slot = FieldSlot::from_validated_index(slot_index);
        lowered = match cell {
            DynamicWriteCell::Omitted => lowered,
            DynamicWriteCell::Default => match mode {
                MutationMode::Insert | MutationMode::Replace => {
                    lowered.set_explicit_insert_default(slot)
                }
                MutationMode::Update => lowered.set_explicit_update_default(slot),
            },
            DynamicWriteCell::Null => lowered.set_authored(slot, InputValue::Null),
            DynamicWriteCell::Value(value) => lowered.set_authored(slot, value.clone()),
        };
    }
    Ok(lowered)
}

fn preserve_dynamic_replacement_identity(
    key: &DecodedDataStoreKey,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    mut patch: AcceptedMutationIntentPatch,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    let primary_key_slots = descriptor.primary_key_slot_indices();
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
        let _ = descriptor
            .field_for_slot_index(slot)
            .ok_or_else(InternalError::executor_invariant)?;
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

// Locate the sole accepted Identity owner that is eligible to resolve a
// keyless insert. Accepted-schema integrity already freezes the exact shape;
// this runtime check fails closed if a malformed contract reaches execution.
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

fn checked_pre_key_candidate_count(count: usize) -> Result<u32, InternalError> {
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

fn data_key_from_row(
    entity_tag: crate::types::EntityTag,
    contract: &StructuralRowContract,
    row: &RawRow,
) -> Result<DecodedDataStoreKey, InternalError> {
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, contract)?;
    let values = contract
        .primary_key_slot_indices()
        .iter()
        .map(|slot| reader.required_cached_value(*slot).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let value = match values.as_slice() {
        [value] => value.clone(),
        _ => Value::List(values),
    };
    DecodedDataStoreKey::try_from_structural_key(entity_tag, &value)
}

#[cfg(feature = "sql")]
pub(in crate::db::session) fn structural_data_key_from_runtime_values(
    entity_tag: crate::types::EntityTag,
    values: Vec<Value>,
) -> Result<DecodedDataStoreKey, InternalError> {
    let value = match values.as_slice() {
        [value] => value.clone(),
        _ => Value::List(values),
    };
    DecodedDataStoreKey::try_from_structural_key(entity_tag, &value)
}

fn validated_existing_row(
    store: crate::db::registry::StoreHandle,
    data_key: &DecodedDataStoreKey,
    contract: &StructuralRowContract,
) -> Result<Option<RawRow>, InternalError> {
    let raw_key = data_key.to_raw()?;
    let row = store.with_data(|data| data.get(&raw_key));
    if let Some(row) = row.as_ref() {
        let reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, contract)?;
        reader.validate_primary_key(data_key)?;
    }
    Ok(row)
}

fn prepare_dynamic_mutation_result(
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    rows: Vec<AcceptedStructuralMutationRow>,
    enforce_mixed_batch_result_bound: bool,
) -> Result<DynamicMutationResult, InternalError> {
    let affected_rows = rows.iter().try_fold(0_u32, |total, row| {
        total
            .checked_add(u32::from(row.logical_changed()))
            .ok_or_else(InternalError::executor_invariant)
    })?;
    let columns = descriptor
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    let rows = rows
        .into_iter()
        .map(|row| {
            row.values
                .iter()
                .map(|value| {
                    output_value_from_runtime(catalog.enum_catalog(), value)
                        .map_err(|_| InternalError::store_invariant())
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = DynamicMutationResult {
        entity: catalog.snapshot().entity_name().to_string(),
        columns,
        rows,
        affected_rows,
    };
    if enforce_mixed_batch_result_bound {
        let encoded =
            candid::encode_one(&result).map_err(|_| InternalError::executor_invariant())?;
        validate_structural_mutation_result_bytes(encoded.len())?;
    }
    Ok(result)
}

fn dynamic_typed_field_type(
    field_type: DynamicTypedFieldType,
) -> Result<FieldType, DynamicTypedBindingError> {
    match field_type {
        DynamicTypedFieldType::Scalar(scalar) => Ok(FieldType::Scalar(scalar)),
        DynamicTypedFieldType::List(item) => {
            Ok(FieldType::List(Box::new(dynamic_typed_field_type(*item)?)))
        }
        DynamicTypedFieldType::Named(source_key) => TypeSourceKey::try_new(source_key)
            .map(FieldType::Named)
            .map_err(|_| DynamicTypedBindingError::FieldUnavailable),
    }
}

fn typed_adapter_field_kind_matches(
    accepted: &AcceptedFieldKind,
    expected: &AcceptedFieldKind,
) -> bool {
    if accepted == expected {
        return true;
    }
    match (accepted, expected) {
        (AcceptedFieldKind::Relation { key_kind, .. }, expected) => {
            typed_adapter_field_kind_matches(key_kind, expected)
        }
        (AcceptedFieldKind::List(accepted), AcceptedFieldKind::List(expected)) => {
            typed_adapter_field_kind_matches(accepted, expected)
        }
        _ => false,
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Issue one opaque accepted binding for immutable generated source keys.
    pub fn issue_typed_entity_binding(
        &self,
        entity_source_key: &str,
        field_requests: &[DynamicTypedFieldBindingRequest],
    ) -> Result<DynamicTypedEntityBinding, DynamicTypedBindingError> {
        let entity_source = EntitySourceKey::try_new(entity_source_key)
            .map_err(|_| DynamicTypedBindingError::FieldUnavailable)?;
        let field_requests = field_requests
            .iter()
            .map(|request| {
                Ok((
                    FieldSourceKey::try_new(request.source_key.clone())
                        .map_err(|_| DynamicTypedBindingError::FieldUnavailable)?,
                    dynamic_typed_field_type(request.field_type.clone())?,
                    request.nullable,
                ))
            })
            .collect::<Result<Vec<_>, DynamicTypedBindingError>>()?;
        let catalog = self
            .find_accepted_schema_catalog_context_for_entity_source_key(entity_source.as_str())?
            .ok_or(DynamicTypedBindingError::FieldUnavailable)?;
        let identity = catalog.identity();
        if identity.entity_path() != entity_source.as_str() {
            return Err(InternalError::store_invariant().into());
        }
        let store = self.db.recovered_store(identity.store_path())?;
        let bundle = store
            .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
            .ok_or_else(InternalError::store_invariant)?;
        let entity_tag = identity.entity_tag();
        if bundle.source_bindings().entity(&entity_source) != Some(entity_tag)
            || bundle.revision() != catalog.revision()
        {
            return Err(InternalError::store_invariant().into());
        }
        let snapshot = bundle
            .entity_snapshots()
            .get(&entity_tag)
            .ok_or_else(InternalError::store_invariant)?;
        let row_contract = catalog.inspection_plan().row_contract();
        let mut fields = Vec::with_capacity(field_requests.len());
        for (source, field_type, nullable) in &field_requests {
            let field_id = bundle
                .source_bindings()
                .field(entity_tag, source)
                .ok_or(DynamicTypedBindingError::FieldUnavailable)?;
            let field = snapshot
                .fields()
                .iter()
                .find(|field| field.id() == field_id)
                .ok_or_else(InternalError::store_invariant)?;
            let runtime_field =
                row_contract.required_accepted_field_contract(usize::from(field.slot().get()))?;
            if runtime_field.field_id() != field_id {
                return Err(InternalError::store_invariant().into());
            }
            let expected_kind = lower_field_type(field_type, bundle.source_bindings())
                .map_err(|_| DynamicTypedBindingError::IncompatibleField)?;
            if field.nullable() != *nullable
                || !typed_adapter_field_kind_matches(field.kind(), &expected_kind)
            {
                return Err(DynamicTypedBindingError::IncompatibleField);
            }
            fields.push((
                source.as_str().to_string(),
                field_id.get(),
                field.slot().get(),
                field.name().to_string(),
            ));
        }
        let adapter_names = bundle.typed_adapter_names()?;

        DynamicTypedEntityBinding::new(
            database_incarnation_id()?.to_bytes(),
            entity_source.as_str().to_string(),
            snapshot.entity_name().to_string(),
            entity_tag.value(),
            catalog.revision().get(),
            catalog.fingerprint(),
            row_contract.current_layout_version().get(),
            fields,
            adapter_names.named_types,
            adapter_names.enum_variants,
            adapter_names.composite_fields,
        )
        .map_err(Into::into)
    }

    pub(in crate::db::session) fn current_typed_entity_binding_catalog(
        &self,
        binding: &DynamicTypedEntityBinding,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        if database_incarnation_id()?.to_bytes() != binding.database_incarnation {
            return Ok(None);
        }
        let Some(catalog) = self.find_accepted_schema_catalog_context_for_entity_source_key(
            binding.entity_source.as_str(),
        )?
        else {
            return Ok(None);
        };
        let row_contract = catalog.inspection_plan().row_contract();
        let identity = catalog.identity();
        if identity.entity_path() != binding.entity_source.as_str()
            || identity.entity_tag().value() != binding.entity_tag
            || catalog.revision().get() != binding.accepted_revision
            || catalog.fingerprint() != binding.accepted_fingerprint
            || row_contract.current_layout_version().get() != binding.entity_generation
        {
            return Ok(None);
        }
        let entity_source = EntitySourceKey::try_new(binding.entity_source.clone())
            .map_err(|_| InternalError::store_invariant())?;
        let store = self.db.recovered_store(identity.store_path())?;
        let bundle = store
            .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
            .ok_or_else(InternalError::store_invariant)?;
        if bundle.revision() != catalog.revision()
            || bundle.source_bindings().entity(&entity_source) != Some(identity.entity_tag())
        {
            return Ok(None);
        }
        let snapshot = bundle
            .entity_snapshots()
            .get(&identity.entity_tag())
            .ok_or_else(InternalError::store_invariant)?;
        for (source_key, expected_field_id, expected_slot) in binding.field_identity_bindings() {
            let source = FieldSourceKey::try_new(source_key)
                .map_err(|_| InternalError::store_invariant())?;
            let Some(field_id) = bundle
                .source_bindings()
                .field(identity.entity_tag(), &source)
            else {
                return Ok(None);
            };
            let Some(field) = snapshot
                .fields()
                .iter()
                .find(|field| field.id() == field_id)
            else {
                return Err(InternalError::store_invariant());
            };
            if field_id.get() != expected_field_id || field.slot().get() != expected_slot {
                return Ok(None);
            }
        }
        Ok(Some(catalog))
    }

    /// Verify that an opaque typed binding still names the exact accepted authority.
    pub fn typed_entity_binding_is_current(
        &self,
        binding: &DynamicTypedEntityBinding,
    ) -> Result<bool, InternalError> {
        self.current_typed_entity_binding_catalog(binding)
            .map(|catalog| catalog.is_some())
    }

    /// Materialize one accepted delete batch, run bounded frontend validation,
    /// then commit it atomically.
    #[cfg(feature = "sql")]
    pub(in crate::db::session) fn execute_accepted_structural_delete_batch(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        keys: Vec<DecodedDataStoreKey>,
        precommit_validation: impl FnOnce(&[Vec<Value>]) -> Result<(), InternalError>,
    ) -> Result<Vec<Vec<Value>>, InternalError> {
        let mutations = keys
            .into_iter()
            .map(AcceptedStructuralMutation::delete)
            .collect();
        self.execute_accepted_structural_mutation_batch_inner(
            catalog,
            descriptor,
            mutations,
            Timestamp::now(),
            AcceptedStructuralMutationCommitOptions::standard(),
            |rows| {
                let rows = rows
                    .into_iter()
                    .map(AcceptedStructuralMutationRow::into_values)
                    .collect::<Vec<_>>();
                precommit_validation(rows.as_slice())?;
                Ok(rows)
            },
        )
    }

    /// Materialize one accepted structural batch, let its caller prepare and
    /// validate the final after-images, then commit atomically.
    ///
    /// The caller freezes one operation timestamp and supplies frontend-lowered
    /// intent only. Accepted defaults, generated values, managed timestamps,
    /// constraints, relations, row encoding, and commit preparation remain
    /// owned by this database boundary.
    pub(in crate::db::session) fn execute_accepted_structural_save_batch<T>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        precommit_preparation: impl FnOnce(
            Vec<AcceptedStructuralMutationRow>,
        ) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        self.execute_accepted_structural_mutation_batch_inner(
            catalog,
            descriptor,
            mutations,
            operation_timestamp,
            AcceptedStructuralMutationCommitOptions::standard(),
            precommit_preparation,
        )
    }

    /// Commit the largest durable prefix of one accepted resumable update page.
    #[cfg(feature = "sql")]
    pub(in crate::db::session) fn execute_accepted_structural_update_prefix(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
    ) -> Result<usize, InternalError> {
        self.execute_accepted_structural_mutation_batch_inner(
            catalog,
            descriptor,
            mutations,
            operation_timestamp,
            AcceptedStructuralMutationCommitOptions::largest_journaled_prefix(),
            |rows| Ok(rows.len()),
        )
    }

    #[cfg(test)]
    fn execute_accepted_structural_save_batch_with_mutation_progress(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        mutation_progress: MutationProgressRecordOp,
    ) -> Result<Vec<AcceptedStructuralMutationRow>, InternalError> {
        self.execute_accepted_structural_mutation_batch_inner(
            catalog,
            descriptor,
            mutations,
            operation_timestamp,
            AcceptedStructuralMutationCommitOptions::with_mutation_progress(mutation_progress),
            Ok,
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one phased owner keeps accepted authority, mutation context, precommit preparation, output capture, and commit staging inseparable"
    )]
    fn execute_accepted_structural_mutation_batch_inner<T>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        options: AcceptedStructuralMutationCommitOptions,
        precommit_preparation: impl FnOnce(
            Vec<AcceptedStructuralMutationRow>,
        ) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        let identity = catalog.identity();
        let AcceptedStructuralMutationCommitOptions {
            largest_journaled_prefix,
            mutation_progress,
        } = options;
        let entity_path = identity.entity_path();
        let store_path = identity.store_path();
        let row_decode_contract =
            descriptor.row_decode_contract(catalog.value_catalog_handle().clone());
        let row_contract = StructuralRowContract::from_accepted_decode_contract(
            entity_path,
            row_decode_contract.clone(),
        );
        let store = self.db.recovered_store(store_path)?;
        let write_context = dynamic_write_context(operation_timestamp);
        let identity_field = accepted_identity_insert_field(descriptor)?;
        let identity_incarnation = identity_field
            .as_ref()
            .map(|_| database_incarnation_id())
            .transpose()?;
        let mutation_count = mutations.len();
        if mutation_count > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items(
                mutation_count,
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
            ));
        }
        let identity_candidate_count = mutations
            .iter()
            .filter(|mutation| {
                matches!(
                    mutation,
                    AcceptedStructuralMutation::Save {
                        mode: MutationMode::Insert,
                        target: AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                        ..
                    }
                )
            })
            .count();
        let _ = checked_pre_key_candidate_count(identity_candidate_count)?;
        let mut identity_cursor: Option<IdentityStatementCursor> = None;
        let mut identity_insert_ordinal = 0_u32;
        let mut scheduler = AcceptedMutationConstraintScheduler::new(
            entity_path,
            identity.entity_tag(),
            row_decode_contract.clone(),
            catalog.fingerprint(),
            catalog.fingerprint_method_version(),
            catalog.accepted_row_constraints(),
            mutation_count,
        );
        let mut output = Vec::with_capacity(mutation_count);
        let mut staged_bytes = 0_usize;

        for (input_index, mutation) in mutations.into_iter().enumerate() {
            let batch_input_ordinal = u32::try_from(input_index).map_err(|_| {
                InternalError::mutation_batch_too_many_items(
                    mutation_count,
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
                )
            })?;
            let AcceptedStructuralMutation::Save {
                mode,
                target,
                patch: authored_patch,
            } = mutation
            else {
                let AcceptedStructuralMutation::Delete { key } = mutation else {
                    return Err(InternalError::executor_invariant());
                };
                let before = validated_existing_row(store, &key, &row_contract)?
                    .ok_or_else(|| InternalError::store_not_found(&key))?;
                let raw_key = key.to_raw()?;
                let canonical_before = canonical_row_from_raw_row_with_accepted_decode_contract(
                    entity_path,
                    row_decode_contract.clone(),
                    &before,
                )?;
                add_structural_mutation_staged_bytes(
                    &mut staged_bytes,
                    [
                        raw_key.as_bytes().len(),
                        canonical_before.as_raw_row().as_bytes().len(),
                    ],
                )?;
                scheduler.schedule_delete(
                    CommitRowOp::new(
                        entity_path,
                        raw_key,
                        Some(canonical_before.as_raw_row().as_bytes().to_vec()),
                        None,
                        catalog.fingerprint(),
                    ),
                    batch_input_ordinal,
                )?;
                let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    canonical_before.as_raw_row(),
                    &row_contract,
                )?;
                let mut values = Vec::with_capacity(descriptor.fields().len());
                for field in descriptor.fields() {
                    values.push(
                        reader
                            .required_cached_value(usize::from(field.slot().get()))?
                            .clone(),
                    );
                }
                output.push(AcceptedStructuralMutationRow {
                    values,
                    logical_changed: true,
                });
                continue;
            };
            let mutation_context =
                mutation_diagnostic_context(identity.entity_tag(), mode, batch_input_ordinal);
            let (expected_key, pre_key_insert, mut keyed_patch) = match target {
                AcceptedStructuralMutationTarget::ResolveFromAfterImage => {
                    let candidate_ordinal =
                        if identity_field.is_some() && matches!(mode, MutationMode::Insert) {
                            identity_insert_ordinal
                        } else {
                            batch_input_ordinal
                        };
                    (
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
                    (Some(*key), None, Some(authored_patch))
                }
            };
            if matches!(mode, MutationMode::Replace)
                && let Some(key) = expected_key.as_ref()
            {
                let patch = keyed_patch
                    .take()
                    .ok_or_else(InternalError::executor_invariant)?;
                keyed_patch = Some(preserve_dynamic_replacement_identity(
                    key, descriptor, patch,
                )?);
            }
            let patch = pre_key_insert
                .as_ref()
                .map(AcceptedPreKeyInsert::fields)
                .or(keyed_patch.as_ref())
                .ok_or_else(InternalError::executor_invariant)?;
            let before = expected_key
                .as_ref()
                .map(|key| validated_existing_row(store, key, &row_contract))
                .transpose()?
                .flatten();
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
                if identity_cursor.is_none() {
                    let incarnation = identity_incarnation
                        .ok_or_else(InternalError::identity_state_corruption)?;
                    identity_cursor = Some(store.with_schema(|schema_store| {
                        schema_store.identity_statement_cursor(
                            incarnation,
                            identity.entity_tag(),
                            identity_field.field_id,
                            &identity_field.accepted_kind,
                        )
                    })?);
                }
                let allocation = identity_cursor
                    .as_mut()
                    .ok_or_else(InternalError::identity_state_corruption)?
                    .allocate(identity_field.field_slot, candidate.input_ordinal())?;
                identity_insert_ordinal = identity_insert_ordinal
                    .checked_add(1)
                    .ok_or_else(InternalError::identity_candidate_count_exhausted)?;
                Some(allocation)
            } else if let Some(identity_field) = identity_field.as_ref()
                && matches!(mode, MutationMode::Replace)
                && before.is_none()
            {
                return Err(InternalError::mutation_database_owned_field_explicit(
                    mutation_context,
                    identity_field.field_id.get(),
                ));
            } else {
                None
            };

            let resolved = match (mode, before.as_ref()) {
                (MutationMode::Insert | MutationMode::Replace, None) => {
                    resolve_insert_structural_patch_with_accepted_contract(
                        entity_path,
                        row_decode_contract.clone(),
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
                        entity_path,
                        row_decode_contract.clone(),
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
                        entity_path,
                        row_decode_contract.clone(),
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
            let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                after.as_raw_row(),
                &row_contract,
            )?;
            let data_key = match expected_key {
                Some(key) => {
                    reader.validate_primary_key(&key)?;
                    key
                }
                None => {
                    data_key_from_row(identity.entity_tag(), &row_contract, after.as_raw_row())?
                }
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
                && validated_existing_row(store, &data_key, &row_contract)?.is_some()
            {
                return Err(insert_key_exists_after_generation(
                    identity_allocation.is_some(),
                ));
            }
            let raw_key = data_key.to_raw()?;
            let canonical_before = before
                .as_ref()
                .map(|before| {
                    canonical_row_from_raw_row_with_accepted_decode_contract(
                        entity_path,
                        row_decode_contract.clone(),
                        before,
                    )
                })
                .transpose()?;
            let logical_changed = canonical_before.as_ref().is_none_or(|before| {
                before.as_raw_row().as_bytes() != after.as_raw_row().as_bytes()
            });
            let physical_changed = before
                .as_ref()
                .is_none_or(|before| before.as_bytes() != after.as_raw_row().as_bytes());
            add_structural_mutation_staged_bytes(
                &mut staged_bytes,
                [
                    raw_key.as_bytes().len(),
                    canonical_before
                        .as_ref()
                        .map_or(0, |before| before.as_raw_row().as_bytes().len()),
                    after.as_raw_row().as_bytes().len(),
                ],
            )?;
            let row_op = physical_changed.then(|| {
                CommitRowOp::new(
                    entity_path,
                    raw_key.clone(),
                    canonical_before
                        .as_ref()
                        .map(|before| before.as_raw_row().as_bytes().to_vec()),
                    Some(after.as_raw_row().as_bytes().to_vec()),
                    catalog.fingerprint(),
                )
            });
            scheduler.schedule_save_after_image(
                mode,
                &data_key,
                after.as_raw_row(),
                provenance.as_slice(),
                row_op,
                batch_input_ordinal,
            )?;
            if physical_changed {
                #[cfg(feature = "sql")]
                if largest_journaled_prefix
                    && !crate::db::commit::journaled_row_ops_fit_commit_window(scheduler.rows())
                {
                    scheduler.pop_last_save_row()?;
                    if output.is_empty() {
                        return Err(InternalError::query_sql_write_boundary(
                            icydb_diagnostic_code::SqlWriteBoundaryCode::ResumableUpdateSingleRowResourceExceeded,
                        ));
                    }
                    break;
                }
            }

            let mut values = Vec::with_capacity(descriptor.fields().len());
            for field in descriptor.fields() {
                values.push(
                    reader
                        .required_cached_value(usize::from(field.slot().get()))?
                        .clone(),
                );
            }
            output.push(AcceptedStructuralMutationRow {
                values,
                logical_changed,
            });
        }

        #[cfg(not(feature = "sql"))]
        let _ = largest_journaled_prefix;

        let batch = scheduler.finish();
        let prepared = precommit_preparation(output)?;
        let identity_ranges = identity_cursor
            .map(IdentityStatementCursor::into_range_advance)
            .transpose()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if batch.is_empty() && !identity_ranges.is_empty() {
            return Err(InternalError::identity_corruption());
        }
        if batch.is_empty() {
            if mutation_progress.is_some() {
                return Err(InternalError::executor_invariant());
            }
        } else if let Some(mutation_progress) = mutation_progress {
            commit_structural_row_ops_with_mutation_progress_for_path(
                &self.db,
                entity_path,
                batch,
                identity_ranges,
                mutation_progress,
                "accepted_structural_batch_apply",
            )?;
        } else {
            commit_structural_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                batch,
                identity_ranges,
                "accepted_structural_batch_apply",
            )?;
        }
        Ok(prepared)
    }

    fn execute_one_accepted_save_mutation(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mode: MutationMode,
        target: AcceptedStructuralMutationTarget,
        patch: AcceptedMutationIntentPatch,
    ) -> Result<DynamicMutationResult, InternalError> {
        let identity = catalog.identity();
        let entity_path = identity.entity_path();
        let result = self.execute_accepted_structural_save_batch(
            catalog,
            descriptor,
            vec![AcceptedStructuralMutation::save(mode, target, patch)],
            Timestamp::now(),
            |rows| prepare_dynamic_mutation_result(catalog, descriptor, rows, false),
        )?;
        record(MetricsEvent::SaveMutation {
            entity_path: entity_path.into(),
            kind: match mode {
                MutationMode::Insert => SaveMutationKind::Insert,
                MutationMode::Replace => SaveMutationKind::Replace,
                MutationMode::Update => SaveMutationKind::Update,
            },
            rows_touched: u64::from(result.affected_rows),
        });
        Ok(result)
    }

    /// Execute one trusted entity-name-driven structural mutation.
    ///
    /// This lane resolves public values, defaults, generation, management,
    /// constraints, relations, and commit preparation from accepted schema.
    /// It never materializes a generated entity or invokes application
    /// validators/normalizers.
    pub fn execute_trusted_dynamic_mutation(
        &self,
        request: &DynamicMutation,
    ) -> Result<DynamicMutationResult, InternalError> {
        self.execute_trusted_dynamic_mutation_batch_with_result_policy(vec![request.clone()], false)
    }

    /// Execute one bounded same-entity structural mutation batch atomically.
    ///
    /// Every item binds to the same accepted catalog identity, shares one
    /// operation timestamp, and is projected to its public result before the
    /// commit marker can be published.
    pub fn execute_trusted_dynamic_mutation_batch(
        &self,
        requests: Vec<DynamicMutation>,
    ) -> Result<DynamicMutationResult, InternalError> {
        self.execute_trusted_dynamic_mutation_batch_with_result_policy(requests, true)
    }

    fn execute_trusted_dynamic_mutation_batch_with_result_policy(
        &self,
        requests: Vec<DynamicMutation>,
        enforce_mixed_batch_result_bound: bool,
    ) -> Result<DynamicMutationResult, InternalError> {
        if requests.is_empty() {
            return Err(InternalError::mutation_batch_empty());
        }
        if requests.len() > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items(
                requests.len(),
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
            ));
        }
        let first = requests
            .first()
            .ok_or_else(InternalError::mutation_batch_empty)?;
        if first.entity().is_empty() {
            return Err(InternalError::executor_unsupported());
        }
        let catalog = self.accepted_schema_catalog_context_for_entity_name(Some(first.entity()))?;
        let accepted_identity = catalog.identity();
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let mut mutations = Vec::with_capacity(requests.len());
        let mut save_kinds = Vec::with_capacity(requests.len());

        for (batch_position, request) in requests.iter().enumerate() {
            let batch_position = u32::try_from(batch_position).map_err(|_| {
                InternalError::mutation_batch_too_many_items(
                    requests.len(),
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
                )
            })?;
            if request.entity().is_empty() {
                return Err(InternalError::executor_unsupported());
            }
            let item_catalog =
                self.accepted_schema_catalog_context_for_entity_name(Some(request.entity()))?;
            if item_catalog.identity() != accepted_identity {
                return Err(InternalError::mutation_batch_entity_mismatch(
                    batch_position,
                    accepted_identity.entity_tag().value(),
                    item_catalog.identity().entity_tag().value(),
                ));
            }
            let (mutation, save_kind) = lower_dynamic_mutation_intent(
                accepted_identity.entity_tag(),
                accepted_identity.entity_path(),
                &descriptor,
                request,
                batch_position,
            )?;
            mutations.push(mutation);
            save_kinds.push(save_kind);
        }

        let entity_path = accepted_identity.entity_path_handle();
        let (result, metrics) = self.execute_accepted_structural_mutation_batch_inner(
            &catalog,
            &descriptor,
            mutations,
            Timestamp::now(),
            AcceptedStructuralMutationCommitOptions::standard(),
            |rows| {
                if rows.len() != save_kinds.len() {
                    return Err(InternalError::executor_invariant());
                }
                let metrics = rows
                    .iter()
                    .zip(save_kinds)
                    .filter_map(|(row, kind)| kind.map(|kind| (kind, row.logical_changed())))
                    .collect::<Vec<_>>();
                let result = prepare_dynamic_mutation_result(
                    &catalog,
                    &descriptor,
                    rows,
                    enforce_mixed_batch_result_bound,
                )?;
                Ok((result, metrics))
            },
        )?;
        for (kind, logical_changed) in metrics {
            record(MetricsEvent::SaveMutation {
                entity_path: entity_path.clone(),
                kind,
                rows_touched: u64::from(logical_changed),
            });
        }
        Ok(result)
    }

    /// Execute one generated typed write through immutable accepted entity and
    /// field identities. `None` means the opaque binding is stale.
    #[doc(hidden)]
    pub fn execute_trusted_typed_mutation(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: &DynamicTypedMutation,
    ) -> Result<Option<DynamicMutationResult>, InternalError> {
        let Some(catalog) = self.current_typed_entity_binding_catalog(binding)? else {
            return Ok(None);
        };
        let identity = catalog.identity();
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let mode = dynamic_typed_mutation_mode(request);
        let (target, patch) = match request {
            DynamicTypedMutation::Insert { patch } => (
                AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                patch,
            ),
            DynamicTypedMutation::Update { key, patch }
            | DynamicTypedMutation::Replace { key, patch } => (
                AcceptedStructuralMutationTarget::expected(dynamic_key(
                    identity.entity_tag(),
                    key,
                )?),
                patch,
            ),
        };
        if !patch.is_bound_to(binding) {
            return Ok(None);
        }
        let patch = lower_typed_patch(
            &descriptor,
            patch,
            mode,
            mutation_diagnostic_context(identity.entity_tag(), mode, 0),
        )?;
        self.execute_one_accepted_save_mutation(&catalog, &descriptor, mode, target, patch)
            .map(Some)
    }

    /// Execute one trusted atomic insert batch from entity-name-driven patches.
    ///
    /// Every patch is lowered against the same accepted snapshot and shares
    /// one operation timestamp before the canonical structural batch owner
    /// stages any durable effect.
    pub fn execute_trusted_dynamic_insert_batch(
        &self,
        entity: &str,
        patches: Vec<DynamicStructuralPatch>,
    ) -> Result<DynamicMutationResult, InternalError> {
        let mutations = patches
            .into_iter()
            .map(|patch| DynamicMutation::Insert {
                entity: entity.to_string(),
                patch,
            })
            .collect();
        self.execute_trusted_dynamic_mutation_batch_with_result_policy(mutations, false)
    }
}

#[cfg(test)]
mod typed_adapter_tests {
    use super::{
        AcceptedFieldKind, DbSession, DynamicTypedBindingError, DynamicTypedFieldBindingRequest,
        DynamicTypedFieldType, DynamicTypedMutation, DynamicWriteCell, dynamic_typed_field_type,
        typed_adapter_field_kind_matches,
    };
    use crate::{
        db::{
            data::DataStore,
            index::IndexStore,
            registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
            schema::{
                AcceptedSchemaRevision, FieldId, FieldStorageDecode, LeafCodec,
                PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot,
                SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaVersion,
                accepted_schema_candidate_with_field_bindings_for_tests,
            },
        },
        traits::{CanisterKind, Path},
        types::EntityTag,
        value::InputValue,
    };
    use icydb_schema::{EntitySourceKey, FieldSourceKey, ScalarType};
    use std::{cell::RefCell, collections::BTreeMap};

    const STORE_PATH: &str = "session::write::typed_adapter_tests::Store";
    const ENTITY_SOURCE: &str = "session::write::typed_adapter_tests::Entity";
    const OTHER_ENTITY_SOURCE: &str = "session::write::typed_adapter_tests::OtherEntity";
    const ID_SOURCE: &str = "session::write::typed_adapter_tests::Entity::id";
    const VALUE_SOURCE: &str = "session::write::typed_adapter_tests::Entity::value";
    const REPLACEMENT_SOURCE: &str =
        "session::write::typed_adapter_tests::Entity::replacement_value";
    const OTHER_ID_SOURCE: &str = "session::write::typed_adapter_tests::OtherEntity::id";

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "session::write::typed_adapter_tests::Canister";
    }

    impl CanisterKind for TestCanister {
        const COMMIT_MEMORY_ID: u8 = 41;
        const COMMIT_STABLE_KEY: &'static str = "icydb.typed_adapter_tests.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 42;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.typed_adapter_tests.integrity.progress.v1";
    }

    thread_local! {
        static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA_STORE: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_store(
                STORE_PATH,
                &DATA_STORE,
                &INDEX_STORE,
                &SCHEMA_STORE,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("typed adapter test store should register");
            registry
        };
    }

    fn nat64_field(id: u32, name: &str, slot: u16) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(id),
            name.to_string(),
            SchemaFieldSlot::new(slot),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )
    }

    fn snapshot(
        entity_source: &str,
        entity_name: &str,
        fields: Vec<PersistedFieldSnapshot>,
    ) -> PersistedSchemaSnapshot {
        let layout = SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        );
        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            entity_source.to_string(),
            entity_name.to_string(),
            FieldId::new(1),
            layout,
            fields,
        )
    }

    fn field_source(source: &str) -> FieldSourceKey {
        FieldSourceKey::try_new(source).expect("typed field source should admit")
    }

    fn entity_source(source: &str) -> EntitySourceKey {
        EntitySourceKey::try_new(source).expect("typed entity source should admit")
    }

    fn publish(
        session: &DbSession<TestCanister>,
        expected: AcceptedSchemaRevision,
        revision: AcceptedSchemaRevision,
        snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
        fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
    ) {
        let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
            STORE_PATH, revision, snapshots, fields,
        );
        let store = session
            .db
            .store_handle(STORE_PATH)
            .expect("typed adapter test store should resolve");
        crate::db::commit::publish_accepted_schema_candidate(
            STORE_PATH, store, expected, &candidate,
        )
        .expect("typed binding candidate should publish");
    }

    fn request(source: &str) -> DynamicTypedFieldBindingRequest {
        DynamicTypedFieldBindingRequest::new(
            source.to_string(),
            DynamicTypedFieldType::Scalar(ScalarType::Nat64),
            false,
        )
    }

    fn assert_query_diagnostic(
        error: crate::db::QueryError,
        code: icydb_diagnostic_code::DiagnosticCode,
        origin: icydb_diagnostic_code::ErrorOrigin,
        detail: icydb_diagnostic_code::DiagnosticDetail,
    ) {
        let diagnostic = error.diagnostic();
        assert_eq!(diagnostic.code(), code);
        assert_eq!(diagnostic.origin(), origin);
        assert_eq!(diagnostic.detail(), Some(&detail));
    }

    #[test]
    fn typed_adapter_kind_matching_is_exact_but_accepts_relation_key_wrappers() {
        let relation = AcceptedFieldKind::Relation {
            target_path: "test::Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(7),
            target_store_path: "test::Store".to_string(),
            key_kind: Box::new(AcceptedFieldKind::Nat64),
        };

        assert!(typed_adapter_field_kind_matches(
            &relation,
            &AcceptedFieldKind::Nat64,
        ));
        assert!(typed_adapter_field_kind_matches(
            &AcceptedFieldKind::List(Box::new(relation)),
            &AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        ));
        assert!(!typed_adapter_field_kind_matches(
            &AcceptedFieldKind::Nat64,
            &AcceptedFieldKind::Nat32,
        ));
    }

    #[test]
    fn typed_adapter_field_contract_rejects_invalid_named_source_identity() {
        assert!(matches!(
            dynamic_typed_field_type(DynamicTypedFieldType::Named(String::new())),
            Err(DynamicTypedBindingError::FieldUnavailable),
        ));
        assert!(matches!(
            dynamic_typed_field_type(DynamicTypedFieldType::Scalar(ScalarType::Nat16)),
            Ok(icydb_schema::FieldType::Scalar(ScalarType::Nat16)),
        ));
    }

    // Keep the full rename, stale-binding, and old-name-reuse lifecycle in one
    // regression so each issued binding is checked against the next revision.
    #[expect(clippy::too_many_lines)]
    #[test]
    fn typed_binding_uses_accepted_ids_and_slots_across_renames_and_name_reuse() {
        let entity_tag = EntityTag::new(91);
        let other_entity_tag = EntityTag::new(92);
        DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
        INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
        SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

        let session = DbSession::<TestCanister>::new(
            &STORE_REGISTRY,
            &crate::db::RequestExecutionRoot::__new_runtime_root(),
        );
        session
            .db
            .ensure_recovered_state()
            .expect("typed adapter test database should initialize");
        publish(
            &session,
            AcceptedSchemaRevision::NONE,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(
                entity_tag,
                snapshot(
                    ENTITY_SOURCE,
                    "Entity",
                    vec![nat64_field(1, "id", 0), nat64_field(2, "value", 1)],
                ),
            )]),
            BTreeMap::from([
                ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
                ((entity_tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
            ]),
        );

        let initial_catalog = session
            .find_accepted_schema_catalog_context_for_entity_source_key(ENTITY_SOURCE)
            .expect("initial source catalog lookup should inspect")
            .expect("initial source catalog should exist");
        assert_eq!(initial_catalog.identity().entity_tag(), entity_tag);
        let initial = session
            .issue_typed_entity_binding(
                entity_source(ENTITY_SOURCE).as_str(),
                &[request(ID_SOURCE), request(VALUE_SOURCE)],
            )
            .expect("initial typed binding should issue");
        assert_eq!(initial.field_slot(ID_SOURCE), Some(0));
        assert_eq!(initial.field_slot(VALUE_SOURCE), Some(1));
        assert_eq!(initial.output_field_slot("value"), Some(1));
        let initial_patch = initial
            .bind_write_fields(vec![(
                VALUE_SOURCE.to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(7)),
            )])
            .expect("source-bound patch should lower");
        assert_eq!(
            initial_patch.fields(),
            &[(2, 1, DynamicWriteCell::Value(InputValue::Nat64(7)))]
        );

        publish(
            &session,
            AcceptedSchemaRevision::INITIAL,
            AcceptedSchemaRevision::new(2),
            BTreeMap::from([
                (
                    entity_tag,
                    snapshot(
                        ENTITY_SOURCE,
                        "RenamedEntity",
                        vec![
                            nat64_field(1, "id", 0),
                            nat64_field(2, "renamed_value", 1),
                            nat64_field(3, "value", 2),
                        ],
                    ),
                ),
                (
                    other_entity_tag,
                    snapshot(OTHER_ENTITY_SOURCE, "Entity", vec![nat64_field(1, "id", 0)]),
                ),
            ]),
            BTreeMap::from([
                ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
                ((entity_tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
                (
                    (entity_tag, field_source(REPLACEMENT_SOURCE)),
                    FieldId::new(3),
                ),
                (
                    (other_entity_tag, field_source(OTHER_ID_SOURCE)),
                    FieldId::new(1),
                ),
            ]),
        );

        let stale_authority = session
            .ensure_accepted_schema_authority_is_current_for_store_path(
                STORE_PATH,
                initial_catalog.value_catalog_handle().authority(),
            )
            .expect_err("the initial accepted authority must be stale after revision two");
        assert_eq!(
            stale_authority.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ExpectedRevision,
                    AcceptedSchemaRevision::INITIAL.get(),
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::CurrentRevision,
                    AcceptedSchemaRevision::new(2).get(),
                ),
            ],
        );

        assert!(
            !session
                .typed_entity_binding_is_current(&initial)
                .expect("renamed binding currentness should inspect")
        );
        let renamed = session
            .issue_typed_entity_binding(ENTITY_SOURCE, &[request(ID_SOURCE), request(VALUE_SOURCE)])
            .expect("renamed source-bound adapter should rebind");
        assert_eq!(renamed.entity(), "RenamedEntity");
        assert_eq!(renamed.field_slot(VALUE_SOURCE), Some(1));
        assert_eq!(renamed.output_field_slot("renamed_value"), Some(1));
        assert_eq!(renamed.output_field_slot("value"), None);

        publish(
            &session,
            AcceptedSchemaRevision::new(2),
            AcceptedSchemaRevision::new(3),
            BTreeMap::from([
                (
                    entity_tag,
                    snapshot(
                        ENTITY_SOURCE,
                        "RenamedEntity",
                        vec![nat64_field(1, "id", 0), nat64_field(2, "value", 1)],
                    ),
                ),
                (
                    other_entity_tag,
                    snapshot(OTHER_ENTITY_SOURCE, "Entity", vec![nat64_field(1, "id", 0)]),
                ),
            ]),
            BTreeMap::from([
                ((entity_tag, field_source(ID_SOURCE)), FieldId::new(1)),
                (
                    (entity_tag, field_source(REPLACEMENT_SOURCE)),
                    FieldId::new(2),
                ),
                (
                    (other_entity_tag, field_source(OTHER_ID_SOURCE)),
                    FieldId::new(1),
                ),
            ]),
        );

        assert!(matches!(
            session.issue_typed_entity_binding(
                ENTITY_SOURCE,
                &[request(ID_SOURCE), request(VALUE_SOURCE)],
            ),
            Err(DynamicTypedBindingError::FieldUnavailable),
        ));
        assert!(
            !session
                .typed_entity_binding_is_current(&renamed)
                .expect("removed source binding should become stale")
        );

        let replacement = session
            .issue_typed_entity_binding(
                ENTITY_SOURCE,
                &[request(ID_SOURCE), request(REPLACEMENT_SOURCE)],
            )
            .expect("explicit replacement source should bind");
        assert!(
            session
                .execute_trusted_typed_mutation(
                    &replacement,
                    &DynamicTypedMutation::Insert {
                        patch: initial_patch
                    },
                )
                .expect("cross-binding patch should fail closed")
                .is_none()
        );
        let patch = replacement
            .bind_write_fields(vec![
                (
                    ID_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(1)),
                ),
                (
                    REPLACEMENT_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(9)),
                ),
            ])
            .expect("replacement source write should bind by accepted IDs and slots");
        let result = session
            .execute_trusted_typed_mutation(&replacement, &DynamicTypedMutation::Insert { patch })
            .expect("typed insert should use the accepted mutation pipeline")
            .expect("replacement binding should remain current");
        assert_eq!(result.entity, "RenamedEntity");
        assert_eq!(result.columns, vec!["id".to_string(), "value".to_string()]);
        assert_eq!(
            result.rows,
            vec![vec![
                crate::value::OutputValue::Nat64(1),
                crate::value::OutputValue::Nat64(9)
            ]]
        );
        assert_eq!(result.affected_rows, 1);

        let second_patch = replacement
            .bind_write_fields(vec![
                (
                    ID_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(2)),
                ),
                (
                    REPLACEMENT_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(10)),
                ),
            ])
            .expect("second source-bound patch should lower");
        session
            .execute_trusted_typed_mutation(
                &replacement,
                &DynamicTypedMutation::Insert {
                    patch: second_patch,
                },
            )
            .expect("second typed insert should use the accepted mutation pipeline")
            .expect("replacement binding should remain current");

        {
            let query = crate::db::DynamicQuery::new("RenamedEntity")
                .select(["id", "value"])
                .order_by(crate::db::asc("id"))
                .limit(1);
            let result = session
                .execute_trusted_live_page(&query, None)
                .expect("SQL-free dynamic execution should use accepted authority");
            assert_eq!(result.entity, "RenamedEntity");
            assert_eq!(result.columns, vec!["id".to_string(), "value".to_string()]);
            assert_eq!(
                result.rows,
                vec![vec![
                    crate::value::OutputValue::Nat64(1),
                    crate::value::OutputValue::Nat64(9)
                ]]
            );
            assert_eq!(result.row_count, 1);
            assert_query_diagnostic(
                session
                    .execute_trusted_live_page(&query.cursor("00"), None)
                    .expect_err("scalar execution must reject grouped cursor state"),
                icydb_diagnostic_code::DiagnosticCode::QueryIntent,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                    kind: icydb_diagnostic_code::QueryErrorKind::Intent,
                },
            );
            assert_query_diagnostic(
                session
                    .execute_public_dynamic_grouped_query(
                        &crate::db::DynamicQuery::new("RenamedEntity").grouped_limits(1, 1024),
                    )
                    .expect_err("grouped execution must reject scalar query state"),
                icydb_diagnostic_code::DiagnosticCode::QueryIntent,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                    kind: icydb_diagnostic_code::QueryErrorKind::Intent,
                },
            );

            let grouped_query = crate::db::DynamicQuery::new("RenamedEntity")
                .filter(crate::db::FieldRef::new("id").eq(1_u64))
                .group_by("value")
                .aggregate(crate::db::count())
                .grouped_limits(1, 16 * 1024)
                .limit(1);
            let grouped = session
                .execute_public_dynamic_grouped_query(&grouped_query)
                .expect("SQL-free grouped execution should use accepted authority");
            let typed_grouped = session
                .execute_public_dynamic_grouped_query_for_typed_binding(
                    &replacement,
                    &grouped_query,
                )
                .expect("typed grouped execution should inspect accepted authority")
                .expect("replacement binding should remain current");
            assert_eq!(typed_grouped, grouped);
            assert!(
                session
                    .execute_public_dynamic_grouped_query_for_typed_binding(
                        &renamed,
                        &grouped_query,
                    )
                    .expect("stale grouped binding should inspect accepted authority")
                    .is_none(),
                "stale typed grouped bindings must fail closed before execution"
            );
            assert_eq!(grouped.entity, "RenamedEntity");
            assert_eq!(grouped.row_count, 1);
            assert_eq!(grouped.rows.len(), 1);
            assert_eq!(
                grouped.rows[0].group_key(),
                &[crate::value::OutputValue::Nat64(9)]
            );
            assert_eq!(
                grouped.rows[0].aggregate_values(),
                &[crate::value::OutputValue::Nat64(1)]
            );
            assert_eq!(grouped.next_cursor, None);

            let grouped_state_error = session
                .execute_trusted_dynamic_grouped_query(&grouped_query.clone().grouped_limits(1, 1))
                .expect_err("grouped retained state must respect its explicit byte ceiling");
            assert!(matches!(
                grouped_state_error.diagnostic().detail(),
                Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
                })
            ));
            assert_eq!(
                grouped_state_error.diagnostic_facts()[0],
                (
                    icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                    icydb_diagnostic_code::DiagnosticExecutionBudgetResource::GroupDistinctStateBytes.raw(),
                ),
            );

            assert_query_diagnostic(
                session
                    .execute_public_dynamic_grouped_query(&grouped_query.clone().select(["value"]))
                    .expect_err("grouped output must reject scalar selection"),
                icydb_diagnostic_code::DiagnosticCode::QueryIntent,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                    kind: icydb_diagnostic_code::QueryErrorKind::Intent,
                },
            );
            assert_query_diagnostic(
                session
                    .execute_public_dynamic_grouped_query(
                        &crate::db::DynamicQuery::new("RenamedEntity")
                            .group_by("value")
                            .aggregate(crate::db::count()),
                    )
                    .expect_err("public grouped execution must require explicit limits"),
                icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                    reason:
                        icydb_diagnostic_code::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
                },
            );
            assert_query_diagnostic(
                session
                    .execute_trusted_dynamic_grouped_query(
                        &crate::db::DynamicQuery::new("RenamedEntity")
                            .group_by("value")
                            .aggregate(crate::db::count())
                            .grouped_limits(0, 1024),
                    )
                    .expect_err("trusted grouped execution must reject zero limits"),
                icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                    reason:
                        icydb_diagnostic_code::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
                },
            );
            assert_query_diagnostic(
                session
                    .execute_public_dynamic_grouped_query(&grouped_query.grouped_limits(101, 1024))
                    .expect_err("public grouped execution must enforce its group budget"),
                icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                    reason:
                        icydb_diagnostic_code::QueryReadAdmissionCode::GroupedQueryExceedsBudget,
                },
            );

            let paged_query = crate::db::DynamicQuery::new("RenamedEntity")
                .group_by("value")
                .aggregate(crate::db::count())
                .grouped_limits(2, 16 * 1024)
                .limit(1);
            assert_query_diagnostic(
                session
                    .execute_public_dynamic_grouped_query(&paged_query)
                    .expect_err("public grouped execution must reject an unbounded full scan"),
                icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
                icydb_diagnostic_code::ErrorOrigin::Query,
                icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                    reason:
                        icydb_diagnostic_code::QueryReadAdmissionCode::UnboundedFullScanRejected,
                },
            );
            let first_page = session
                .execute_trusted_dynamic_grouped_query(&paged_query)
                .expect("SQL-free grouped first page should execute");
            assert_eq!(first_page.row_count, 1);
            assert_eq!(
                first_page.rows[0].group_key(),
                &[crate::value::OutputValue::Nat64(9)]
            );
            let cursor = first_page
                .next_cursor
                .expect("first grouped page should return a continuation cursor");
            assert_query_diagnostic(
                session
                    .execute_trusted_dynamic_grouped_query(
                        &paged_query.clone().cursor(format!("{cursor}0")),
                    )
                    .expect_err("tampered grouped cursor must fail closed"),
                icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
                icydb_diagnostic_code::ErrorOrigin::Cursor,
                icydb_diagnostic_code::DiagnosticDetail::QueryKind {
                    kind: icydb_diagnostic_code::QueryErrorKind::InvalidContinuationCursor,
                },
            );
            let second_page = session
                .execute_trusted_dynamic_grouped_query(&paged_query.cursor(cursor))
                .expect("SQL-free grouped continuation should execute");
            assert_eq!(second_page.row_count, 1);
            assert_eq!(
                second_page.rows[0].group_key(),
                &[crate::value::OutputValue::Nat64(10)]
            );
            assert_eq!(second_page.next_cursor, None);
        }
    }
}

#[cfg(test)]
mod mixed_relation_batch_tests {
    use super::{DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell};
    use crate::{
        db::{
            DynamicQuery, asc,
            data::DataStore,
            desc,
            index::IndexStore,
            query::expr::FilterExpr,
            registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
            schema::{
                AcceptedConstraintCatalog, AcceptedFieldKind, AcceptedSchemaRevision, FieldId,
                FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
                PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
                PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId, ScalarCodec,
                SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaStore,
                SchemaVersion, accepted_schema_candidate_with_field_bindings_for_tests,
            },
        },
        error::ErrorClass,
        traits::{CanisterKind, Path},
        types::EntityTag,
        value::{InputValue, OutputValue},
    };
    use icydb_schema::FieldSourceKey;
    use std::{cell::RefCell, collections::BTreeMap};

    const STORE_PATH: &str = "session::write::mixed_relation_batch_tests::Store";
    const ENTITY_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node";
    const ID_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node::id";
    const PARENT_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node::parent_id";
    const CODE_SOURCE: &str = "session::write::mixed_relation_batch_tests::Node::code";
    const ENTITY_NAME: &str = "MixedRelationNode";
    const ENTITY_TAG: EntityTag = EntityTag::new(94);
    const OTHER_ENTITY_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other";
    const OTHER_ID_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other::id";
    const OTHER_VALUE_SOURCE: &str = "session::write::mixed_relation_batch_tests::Other::value";
    const OTHER_ENTITY_NAME: &str = "MixedRelationOther";
    const OTHER_ENTITY_TAG: EntityTag = EntityTag::new(95);

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "session::write::mixed_relation_batch_tests::Canister";
    }

    impl CanisterKind for TestCanister {
        const COMMIT_MEMORY_ID: u8 = 47;
        const COMMIT_STABLE_KEY: &'static str = "icydb.mixed_relation_batch_tests.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 48;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.mixed_relation_batch_tests.integrity.progress.v1";
    }

    thread_local! {
        static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA_STORE: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_store(
                STORE_PATH,
                &DATA_STORE,
                &INDEX_STORE,
                &SCHEMA_STORE,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("mixed relation test store should register");
            registry
        };
    }

    fn source_key(source: &str) -> FieldSourceKey {
        FieldSourceKey::try_new(source).expect("mixed relation field source should admit")
    }

    fn relation_snapshot() -> PersistedSchemaSnapshot {
        let fields = vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "parent_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                true,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(3),
                "code".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ];
        let relation = PersistedRelationEdgeSnapshot::new(
            RelationId::new(1).expect("mixed relation identity should be non-zero"),
            "parent".to_string(),
            ENTITY_SOURCE.to_string(),
            vec![FieldId::new(2)],
        );
        let snapshot = PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            ENTITY_SOURCE.to_string(),
            ENTITY_NAME.to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
            vec![PersistedIndexSnapshot::new(
                SchemaIndexId::new(1).expect("mixed unique index identity should be non-zero"),
                1,
                "by_code".to_string(),
                STORE_PATH.to_string(),
                true,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(3),
                    SchemaFieldSlot::new(2),
                    vec!["code".to_string()],
                    AcceptedFieldKind::Nat64,
                    false,
                )]),
                None,
            )],
        )
        .with_relations(vec![relation]);
        let constraints = AcceptedConstraintCatalog::initial(
            snapshot.fields(),
            snapshot.indexes(),
            snapshot.relations(),
        )
        .expect("mixed relation constraints should close");
        snapshot.with_constraint_catalog(constraints)
    }

    fn other_snapshot() -> PersistedSchemaSnapshot {
        let fields = vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "value".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ];
        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            OTHER_ENTITY_SOURCE.to_string(),
            OTHER_ENTITY_NAME.to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        )
    }

    fn initialize() -> DbSession<TestCanister> {
        DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
        INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
        SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
        let session = DbSession::<TestCanister>::new(
            &STORE_REGISTRY,
            &crate::db::RequestExecutionRoot::__new_runtime_root(),
        );
        session
            .db
            .ensure_recovered_state()
            .expect("mixed relation database should initialize");
        let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
            STORE_PATH,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([
                (ENTITY_TAG, relation_snapshot()),
                (OTHER_ENTITY_TAG, other_snapshot()),
            ]),
            BTreeMap::from([
                ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
                ((ENTITY_TAG, source_key(PARENT_SOURCE)), FieldId::new(2)),
                ((ENTITY_TAG, source_key(CODE_SOURCE)), FieldId::new(3)),
                (
                    (OTHER_ENTITY_TAG, source_key(OTHER_ID_SOURCE)),
                    FieldId::new(1),
                ),
                (
                    (OTHER_ENTITY_TAG, source_key(OTHER_VALUE_SOURCE)),
                    FieldId::new(2),
                ),
            ]),
        );
        let store = session
            .db
            .store_handle(STORE_PATH)
            .expect("mixed relation store should resolve");
        crate::db::commit::publish_accepted_schema_candidate(
            STORE_PATH,
            store,
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect("mixed relation candidate should publish");
        session
    }

    fn patch(id: Option<u64>, parent: Option<u64>, code: Option<u64>) -> DynamicStructuralPatch {
        let mut fields = Vec::new();
        if let Some(id) = id {
            fields.push((
                "id".to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(id)),
            ));
        }
        fields.push((
            "parent_id".to_string(),
            parent.map_or(DynamicWriteCell::Null, |parent| {
                DynamicWriteCell::Value(InputValue::Nat64(parent))
            }),
        ));
        if let Some(code) = code {
            fields.push((
                "code".to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(code)),
            ));
        }
        DynamicStructuralPatch::new(fields)
    }

    fn insert(id: u64, parent: Option<u64>) -> DynamicMutation {
        insert_with_code(id, parent, id)
    }

    fn insert_with_code(id: u64, parent: Option<u64>, code: u64) -> DynamicMutation {
        DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: patch(Some(id), parent, Some(code)),
        }
    }

    fn update_parent(id: u64, parent: Option<u64>) -> DynamicMutation {
        DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::Nat64(id),
            patch: patch(None, parent, None),
        }
    }

    fn update_code(id: u64, code: u64) -> DynamicMutation {
        DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::Nat64(id),
            patch: DynamicStructuralPatch::new(vec![(
                "code".to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(code)),
            )]),
        }
    }

    fn delete(id: u64) -> DynamicMutation {
        DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::Nat64(id),
        }
    }

    fn expected_row(id: u64, parent: Option<u64>) -> Vec<OutputValue> {
        expected_row_with_code(id, parent, id)
    }

    fn expected_row_with_code(id: u64, parent: Option<u64>, code: u64) -> Vec<OutputValue> {
        vec![
            OutputValue::Nat64(id),
            parent.map_or(OutputValue::Null, OutputValue::Nat64),
            OutputValue::Nat64(code),
        ]
    }

    fn other_patch(id: Option<u64>, value: u64) -> DynamicStructuralPatch {
        let mut fields = Vec::new();
        if let Some(id) = id {
            fields.push((
                "id".to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(id)),
            ));
        }
        fields.push((
            "value".to_string(),
            DynamicWriteCell::Value(InputValue::Nat64(value)),
        ));
        DynamicStructuralPatch::new(fields)
    }

    fn assert_relation_violation(error: &crate::error::InternalError) {
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
        )));
    }

    #[test]
    fn live_pages_resume_mixed_projection_from_authenticated_hidden_order_values() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation_batch(vec![
                insert_with_code(1, None, 10),
                insert_with_code(2, Some(1), 20),
                insert_with_code(3, None, 30),
            ])
            .expect("live-page rows should insert");
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .order_by(desc("code"));

        let first = session
            .execute_public_live_page(&query, None)
            .expect("initial live page should execute");
        assert_eq!(
            first.rows,
            vec![vec![OutputValue::Nat64(3)], vec![OutputValue::Nat64(2)]]
        );
        let cursor = first
            .continuation
            .as_deref()
            .expect("unreturned matching row should produce continuation");
        let second = session
            .execute_public_live_page(&query, Some(cursor))
            .expect("authenticated live continuation should resume");
        assert_eq!(second.rows, vec![vec![OutputValue::Nat64(1)]]);
        assert_eq!(second.continuation, None);

        let total_limit = session
            .execute_public_live_page(&query.clone().limit(2), None)
            .expect("total live-page limit should execute");
        assert_eq!(
            total_limit.rows,
            vec![vec![OutputValue::Nat64(3)], vec![OutputValue::Nat64(2)]],
        );
        assert_eq!(
            total_limit.continuation, None,
            "query LIMIT is a total traversal window rather than a page size",
        );

        let three_row_window = query.clone().limit(3);
        let limited_first = session
            .execute_public_live_page(&three_row_window, None)
            .expect("first total-window page should execute");
        let limited_cursor = limited_first
            .continuation
            .as_deref()
            .expect("a partially consumed total window should continue");
        let limited_second = session
            .execute_public_live_page(&three_row_window, Some(limited_cursor))
            .expect("remaining total window should preserve the plan signature");
        assert_eq!(limited_second.rows, vec![vec![OutputValue::Nat64(1)]]);
        assert_eq!(limited_second.continuation, None);

        let mixed_order = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .order_by(desc("parent_id"))
            .order_by(asc("id"));
        let mixed_first = session
            .execute_trusted_live_page(&mixed_order, None)
            .expect("mixed-direction nullable order should execute");
        assert_eq!(
            mixed_first.rows,
            vec![vec![OutputValue::Nat64(2)], vec![OutputValue::Nat64(1)]],
        );
        let mixed_cursor = mixed_first
            .continuation
            .as_deref()
            .expect("duplicate null order values should retain continuation");
        let mixed_second = session
            .execute_trusted_live_page(&mixed_order, Some(mixed_cursor))
            .expect("mixed-direction nullable order should resume");
        assert_eq!(mixed_second.rows, vec![vec![OutputValue::Nat64(3)]]);
        assert_eq!(mixed_second.continuation, None);

        let mismatched_window = session
            .execute_public_live_page(&query.clone().limit(3), Some(cursor))
            .expect_err("a changed total limit must invalidate the continuation");
        assert_eq!(
            mismatched_window.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
        );

        let mut tampered = cursor.as_bytes().to_vec();
        let last = tampered.len().saturating_sub(1);
        tampered[last] = if tampered[last] == b'0' { b'1' } else { b'0' };
        let tampered = String::from_utf8(tampered).expect("hex cursor should remain UTF-8");
        let error = session
            .execute_public_live_page(&query, Some(tampered.as_str()))
            .expect_err("tampered cursor must fail closed");
        assert_eq!(
            error.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
        );
    }

    #[test]
    fn live_pages_resume_across_changed_output_work_envelopes() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation_batch(vec![
                insert(1, None),
                insert(2, None),
                insert(3, None),
            ])
            .expect("output-envelope rows should insert");
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .order_by(desc("code"));
        let first = session
            .execute_trusted_live_page_with_result_bytes_limit_for_tests(&query, None, 32)
            .expect("small output envelope should publish the first bounded page");
        assert_eq!(first.rows, vec![vec![OutputValue::Nat64(3)]]);
        let continuation = first
            .continuation
            .expect("small output envelope should leave authenticated progress");

        let second = session
            .execute_trusted_live_page_with_result_bytes_limit_for_tests(
                &query,
                Some(continuation.as_str()),
                64,
            )
            .unwrap_or_else(|error| {
                panic!(
                    "larger output envelope should resume the same query: {error:?}, facts={:?}",
                    error.diagnostic_facts(),
                )
            });
        assert_eq!(
            second.rows,
            vec![vec![OutputValue::Nat64(2)], vec![OutputValue::Nat64(1)]]
        );
        let second_continuation = second
            .continuation
            .as_deref()
            .expect("an exact-full page still needs to prove physical exhaustion");
        assert_ne!(first.work.envelope_identity, second.work.envelope_identity);

        let terminal = session
            .execute_trusted_live_page_with_result_bytes_limit_for_tests(
                &query,
                Some(second_continuation),
                48,
            )
            .expect("a third finite envelope should prove exhaustion without replaying rows");
        assert!(terminal.rows.is_empty());
        assert_eq!(terminal.continuation, None);
        assert_ne!(
            second.work.envelope_identity,
            terminal.work.envelope_identity
        );

        assert_eq!(
            [first.rows, second.rows, terminal.rows].concat(),
            vec![
                vec![OutputValue::Nat64(3)],
                vec![OutputValue::Nat64(2)],
                vec![OutputValue::Nat64(1)],
            ]
        );
    }

    #[test]
    fn distinct_live_pages_resume_adjacent_groups_and_global_replay_end_to_end() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation_batch(vec![
                insert(1, None),
                insert(2, None),
                insert(3, Some(1)),
                insert(4, Some(2)),
                insert(5, Some(1)),
                insert(6, Some(3)),
                insert(7, Some(2)),
            ])
            .expect("DISTINCT continuation rows should insert atomically");

        let adjacent = DynamicQuery::new(ENTITY_NAME)
            .select(["parent_id"])
            .order_by(asc("parent_id"))
            .order_by(asc("id"))
            .distinct_for_internal_execution();
        let global = DynamicQuery::new(ENTITY_NAME)
            .select(["parent_id"])
            .order_by(asc("id"))
            .distinct_for_internal_execution();

        let traverse = |query: &DynamicQuery, strategy: &str| {
            let mut continuation = None;
            let mut rows = Vec::new();
            let mut cursors = std::collections::BTreeSet::new();
            let mut pages = 0_u32;
            let mut entries_visited = 0_u64;
            loop {
                let page = session
                    .execute_trusted_live_page(query, continuation.as_deref())
                    .unwrap_or_else(|error| {
                        panic!("{strategy} DISTINCT page should execute: {error:?}")
                    });
                pages = pages.saturating_add(1);
                entries_visited = entries_visited.saturating_add(page.work.entries_visited);
                assert_eq!(page.row_count as usize, page.rows.len());
                assert_eq!(page.work.result_rows, page.row_count);
                rows.extend(page.rows);
                let Some(cursor) = page.continuation else {
                    break;
                };
                assert!(
                    cursors.insert(cursor.clone()),
                    "{strategy} DISTINCT continuation must advance monotonically",
                );
                continuation = Some(cursor);
                assert!(pages < 8, "{strategy} DISTINCT traversal must terminate");
            }

            (rows, pages, entries_visited)
        };

        let expected = vec![
            vec![OutputValue::Null],
            vec![OutputValue::Nat64(1)],
            vec![OutputValue::Nat64(2)],
            vec![OutputValue::Nat64(3)],
        ];
        let (adjacent_rows, adjacent_pages, adjacent_entries) = traverse(&adjacent, "adjacent");
        let (global_rows, global_pages, global_entries) = traverse(&global, "global");

        assert_eq!(adjacent_rows, expected);
        assert_eq!(global_rows, expected);
        assert_eq!(adjacent_pages, 2);
        assert_eq!(global_pages, 2);
        assert!(adjacent_entries > 0);
        assert!(global_entries > 0);
    }

    #[test]
    fn selective_live_pages_publish_monotonic_empty_physical_progress() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation_batch(
                (1..=9)
                    .map(|id| {
                        let parent = match id {
                            1 => Some(2),
                            9 => Some(1),
                            _ => None,
                        };
                        insert(id, parent)
                    })
                    .collect(),
            )
            .expect("selective live-page rows should insert");
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .filter(FilterExpr::eq("parent_id", 1_u64))
            .order_by(asc("id"))
            .limit(1);

        let first = session
            .execute_trusted_live_page(&query, None)
            .expect("first selective page should stop with physical progress");
        assert!(first.rows.is_empty());
        assert_eq!(first.work.entries_visited, 4);
        let first_cursor = first
            .continuation
            .expect("filtered physical progress must return a continuation");

        let second = session
            .execute_trusted_live_page(&query, Some(first_cursor.as_str()))
            .expect("second selective page should resume after the first physical frontier");
        assert!(second.rows.is_empty());
        assert_eq!(second.work.entries_visited, 4);
        let second_cursor = second
            .continuation
            .expect("second filtered frontier must remain resumable");
        assert_ne!(second_cursor, first_cursor);

        let third = session
            .execute_trusted_live_page(&query, Some(second_cursor.as_str()))
            .expect("final selective page should return the late match");
        assert_eq!(third.rows, vec![vec![OutputValue::Nat64(9)]]);
        assert_eq!(third.work.entries_visited, 1);
        assert_eq!(third.continuation, None);

        let descending = DynamicQuery::new(ENTITY_NAME)
            .select(["id"])
            .filter(FilterExpr::eq("parent_id", 2_u64))
            .order_by(desc("id"))
            .limit(1);
        let descending_first = session
            .execute_trusted_live_page(&descending, None)
            .expect("descending selective page should stop with physical progress");
        assert!(descending_first.rows.is_empty());
        let descending_first_cursor = descending_first
            .continuation
            .expect("descending filtered progress must return a continuation");
        let descending_second = session
            .execute_trusted_live_page(&descending, Some(descending_first_cursor.as_str()))
            .expect("descending progress should resume after its physical frontier");
        assert!(descending_second.rows.is_empty());
        let descending_second_cursor = descending_second
            .continuation
            .expect("descending second frontier must remain resumable");
        assert_ne!(descending_second_cursor, descending_first_cursor);
        let descending_third = session
            .execute_trusted_live_page(&descending, Some(descending_second_cursor.as_str()))
            .expect("descending final page should return the late match");
        assert_eq!(descending_third.rows, vec![vec![OutputValue::Nat64(1)]]);
        assert_eq!(descending_third.continuation, None);
    }

    #[test]
    fn accepted_relation_edges_drive_catalog_and_describe_introspection() {
        let session = initialize();
        let entities = session
            .show_entities()
            .expect("accepted entity catalog should resolve");
        let source = entities
            .iter()
            .find(|entity| entity.entity_name() == ENTITY_NAME)
            .expect("relation source should be listed");
        assert_eq!(source.relations(), 1);

        let description = session
            .try_describe_entity_by_name(ENTITY_NAME)
            .expect("accepted relation source should describe");
        let [relation] = description.relations() else {
            panic!("accepted relation edge should produce one relation row");
        };
        assert_eq!(relation.field(), "parent_id");
        assert_eq!(relation.target_path(), ENTITY_SOURCE);
        assert_eq!(relation.target_entity_name(), ENTITY_NAME);
        assert_eq!(relation.target_store_path(), STORE_PATH);
        assert_eq!(
            relation.cardinality(),
            crate::db::EntityRelationCardinality::Single,
        );
    }

    #[test]
    fn mixed_relation_validation_uses_the_complete_final_row_overlay() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation_batch(vec![insert(1, None), insert(2, Some(1))])
            .expect("the initial relation should commit");

        let blocked = session
            .execute_trusted_dynamic_mutation(&delete(1))
            .expect_err("an unaffected committed source must block target deletion");
        assert_relation_violation(&blocked);

        let deleted = session
            .execute_trusted_dynamic_mutation_batch(vec![delete(2), delete(1)])
            .expect("a source and its target should delete atomically");
        assert_eq!(
            deleted.rows,
            vec![expected_row(2, Some(1)), expected_row(1, None)],
        );

        session
            .execute_trusted_dynamic_mutation_batch(vec![insert(3, None), insert(4, Some(3))])
            .expect("the update-away fixture should commit");
        let updated_away = session
            .execute_trusted_dynamic_mutation_batch(vec![update_parent(4, None), delete(3)])
            .expect("an updated final source may release a deleted target");
        assert_eq!(
            updated_away.rows,
            vec![expected_row(4, None), expected_row(3, None)],
        );

        session
            .execute_trusted_dynamic_mutation_batch(vec![insert(5, None), insert(6, Some(5))])
            .expect("the retained-reference fixture should commit");
        let retained = session
            .execute_trusted_dynamic_mutation_batch(vec![update_parent(6, Some(5)), delete(5)])
            .expect_err("a final updated source must still block target deletion");
        assert_relation_violation(&retained);

        session
            .execute_trusted_dynamic_mutation(&insert(7, None))
            .expect("the inserted-reference fixture target should commit");
        let inserted_reference = session
            .execute_trusted_dynamic_mutation_batch(vec![insert(8, Some(7)), delete(7)])
            .expect_err("a final inserted source must not reference a deleted target");
        assert_relation_violation(&inserted_reference);

        let inserted_target = session
            .execute_trusted_dynamic_mutation_batch(vec![insert(10, Some(9)), insert(9, None)])
            .expect("an inserted relation should see its batch-final target");
        assert_eq!(
            inserted_target.rows,
            vec![expected_row(10, Some(9)), expected_row(9, None)],
        );

        session
            .execute_trusted_dynamic_mutation(&insert(11, None))
            .expect("the updated-reference fixture source should commit");
        let updated_target = session
            .execute_trusted_dynamic_mutation_batch(vec![
                update_parent(11, Some(12)),
                insert(12, None),
            ])
            .expect("an updated relation should see its batch-final target");
        assert_eq!(
            updated_target.rows,
            vec![expected_row(11, Some(12)), expected_row(12, None)],
        );
    }

    #[test]
    fn mixed_batch_rejects_cross_entity_missing_and_collision_then_honors_replace() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation(&insert(1, None))
            .expect("the primary mixed fixture row should commit");
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: OTHER_ENTITY_NAME.to_string(),
                patch: other_patch(Some(1), 10),
            })
            .expect("the secondary mixed fixture row should commit");

        let mixed_entity = session
            .execute_trusted_dynamic_mutation_batch(vec![
                update_code(1, 11),
                DynamicMutation::Update {
                    entity: OTHER_ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: other_patch(None, 11),
                },
            ])
            .expect_err("one atomic batch must not cross accepted entities");
        assert_eq!(mixed_entity.class(), ErrorClass::Conflict);
        assert_eq!(
            mixed_entity.diagnostic_facts(),
            vec![
                (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 1,),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ExpectedEntityTag,
                    ENTITY_TAG.value(),
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualEntityTag,
                    OTHER_ENTITY_TAG.value(),
                ),
            ],
        );

        let missing = session
            .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 12), delete(99)])
            .expect_err("a late missing delete must reject the earlier staged update");
        assert_eq!(missing.class(), ErrorClass::NotFound);

        session
            .execute_trusted_dynamic_mutation(&insert(2, None))
            .expect("the collision fixture should commit");
        let collision = session
            .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 13), insert(2, None)])
            .expect_err("an insert collision must reject the earlier staged update");
        assert_eq!(collision.class(), ErrorClass::Conflict);
        let failures_unchanged = session
            .execute_trusted_dynamic_mutation(&update_code(1, 1))
            .expect("failed batches must preserve the original unique value");
        assert_eq!(failures_unchanged.affected_rows, 0);

        let replaced = session
            .execute_trusted_dynamic_mutation_batch(vec![
                update_code(1, 14),
                DynamicMutation::Replace {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(99),
                    patch: patch(None, None, Some(99)),
                },
            ])
            .expect("ordinary caller-key replace should insert its absent final row");
        assert_eq!(
            replaced.rows,
            vec![
                expected_row_with_code(1, None, 14),
                expected_row_with_code(99, None, 99),
            ],
        );

        let unchanged = session
            .execute_trusted_dynamic_mutation(&update_code(1, 14))
            .expect("the successful mixed replace must publish its preceding update");
        assert_eq!(unchanged.affected_rows, 0);
        let other_unchanged = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: OTHER_ENTITY_NAME.to_string(),
                key: InputValue::Nat64(1),
                patch: other_patch(None, 10),
            })
            .expect("cross-entity rejection must preserve the secondary row");
        assert_eq!(other_unchanged.affected_rows, 0);
    }

    #[test]
    fn mixed_batch_unique_swap_and_delete_release_use_the_final_overlay() {
        let session = initialize();
        session
            .execute_trusted_dynamic_mutation_batch(vec![
                insert_with_code(1, None, 10),
                insert_with_code(2, None, 20),
            ])
            .expect("the unique-overlay fixture should commit");

        let swapped = session
            .execute_trusted_dynamic_mutation_batch(vec![update_code(1, 20), update_code(2, 10)])
            .expect("two final rows should atomically swap unique memberships");
        assert_eq!(
            swapped.rows,
            vec![
                expected_row_with_code(1, None, 20),
                expected_row_with_code(2, None, 10),
            ],
        );

        let released = session
            .execute_trusted_dynamic_mutation_batch(vec![delete(1), insert_with_code(3, None, 20)])
            .expect("a delete should release unique membership to a final inserted row");
        assert_eq!(
            released.rows,
            vec![
                expected_row_with_code(1, None, 20),
                expected_row_with_code(3, None, 20),
            ],
        );
    }
}

#[cfg(test)]
mod identity_pre_key_tests {
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    use super::DynamicTypedEntityBinding;
    use super::{
        AcceptedMutationIntentPatch, AcceptedRowLayoutRuntimeContract, AcceptedStructuralMutation,
        AcceptedStructuralMutationTarget, DbSession, DynamicMutation, DynamicStructuralPatch,
        DynamicTypedFieldBindingRequest, DynamicTypedFieldType, DynamicTypedMutation,
        DynamicWriteCell, FieldSlot, MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
        MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES, MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES,
        MutationProgressRecordOp, add_structural_mutation_staged_bytes,
        checked_pre_key_candidate_count, insert_key_exists_after_generation,
        validate_structural_mutation_result_bytes,
    };
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    use crate::db::data::DecodedDataStoreKey;
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    use crate::db::executor::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        with_query_execution_budget_for_tests,
    };
    use crate::db::mutation_job::{MutationJobRecord, MutationJobTransition};
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    use crate::db::{
        CompareProofAndAdvanceError, DynamicQuery, ExhaustiveReadError, RawDataStoreKey,
        ReadSetRevisionError, ResumableJobAdvance, ResumableJobAdvanceRequest,
        ResumableJobAdvanceStatus, ResumableJobError, ResumableJobId, ResumableJobIdempotencyKey,
        ResumableJobStatus, asc,
    };
    use crate::{
        db::{
            MutationJobAdvanceRequest, MutationJobId, MutationJobIdempotencyKey, MutationJobPhase,
            MutationJobStatus,
            commit::{database_incarnation_id, forget_recovered_domain_for_tests},
            data::DataStore,
            executor::{MutationCommitInterruption, interrupt_next_mutation_commit_for_tests},
            index::IndexStore,
            integrity::{
                InsertMutationJobResult, PhysicalUnitCheckpoint, QuickIntegrityStatus,
                RowInspectionLimits, execute_quick_integrity, execute_row_integrity_page,
                with_mutation_progress_store,
            },
            journal::JournalTailStore,
            registry::{
                StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
                StoreRuntimeStorageCapabilities,
            },
            schema::{
                AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldInsertGeneration,
                FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
                PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
                PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaFieldWritePolicy,
                SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaVersion,
                accepted_schema_candidate_with_field_bindings_for_tests,
            },
            write_context::MutationMode,
        },
        error::{ErrorClass, ErrorOrigin, InternalError},
        testing::test_memory,
        traits::{CanisterKind, Path},
        types::{EntityTag, Timestamp},
        value::{InputValue, OutputValue, Value},
    };
    use icydb_schema::{FieldSourceKey, ScalarType};
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    use std::cell::Cell;
    use std::{cell::RefCell, collections::BTreeMap, time::Instant};

    const STORE_PATH: &str = "session::write::identity_pre_key_tests::Store";
    const ENTITY_SOURCE: &str = "session::write::identity_pre_key_tests::Entity";
    const ID_SOURCE: &str = "session::write::identity_pre_key_tests::Entity::id";
    const PAYLOAD_SOURCE: &str = "session::write::identity_pre_key_tests::Entity::payload";
    const ENTITY_NAME: &str = "IdentityRow";
    const ENTITY_TAG: EntityTag = EntityTag::new(93);
    const JOURNALED_STORE_PATH: &str = "session::write::identity_pre_key_tests::JournaledStore";
    const UNRELATED_STORE_PATH: &str = "session::write::identity_pre_key_tests::UnrelatedStore";

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "session::write::identity_pre_key_tests::Canister";
    }

    impl CanisterKind for TestCanister {
        const COMMIT_MEMORY_ID: u8 = 45;
        const COMMIT_STABLE_KEY: &'static str = "icydb.identity_pre_key_tests.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 46;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.identity_pre_key_tests.integrity.progress.v1";
    }

    thread_local! {
        static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA_STORE: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static UNRELATED_DATA_STORE: RefCell<DataStore> =
            const { RefCell::new(DataStore::init_heap()) };
        static UNRELATED_INDEX_STORE: RefCell<IndexStore> =
            const { RefCell::new(IndexStore::init_heap()) };
        static UNRELATED_SCHEMA_STORE: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_store(
                STORE_PATH,
                &DATA_STORE,
                &INDEX_STORE,
                &SCHEMA_STORE,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("identity pre-key test store should register");
            registry.register_store(
                UNRELATED_STORE_PATH,
                &UNRELATED_DATA_STORE,
                &UNRELATED_INDEX_STORE,
                &UNRELATED_SCHEMA_STORE,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("unrelated identity test store should register");
            registry
        };
        static JOURNALED_DATA_STORE: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(186)));
        static JOURNALED_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(187)));
        static JOURNALED_SCHEMA_STORE: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(188)));
        static JOURNALED_TAIL_STORE: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(189)));
        static JOURNALED_STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                JOURNALED_STORE_PATH,
                &JOURNALED_DATA_STORE,
                &JOURNALED_INDEX_STORE,
                &JOURNALED_SCHEMA_STORE,
                &JOURNALED_TAIL_STORE,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(186, "icydb.test.identity-range.data.v1"),
                    StoreAllocationIdentity::new(187, "icydb.test.identity-range.index.v1"),
                    StoreAllocationIdentity::new(188, "icydb.test.identity-range.schema.v1"),
                    StoreAllocationIdentity::new(189, "icydb.test.identity-range.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("identity range journaled store should register");
            registry
        };
    }

    struct JournaledTestCanister;

    impl Path for JournaledTestCanister {
        const PATH: &'static str = "session::write::identity_pre_key_tests::JournaledCanister";
    }

    impl CanisterKind for JournaledTestCanister {
        const COMMIT_MEMORY_ID: u8 = 190;
        const COMMIT_STABLE_KEY: &'static str = "icydb.identity_range_tests.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 191;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.identity_range_tests.integrity.progress.v1";
    }

    fn source_key(source: &str) -> FieldSourceKey {
        FieldSourceKey::try_new(source).expect("identity test field source should admit")
    }

    fn identity_snapshot(store_path: &str) -> PersistedSchemaSnapshot {
        let fields = vec![
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(
                    Some(FieldInsertGeneration::Identity),
                    None,
                ),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "payload".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ];
        PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            ENTITY_SOURCE.to_string(),
            ENTITY_NAME.to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
            vec![PersistedIndexSnapshot::new(
                SchemaIndexId::new(1).expect("identity test index ID should admit"),
                1,
                "by_payload".to_string(),
                store_path.to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["payload".to_string()],
                    AcceptedFieldKind::Nat64,
                    false,
                )]),
                None,
            )],
        )
    }

    fn initialize() -> DbSession<TestCanister> {
        DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
        INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
        SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
        UNRELATED_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
        UNRELATED_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
        UNRELATED_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
        let session = DbSession::<TestCanister>::new(
            &STORE_REGISTRY,
            &crate::db::RequestExecutionRoot::__new_runtime_root(),
        );
        session
            .db
            .ensure_recovered_state()
            .expect("identity pre-key test database should initialize");
        let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
            STORE_PATH,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(ENTITY_TAG, identity_snapshot(STORE_PATH))]),
            BTreeMap::from([
                ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
                ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
            ]),
        );
        let store = session
            .db
            .store_handle(STORE_PATH)
            .expect("identity pre-key test store should resolve");
        crate::db::commit::publish_accepted_schema_candidate(
            STORE_PATH,
            store,
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect("identity candidate should publish with explicit zero state");
        session
    }

    fn initialize_journaled_with_root() -> (
        DbSession<JournaledTestCanister>,
        crate::db::RequestExecutionRoot,
    ) {
        let root = crate::db::RequestExecutionRoot::__new_runtime_root();
        let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
        session
            .db
            .ensure_recovered_state()
            .expect("journaled identity database should initialize");
        let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
            JOURNALED_STORE_PATH,
            AcceptedSchemaRevision::INITIAL,
            BTreeMap::from([(ENTITY_TAG, identity_snapshot(JOURNALED_STORE_PATH))]),
            BTreeMap::from([
                ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
                ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
            ]),
        );
        let store = session
            .db
            .store_handle(JOURNALED_STORE_PATH)
            .expect("journaled identity store should resolve");
        crate::db::commit::publish_accepted_schema_candidate(
            JOURNALED_STORE_PATH,
            store,
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect("journaled identity candidate should publish");
        (session, root)
    }

    fn initialize_journaled() -> DbSession<JournaledTestCanister> {
        initialize_journaled_with_root().0
    }

    fn payload_patch(value: u64) -> AcceptedMutationIntentPatch {
        AcceptedMutationIntentPatch::new()
            .set_authored(FieldSlot::from_validated_index(1), InputValue::Nat64(value))
    }

    fn dynamic_payload_patch(value: u64) -> DynamicStructuralPatch {
        DynamicStructuralPatch::new(vec![(
            "payload".to_string(),
            DynamicWriteCell::Value(InputValue::Nat64(value)),
        )])
    }

    fn expected_dynamic_row(id: u64, payload: u64) -> Vec<OutputValue> {
        vec![OutputValue::Nat64(id), OutputValue::Nat64(payload)]
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn exact_key_binding<C: CanisterKind>(session: &DbSession<C>) -> DynamicTypedEntityBinding {
        session
            .issue_typed_entity_binding(
                ENTITY_SOURCE,
                &[
                    DynamicTypedFieldBindingRequest::new(
                        ID_SOURCE.to_string(),
                        DynamicTypedFieldType::Scalar(ScalarType::Nat64),
                        false,
                    ),
                    DynamicTypedFieldBindingRequest::new(
                        PAYLOAD_SOURCE.to_string(),
                        DynamicTypedFieldType::Scalar(ScalarType::Nat64),
                        false,
                    ),
                ],
            )
            .expect("exact-key test binding should issue")
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn insert_exact_key_fixture<C: CanisterKind>(session: &DbSession<C>, payload: u64) -> u64 {
        let output = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(payload),
            })
            .expect("exact-key fixture insert should commit");
        match output.rows.as_slice() {
            [row] => match row.as_slice() {
                [OutputValue::Nat64(id), OutputValue::Nat64(actual_payload)]
                    if *actual_payload == payload =>
                {
                    *id
                }
                _ => panic!("exact-key fixture should return its identity and payload"),
            },
            _ => panic!("exact-key fixture insert should return one row"),
        }
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn identity_row_stored_bytes<C: CanisterKind>(
        session: &DbSession<C>,
        store_path: &'static str,
        key: u64,
    ) -> u64 {
        let data_key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(key))
            .expect("identity row key should encode");
        let raw_key = data_key.to_raw().expect("identity raw key should encode");
        let store = session
            .db
            .recovered_store(store_path)
            .expect("identity store should resolve");
        store.with_data(|data_store| {
            u64::try_from(
                data_store
                    .get(&raw_key)
                    .expect("inserted identity row should exist")
                    .len(),
            )
            .expect("bounded row length should fit u64")
        })
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn with_stored_bytes_limit<T>(
        limit: u64,
        shape_fingerprint_prefix: u64,
        operation: impl FnOnce() -> Result<T, crate::db::query::intent::QueryError>,
    ) -> Result<T, crate::db::query::intent::QueryError> {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::StoredBytesRead,
            limit,
        );
        let context = HardExecutionContext::new(
            icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
            icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            shape_fingerprint_prefix,
        );

        with_query_execution_budget_for_tests(budget, context, operation)
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn assert_exact_key_batch<C: CanisterKind>(session: &DbSession<C>) {
        let first = insert_exact_key_fixture(session, 41);
        let second = insert_exact_key_fixture(session, 42);
        let missing = u64::MAX;
        let binding = exact_key_binding(session);
        let gets_before = DataStore::current_get_call_count();
        let result = session
            .execute_public_exact_key_batch_for_typed_binding(
                &binding,
                &[second, missing, first, second],
            )
            .expect("exact-key batch should execute")
            .expect("exact-key binding should remain current");

        assert_eq!(result.positions, vec![0, 1, 2, 0]);
        assert_eq!(
            result.distinct_rows,
            vec![
                Some(expected_dynamic_row(second, 42)),
                None,
                Some(expected_dynamic_row(first, 41)),
            ],
        );
        assert_eq!(
            DataStore::current_get_call_count().saturating_sub(gets_before),
            3,
            "four input positions with one duplicate must perform three physical reads",
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn exact_key_batches_preserve_semantics_across_heap_and_journaled_stores() {
        assert_exact_key_batch(&initialize());
        assert_exact_key_batch(&initialize_journaled());
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn assert_primary_range_materialization_fetches_once<C: CanisterKind>(
        session: &DbSession<C>,
        store_path: &'static str,
    ) {
        let key = insert_exact_key_fixture(session, 41);
        let stored_bytes = identity_row_stored_bytes(session, store_path, key);

        let scalar = DynamicQuery::new(ENTITY_NAME)
            .select(["id", "payload"])
            .order_by(asc("id"))
            .limit(1);
        let gets_before = DataStore::current_get_call_count();
        let scalar_page = with_stored_bytes_limit(stored_bytes, 0x7072_696d_6172_792d, || {
            session.execute_trusted_live_page(&scalar, None)
        })
        .expect("one scalar primary-range row should fit one payload-read allowance");
        assert_eq!(scalar_page.row_count, 1);
        assert_eq!(
            DataStore::current_get_call_count().saturating_sub(gets_before),
            1,
            "scalar primary traversal should fetch its emitted row exactly once",
        );

        let grouped = DynamicQuery::new(ENTITY_NAME)
            .group_by("payload")
            .aggregate(crate::db::count())
            .grouped_limits(10, 16 * 1_024)
            .limit(1);
        let gets_before = DataStore::current_get_call_count();
        let grouped_page = with_stored_bytes_limit(stored_bytes, 0x6772_6f75_7065_642d, || {
            session.execute_trusted_dynamic_grouped_query(&grouped)
        })
        .expect("one grouped primary-range row should fit one payload-read allowance");
        assert_eq!(grouped_page.row_count, 1);
        assert_eq!(
            DataStore::current_get_call_count().saturating_sub(gets_before),
            1,
            "grouped primary traversal should fetch its source row exactly once",
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn row_materialization_fetches_each_required_payload_at_most_once() {
        assert_primary_range_materialization_fetches_once(&initialize(), STORE_PATH);
        assert_primary_range_materialization_fetches_once(
            &initialize_journaled(),
            JOURNALED_STORE_PATH,
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn ordered_grouped_pages_close_a_group_spanning_physical_refills_before_resume() {
        let session = initialize();
        let mut patches = Vec::new();
        for _ in 0..70 {
            patches.push(dynamic_payload_patch(10));
        }
        for _ in 0..3 {
            patches.push(dynamic_payload_patch(20));
        }
        patches.push(dynamic_payload_patch(30));
        let inserted = session
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, patches)
            .expect("ordered grouped continuation rows should insert");
        assert_eq!(inserted.rows.len(), 74);

        let query = DynamicQuery::new(ENTITY_NAME)
            .group_by("payload")
            .aggregate(crate::db::count())
            .aggregate(crate::db::sum("id"))
            .order_by(asc("payload"))
            .grouped_limits(4, 16 * 1_024)
            .limit(1);
        let expected = [
            (10_u64, 70_u64, crate::types::Decimal::new(2_485, 0)),
            (20, 3, crate::types::Decimal::new(216, 0)),
            (30, 1, crate::types::Decimal::new(74, 0)),
        ];
        let mut continuation: Option<String> = None;
        let mut seen_cursors = std::collections::BTreeSet::new();

        for (page_index, (group_key, row_count, id_sum)) in expected.into_iter().enumerate() {
            let request = continuation.as_ref().map_or_else(
                || query.clone(),
                |cursor| query.clone().cursor(cursor.clone()),
            );
            let entries_before = IndexStore::current_entry_read_count();
            let rows_before = DataStore::current_get_call_count();
            let page = session
                .execute_trusted_dynamic_grouped_query(&request)
                .unwrap_or_else(|error| {
                    panic!("ordered grouped page {page_index} should execute: {error:?}")
                });
            let entries_read =
                IndexStore::current_entry_read_count().saturating_sub(entries_before);
            let rows_read = DataStore::current_get_call_count().saturating_sub(rows_before);

            assert_eq!(page.row_count, 1);
            let [row] = page.rows.as_slice() else {
                panic!("ordered grouped page must contain exactly one closed group")
            };
            assert_eq!(row.group_key(), &[OutputValue::Nat64(group_key)]);
            assert_eq!(
                row.aggregate_values(),
                &[OutputValue::Nat64(row_count), OutputValue::Decimal(id_sum),],
            );
            if page_index == 0 {
                assert!(
                    entries_read.saturating_add(rows_read) >= 70,
                    "the first closed group must span the maintained 64-entry physical refill",
                );
            }

            continuation = page.next_cursor;
            if page_index + 1 < expected.len() {
                let cursor = continuation
                    .as_ref()
                    .expect("another closed group should retain continuation");
                assert!(
                    seen_cursors.insert(cursor.clone()),
                    "ordered grouped continuation must advance monotonically",
                );
            } else {
                assert_eq!(continuation, None);
            }
        }
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn exhaustive_pages_require_and_recompare_the_complete_source_proof() {
        let session = initialize();
        let first = insert_exact_key_fixture(&session, 41);
        let second = insert_exact_key_fixture(&session, 42);
        let third = insert_exact_key_fixture(&session, 43);
        let query = DynamicQuery::new(ENTITY_NAME)
            .select(["id", "payload"])
            .order_by(asc("id"));

        let page = session
            .execute_trusted_exhaustive_page(&query, None, None)
            .expect("initial exhaustive page should capture its source proof");
        assert_eq!(
            page.rows,
            vec![
                expected_dynamic_row(first, 41),
                expected_dynamic_row(second, 42),
            ],
        );
        let continuation = page
            .continuation
            .as_deref()
            .expect("unreturned row should retain exhaustive continuation");
        assert!(matches!(
            session.execute_trusted_exhaustive_page(&query, Some(continuation), None),
            Err(ExhaustiveReadError::Revision(
                ReadSetRevisionError::ResumeProofRequired
            )),
        ));
        let resumed = session
            .execute_trusted_exhaustive_page(&query, Some(continuation), Some(&page.proof))
            .expect("unchanged proof should resume exhaustive traversal");
        assert_eq!(resumed.rows, vec![expected_dynamic_row(third, 43)]);
        assert_eq!(resumed.continuation, None);

        let stale_page = session
            .execute_trusted_exhaustive_page(&query, None, None)
            .expect("fresh exhaustive page should capture current revision");
        let stale_continuation = stale_page
            .continuation
            .as_deref()
            .expect("fresh three-row traversal should retain continuation");
        let _ = insert_exact_key_fixture(&session, 44);
        assert!(matches!(
            session.execute_trusted_exhaustive_page(
                &query,
                Some(stale_continuation),
                Some(&stale_page.proof),
            ),
            Err(ExhaustiveReadError::Revision(
                ReadSetRevisionError::StoreDataChanged { .. }
            )),
        ));
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn heap_sources_cannot_back_durable_resumable_jobs() {
        let session = initialize();
        let proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("heap source proof should capture for one-call exhaustive reads");
        let job_id = ResumableJobId::try_from_bytes([70; 32])
            .expect("nonzero heap test job identity should admit");

        assert!(matches!(
            session.start_resumable_job(job_id, proof, Vec::new()),
            Err(ResumableJobError::SourceProof(
                ReadSetRevisionError::DurableStoreRequired { .. }
            )),
        ));
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn proof_and_progress_controls_charge_one_shared_request_scope() {
        let (session, root) = initialize_journaled_with_root();
        let resource = icydb_diagnostic_code::DiagnosticExecutionBudgetResource::QueryExecutions;
        let before = root.observed(resource);
        let proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("proof capture should use the retained request scope");
        let job_id = ResumableJobId::try_from_bytes([75; 32])
            .expect("nonzero accounting job identity should admit");
        session
            .start_resumable_job(job_id, proof, Vec::new())
            .expect("job start should use the same retained request scope");
        let _ = session
            .resumable_job_state(job_id)
            .expect("job load should use the same retained request scope");

        assert_eq!(root.observed(resource).saturating_sub(before), 3);
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn source_proofs_ignore_unrelated_stores_but_bind_access_state_changes() {
        let session = initialize();
        let proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("source proof should cover only the entity's physical store");
        let shared_store_proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME, ENTITY_NAME])
            .expect("entities sharing one physical source should deduplicate");
        assert_eq!(shared_store_proof, proof);
        assert_eq!(shared_store_proof.stores().len(), 1);
        let unrelated = session
            .db
            .store_handle(UNRELATED_STORE_PATH)
            .expect("unrelated registered store should resolve");
        unrelated.with_data_mut(|store| {
            let _ = store.remove(&RawDataStoreKey::from_persisted_bytes(vec![1]));
        });
        session
            .verify_read_set_revision_proof(&proof)
            .expect("a nonparticipating store mutation must not invalidate the proof");

        let source = session
            .db
            .store_handle(STORE_PATH)
            .expect("participating source store should resolve");
        source
            .mark_index_building()
            .expect("source access-state transition should advance its revision");
        assert!(matches!(
            session.verify_read_set_revision_proof(&proof),
            Err(ExhaustiveReadError::Revision(
                ReadSetRevisionError::StoreAccessChanged { .. }
            )),
        ));
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[expect(
        clippy::too_many_lines,
        reason = "one lifecycle test proves successful replay plus pre-page and post-page source invalidation without sharing progress state across tests"
    )]
    #[test]
    fn journaled_job_advance_is_idempotent_and_revision_checked_on_both_sides() {
        let session = initialize_journaled();
        let proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("journaled source proof should capture");
        let job_id =
            ResumableJobId::try_from_bytes([71; 32]).expect("nonzero job identity should admit");
        session
            .start_resumable_job(job_id, proof, vec![0])
            .expect("journaled job should start outside its protected source revision");
        let request = ResumableJobAdvanceRequest::new(
            job_id,
            0,
            ResumableJobIdempotencyKey::new("page-0")
                .expect("bounded idempotency key should admit"),
        );
        let calls = Cell::new(0_u8);
        let receipt = session
            .compare_proof_and_advance(&request, |state| {
                calls.set(calls.get() + 1);
                assert_eq!(state.application_state, vec![0]);
                Ok::<_, ()>(
                    ResumableJobAdvance::new(Some("cursor-1".to_string()), vec![1], vec![9])
                        .expect("bounded application advance should admit"),
                )
            })
            .expect("unchanged source should advance exactly once");
        assert_eq!(calls.get(), 1);
        assert_eq!(receipt.status, ResumableJobAdvanceStatus::Advanced);
        assert_eq!(receipt.committed_sequence, 1);

        let replay = session
            .compare_proof_and_advance::<()>(&request, |_| {
                panic!("lost-response replay must not execute application work")
            })
            .expect("same request identity should return its persisted receipt");
        assert_eq!(replay, receipt);
        let retained = session
            .resumable_job_state(job_id)
            .expect("advanced state should remain durable");
        assert_eq!(retained.sequence, 1);
        assert_eq!(retained.application_state, vec![1]);

        let _ = insert_exact_key_fixture(&session, 51);
        let pre_change_request = ResumableJobAdvanceRequest::new(
            job_id,
            1,
            ResumableJobIdempotencyKey::new("page-1")
                .expect("bounded idempotency key should admit"),
        );
        let pre_change_calls = Cell::new(0_u8);
        let invalidated = session
            .compare_proof_and_advance::<()>(&pre_change_request, |_| {
                pre_change_calls.set(pre_change_calls.get() + 1);
                unreachable!("pre-page proof failure must reject before application work")
            })
            .expect("source drift should persist one replayable invalidation receipt");
        assert_eq!(pre_change_calls.get(), 0);
        assert_eq!(invalidated.status, ResumableJobAdvanceStatus::Invalidated);
        let invalidated_state = session
            .resumable_job_state(job_id)
            .expect("invalidated job should remain inspectable");
        assert_eq!(invalidated_state.status, ResumableJobStatus::Invalidated);
        assert_eq!(invalidated_state.continuation, None);
        assert_eq!(invalidated_state.application_state, vec![1]);
        assert_eq!(
            session
                .compare_proof_and_advance::<()>(&pre_change_request, |_| {
                    panic!("invalidation replay must not execute application work")
                })
                .expect("lost invalidation reply should replay exactly"),
            invalidated,
        );

        let post_proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("post-change journaled proof should capture");
        let post_job_id = ResumableJobId::try_from_bytes([72; 32])
            .expect("nonzero post-change job identity should admit");
        session
            .start_resumable_job(post_job_id, post_proof, vec![7])
            .expect("post-change journaled job should start");
        let post_request = ResumableJobAdvanceRequest::new(
            post_job_id,
            0,
            ResumableJobIdempotencyKey::new("post-page-0")
                .expect("bounded idempotency key should admit"),
        );
        let post_receipt = session
            .compare_proof_and_advance::<()>(&post_request, |_| {
                let _ = insert_exact_key_fixture(&session, 52);
                Ok(ResumableJobAdvance::new(None, vec![8], vec![10])
                    .expect("bounded post-change candidate should admit"))
            })
            .expect("post-page drift should discard the candidate and persist invalidation");
        assert_eq!(post_receipt.status, ResumableJobAdvanceStatus::Invalidated);
        let post_state = session
            .resumable_job_state(post_job_id)
            .expect("post-page invalidation should remain inspectable");
        assert_eq!(post_state.status, ResumableJobStatus::Invalidated);
        assert_eq!(post_state.application_state, vec![7]);
        session
            .acknowledge_resumable_job(post_job_id, post_state.sequence)
            .expect("terminal job acknowledgement should remove retained progress");
        session
            .acknowledge_resumable_job(post_job_id, post_state.sequence)
            .expect("lost acknowledgement reply should be safely replayable");
        assert_eq!(
            session.resumable_job_state(post_job_id),
            Err(ResumableJobError::NotFound),
        );

        let completed_job_id = ResumableJobId::try_from_bytes([74; 32])
            .expect("nonzero completed job identity should admit");
        let completed_proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("completed-job source proof should capture");
        session
            .start_resumable_job(completed_job_id, completed_proof, Vec::new())
            .expect("completed-job fixture should start");
        let completed_request = ResumableJobAdvanceRequest::new(
            completed_job_id,
            0,
            ResumableJobIdempotencyKey::new("complete")
                .expect("bounded completion key should admit"),
        );
        let completed_receipt = session
            .compare_proof_and_advance::<()>(&completed_request, |_| {
                Ok(ResumableJobAdvance::new(None, vec![99], vec![100])
                    .expect("bounded terminal advance should admit"))
            })
            .expect("null continuation should commit terminal completion");
        let completed_state = session
            .resumable_job_state(completed_job_id)
            .expect("completed state should remain replayable before acknowledgement");
        assert_eq!(completed_state.status, ResumableJobStatus::Completed);
        assert_eq!(
            session
                .compare_proof_and_advance::<()>(&completed_request, |_| {
                    panic!("completed request replay must not execute application work")
                })
                .expect("completed request should replay until acknowledgement"),
            completed_receipt,
        );
        let after_completion = ResumableJobAdvanceRequest::new(
            completed_job_id,
            1,
            ResumableJobIdempotencyKey::new("after-complete")
                .expect("bounded post-completion key should admit"),
        );
        assert!(matches!(
            session.compare_proof_and_advance::<()>(&after_completion, |_| {
                panic!("completed jobs cannot execute another page")
            }),
            Err(CompareProofAndAdvanceError::Protocol(
                ResumableJobError::Completed
            )),
        ));
        session
            .acknowledge_resumable_job(completed_job_id, completed_state.sequence)
            .expect("completed job should acknowledge and free capacity");
        session
            .acknowledge_resumable_job(completed_job_id, completed_state.sequence)
            .expect("completion acknowledgement should be idempotent");

        let stale_job_id = ResumableJobId::try_from_bytes([73; 32])
            .expect("nonzero stale-sequence job identity should admit");
        let stale_proof = session
            .capture_read_set_revision_proof(&[ENTITY_NAME])
            .expect("stale-sequence source proof should capture");
        session
            .start_resumable_job(stale_job_id, stale_proof, Vec::new())
            .expect("stale-sequence job should start");
        let stale_request = ResumableJobAdvanceRequest::new(
            stale_job_id,
            4,
            ResumableJobIdempotencyKey::new("stale").expect("bounded idempotency key should admit"),
        );
        assert!(matches!(
            session.compare_proof_and_advance::<()>(&stale_request, |_| {
                panic!("stale sequence must reject before application work")
            }),
            Err(CompareProofAndAdvanceError::Protocol(
                ResumableJobError::StaleSequence {
                    expected: 4,
                    actual: 0,
                }
            )),
        ));
        assert_eq!(
            session.acknowledge_resumable_job(stale_job_id, 0),
            Err(ResumableJobError::NotTerminal),
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn exact_key_batch_uses_typed_hard_execution_budget() {
        let session = initialize();
        let binding = exact_key_binding(&session);
        let budget =
            HardExecutionBudget::uniform_for_tests(0, HardExecutionFailureHeadroom::new(500, 256));
        let error = session
            .execute_exact_key_batch_with_hard_budget_for_tests(&binding, &[u64::MAX], &budget)
            .expect_err("zero query budget should reject the exact-key route");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        let facts = error.diagnostic_facts();
        assert_eq!(
            &facts[..5],
            &[
                (
                    icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                    icydb_diagnostic_code::DiagnosticExecutionBudgetResource::QueryExecutions.raw(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::Limit, 0),
                (icydb_diagnostic_code::DiagnosticFactTag::Actual, 1),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ExecutionBudgetScope,
                    icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution.raw(),
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ExecutionLane,
                    icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead.raw(),
                ),
            ],
        );
        assert_eq!(
            facts[5].0,
            icydb_diagnostic_code::DiagnosticFactTag::QueryShapeFingerprintPrefix,
        );
        assert_ne!(facts[5].1, 0);
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn assert_planned_query_exhausts(
        session: &DbSession<TestCanister>,
        query: &crate::db::DynamicQuery,
        resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
    ) {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(resource, 0);
        let context = HardExecutionContext::new(
            icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
            icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            0x7068_7973_6963_616c,
        );
        let error = with_query_execution_budget_for_tests(budget, context, || {
            session.execute_trusted_live_page(query, None)
        })
        .expect_err("the injected zero resource allowance should reject planned execution");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts()[0],
            (
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                resource.raw(),
            ),
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn assert_grouped_query_exhausts(
        session: &DbSession<TestCanister>,
        query: &crate::db::DynamicQuery,
        resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
    ) {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(resource, 0);
        let context = HardExecutionContext::new(
            icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
            icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            0x6772_6f75_7065_642d,
        );
        let error = with_query_execution_budget_for_tests(budget, context, || {
            session.execute_trusted_dynamic_grouped_query(query)
        })
        .expect_err("the injected zero resource allowance should reject grouped execution");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts()[0],
            (
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                resource.raw(),
            ),
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    fn assert_sql_query_exhausts(
        session: &DbSession<TestCanister>,
        sql: &str,
        resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
    ) {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(resource, 0);
        let context = HardExecutionContext::new(
            icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
            icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
            0x7371_6c2d_736f_7274,
        );
        let error = with_query_execution_budget_for_tests(budget, context, || {
            session.execute_trusted_sql_query(sql)
        })
        .expect_err("the injected zero resource allowance should reject SQL execution");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts()[0],
            (
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                resource.raw(),
            ),
        );
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[test]
    fn planned_read_routes_share_physical_resource_accounting() {
        let session = initialize();
        let first = insert_exact_key_fixture(&session, 41);
        insert_exact_key_fixture(&session, 42);

        let fallback = crate::db::DynamicQuery::new(ENTITY_NAME)
            .filter(crate::db::FieldRef::new("id").eq(first))
            .select(["id", "payload"])
            .order_by(crate::db::asc("id"))
            .limit(1);
        assert_eq!(
            session
                .execute_trusted_live_page(&fallback, None)
                .expect("bounded fallback execution should preserve its result")
                .row_count,
            1,
        );
        assert_planned_query_exhausts(
            &session,
            &fallback,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
        );

        let covering = crate::db::DynamicQuery::new(ENTITY_NAME)
            .filter(crate::db::FieldRef::new("payload").eq(41_u64))
            .select(["payload"])
            .order_by(crate::db::asc("payload"))
            .limit(1);
        assert_eq!(
            session
                .execute_trusted_live_page(&covering, None)
                .expect("bounded covering execution should preserve its result")
                .row_count,
            1,
        );
        assert_planned_query_exhausts(
            &session,
            &covering,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        );

        let residual = crate::db::DynamicQuery::new(ENTITY_NAME)
            .filter(crate::db::FieldRef::new("payload").eq_field("id"))
            .select(["id"])
            .order_by(crate::db::asc("id"))
            .limit(1);
        assert_eq!(
            session
                .execute_trusted_live_page(&residual, None)
                .expect("bounded residual execution should preserve its result")
                .row_count,
            0,
        );
        assert_planned_query_exhausts(
            &session,
            &residual,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        );

        assert_planned_query_exhausts(
            &session,
            &fallback,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::ResultBytes,
        );

        let grouped = crate::db::DynamicQuery::new(ENTITY_NAME)
            .group_by("payload")
            .aggregate(crate::db::count())
            .order_by(crate::db::asc("payload"))
            .grouped_limits(10, 16 * 1_024)
            .limit(1);
        let grouped_result = session
            .execute_trusted_dynamic_grouped_query(&grouped)
            .expect("bounded grouped execution should preserve its result");
        assert_eq!(grouped_result.row_count, 1);
        assert!(grouped_result.next_cursor.is_some());
        assert_grouped_query_exhausts(
            &session,
            &grouped,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::GroupDistinctEntries,
        );
        assert_grouped_query_exhausts(
            &session,
            &grouped,
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::CursorSteps,
        );

        assert_sql_query_exhausts(
            &session,
            "SELECT payload, COUNT(*) AS row_count FROM IdentityRow \
             GROUP BY payload ORDER BY row_count DESC, payload ASC LIMIT 1",
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::SortEntries,
        );
    }

    fn assert_dynamic_payload<C: CanisterKind>(
        session: &DbSession<C>,
        key: u64,
        expected_payload: u64,
    ) {
        let unchanged = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(key),
                patch: dynamic_payload_patch(expected_payload),
            })
            .expect("the expected row should remain readable through a no-op update");
        assert_eq!(unchanged.affected_rows, 0);
        assert_eq!(
            unchanged.rows,
            vec![expected_dynamic_row(key, expected_payload)],
        );
    }

    fn batch(values: &[u64]) -> Vec<AcceptedStructuralMutation> {
        values
            .iter()
            .map(|value| {
                AcceptedStructuralMutation::save(
                    MutationMode::Insert,
                    AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                    payload_patch(*value),
                )
            })
            .collect()
    }

    fn atomic_progress_fixture(
        identity_byte: u8,
    ) -> (
        MutationJobRecord,
        MutationJobRecord,
        MutationProgressRecordOp,
    ) {
        let job_id = MutationJobId::try_from_bytes([identity_byte; 32])
            .expect("nonzero atomic progress job id should admit");
        let before = MutationJobRecord::new(job_id, vec![1, identity_byte], vec![2])
            .expect("atomic progress predecessor should admit");
        let request = MutationJobAdvanceRequest::new(
            job_id,
            0,
            MutationJobIdempotencyKey::new(format!("atomic-{identity_byte}"))
                .expect("atomic progress replay key should admit"),
        );
        let (after, _) = before
            .apply_transition(
                &request,
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Forward,
                    vec![3],
                    1,
                    1,
                    0,
                ),
            )
            .expect("atomic progress successor should admit");
        let operation = MutationProgressRecordOp::replace(&before, &after)
            .expect("atomic progress replacement should admit");
        (before, after, operation)
    }

    fn assert_identity_boundary(error: &InternalError) {
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
    }

    #[test]
    fn generated_candidate_collision_is_identity_corruption_before_generic_uniqueness() {
        let generated = insert_key_exists_after_generation(true);
        assert_eq!(generated.class(), ErrorClass::Corruption);
        assert_eq!(generated.origin(), ErrorOrigin::Identity);

        let ordinary = insert_key_exists_after_generation(false);
        assert_ne!(ordinary.origin(), ErrorOrigin::Identity);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn pre_key_candidate_count_rejects_values_beyond_the_persisted_u32_bound() {
        let error = checked_pre_key_candidate_count(
            usize::try_from(u64::from(u32::MAX) + 1).expect("64-bit usize should hold u32 + 1"),
        )
        .expect_err("candidate counts beyond u32 must reject");
        assert_identity_boundary(&error);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one holding lifecycle proves split, merge, transfer, late-failure neutrality, result order, and Identity state"
    )]
    fn mixed_structural_batch_preserves_holding_conservation_and_failure_atomicity() {
        let session = initialize();
        let seeded = session
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
            .expect("seed rows should commit");
        assert_eq!(seeded.affected_rows, 1);

        let split = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(60),
                },
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: dynamic_payload_patch(40),
                },
            ])
            .expect("one holding should split atomically");
        assert_eq!(split.affected_rows, 2);
        assert_eq!(
            split.rows,
            vec![expected_dynamic_row(1, 60), expected_dynamic_row(2, 40),],
            "split after-images must retain input order and exact quantity",
        );

        let rejected_split = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(50),
                },
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: DynamicStructuralPatch::new(Vec::new()),
                },
            ])
            .expect_err("an invalid split output must reject the staged source update");
        assert_eq!(rejected_split.class(), ErrorClass::Unsupported);
        assert_eq!(rejected_split.origin(), ErrorOrigin::Executor);
        assert_eq!(
            rejected_split.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                    ENTITY_TAG.value(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::FieldId, 2),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                    icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 1,),
            ],
        );
        assert_dynamic_payload(&session, 1, 60);
        assert_dynamic_payload(&session, 2, 40);

        let transfer = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(70),
                },
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(2),
                    patch: dynamic_payload_patch(30),
                },
            ])
            .expect("distinct transfer patches should share one atomic batch");
        assert_eq!(
            transfer.rows,
            vec![expected_dynamic_row(1, 70), expected_dynamic_row(2, 30),],
            "the transfer must preserve the exact total quantity",
        );

        let merge = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(2),
                },
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(100),
                },
            ])
            .expect("two holdings should merge atomically");
        assert_eq!(
            merge.rows,
            vec![expected_dynamic_row(2, 30), expected_dynamic_row(1, 100),],
            "delete before-images and update after-images must retain input order",
        );

        let resplit = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(60),
                },
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: dynamic_payload_patch(40),
                },
            ])
            .expect("the merged holding should split again");
        assert_eq!(
            resplit.rows,
            vec![expected_dynamic_row(1, 60), expected_dynamic_row(3, 40),],
        );

        let rejected_merge = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(3),
                },
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(99),
                    patch: dynamic_payload_patch(100),
                },
            ])
            .expect_err("a late missing merge target must preserve the earlier staged delete");
        assert_eq!(rejected_merge.class(), ErrorClass::NotFound);
        assert_dynamic_payload(&session, 1, 60);
        assert_dynamic_payload(&session, 3, 40);

        SCHEMA_STORE.with(|store| {
            let cursor = store
                .borrow()
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    ENTITY_TAG,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("mixed Identity state should remain readable");
            assert_eq!(cursor.expected_high_water(), 3);
            assert!(!cursor.has_allocations());
        });
    }

    #[test]
    fn mixed_structural_batch_rejects_duplicate_holding_targets_without_mutation() {
        let session = initialize();
        session
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
            .expect("the holding fixture should initialize");

        let duplicate = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(60),
                },
                DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                },
            ])
            .expect_err("duplicate targets across operation kinds must reject");
        assert!(matches!(
            duplicate.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchDuplicateKey,
            }),
        ));
        assert_eq!(
            duplicate.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                    ENTITY_TAG.value(),
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::FirstBatchPosition,
                    0,
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::DuplicateBatchPosition,
                    1,
                ),
            ],
        );
        assert_dynamic_payload(&session, 1, 100);
    }

    #[test]
    fn mixed_structural_batch_rejects_empty_and_over_bound_before_resolution() {
        let session = initialize();
        let empty = session
            .execute_trusted_dynamic_mutation_batch(Vec::new())
            .expect_err("an empty public batch must reject");
        assert!(matches!(
            empty.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchEmpty,
            }),
        ));
        assert_eq!(
            empty.diagnostic_facts(),
            vec![(icydb_diagnostic_code::DiagnosticFactTag::ActualCount, 0,)],
        );

        let requests = (0..=MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS)
            .map(|_| DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(1),
            })
            .collect();
        let over_bound = session
            .execute_trusted_dynamic_mutation_batch(requests)
            .expect_err("operation cap plus one must reject before row resolution");
        assert!(matches!(
            over_bound.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchTooManyItems,
            }),
        ));
        assert_eq!(
            over_bound.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                    (MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS + 1) as u64,
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::Limit,
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS as u64,
                ),
            ],
        );
    }

    #[test]
    fn mixed_structural_batch_staged_byte_bound_uses_checked_exact_boundary() {
        let mut exact = 0;
        add_structural_mutation_staged_bytes(
            &mut exact,
            [MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES],
        )
        .expect("the exact staged-byte boundary should admit");
        assert_eq!(exact, MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES);

        let error = add_structural_mutation_staged_bytes(&mut exact, [1])
            .expect_err("one byte above the staged-byte boundary must reject");
        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary:
                    icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchStagedBytesExceeded,
            }),
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                    (MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES + 1) as u64,
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::Limit,
                    MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES as u64,
                ),
            ],
        );

        validate_structural_mutation_result_bytes(MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES)
            .expect("the exact result-byte boundary should admit");
        let error = validate_structural_mutation_result_bytes(
            MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES + 1,
        )
        .expect_err("one byte above the result-byte boundary must reject");
        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary:
                    icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchResultBytesExceeded,
            }),
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                    (MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES + 1) as u64,
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::Limit,
                    MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES as u64,
                ),
            ],
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one lifecycle proves shared materialization and every maintained frontend against the same zero-state owner"
    )]
    #[test]
    fn identity_insert_frontends_share_one_committed_range_without_rejected_consumption() {
        let session = initialize();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("identity catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("identity row layout should build");
        let initial_description = session
            .try_describe_entity_by_name(ENTITY_NAME)
            .expect("accepted Identity description should resolve");
        assert_eq!(
            initial_description.entity_tag(),
            catalog.identity().entity_tag().value()
        );
        assert_eq!(
            initial_description.accepted_schema_fingerprint_method(),
            catalog.fingerprint_method_version()
        );
        assert_eq!(
            initial_description.accepted_schema_fingerprint(),
            catalog.fingerprint()
        );
        let initial_identity = initial_description
            .identity()
            .expect("accepted Identity policy should be described");
        assert_eq!(initial_identity.field(), "id");
        assert_eq!(initial_identity.generator(), "Identity::next");
        assert_eq!(initial_identity.accepted_kind(), "nat64");
        assert_eq!(initial_identity.minimum(), 1);
        assert_eq!(initial_identity.maximum(), u128::from(u64::MAX));
        assert_eq!(initial_identity.high_water(), 0);
        assert_eq!(initial_identity.remaining(), u128::from(u64::MAX));
        assert!(!initial_identity.exhausted());

        let rejected = session
            .execute_accepted_structural_save_batch(
                &catalog,
                &descriptor,
                batch(&[1_000, 2_000]),
                Timestamp::from_millis(6),
                |_| Err::<(), _>(InternalError::executor_unsupported()),
            )
            .expect_err("a rejected precommit result must not publish its tentative range");
        assert_eq!(rejected.class(), ErrorClass::Unsupported);
        assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 0);

        let rows = session
            .execute_accepted_structural_save_batch(
                &catalog,
                &descriptor,
                batch(&[10, 20, 30]),
                Timestamp::from_millis(7),
                Ok,
            )
            .expect("one accepted batch should commit rows and one identity range");
        assert_eq!(
            rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![
                vec![Value::Nat64(1), Value::Nat64(10)],
                vec![Value::Nat64(2), Value::Nat64(20)],
                vec![Value::Nat64(3), Value::Nat64(30)],
            ],
        );

        let dynamic = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: DynamicStructuralPatch::new(vec![(
                    "payload".to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(40)),
                )]),
            })
            .expect("dynamic omission should commit through shared Identity generation");
        assert_eq!(dynamic.affected_rows, 1);

        for (request, operation) in [
            (
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: DynamicStructuralPatch::new(vec![
                        (
                            "id".to_string(),
                            DynamicWriteCell::Value(InputValue::Nat64(41)),
                        ),
                        (
                            "payload".to_string(),
                            DynamicWriteCell::Value(InputValue::Nat64(42)),
                        ),
                    ]),
                },
                icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
            ),
            (
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: DynamicStructuralPatch::new(vec![(
                        "id".to_string(),
                        DynamicWriteCell::Default,
                    )]),
                },
                icydb_diagnostic_code::DiagnosticMutationOperation::Update,
            ),
        ] {
            let error = session
                .execute_trusted_dynamic_mutation(&request)
                .expect_err("structural Identity authorship and regeneration must reject");
            assert_eq!(error.class(), ErrorClass::Unsupported);
            assert_eq!(error.origin(), ErrorOrigin::Executor);
            assert_eq!(
                error.diagnostic_facts(),
                vec![
                    (
                        icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                        ENTITY_TAG.value(),
                    ),
                    (icydb_diagnostic_code::DiagnosticFactTag::FieldId, 1),
                    (
                        icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                        operation.raw(),
                    ),
                    (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 0,),
                ],
            );
        }

        let binding = session
            .issue_typed_entity_binding(
                ENTITY_SOURCE,
                &[
                    DynamicTypedFieldBindingRequest::new(
                        ID_SOURCE.to_string(),
                        DynamicTypedFieldType::Scalar(ScalarType::Nat64),
                        false,
                    ),
                    DynamicTypedFieldBindingRequest::new(
                        PAYLOAD_SOURCE.to_string(),
                        DynamicTypedFieldType::Scalar(ScalarType::Nat64),
                        false,
                    ),
                ],
            )
            .expect("typed output should bind the Identity field");
        let typed_patch = binding
            .bind_write_fields(vec![(
                PAYLOAD_SOURCE.to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(50)),
            )])
            .expect("typed payload should lower");
        let typed = session
            .execute_trusted_typed_mutation(
                &binding,
                &DynamicTypedMutation::Insert { patch: typed_patch },
            )
            .expect("typed omission should commit through shared Identity generation");
        assert_eq!(
            typed
                .expect("typed insert should return one mutation result")
                .affected_rows,
            1,
        );
        let explicit_typed_patch = binding
            .bind_write_fields(vec![
                (
                    ID_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(51)),
                ),
                (
                    PAYLOAD_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(52)),
                ),
            ])
            .expect("the low-level binding should retain exact authored intent");
        let explicit_typed_error = session
            .execute_trusted_typed_mutation(
                &binding,
                &DynamicTypedMutation::Insert {
                    patch: explicit_typed_patch,
                },
            )
            .expect_err("typed Identity authorship must reject before allocation");
        assert_eq!(explicit_typed_error.class(), ErrorClass::Unsupported);
        assert_eq!(explicit_typed_error.origin(), ErrorOrigin::Executor);
        assert_eq!(
            explicit_typed_error.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                    ENTITY_TAG.value(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::FieldId, 1),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                    icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 0,),
            ],
        );

        let replace_error = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Replace {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(99),
                patch: DynamicStructuralPatch::new(vec![(
                    "payload".to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(60)),
                )]),
            })
            .expect_err("save-as-insert with a chosen Identity must reject");
        assert_eq!(replace_error.class(), ErrorClass::Unsupported);
        assert_eq!(replace_error.origin(), ErrorOrigin::Executor);

        #[cfg(feature = "sql")]
        {
            for sql in [
                "INSERT INTO IdentityRow (payload) VALUES (70) RETURNING id, payload",
                "INSERT INTO IdentityRow (id, payload) VALUES (DEFAULT, 80) RETURNING id",
            ] {
                let _result = session
                    .execute_trusted_sql_mutation(sql)
                    .expect("SQL omission and DEFAULT should commit Identity generation");
            }

            let error = session
                .execute_trusted_sql_mutation(
                    "INSERT INTO IdentityRow (id, payload) VALUES (42, 90)",
                )
                .expect_err("an explicit SQL Identity value must reject before allocation");
            let diagnostic = error.diagnostic();
            assert_eq!(
                diagnostic.code(),
                icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
            );
            assert!(matches!(
                diagnostic.detail(),
                Some(icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
                    boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::ExplicitGeneratedField,
                }),
            ));
        }

        let expected_committed = if cfg!(feature = "sql") { 7 } else { 5 };
        assert_eq!(
            DATA_STORE.with(|store| store.borrow().len()),
            expected_committed
        );
        SCHEMA_STORE.with(|store| {
            let cursor = store
                .borrow()
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    ENTITY_TAG,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("committed writes must leave active state readable");
            assert_eq!(cursor.expected_high_water(), u128::from(expected_committed),);
            assert!(!cursor.has_allocations());
        });
        let committed_description = session
            .try_describe_entity_by_name(ENTITY_NAME)
            .expect("committed Identity description should resolve");
        let committed_identity = committed_description
            .identity()
            .expect("accepted Identity policy should remain described");
        assert_eq!(
            committed_identity.high_water(),
            u128::from(expected_committed),
        );
        assert_eq!(
            committed_identity.remaining(),
            u128::from(u64::MAX - expected_committed),
        );
        assert!(!committed_identity.exhausted());
    }

    #[test]
    fn mutation_progress_and_target_rows_recover_as_one_marker_transition() {
        let session = initialize_journaled();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("journaled atomic-progress catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("journaled atomic-progress row layout should build");

        for (ordinal, interruption) in [
            MutationCommitInterruption::MarkerPersisted,
            MutationCommitInterruption::JournalPublished,
            MutationCommitInterruption::RowsPublished,
            MutationCommitInterruption::ProgressReplaced,
        ]
        .into_iter()
        .enumerate()
        {
            let identity_byte = 31 + u8::try_from(ordinal).expect("small ordinal should fit");
            let (before, after, operation) = atomic_progress_fixture(identity_byte);
            with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
                match store.insert_mutation(&before)? {
                    InsertMutationJobResult::Inserted => Ok(()),
                    InsertMutationJobResult::Occupied(_) => {
                        Err(crate::db::MutationJobError::IdentityConflict)
                    }
                }
            })
            .expect("atomic predecessor should insert once");

            interrupt_next_mutation_commit_for_tests(interruption);
            let interrupted = session
                .execute_accepted_structural_save_batch_with_mutation_progress(
                    &catalog,
                    &descriptor,
                    batch(&[700 + u64::try_from(ordinal).expect("small ordinal should fit")]),
                    Timestamp::from_millis(17),
                    operation,
                );
            assert!(
                interrupted.is_err(),
                "selected atomic boundary should interrupt"
            );

            forget_recovered_domain_for_tests(&session.db)
                .expect("interruption should reset volatile recovery ownership");
            session
                .db
                .ensure_recovered_state()
                .expect("marker recovery should finish target and progress together");
            let retained = with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
                store.load_mutation(before.state().job_id)
            })
            .expect("recovered successor should load");
            assert_eq!(retained, after);
            assert_eq!(
                JOURNALED_DATA_STORE.with(|store| store.borrow().len()),
                u64::try_from(ordinal + 1).expect("small row count should fit"),
            );
        }

        let (before, after, operation) = atomic_progress_fixture(39);
        with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            match store.insert_mutation(&before)? {
                InsertMutationJobResult::Inserted => Ok(()),
                InsertMutationJobResult::Occupied(_) => {
                    Err(crate::db::MutationJobError::IdentityConflict)
                }
            }
        })
        .expect("final predecessor should insert once");
        session
            .execute_accepted_structural_save_batch_with_mutation_progress(
                &catalog,
                &descriptor,
                batch(&[799]),
                Timestamp::from_millis(18),
                operation,
            )
            .expect("uninterrupted atomic transition should clear its marker");
        let retained = with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            store.load_mutation(before.state().job_id)
        })
        .expect("final successor should load");
        assert_eq!(retained, after);
        forget_recovered_domain_for_tests(&session.db)
            .expect("post-clear recovery ownership should reset");
        session
            .db
            .ensure_recovered_state()
            .expect("post-clear recovery should remain a no-op");
    }

    #[test]
    fn mutation_progress_neither_side_mismatch_blocks_recovery() {
        let session = initialize_journaled();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("journaled corruption catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("journaled corruption row layout should build");
        let (before, _after, operation) = atomic_progress_fixture(41);
        with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            match store.insert_mutation(&before)? {
                InsertMutationJobResult::Inserted => Ok(()),
                InsertMutationJobResult::Occupied(_) => {
                    Err(crate::db::MutationJobError::IdentityConflict)
                }
            }
        })
        .expect("corruption predecessor should insert once");

        interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
        assert!(
            session
                .execute_accepted_structural_save_batch_with_mutation_progress(
                    &catalog,
                    &descriptor,
                    batch(&[811]),
                    Timestamp::from_millis(19),
                    operation,
                )
                .is_err(),
            "marker interruption should retain recovery authority",
        );
        let (unexpected, _) = before
            .apply_transition(
                &MutationJobAdvanceRequest::new(
                    before.state().job_id,
                    0,
                    MutationJobIdempotencyKey::new("unexpected-third-state")
                        .expect("unexpected replay key should admit"),
                ),
                MutationJobTransition::new(
                    MutationJobStatus::Active,
                    MutationJobPhase::Forward,
                    vec![99],
                    2,
                    0,
                    0,
                ),
            )
            .expect("unexpected but valid progress state should admit");
        with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            store.replace_mutation(&unexpected)
        })
        .expect("test should install the neither-side state");

        forget_recovered_domain_for_tests(&session.db)
            .expect("corrupt recovery ownership should reset");
        let error = session
            .db
            .ensure_recovered_state()
            .expect_err("neither-side progress must block recovery");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Recovery);
        assert_eq!(
            with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
                store.load_mutation(before.state().job_id)
            })
            .expect("unexpected state should remain inspectable to the test"),
            unexpected,
        );
        assert!(
            session.db.ensure_recovered_state().is_err(),
            "a retained corrupt marker must continue blocking database access",
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one ordered scenario exercises every durable interruption boundary, guarded recovery, derived rebuild, and both integrity tiers"
    )]
    fn journaled_identity_recovery_quiesces_every_publication_interruption_before_reallocation() {
        let session = initialize_journaled();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("journaled identity catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("journaled identity row layout should build");

        for (ordinal, interruption) in [
            MutationCommitInterruption::MarkerPersisted,
            MutationCommitInterruption::JournalPublished,
            MutationCommitInterruption::RowsPublished,
            MutationCommitInterruption::StateMaterialized,
        ]
        .into_iter()
        .enumerate()
        {
            interrupt_next_mutation_commit_for_tests(interruption);
            let interrupted = session.execute_accepted_structural_save_batch(
                &catalog,
                &descriptor,
                batch(&[u64::try_from(ordinal).expect("ordinal should fit")]),
                Timestamp::from_millis(8),
                Ok,
            );
            assert!(
                interrupted.is_err(),
                "the selected durable boundary should interrupt",
            );

            let committed = session
                .execute_accepted_structural_save_batch(
                    &catalog,
                    &descriptor,
                    batch(&[100 + u64::try_from(ordinal).expect("ordinal should fit")]),
                    Timestamp::from_millis(9),
                    Ok,
                )
                .expect("the next mutation must recover before allocating");
            let expected_high_water =
                u64::try_from((ordinal + 1) * 2).expect("small test high-water should fit");
            assert_eq!(
                committed
                    .into_iter()
                    .map(|row| row.values)
                    .collect::<Vec<_>>(),
                vec![vec![
                    Value::Nat64(expected_high_water),
                    Value::Nat64(100 + u64::try_from(ordinal).expect("ordinal should fit")),
                ]],
            );
            assert_eq!(
                JOURNALED_DATA_STORE.with(|store| store.borrow().len()),
                expected_high_water,
            );
            JOURNALED_SCHEMA_STORE.with(|store| {
                let cursor = store
                    .borrow()
                    .identity_statement_cursor(
                        database_incarnation_id()
                            .expect("database incarnation should remain readable"),
                        ENTITY_TAG,
                        FieldId::new(1),
                        &AcceptedFieldKind::Nat64,
                    )
                    .expect("guarded recovery must leave quiescent active state");
                assert_eq!(
                    cursor.expected_high_water(),
                    u128::from(expected_high_water),
                );
                assert!(!cursor.has_allocations());
            });
        }

        for (ordinal, (interruption, deleted_key)) in [
            (MutationCommitInterruption::MarkerPersisted, 2),
            (MutationCommitInterruption::JournalPublished, 4),
            (MutationCommitInterruption::RowPrefixPublished, 6),
            (MutationCommitInterruption::RowsPublished, 8),
            (MutationCommitInterruption::StateMaterialized, 7),
        ]
        .into_iter()
        .enumerate()
        {
            let expected_payload =
                501 + u64::try_from(ordinal).expect("small interruption ordinal should fit");
            interrupt_next_mutation_commit_for_tests(interruption);
            let interrupted = session.execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(expected_payload),
                },
                DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(deleted_key),
                },
            ]);
            assert!(
                interrupted.is_err(),
                "the selected caller-key mixed publication boundary should interrupt",
            );
            let recovered_update = session
                .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(expected_payload),
                })
                .expect("guarded reentry should complete the marker-authorized mixed batch");
            assert_eq!(
                recovered_update.affected_rows, 0,
                "the recovered update must already expose its admitted final image",
            );
            let recovered_delete = session
                .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(deleted_key),
                })
                .expect_err("the recovered delete must already be materialized");
            assert_eq!(recovered_delete.class(), ErrorClass::NotFound);
            JOURNALED_SCHEMA_STORE.with(|store| {
                let cursor = store
                    .borrow()
                    .identity_statement_cursor(
                        database_incarnation_id()
                            .expect("database incarnation should remain readable"),
                        ENTITY_TAG,
                        FieldId::new(1),
                        &AcceptedFieldKind::Nat64,
                    )
                    .expect("caller-key recovery must preserve active Identity state");
                assert_eq!(cursor.expected_high_water(), 8);
                assert!(!cursor.has_allocations());
            });
        }

        forget_recovered_domain_for_tests(&session.db)
            .expect("the final journal tail should remain recoverable");
        session
            .db
            .ensure_recovered_state()
            .expect("derived rebuild must not allocate another identity");

        let data_generation = JOURNALED_DATA_STORE.with(|store| store.borrow().generation());
        let index_generation = JOURNALED_INDEX_STORE.with(|store| store.borrow().generation());
        forget_recovered_domain_for_tests(&session.db)
            .expect("an empty-tail upgrade should reset recovery ownership");
        session
            .db
            .ensure_recovered_state()
            .expect("an empty-tail upgrade should admit without rebuilding stored rows or indexes");
        assert_eq!(
            JOURNALED_DATA_STORE.with(|store| store.borrow().generation()),
            data_generation,
            "empty-tail recovery must not traverse or rewrite authoritative rows",
        );
        assert_eq!(
            JOURNALED_INDEX_STORE.with(|store| store.borrow().generation()),
            index_generation,
            "empty-tail recovery must not clear or rebuild secondary indexes",
        );

        let quick = execute_quick_integrity(&session.db, catalog.inspection_plan())
            .expect("quiescent Identity control inventory should be inspectable");
        assert_eq!(quick.status(), &QuickIntegrityStatus::CompleteClean);
        let row_page = execute_row_integrity_page(
            &session.db,
            catalog.inspection_plan(),
            PhysicalUnitCheckpoint::BeforeFirst,
            RowInspectionLimits::standard(),
        )
        .expect("Identity rows should remain within committed high-water");
        assert!(row_page.exhausted());
        assert!(row_page.findings().is_empty());

        assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 3);
        assert!(
            JOURNALED_INDEX_STORE.with(|store| !store.borrow().is_empty()),
            "derived index rebuild should restore witnesses without allocating identities",
        );
        assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
        JOURNALED_SCHEMA_STORE.with(|store| {
            let cursor = store
                .borrow()
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    ENTITY_TAG,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("folded identity state should reopen without allocating");
            assert_eq!(cursor.expected_high_water(), 8);
            assert!(!cursor.has_allocations());
        });
    }

    #[test]
    fn journaled_startup_recovery_resumes_by_durable_pages_without_reallocating_ids() {
        let session = initialize_journaled();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("journaled identity catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("journaled identity row layout should build");

        for payload in 0_u64..129 {
            session
                .execute_accepted_structural_save_batch(
                    &catalog,
                    &descriptor,
                    batch(&[payload]),
                    Timestamp::from_millis(8),
                    Ok,
                )
                .expect("journaled identity fixture row should commit");
        }

        forget_recovered_domain_for_tests(&session.db)
            .expect("upgrade should reset recovery ownership");
        assert!(
            !session
                .db
                .continue_startup_recovery()
                .expect("the first bounded recovery page should commit"),
            "one page must not consume a tail larger than the production page bound",
        );
        assert!(JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
        let mut pages = 1;
        while !session
            .db
            .continue_startup_recovery()
            .expect("each bounded recovery page should commit")
        {
            pages += 1;
            assert!(pages <= 4, "the small fixture should finish promptly");
        }
        assert!(pages >= 2);

        assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 129);
        assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
        assert_dynamic_payload(&session, 1, 0);
        assert_dynamic_payload(&session, 129, 128);
        JOURNALED_SCHEMA_STORE.with(|store| {
            let cursor = store
                .borrow()
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    ENTITY_TAG,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("paged recovery must preserve active Identity state");
            assert_eq!(cursor.expected_high_water(), 129);
            assert!(!cursor.has_allocations());
        });
    }

    #[test]
    fn journaled_startup_recovery_resumes_within_one_large_batch() {
        let session = initialize_journaled();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("journaled identity catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("journaled identity row layout should build");
        let payloads = (0_u64..129).collect::<Vec<_>>();
        session
            .execute_accepted_structural_save_batch(
                &catalog,
                &descriptor,
                batch(&payloads),
                Timestamp::from_millis(9),
                Ok,
            )
            .expect("one large journal batch should commit");

        forget_recovered_domain_for_tests(&session.db)
            .expect("upgrade should reset recovery ownership");
        assert!(
            !session
                .db
                .continue_startup_recovery()
                .expect("the first record-bounded recovery page should commit"),
            "a single batch larger than the record bound must remain resumable",
        );
        JOURNALED_TAIL_STORE.with(|tail| {
            let tail = tail.borrow();
            let cursor = tail
                .fold_record_cursor()
                .expect("the fold cursor should decode")
                .expect("the incomplete batch should retain a fold cursor");
            assert_eq!(cursor.next_record_ordinal(), 128);
            assert!(tail.has_stored_batch());
        });
        assert!(
            session
                .db
                .continue_startup_recovery()
                .expect("the terminal record-bounded recovery page should commit"),
        );

        assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 129);
        JOURNALED_TAIL_STORE.with(|tail| {
            let tail = tail.borrow();
            assert!(!tail.has_stored_batch());
            assert!(!tail.has_fold_record_cursor());
        });
        assert_dynamic_payload(&session, 1, 0);
        assert_dynamic_payload(&session, 129, 128);
    }

    #[test]
    #[ignore = "release-closeout native timing probe for one marker-authorized Identity recovery"]
    fn identity_recovery_closeout_reports_guarded_reentry_time() {
        let session = initialize_journaled();
        let catalog = session
            .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
            .expect("journaled identity catalog should resolve");
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .expect("journaled identity row layout should build");

        interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
        let interrupted = session.execute_accepted_structural_save_batch(
            &catalog,
            &descriptor,
            batch(&[1]),
            Timestamp::from_millis(10),
            Ok,
        );
        assert!(
            interrupted.is_err(),
            "the selected publication boundary should interrupt",
        );

        let start = Instant::now();
        let committed = session
            .execute_accepted_structural_save_batch(
                &catalog,
                &descriptor,
                batch(&[2]),
                Timestamp::from_millis(11),
                Ok,
            )
            .expect("guarded reentry should recover before allocation");
        let elapsed = start.elapsed();
        assert_eq!(
            committed
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![vec![Value::Nat64(2), Value::Nat64(2)]],
        );

        println!(
            "identity recovery closeout: guarded_reentry_nanos={}",
            elapsed.as_nanos(),
        );
    }
}

#[cfg(test)]
mod targeted_rule_mutation_tests {
    use super::{
        DbSession, DynamicMutation, DynamicStructuralPatch, DynamicTypedFieldBindingRequest,
        DynamicTypedFieldType, DynamicTypedMutation, DynamicWriteCell,
    };
    use crate::{
        db::{
            data::{DataStore, encode_input_value_for_candidate_field_contract},
            index::IndexStore,
            registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
            schema::{
                AcceptedCheckLiteralV1, AcceptedCompositeCatalog, AcceptedFieldDecodeContract,
                AcceptedFieldKind, AcceptedNamedTypeIdentity, AcceptedRuleOperation,
                AcceptedRuleTarget, AcceptedSchemaRevision, AcceptedSourceBindingCatalog,
                ConstraintOrigin, FieldId, FieldStorageDecode, FieldWriteManagement, LeafCodec,
                PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
                ScalarCodec, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaInsertDefault,
                SchemaRowLayout, SchemaStore, SchemaVersion,
                accepted_schema_candidate_with_catalogs_for_tests,
                build_record_newtype_composite_catalog_for_tests,
                empty_accepted_enum_catalog_for_tests, enum_catalog::ValueAdmissionBudget,
            },
        },
        error::InternalError,
        traits::{CanisterKind, Path},
        types::EntityTag,
        value::InputValue,
    };
    use icydb_schema::{
        ConstraintSourceKey, EntitySourceKey, FieldSourceKey, ScalarType, TypeSourceKey,
    };
    use std::{cell::RefCell, collections::BTreeMap};

    const STORE_PATH: &str = "session::write::targeted_rule_mutation_tests::Store";
    const ENTITY_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity";
    const ID_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity::id";
    const PROFILE_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Entity::profile";
    const UPDATED_AT_SOURCE: &str =
        "session::write::targeted_rule_mutation_tests::Entity::updated_at";
    const PROFILE_TYPE_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Profile";
    const DEGREE_TYPE_SOURCE: &str = "session::write::targeted_rule_mutation_tests::Degree";
    const DEGREE_MEMBER_SOURCE: &str =
        "session::write::targeted_rule_mutation_tests::Profile::degree";
    const DEGREE_RULE_SOURCE: &str =
        "session::write::targeted_rule_mutation_tests::Profile::degree_multiple";

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "session::write::targeted_rule_mutation_tests::Canister";
    }

    impl CanisterKind for TestCanister {
        const COMMIT_MEMORY_ID: u8 = 43;
        const COMMIT_STABLE_KEY: &'static str = "icydb.targeted_mutation_tests.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 44;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.targeted_mutation_tests.integrity.progress.v1";
    }

    thread_local! {
        static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA_STORE: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_store(
                STORE_PATH,
                &DATA_STORE,
                &INDEX_STORE,
                &SCHEMA_STORE,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("targeted mutation test store should register");
            registry
        };
    }

    fn source<T, E: std::fmt::Debug>(raw: &str, parse: impl FnOnce(String) -> Result<T, E>) -> T {
        parse(raw.to_string()).expect("test source identity should admit")
    }

    fn profile_input(degree: u64) -> InputValue {
        InputValue::Map(vec![(
            InputValue::Text("degree".to_string()),
            InputValue::Nat64(degree),
        )])
    }

    fn structural_patch(id: u64, degree: u64) -> DynamicStructuralPatch {
        DynamicStructuralPatch::new(vec![
            (
                "id".to_string(),
                DynamicWriteCell::Value(InputValue::Nat64(id)),
            ),
            (
                "profile".to_string(),
                DynamicWriteCell::Value(profile_input(degree)),
            ),
        ])
    }

    fn encoded_value(
        enum_catalog: &crate::db::schema::AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
        name: &str,
        kind: &AcceptedFieldKind,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
        value: InputValue,
    ) -> Vec<u8> {
        let field = AcceptedFieldDecodeContract::new(name, kind, false, storage_decode, leaf_codec);
        encode_input_value_for_candidate_field_contract(
            enum_catalog,
            composite_catalog,
            field,
            value,
            &mut ValueAdmissionBudget::standard(),
        )
        .expect("test accepted value should encode")
    }

    fn nat64_literal(
        enum_catalog: &crate::db::schema::AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
        value: u64,
    ) -> AcceptedCheckLiteralV1 {
        let kind = AcceptedFieldKind::Nat64;
        AcceptedCheckLiteralV1::from_accepted_parts(
            kind.clone(),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
            encoded_value(
                enum_catalog,
                composite_catalog,
                "degree_bound",
                &kind,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
                InputValue::Nat64(value),
            ),
        )
    }

    fn targeted_constraint_id(error: &InternalError) -> u32 {
        let facts = error.diagnostic_facts();
        assert!(facts.contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
            icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
        )));
        assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 0,)));
        assert!(facts.contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule.raw(),
        )));
        assert_eq!(
            facts
                .iter()
                .filter(|(tag, _)| matches!(
                    tag,
                    icydb_diagnostic_code::DiagnosticFactTag::RootField
                        | icydb_diagnostic_code::DiagnosticFactTag::RecordMember
                ))
                .copied()
                .collect::<Vec<_>>(),
            vec![
                (icydb_diagnostic_code::DiagnosticFactTag::RootField, 2),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::RecordMember,
                    icydb_diagnostic_code::pack_u32_pair(1, 1),
                ),
            ]
        );
        let value = facts
            .iter()
            .find_map(|(tag, value)| {
                (*tag == icydb_diagnostic_code::DiagnosticFactTag::ConstraintId).then_some(*value)
            })
            .expect("targeted mutation should retain its accepted constraint ID");
        u32::try_from(value).expect("accepted constraint ID fits u32")
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end fixture proves every maintained write frontend converges on the same accepted targeted-rule schedule"
    )]
    #[test]
    fn targeted_rules_converge_across_dynamic_typed_sql_default_timestamp_and_batch_writes() {
        DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
        INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
        SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

        let entity_tag = EntityTag::new(93);
        let enum_catalog = empty_accepted_enum_catalog_for_tests();
        let (composite_catalog, profile_type, degree_type, degree_member) =
            build_record_newtype_composite_catalog_for_tests(
                "tests::TargetedProfile".to_string(),
                "degree".to_string(),
                "tests::TargetedDegree".to_string(),
                AcceptedFieldKind::Nat64,
                &enum_catalog,
            )
            .expect("targeted mutation composites should close");
        let profile_kind = AcceptedFieldKind::Composite {
            type_id: profile_type,
        };
        let profile_default = encoded_value(
            &enum_catalog,
            &composite_catalog,
            "profile",
            &profile_kind,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
            profile_input(12),
        );
        let fields = vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "profile".to_string(),
                SchemaFieldSlot::new(1),
                profile_kind,
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["degree".to_string()],
                    AcceptedFieldKind::Composite {
                        type_id: degree_type,
                    },
                    false,
                )],
                false,
                SchemaInsertDefault::SlotPayload(profile_default),
                FieldStorageDecode::CatalogValue,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(3),
                "updated_at".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Timestamp,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(
                    None,
                    Some(FieldWriteManagement::UpdatedAt),
                ),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Timestamp),
            ),
        ];
        let mut snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            ENTITY_SOURCE.to_string(),
            "TargetedMutation".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        );
        let constraint_catalog = snapshot
            .constraint_catalog()
            .clone()
            .with_added_targeted_rule(
                "profile_degree_multiple".to_string(),
                ConstraintOrigin::Generated,
                AcceptedRuleTarget::new(
                    FieldId::new(2),
                    AcceptedNamedTypeIdentity::Composite(degree_type),
                ),
                AcceptedRuleOperation::MultipleOf {
                    divisor: nat64_literal(&enum_catalog, &composite_catalog, 5),
                },
            )
            .expect("targeted mutation rule should allocate");
        let targeted_rule_id = constraint_catalog
            .constraints()
            .last()
            .expect("targeted mutation rule should persist")
            .id();
        snapshot = snapshot.with_constraint_catalog(constraint_catalog);

        let entity_source = source(ENTITY_SOURCE, EntitySourceKey::try_new);
        let id_source = source(ID_SOURCE, FieldSourceKey::try_new);
        let profile_source = source(PROFILE_SOURCE, FieldSourceKey::try_new);
        let updated_at_source = source(UPDATED_AT_SOURCE, FieldSourceKey::try_new);
        let profile_type_source = source(PROFILE_TYPE_SOURCE, TypeSourceKey::try_new);
        let degree_type_source = source(DEGREE_TYPE_SOURCE, TypeSourceKey::try_new);
        let degree_member_source = source(DEGREE_MEMBER_SOURCE, FieldSourceKey::try_new);
        let degree_rule_source = source(DEGREE_RULE_SOURCE, ConstraintSourceKey::try_new);
        let source_bindings = AcceptedSourceBindingCatalog::initial_for_tests(
            BTreeMap::from([(entity_source, entity_tag)]),
            BTreeMap::from([
                ((entity_tag, id_source), FieldId::new(1)),
                ((entity_tag, profile_source), FieldId::new(2)),
                ((entity_tag, updated_at_source), FieldId::new(3)),
            ]),
            BTreeMap::from([((entity_tag, degree_rule_source), targeted_rule_id)]),
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .with_initial_named_types_for_tests(
            BTreeMap::from([
                (
                    profile_type_source,
                    AcceptedNamedTypeIdentity::Composite(profile_type),
                ),
                (
                    degree_type_source,
                    AcceptedNamedTypeIdentity::Composite(degree_type),
                ),
            ]),
            BTreeMap::new(),
            BTreeMap::from([((profile_type, degree_member_source), degree_member)]),
        );
        let candidate = accepted_schema_candidate_with_catalogs_for_tests(
            STORE_PATH,
            AcceptedSchemaRevision::INITIAL,
            enum_catalog,
            composite_catalog,
            source_bindings,
            BTreeMap::from([(entity_tag, snapshot)]),
        );

        let session = DbSession::<TestCanister>::new(
            &STORE_REGISTRY,
            &crate::db::RequestExecutionRoot::__new_runtime_root(),
        );
        session
            .db
            .ensure_recovered_state()
            .expect("targeted mutation test database should initialize");
        let store = session
            .db
            .store_handle(STORE_PATH)
            .expect("targeted mutation test store should resolve");
        crate::db::commit::publish_accepted_schema_candidate(
            STORE_PATH,
            store,
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .expect("targeted mutation candidate should publish");

        let dynamic_error = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "TargetedMutation".to_string(),
                patch: structural_patch(1, 12),
            })
            .expect_err("dynamic write must enforce the targeted rule");
        assert_eq!(
            targeted_constraint_id(&dynamic_error),
            targeted_rule_id.get()
        );

        let binding = session
            .issue_typed_entity_binding(
                ENTITY_SOURCE,
                &[
                    DynamicTypedFieldBindingRequest::new(
                        ID_SOURCE.to_string(),
                        DynamicTypedFieldType::Scalar(ScalarType::Nat64),
                        false,
                    ),
                    DynamicTypedFieldBindingRequest::new(
                        PROFILE_SOURCE.to_string(),
                        DynamicTypedFieldType::Named(PROFILE_TYPE_SOURCE.to_string()),
                        false,
                    ),
                    DynamicTypedFieldBindingRequest::new(
                        UPDATED_AT_SOURCE.to_string(),
                        DynamicTypedFieldType::Scalar(ScalarType::Timestamp),
                        false,
                    ),
                ],
            )
            .expect("targeted typed binding should issue");
        let typed_patch = binding
            .bind_write_fields(vec![
                (
                    ID_SOURCE.to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(2)),
                ),
                (
                    PROFILE_SOURCE.to_string(),
                    DynamicWriteCell::Value(profile_input(12)),
                ),
            ])
            .expect("targeted typed patch should bind");
        let typed_error = session
            .execute_trusted_typed_mutation(
                &binding,
                &DynamicTypedMutation::Insert { patch: typed_patch },
            )
            .expect_err("typed write must enforce the targeted rule");
        assert_eq!(targeted_constraint_id(&typed_error), targeted_rule_id.get());

        #[cfg(feature = "sql")]
        {
            let sql_error = session
                .execute_trusted_sql_mutation("INSERT INTO TargetedMutation (id) VALUES (3)")
                .expect_err("SQL default resolution must enforce the targeted rule");
            let crate::db::QueryError::Execute(execute) = sql_error else {
                panic!("targeted SQL write should fail at shared execution admission");
            };
            assert_eq!(
                targeted_constraint_id(execute.as_internal()),
                targeted_rule_id.get()
            );
        }

        session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Insert {
                    entity: "TargetedMutation".to_string(),
                    patch: structural_patch(4, 5),
                },
                DynamicMutation::Insert {
                    entity: "TargetedMutation".to_string(),
                    patch: structural_patch(5, 12),
                },
            ])
            .expect_err("one invalid targeted value must reject the whole batch");
        assert_eq!(
            DATA_STORE.with(|store| store.borrow().exact_entity_count(entity_tag)),
            Some(0),
            "no frontend or earlier valid batch row may escape targeted admission",
        );

        let admitted = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Insert {
                    entity: "TargetedMutation".to_string(),
                    patch: structural_patch(6, 5),
                },
                DynamicMutation::Insert {
                    entity: "TargetedMutation".to_string(),
                    patch: structural_patch(7, 10),
                },
            ])
            .expect("compliant targeted values should share one accepted batch");
        let [first, second] = admitted.rows.as_slice() else {
            panic!("the mixed targeted batch should return two rows");
        };
        let first_timestamp = first
            .get(2)
            .expect("the first mixed row should contain its managed timestamp");
        assert!(matches!(
            first_timestamp,
            crate::value::OutputValue::Timestamp(_)
        ));
        assert_eq!(
            second.get(2),
            Some(first_timestamp),
            "one accepted mixed batch must materialize one managed timestamp",
        );
        assert_eq!(
            DATA_STORE.with(|store| store.borrow().exact_entity_count(entity_tag)),
            Some(2),
        );
    }
}
