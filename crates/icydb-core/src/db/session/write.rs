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
            AcceptedMutationConstraintScheduler, commit_structural_row_ops_with_window_for_path,
            mutation_key_exists_error,
        },
        schema::{
            AcceptedFieldKind, AcceptedIdentityAllocation, AcceptedRowLayoutRuntimeContract,
            FieldId, FieldInsertGeneration, IdentityStatementCursor, lower_field_type,
            output_value_from_runtime,
        },
        write_context::{AcceptedWriteContext, MutationMode},
    },
    error::InternalError,
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

struct OrderedAcceptedStructuralMutation {
    input_ordinal: u32,
    intent: AcceptedStructuralMutation,
}

const MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS: usize = 4_096;
const MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES: usize = 16 * 1024 * 1024;
const MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES: usize = 1024 * 1024;

fn add_structural_mutation_staged_bytes(
    total: &mut usize,
    lengths: impl IntoIterator<Item = usize>,
) -> Result<(), InternalError> {
    for length in lengths {
        *total = total
            .checked_add(length)
            .ok_or_else(InternalError::mutation_batch_staged_bytes_exceeded)?;
        if *total > MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES {
            return Err(InternalError::mutation_batch_staged_bytes_exceeded());
        }
    }
    Ok(())
}

fn validate_structural_mutation_result_bytes(encoded_bytes: usize) -> Result<(), InternalError> {
    if encoded_bytes > MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES {
        return Err(InternalError::mutation_batch_result_bytes_exceeded());
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
                entity_path,
                field.name(),
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
) -> Result<(AcceptedStructuralMutation, Option<SaveMutationKind>), InternalError> {
    match request {
        DynamicMutation::Insert { patch, .. } => Ok((
            AcceptedStructuralMutation::save(
                MutationMode::Insert,
                AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                lower_dynamic_patch(entity_path, descriptor, patch, MutationMode::Insert)?,
            ),
            Some(SaveMutationKind::Insert),
        )),
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
                    lower_dynamic_patch(entity_path, descriptor, patch, mode)?,
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
    entity_path: &str,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: &DynamicTypedStructuralPatch,
    mode: MutationMode,
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
                entity_path,
                field.name(),
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
    let encoded = candid::encode_one(&result).map_err(|_| InternalError::executor_invariant())?;
    validate_structural_mutation_result_bytes(encoded.len())?;
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
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
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
            let runtime_field = descriptor
                .field_for_slot_index(usize::from(field.slot().get()))
                .ok_or_else(InternalError::store_invariant)?;
            if runtime_field.field_id() != field_id {
                return Err(InternalError::store_invariant().into());
            }
            let expected_kind = lower_field_type(field_type, |source| {
                bundle.source_bindings().named_type(source)
            })
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
            descriptor.current_layout_version().get(),
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
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let identity = catalog.identity();
        if identity.entity_path() != binding.entity_source.as_str()
            || identity.entity_tag().value() != binding.entity_tag
            || catalog.revision().get() != binding.accepted_revision
            || catalog.fingerprint() != binding.accepted_fingerprint
            || descriptor.current_layout_version().get() != binding.entity_generation
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
            false,
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
            false,
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
            true,
            |rows| Ok(rows.len()),
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
        largest_journaled_prefix: bool,
        precommit_preparation: impl FnOnce(
            Vec<AcceptedStructuralMutationRow>,
        ) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        let identity = catalog.identity();
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
        if mutations.len() > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items());
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
        let mutations = mutations
            .into_iter()
            .enumerate()
            .map(|(input_index, intent)| {
                u32::try_from(input_index)
                    .map(|input_ordinal| OrderedAcceptedStructuralMutation {
                        input_ordinal,
                        intent,
                    })
                    .map_err(|_| InternalError::mutation_batch_too_many_items())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut identity_cursor: Option<IdentityStatementCursor> = None;
        let mut identity_insert_ordinal = 0_u32;
        let mut scheduler = AcceptedMutationConstraintScheduler::new(
            entity_path,
            row_decode_contract.clone(),
            catalog.fingerprint(),
            catalog.accepted_row_constraints(),
            mutations.len(),
        );
        let mut output = Vec::with_capacity(mutations.len());
        let mut staged_bytes = 0_usize;

        for mutation in mutations {
            let batch_input_ordinal = mutation.input_ordinal;
            let mutation = mutation.intent;
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
                scheduler.schedule_delete(CommitRowOp::new(
                    entity_path,
                    raw_key,
                    Some(canonical_before.as_raw_row().as_bytes().to_vec()),
                    None,
                    catalog.fingerprint(),
                ))?;
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
                    let field_name = descriptor
                        .field_for_slot_index(identity_field.field_slot)
                        .map_or("", |field| field.name());
                    InternalError::mutation_database_owned_field_explicit(entity_path, field_name)
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
                let field_name = descriptor
                    .field_for_slot_index(identity_field.field_slot)
                    .map_or("", |field| field.name());
                return Err(InternalError::mutation_database_owned_field_explicit(
                    entity_path,
                    field_name,
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
        if !batch.is_empty() {
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
            |rows| prepare_dynamic_mutation_result(catalog, descriptor, rows),
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
        self.execute_trusted_dynamic_mutation_batch(vec![request.clone()])
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
        if requests.is_empty() {
            return Err(InternalError::mutation_batch_empty());
        }
        if requests.len() > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items());
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

        for request in &requests {
            if request.entity().is_empty() {
                return Err(InternalError::executor_unsupported());
            }
            let item_catalog =
                self.accepted_schema_catalog_context_for_entity_name(Some(request.entity()))?;
            if item_catalog.identity() != accepted_identity {
                return Err(InternalError::mutation_batch_entity_mismatch());
            }
            let (mutation, save_kind) = lower_dynamic_mutation_intent(
                accepted_identity.entity_tag(),
                accepted_identity.entity_path(),
                &descriptor,
                request,
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
            false,
            |rows| {
                if rows.len() != save_kinds.len() {
                    return Err(InternalError::executor_invariant());
                }
                let metrics = rows
                    .iter()
                    .zip(save_kinds)
                    .filter_map(|(row, kind)| kind.map(|kind| (kind, row.logical_changed())))
                    .collect::<Vec<_>>();
                let result = prepare_dynamic_mutation_result(&catalog, &descriptor, rows)?;
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
        let patch = lower_typed_patch(identity.entity_path(), &descriptor, patch, mode)?;
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
        self.execute_trusted_dynamic_mutation_batch(mutations)
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

        let session = DbSession::<TestCanister>::new(&STORE_REGISTRY);
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

        #[cfg(feature = "query")]
        {
            let query = crate::db::DynamicQuery::new("RenamedEntity")
                .select(["id", "value"])
                .order_by(crate::db::asc("id"))
                .limit(1);
            let result = session
                .execute_trusted_dynamic_query(&query)
                .expect("query-only dynamic execution should use accepted authority");
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
        }
    }
}

#[cfg(test)]
mod identity_pre_key_tests {
    use super::{
        AcceptedMutationIntentPatch, AcceptedRowLayoutRuntimeContract, AcceptedStructuralMutation,
        AcceptedStructuralMutationTarget, DbSession, DynamicMutation, DynamicStructuralPatch,
        DynamicTypedFieldBindingRequest, DynamicTypedFieldType, DynamicTypedMutation,
        DynamicWriteCell, FieldSlot, MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
        MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES, MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES,
        add_structural_mutation_staged_bytes, checked_pre_key_candidate_count,
        insert_key_exists_after_generation, validate_structural_mutation_result_bytes,
    };
    use crate::{
        db::{
            commit::{database_incarnation_id, forget_recovered_domain_for_tests},
            data::DataStore,
            executor::{MutationCommitInterruption, interrupt_next_mutation_commit_for_tests},
            index::IndexStore,
            integrity::{
                PhysicalUnitCheckpoint, QuickIntegrityStatus, RowInspectionLimits,
                execute_quick_integrity, execute_row_integrity_page,
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
        value::{InputValue, Value},
    };
    use icydb_schema::{FieldSourceKey, ScalarType};
    use std::{cell::RefCell, collections::BTreeMap, time::Instant};

    const STORE_PATH: &str = "session::write::identity_pre_key_tests::Store";
    const ENTITY_SOURCE: &str = "session::write::identity_pre_key_tests::Entity";
    const ID_SOURCE: &str = "session::write::identity_pre_key_tests::Entity::id";
    const PAYLOAD_SOURCE: &str = "session::write::identity_pre_key_tests::Entity::payload";
    const ENTITY_NAME: &str = "IdentityRow";
    const ENTITY_TAG: EntityTag = EntityTag::new(93);
    const JOURNALED_STORE_PATH: &str = "session::write::identity_pre_key_tests::JournaledStore";

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
        let session = DbSession::<TestCanister>::new(&STORE_REGISTRY);
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

    fn initialize_journaled() -> DbSession<JournaledTestCanister> {
        let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY);
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
        session
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
        reason = "one ordered conservation scenario proves mixed success, distinct patches, late failure neutrality, result order, and Identity state"
    )]
    fn mixed_structural_batch_preserves_order_atomicity_and_identity_insert_ordinals() {
        let session = initialize();
        let seeded = session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![dynamic_payload_patch(100), dynamic_payload_patch(40)],
            )
            .expect("seed rows should commit");
        assert_eq!(seeded.affected_rows, 2);

        let mixed = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(60),
                },
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: dynamic_payload_patch(20),
                },
                DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(2),
                },
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: dynamic_payload_patch(20),
                },
            ])
            .expect("mixed split/merge batch should commit atomically");
        assert_eq!(mixed.affected_rows, 4);
        assert_eq!(
            mixed.rows,
            vec![
                vec![
                    crate::value::OutputValue::Nat64(1),
                    crate::value::OutputValue::Nat64(60),
                ],
                vec![
                    crate::value::OutputValue::Nat64(3),
                    crate::value::OutputValue::Nat64(20),
                ],
                vec![
                    crate::value::OutputValue::Nat64(2),
                    crate::value::OutputValue::Nat64(40),
                ],
                vec![
                    crate::value::OutputValue::Nat64(4),
                    crate::value::OutputValue::Nat64(20),
                ],
            ],
            "save after-images and delete before-images must retain input order",
        );

        let distinct_updates = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(50),
                },
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(3),
                    patch: dynamic_payload_patch(30),
                },
            ])
            .expect("distinct update patches should share one atomic batch");
        assert_eq!(
            distinct_updates.rows,
            vec![
                vec![
                    crate::value::OutputValue::Nat64(1),
                    crate::value::OutputValue::Nat64(50),
                ],
                vec![
                    crate::value::OutputValue::Nat64(3),
                    crate::value::OutputValue::Nat64(30),
                ],
            ],
        );

        let duplicate = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(999),
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
        let late_missing_target = session
            .execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(1),
                    patch: dynamic_payload_patch(777),
                },
                DynamicMutation::Delete {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::Nat64(99),
                },
            ])
            .expect_err("a late missing target must reject every earlier staged row");
        assert_eq!(late_missing_target.class(), ErrorClass::NotFound);
        let unchanged = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(1),
                patch: dynamic_payload_patch(50),
            })
            .expect("the rejected first update must not have published");
        assert_eq!(unchanged.affected_rows, 0);

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
            assert_eq!(cursor.expected_high_water(), 4);
            assert!(!cursor.has_allocations());
        });
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

        for request in [
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
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(1),
                patch: DynamicStructuralPatch::new(vec![(
                    "id".to_string(),
                    DynamicWriteCell::Default,
                )]),
            },
        ] {
            let error = session
                .execute_trusted_dynamic_mutation(&request)
                .expect_err("structural Identity authorship and regeneration must reject");
            assert_eq!(error.class(), ErrorClass::Unsupported);
            assert_eq!(error.origin(), ErrorOrigin::Executor);
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

        interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowPrefixPublished);
        let interrupted = session.execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(1),
                patch: dynamic_payload_patch(501),
            },
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(2),
            },
        ]);
        assert!(
            interrupted.is_err(),
            "a caller-key mixed batch should interrupt after its first physical row",
        );
        let recovered_update = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(1),
                patch: dynamic_payload_patch(501),
            })
            .expect("guarded reentry should complete the marker-authorized mixed batch");
        assert_eq!(
            recovered_update.affected_rows, 0,
            "the recovered update must already expose its admitted final image",
        );
        let recovered_delete = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::Nat64(2),
            })
            .expect_err("the recovered delete must already be materialized");
        assert_eq!(recovered_delete.class(), ErrorClass::NotFound);

        forget_recovered_domain_for_tests(&session.db)
            .expect("the final journal tail should remain recoverable");
        session
            .db
            .ensure_recovered_state()
            .expect("derived rebuild must not allocate another identity");

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

        assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 7);
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
        error::{
            ConstraintDiagnostic, ConstraintDiagnosticKind, ConstraintValuePathComponent,
            InternalError,
        },
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
        "session::write::targeted_rule_mutation_tests::Profile::degree_range";

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

    fn targeted_diagnostic(error: &InternalError) -> &ConstraintDiagnostic {
        let diagnostic = error
            .constraint_diagnostic()
            .expect("targeted mutation should retain a public diagnostic");
        assert_eq!(
            diagnostic.constraint_kind(),
            ConstraintDiagnosticKind::TargetedRule
        );
        assert_eq!(diagnostic.field_paths(), &["profile".to_string()]);
        assert_eq!(
            diagnostic
                .value_path()
                .expect("targeted mutation should retain its typed value path")
                .components(),
            &[
                ConstraintValuePathComponent::RootField { field_id: 2 },
                ConstraintValuePathComponent::RecordMember {
                    composite_type_id: 1,
                    member_id: 1,
                },
            ],
        );
        diagnostic
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
                "profile_degree_range".to_string(),
                ConstraintOrigin::Generated,
                AcceptedRuleTarget::new(
                    FieldId::new(2),
                    AcceptedNamedTypeIdentity::Composite(degree_type),
                ),
                AcceptedRuleOperation::NumericRangeInclusive {
                    min: nat64_literal(&enum_catalog, &composite_catalog, 0),
                    max: nat64_literal(&enum_catalog, &composite_catalog, 10),
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

        let session = DbSession::<TestCanister>::new(&STORE_REGISTRY);
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
        let dynamic_diagnostic = targeted_diagnostic(&dynamic_error);
        assert_eq!(dynamic_diagnostic.constraint_id(), targeted_rule_id.get());

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
        assert_eq!(
            targeted_diagnostic(&typed_error).constraint_id(),
            targeted_rule_id.get()
        );

        #[cfg(feature = "sql")]
        {
            let sql_error = session
                .execute_trusted_sql_mutation("INSERT INTO TargetedMutation (id) VALUES (3)")
                .expect_err("SQL default resolution must enforce the targeted rule");
            let crate::db::QueryError::Execute(execute) = sql_error else {
                panic!("targeted SQL write should fail at shared execution admission");
            };
            assert_eq!(
                targeted_diagnostic(execute.as_internal()).constraint_id(),
                targeted_rule_id.get()
            );
        }

        session
            .execute_trusted_dynamic_insert_batch(
                "TargetedMutation",
                vec![structural_patch(4, 5), structural_patch(5, 12)],
            )
            .expect_err("one invalid targeted value must reject the whole batch");
        assert_eq!(
            DATA_STORE.with(|store| store.borrow().exact_entity_count(entity_tag)),
            Some(0),
            "no frontend or earlier valid batch row may escape targeted admission",
        );

        let admitted = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "TargetedMutation".to_string(),
                patch: structural_patch(6, 5),
            })
            .expect("compliant targeted value should commit after managed timestamp resolution");
        assert!(matches!(
            admitted.rows.first().and_then(|row| row.get(2)),
            Some(crate::value::OutputValue::Timestamp(_))
        ));
        assert_eq!(
            DATA_STORE.with(|store| store.borrow().exact_entity_count(entity_tag)),
            Some(1),
        );
    }
}
