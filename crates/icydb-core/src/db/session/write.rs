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
            AcceptedMutationIntentPatch, DecodedDataStoreKey, FieldSlot, RawRow,
            StructuralRowContract, StructuralSlotReader,
            canonical_row_from_raw_row_with_accepted_decode_contract,
            resolve_existing_replace_structural_patch_with_accepted_contract,
            resolve_insert_structural_patch_with_accepted_contract,
            resolve_update_structural_patch_with_accepted_contract,
        },
        executor::{
            AcceptedMutationConstraintScheduler, commit_delete_row_ops_with_window_for_path,
            commit_structural_save_row_ops_with_window_for_path, mutation_key_exists_error,
        },
        schema::{
            AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, lower_field_type,
            output_value_from_runtime,
        },
        write_context::{AcceptedWriteContext, MutationMode},
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, SaveMutationKind, record},
    traits::CanisterKind,
    types::{CurrentTimestamp, Timestamp},
    value::{InputValue, OutputValue, Value},
};
use icydb_schema::{EntitySourceKey, FieldSourceKey, FieldType, TypeSourceKey};
use std::collections::BTreeSet;

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

/// One accepted structural mutation ready for shared batch materialization.
pub(in crate::db::session) struct AcceptedStructuralMutation {
    target: AcceptedStructuralMutationTarget,
    patch: AcceptedMutationIntentPatch,
}

impl AcceptedStructuralMutation {
    pub(in crate::db::session) const fn new(
        target: AcceptedStructuralMutationTarget,
        patch: AcceptedMutationIntentPatch,
    ) -> Self {
        Self { target, patch }
    }
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

fn project_dynamic_mutation_row(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    contract: &StructuralRowContract,
    enum_catalog: &crate::db::schema::AcceptedEnumCatalog,
    row: &RawRow,
) -> Result<(Vec<String>, Vec<OutputValue>), InternalError> {
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, contract)?;
    let mut columns = Vec::with_capacity(descriptor.fields().len());
    let mut values = Vec::with_capacity(descriptor.fields().len());
    for field in descriptor.fields() {
        columns.push(field.name().to_string());
        let value = reader.required_cached_value(usize::from(field.slot().get()))?;
        values.push(
            output_value_from_runtime(enum_catalog, value)
                .map_err(|_| InternalError::store_invariant())?,
        );
    }
    Ok((columns, values))
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
        let identity = catalog.identity();
        let entity_path = identity.entity_path();
        let row_decode_contract =
            descriptor.row_decode_contract(catalog.value_catalog_handle().clone());
        let row_contract = StructuralRowContract::from_accepted_decode_contract(
            entity_path,
            row_decode_contract.clone(),
        );
        let store = self.db.recovered_store(identity.store_path())?;
        let mut rows = Vec::with_capacity(keys.len());
        let mut scheduler =
            AcceptedMutationConstraintScheduler::for_delete(&self.db, entity_path, keys.len());

        for key in keys {
            let before = validated_existing_row(store, &key, &row_contract)?
                .ok_or_else(|| InternalError::store_not_found(&key))?;
            let raw_key = key.to_raw()?;
            let canonical_before = canonical_row_from_raw_row_with_accepted_decode_contract(
                entity_path,
                row_decode_contract.clone(),
                &before,
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
            rows.push(values);
        }

        let batch = scheduler.finish()?;
        precommit_validation(rows.as_slice())?;
        if !batch.is_empty() {
            commit_delete_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                batch,
                "accepted_structural_delete_batch_apply",
            )?;
        }
        Ok(rows)
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
        mode: MutationMode,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        precommit_preparation: impl FnOnce(
            Vec<AcceptedStructuralMutationRow>,
        ) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        self.execute_accepted_structural_save_batch_inner(
            catalog,
            descriptor,
            mode,
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
        self.execute_accepted_structural_save_batch_inner(
            catalog,
            descriptor,
            MutationMode::Update,
            mutations,
            operation_timestamp,
            true,
            |rows| Ok(rows.len()),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one phased owner keeps accepted authority, mutation context, precommit preparation, output capture, and commit staging inseparable"
    )]
    fn execute_accepted_structural_save_batch_inner<T>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mode: MutationMode,
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
        let mut scheduler = AcceptedMutationConstraintScheduler::for_save(
            &self.db,
            entity_path,
            mode,
            row_decode_contract.clone(),
            catalog.fingerprint(),
            catalog.accepted_row_constraints(),
            mutations.len(),
        );
        let mut output = Vec::with_capacity(mutations.len());
        let mut seen_keys = BTreeSet::new();

        for mutation in mutations {
            let expected_key = match mutation.target {
                AcceptedStructuralMutationTarget::ResolveFromAfterImage => None,
                AcceptedStructuralMutationTarget::Expected(key) => Some(*key),
            };
            let mut patch = mutation.patch;
            if matches!(mode, MutationMode::Replace)
                && let Some(key) = expected_key.as_ref()
            {
                patch = preserve_dynamic_replacement_identity(key, descriptor, patch)?;
            }
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

            let resolved = match (mode, before.as_ref()) {
                (MutationMode::Insert | MutationMode::Replace, None) => {
                    resolve_insert_structural_patch_with_accepted_contract(
                        entity_path,
                        row_decode_contract.clone(),
                        catalog.fingerprint(),
                        catalog.accepted_row_constraints(),
                        &patch,
                        write_context,
                    )?
                }
                (MutationMode::Update, Some(before)) => {
                    resolve_update_structural_patch_with_accepted_contract(
                        entity_path,
                        row_decode_contract.clone(),
                        catalog.fingerprint(),
                        catalog.accepted_row_constraints(),
                        before,
                        &patch,
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
                        &patch,
                        write_context,
                    )?
                }
                (MutationMode::Insert, Some(_)) | (MutationMode::Update, None) => {
                    return Err(InternalError::executor_invariant());
                }
            };
            let (after, provenance) = resolved.into_parts();
            let data_key = match expected_key {
                Some(key) => {
                    let reader =
                        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                            after.as_raw_row(),
                            &row_contract,
                        )?;
                    reader.validate_primary_key(&key)?;
                    key
                }
                None => {
                    data_key_from_row(identity.entity_tag(), &row_contract, after.as_raw_row())?
                }
            };
            if matches!(mode, MutationMode::Insert)
                && validated_existing_row(store, &data_key, &row_contract)?.is_some()
            {
                return Err(mutation_key_exists_error());
            }
            let raw_key = data_key.to_raw()?;
            if !seen_keys.insert(raw_key.clone()) {
                return Err(InternalError::mutation_atomic_save_duplicate_key(
                    entity_path,
                    data_key,
                ));
            }
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
                &data_key,
                after.as_raw_row(),
                provenance.as_slice(),
                row_op,
            )?;
            if physical_changed {
                #[cfg(feature = "sql")]
                if largest_journaled_prefix
                    && !crate::db::commit::journaled_row_ops_fit_commit_window(
                        scheduler.save_rows()?,
                    )
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

            let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                after.as_raw_row(),
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
                logical_changed,
            });
        }

        #[cfg(not(feature = "sql"))]
        let _ = largest_journaled_prefix;

        let batch = scheduler.finish()?;
        let prepared = precommit_preparation(output)?;
        if !batch.is_empty() {
            commit_structural_save_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                batch,
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
        let mut rows = self.execute_accepted_structural_save_batch(
            catalog,
            descriptor,
            mode,
            vec![AcceptedStructuralMutation::new(target, patch)],
            Timestamp::now(),
            Ok,
        )?;
        let result = rows.pop().ok_or_else(InternalError::executor_invariant)?;
        record(MetricsEvent::SaveMutation {
            entity_path: entity_path.into(),
            kind: match mode {
                MutationMode::Insert => SaveMutationKind::Insert,
                MutationMode::Replace => SaveMutationKind::Replace,
                MutationMode::Update => SaveMutationKind::Update,
            },
            rows_touched: u64::from(result.logical_changed()),
        });
        let columns = descriptor
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let row = result
            .values
            .iter()
            .map(|value| {
                output_value_from_runtime(catalog.enum_catalog(), value)
                    .map_err(|_| InternalError::store_invariant())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DynamicMutationResult {
            entity: catalog.snapshot().entity_name().to_string(),
            columns,
            rows: vec![row],
            affected_rows: u32::from(result.logical_changed()),
        })
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
        if request.entity().is_empty() {
            return Err(InternalError::executor_unsupported());
        }

        let catalog =
            self.accepted_schema_catalog_context_for_entity_name(Some(request.entity()))?;
        let identity = catalog.identity();
        let entity_path = identity.entity_path();
        let store_path = identity.store_path();
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let row_decode_contract =
            descriptor.row_decode_contract(catalog.value_catalog_handle().clone());
        let row_contract = StructuralRowContract::from_accepted_decode_contract(
            entity_path,
            row_decode_contract.clone(),
        );
        let store = self.db.recovered_store(store_path)?;

        if let DynamicMutation::Delete { key, .. } = request {
            let data_key = dynamic_key(identity.entity_tag(), key)?;
            let before = validated_existing_row(store, &data_key, &row_contract)?
                .ok_or_else(|| InternalError::store_not_found(&data_key))?;
            let raw_key = data_key.to_raw()?;
            let canonical_before = canonical_row_from_raw_row_with_accepted_decode_contract(
                entity_path,
                row_decode_contract,
                &before,
            )?;
            let mut scheduler =
                AcceptedMutationConstraintScheduler::for_delete(&self.db, entity_path, 1);
            scheduler.schedule_delete(CommitRowOp::new(
                entity_path,
                raw_key,
                Some(canonical_before.as_raw_row().as_bytes().to_vec()),
                None,
                catalog.fingerprint(),
            ))?;
            let batch = scheduler.finish()?;
            commit_delete_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                batch,
                "dynamic_delete_row_apply",
            )?;
            let (columns, row) = project_dynamic_mutation_row(
                &descriptor,
                &row_contract,
                catalog.enum_catalog(),
                canonical_before.as_raw_row(),
            )?;

            return Ok(DynamicMutationResult {
                entity: request.entity().to_string(),
                columns,
                rows: vec![row],
                affected_rows: 1,
            });
        }

        let mode = dynamic_mutation_mode(request).ok_or_else(InternalError::executor_invariant)?;
        let (target, patch) = match request {
            DynamicMutation::Insert { patch, .. } => (
                AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                patch,
            ),
            DynamicMutation::Update { key, patch, .. }
            | DynamicMutation::Replace { key, patch, .. } => (
                AcceptedStructuralMutationTarget::expected(dynamic_key(
                    identity.entity_tag(),
                    key,
                )?),
                patch,
            ),
            DynamicMutation::Delete { .. } => return Err(InternalError::executor_invariant()),
        };
        let patch = lower_dynamic_patch(entity_path, &descriptor, patch, mode)?;
        self.execute_one_accepted_save_mutation(&catalog, &descriptor, mode, target, patch)
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
        if entity.is_empty() || patches.is_empty() {
            return Err(InternalError::executor_unsupported());
        }

        let catalog = self.accepted_schema_catalog_context_for_entity_name(Some(entity))?;
        let identity = catalog.identity();
        let entity_path = identity.entity_path();
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let mutations = patches
            .iter()
            .map(|patch| {
                lower_dynamic_patch(entity_path, &descriptor, patch, MutationMode::Insert).map(
                    |patch| {
                        AcceptedStructuralMutation::new(
                            AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                            patch,
                        )
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let results = self.execute_accepted_structural_save_batch(
            &catalog,
            &descriptor,
            MutationMode::Insert,
            mutations,
            Timestamp::now(),
            Ok,
        )?;
        let affected_rows = results.iter().try_fold(0_u32, |total, result| {
            total
                .checked_add(u32::from(result.logical_changed()))
                .ok_or_else(InternalError::executor_invariant)
        })?;
        record(MetricsEvent::SaveMutation {
            entity_path: entity_path.into(),
            kind: SaveMutationKind::Insert,
            rows_touched: u64::from(affected_rows),
        });

        let columns = descriptor
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let rows = results
            .into_iter()
            .map(|result| {
                result
                    .values
                    .iter()
                    .map(|value| {
                        output_value_from_runtime(catalog.enum_catalog(), value)
                            .map_err(|_| InternalError::store_invariant())
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(DynamicMutationResult {
            entity: entity.to_string(),
            columns,
            rows,
            affected_rows,
        })
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
