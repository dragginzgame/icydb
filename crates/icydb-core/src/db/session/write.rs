//! Module: db::session::write
//! Responsibility: session-owned typed write APIs for insert, replace, update,
//! and structural mutation entrypoints over the shared save pipeline.
//! Does not own: commit staging, mutation execution, or persistence encoding.
//! Boundary: keeps public session write semantics above the executor save surface.

use super::AcceptedSchemaCatalogContext;
#[cfg(test)]
use super::accepted_schema::accepted_save_contract_for_catalog_context;
#[cfg(test)]
use crate::db::{data::AuthoredStructuralPatch, schema::accepted_insert_field_is_omittable};
use crate::{
    db::{
        DbSession, DynamicMutation, DynamicMutationResult, DynamicStructuralPatch,
        DynamicTypedBindingError, DynamicTypedEntityBinding, DynamicTypedFieldBindingRequest,
        DynamicTypedFieldType, DynamicWriteCell, PersistedRow, WriteBatchResponse,
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
            commit_delete_row_ops_with_window_for_path,
            commit_structural_save_row_ops_with_window_for_path, mutation_key_exists_error,
            validate_structural_accepted_after_image,
        },
        relation::validate_save_relations_for_structural_row,
        schema::{
            AcceptedFieldKind, AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeContract,
            CompiledAcceptedRowConstraints, lower_field_type, output_value_from_runtime,
        },
        write_context::{AcceptedWriteContext, MutationMode},
    },
    entity::EntityCreateInput,
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

// Append one session-resolved structural field update. The caller passes the
// accepted runtime contract that already crossed schema reconciliation, so
// field-name lookup follows persisted row-layout metadata rather than generated
// declaration order.
#[cfg(test)]
fn append_accepted_structural_patch_field(
    entity_path: &'static str,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: AuthoredStructuralPatch,
    field_name: &str,
    value: InputValue,
) -> Result<AuthoredStructuralPatch, InternalError> {
    let slot = descriptor
        .field_slot_index_by_name(field_name)
        .ok_or_else(|| InternalError::mutation_structural_field_unknown(entity_path, field_name))?;

    Ok(patch.set(FieldSlot::from_validated_index(slot), value))
}

// Enforce public structural patch policy before the executor materializes an
// entity through generated derive code. This keeps database write ownership and
// absence/default policy owned by accepted schema metadata instead of
// accidentally relying on executor-local generated field metadata, Rust
// `Default`, or derive-local missing slot behavior.
#[cfg(test)]
fn validate_structural_patch_schema_policy<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: &AuthoredStructuralPatch,
    mode: MutationMode,
) -> Result<(), InternalError>
where
    E: PersistedRow,
{
    reject_explicit_database_owned_fields_from_accepted_patch::<E>(descriptor, patch)?;

    if matches!(mode, MutationMode::Update) {
        return Ok(());
    }

    let mut provided_slots = vec![false; descriptor.required_slot_count()];
    for entry in patch.entries() {
        let slot = entry.slot().index();
        if slot < provided_slots.len() {
            provided_slots[slot] = true;
        }
    }

    // Every omitted field must be allowed by the accepted insert contract.
    // This check must not inspect Rust `Default` impls or derive-local
    // construction values.
    for field in descriptor.fields() {
        let slot = usize::from(field.slot().get());
        if provided_slots.get(slot).copied().unwrap_or(false) {
            continue;
        }

        if !accepted_insert_field_is_omittable(field.insert_omission_policy(), field.write_policy())
        {
            return Err(
                InternalError::mutation_structural_patch_required_field_missing(
                    E::PATH,
                    field.name(),
                ),
            );
        }
    }

    Ok(())
}

// Preserve database-owned-field diagnostics ahead of sparse-patch
// required-field diagnostics. Public structural writes must not author fields
// whose values are owned by accepted schema write policy.
#[cfg(test)]
fn reject_explicit_database_owned_fields_from_accepted_patch<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: &AuthoredStructuralPatch,
) -> Result<(), InternalError>
where
    E: PersistedRow,
{
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let Some(accepted_field) = descriptor.field_for_slot_index(slot) else {
            continue;
        };
        let write_policy = accepted_field.write_policy();

        if write_policy.insert_generation().is_some() || write_policy.write_management().is_some() {
            return Err(InternalError::mutation_database_owned_field_explicit(
                E::PATH,
                accepted_field.name(),
            ));
        }
    }

    Ok(())
}

const fn dynamic_mutation_mode(request: &DynamicMutation) -> Option<MutationMode> {
    match request {
        DynamicMutation::Insert { .. } => Some(MutationMode::Insert),
        DynamicMutation::Update { .. } => Some(MutationMode::Update),
        DynamicMutation::Replace { .. } => Some(MutationMode::Replace),
        DynamicMutation::Delete { .. } => None,
    }
}

const fn dynamic_write_context(
    mode: MutationMode,
    operation_timestamp: Timestamp,
) -> AcceptedWriteContext {
    AcceptedWriteContext::new(mode, operation_timestamp)
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
    entity_path: &'static str,
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

#[expect(
    clippy::too_many_arguments,
    reason = "one accepted structural after-image keeps identity, row contracts, constraints, provenance, and operation context explicit"
)]
fn validate_dynamic_after_image<C: CanisterKind>(
    session: &DbSession<C>,
    entity_path: &'static str,
    store_path: &'static str,
    data_key: &DecodedDataStoreKey,
    row: &RawRow,
    provenance: &[Option<crate::db::data::AcceptedFieldWriteProvenance>],
    write_context: AcceptedWriteContext,
    row_decode_contract: AcceptedRowDecodeContract,
    schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
) -> Result<(), InternalError> {
    let raw_key = data_key.to_raw()?;
    validate_structural_accepted_after_image(
        entity_path,
        write_context.mode(),
        &raw_key,
        row,
        provenance,
        row_decode_contract.clone(),
        schema_fingerprint,
        constraints,
    )?;
    let contract = StructuralRowContract::from_accepted_decode_contract(
        entity_path,
        row_decode_contract.clone(),
    );
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, &contract)?;
    validate_save_relations_for_structural_row(
        &session.db,
        entity_path,
        store_path,
        &row_decode_contract,
        &reader,
    )
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
        let mut binding = None;
        let mut visited_stores = BTreeSet::new();

        for entity_registration in self.db.entity_registrations {
            let registration = entity_registration.runtime();
            if !visited_stores.insert(registration.store_path) {
                continue;
            }
            let store = self.db.recovered_store(registration.store_path)?;
            let Some(bundle) = store
                .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
            else {
                continue;
            };
            let Some(entity_tag) = bundle.source_bindings().entity(&entity_source) else {
                continue;
            };
            if binding.is_some() {
                return Err(InternalError::store_invariant().into());
            }
            let snapshot = bundle
                .entity_snapshots()
                .get(&entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
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
                let expected_kind = lower_field_type(field_type, |source| {
                    bundle.source_bindings().named_type(source)
                })
                .map_err(|_| DynamicTypedBindingError::IncompatibleField)?;
                if field.nullable() != *nullable
                    || !typed_adapter_field_kind_matches(field.kind(), &expected_kind)
                {
                    return Err(DynamicTypedBindingError::IncompatibleField);
                }
                fields.push((source.as_str().to_string(), field.name().to_string()));
            }
            let catalog =
                self.accepted_schema_catalog_context_for_entity_name(Some(snapshot.entity_name()))?;
            let descriptor =
                AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
            let adapter_names = bundle.typed_adapter_names()?;
            binding = Some(DynamicTypedEntityBinding {
                database_incarnation: database_incarnation_id()?.to_bytes(),
                entity: snapshot.entity_name().to_string(),
                entity_tag: entity_tag.value(),
                accepted_revision: catalog.revision().get(),
                accepted_fingerprint: catalog.fingerprint(),
                entity_generation: descriptor.current_layout_version().get(),
                fields,
                named_types: adapter_names.named_types,
                enum_variants: adapter_names.enum_variants,
                composite_fields: adapter_names.composite_fields,
            });
        }

        binding.ok_or(DynamicTypedBindingError::FieldUnavailable)
    }

    /// Verify that an opaque typed binding still names the exact accepted authority.
    pub fn typed_entity_binding_is_current(
        &self,
        binding: &DynamicTypedEntityBinding,
    ) -> Result<bool, InternalError> {
        if database_incarnation_id()?.to_bytes() != binding.database_incarnation {
            return Ok(false);
        }
        let catalog =
            self.accepted_schema_catalog_context_for_entity_name(Some(binding.entity.as_str()))?;
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        Ok(
            catalog.identity().entity_tag().value() == binding.entity_tag
                && catalog.revision().get() == binding.accepted_revision
                && catalog.fingerprint() == binding.accepted_fingerprint
                && descriptor.current_layout_version().get() == binding.entity_generation,
        )
    }

    /// Validate and atomically commit one accepted single-entity delete batch.
    #[cfg(feature = "sql")]
    pub(in crate::db::session) fn execute_accepted_structural_delete_batch(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        keys: Vec<DecodedDataStoreKey>,
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
        let mut raw_keys = BTreeSet::new();
        let mut rows = Vec::with_capacity(keys.len());
        let mut row_ops = Vec::with_capacity(keys.len());

        for key in keys {
            let before = validated_existing_row(store, &key, &row_contract)?
                .ok_or_else(|| InternalError::store_not_found(&key))?;
            let raw_key = key.to_raw()?;
            if !raw_keys.insert(raw_key.clone()) {
                return Err(InternalError::mutation_atomic_save_duplicate_key(
                    entity_path,
                    key,
                ));
            }
            let canonical_before = canonical_row_from_raw_row_with_accepted_decode_contract(
                entity_path,
                row_decode_contract.clone(),
                &before,
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
            rows.push(values);
            row_ops.push(CommitRowOp::new(
                entity_path,
                raw_key,
                Some(canonical_before.as_raw_row().as_bytes().to_vec()),
                None,
                catalog.fingerprint(),
            ));
        }

        self.db.validate_delete_relations(entity_path, &raw_keys)?;
        if !row_ops.is_empty() {
            commit_delete_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                row_ops,
                "accepted_structural_delete_batch_apply",
            )?;
        }
        Ok(rows)
    }

    /// Materialize and atomically commit one accepted single-entity structural batch.
    ///
    /// The caller freezes one operation timestamp and supplies frontend-lowered
    /// intent only. Accepted defaults, generated values, managed timestamps,
    /// constraints, relations, row encoding, and commit preparation remain
    /// owned by this database boundary.
    pub(in crate::db::session) fn execute_accepted_structural_save_batch(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mode: MutationMode,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
    ) -> Result<Vec<AcceptedStructuralMutationRow>, InternalError> {
        self.execute_accepted_structural_save_batch_inner(
            catalog,
            descriptor,
            mode,
            mutations,
            operation_timestamp,
            false,
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
        )
        .map(|rows| rows.len())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one phased owner keeps accepted resolution, validation, output capture, and commit staging inseparable"
    )]
    fn execute_accepted_structural_save_batch_inner(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mode: MutationMode,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        largest_journaled_prefix: bool,
    ) -> Result<Vec<AcceptedStructuralMutationRow>, InternalError> {
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
        let write_context = dynamic_write_context(mode, operation_timestamp);
        let mut row_ops = Vec::with_capacity(mutations.len());
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
                        &patch,
                        write_context,
                    )?
                }
                (MutationMode::Update, Some(before)) => {
                    resolve_update_structural_patch_with_accepted_contract(
                        entity_path,
                        row_decode_contract.clone(),
                        before,
                        &patch,
                        write_context,
                    )?
                }
                (MutationMode::Replace, Some(before)) => {
                    resolve_existing_replace_structural_patch_with_accepted_contract(
                        entity_path,
                        row_decode_contract.clone(),
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
            validate_dynamic_after_image(
                self,
                entity_path,
                store_path,
                &data_key,
                after.as_raw_row(),
                provenance.as_slice(),
                write_context,
                row_decode_contract.clone(),
                catalog.fingerprint(),
                catalog.accepted_row_constraints(),
            )?;

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
            if physical_changed {
                row_ops.push(CommitRowOp::new(
                    entity_path,
                    raw_key,
                    canonical_before
                        .as_ref()
                        .map(|before| before.as_raw_row().as_bytes().to_vec()),
                    Some(after.as_raw_row().as_bytes().to_vec()),
                    catalog.fingerprint(),
                ));
                #[cfg(feature = "sql")]
                if largest_journaled_prefix
                    && !crate::db::commit::journaled_row_ops_fit_commit_window(row_ops.as_slice())
                {
                    let _ = row_ops.pop();
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

        if !row_ops.is_empty() {
            commit_structural_save_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                row_ops,
                "accepted_structural_batch_apply",
            )?;
        }
        Ok(output)
    }

    /// Execute one trusted entity-name-driven structural mutation.
    ///
    /// This lane resolves public values, defaults, generation, management,
    /// constraints, relations, and commit preparation from accepted schema.
    /// It never materializes a generated entity or invokes application
    /// validators/normalizers.
    #[expect(
        clippy::too_many_lines,
        reason = "the four public mutation variants converge here before entering the accepted batch owner"
    )]
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
        let operation_timestamp = Timestamp::now();

        if let DynamicMutation::Delete { key, .. } = request {
            let data_key = dynamic_key(identity.entity_tag(), key)?;
            let before = validated_existing_row(store, &data_key, &row_contract)?
                .ok_or_else(|| InternalError::store_not_found(&data_key))?;
            let raw_key = data_key.to_raw()?;
            self.db
                .validate_delete_relations(entity_path, &BTreeSet::from([raw_key.clone()]))?;
            let canonical_before = canonical_row_from_raw_row_with_accepted_decode_contract(
                entity_path,
                row_decode_contract,
                &before,
            )?;
            let marker = CommitRowOp::new(
                entity_path,
                raw_key,
                Some(canonical_before.as_raw_row().as_bytes().to_vec()),
                None,
                catalog.fingerprint(),
            );
            commit_delete_row_ops_with_window_for_path(
                &self.db,
                entity_path,
                vec![marker],
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
        let mut rows = self.execute_accepted_structural_save_batch(
            &catalog,
            &descriptor,
            mode,
            vec![AcceptedStructuralMutation::new(target, patch)],
            operation_timestamp,
        )?;
        let result = rows.pop().ok_or_else(InternalError::executor_invariant)?;
        record(MetricsEvent::SaveMutation {
            entity_path,
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
            entity: request.entity().to_string(),
            columns,
            rows: vec![row],
            affected_rows: u32::from(result.logical_changed()),
        })
    }

    /// Insert one entity row.
    pub fn insert<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_entity(|save| save.insert(entity))
    }

    /// Insert one authored typed input.
    pub fn create<I>(&self, input: I) -> Result<I::Entity, InternalError>
    where
        I: EntityCreateInput,
        I::Entity: PersistedRow<Canister = C>,
    {
        self.execute_save_entity(|save| save.create(input))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_batch(|save| save.insert_many_atomic(entities))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::insert_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_batch(|save| save.insert_many_non_atomic(entities))
    }

    /// Replace one existing entity row.
    pub fn replace<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_entity(|save| save.replace(entity))
    }

    /// Apply one structural mutation under one explicit write-mode contract.
    ///
    /// This is the public core session boundary for structural writes:
    /// callers provide the key, field patch, and intended mutation mode, and
    /// the session routes that through the shared structural mutation pipeline.
    #[cfg(test)]
    pub(in crate::db) fn mutate_structural<E>(
        &self,
        key: E::Key,
        patch: AuthoredStructuralPatch,
        mode: MutationMode,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        let context = self.accepted_schema_catalog_context_for_query::<E>()?;
        let (descriptor, _) = AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
            context.snapshot(),
            E::MODEL,
            context.enum_catalog(),
            context.composite_catalog(),
        )?;
        validate_structural_patch_schema_policy::<E>(&descriptor, &patch, mode)?;
        let (
            row_decode_contract,
            mutation_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
            accepted_row_constraints,
        ) = accepted_save_contract_for_catalog_context::<E>(&context, &descriptor);

        self.execute_save_with_checked_accepted_row_contract(
            row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
            accepted_row_constraints,
            |save| save.apply_structural_mutation(mode, key, patch, mutation_row_decode_contract),
            std::convert::identity,
        )
    }

    /// Build one structural patch through the accepted schema row layout.
    ///
    /// This is the session-owned patch construction boundary for callers that
    /// can provide all dynamic field updates at once. It resolves field names
    /// through the accepted row-layout descriptor before the patch reaches the
    /// generated-compatible write codec bridge.
    #[cfg(test)]
    pub(in crate::db) fn structural_patch<E, I, S, V>(
        &self,
        fields: I,
    ) -> Result<AuthoredStructuralPatch, InternalError>
    where
        E: PersistedRow<Canister = C>,
        I: IntoIterator<Item = (S, V)>,
        S: AsRef<str>,
        V: Into<InputValue>,
    {
        let context = self.accepted_schema_catalog_context_for_query::<E>()?;
        let (descriptor, _) = AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
            context.snapshot(),
            E::MODEL,
            context.enum_catalog(),
            context.composite_catalog(),
        )?;
        let mut patch = AuthoredStructuralPatch::new();

        // Phase 1: resolve every caller-provided field name against the
        // accepted descriptor so public structural patch construction no
        // longer has to choose slots from generated model field order.
        for (field_name, value) in fields {
            let field_name = field_name.as_ref();
            patch = append_accepted_structural_patch_field(
                E::PATH,
                &descriptor,
                patch,
                field_name,
                value.into(),
            )?;
        }

        Ok(patch)
    }

    /// Apply one structural replacement, inserting if missing.
    ///
    /// Replace semantics still do not inherit omitted fields from the old row.
    /// Missing fields must materialize through explicit defaults or managed
    /// field preflight, or the write fails closed.
    #[cfg(test)]
    pub(in crate::db) fn replace_structural<E>(
        &self,
        key: E::Key,
        patch: AuthoredStructuralPatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.mutate_structural(key, patch, MutationMode::Replace)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_batch(|save| save.replace_many_atomic(entities))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::replace_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_batch(|save| save.replace_many_non_atomic(entities))
    }

    /// Update one existing entity row.
    pub fn update<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_entity(|save| save.update(entity))
    }

    /// Apply one structural insert from a patch-defined after-image.
    ///
    /// Insert semantics no longer require a pre-built full row image.
    /// Missing fields still fail closed unless derive-owned materialization can
    /// supply them through explicit defaults or managed-field preflight.
    #[cfg(test)]
    pub(in crate::db) fn insert_structural<E>(
        &self,
        key: E::Key,
        patch: AuthoredStructuralPatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.mutate_structural(key, patch, MutationMode::Insert)
    }

    /// Apply one structural field patch to an existing entity row.
    ///
    /// This session-owned boundary keeps structural mutation out of the raw
    /// executor surface while still routing through the same typed save
    /// preflight before commit staging.
    #[cfg(test)]
    pub(in crate::db) fn update_structural<E>(
        &self,
        key: E::Key,
        patch: AuthoredStructuralPatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.mutate_structural(key, patch, MutationMode::Update)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_batch(|save| save.update_many_atomic(entities))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::update_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_save_batch(|save| save.update_many_non_atomic(entities))
    }
}

#[cfg(test)]
mod typed_adapter_tests {
    use super::{
        AcceptedFieldKind, DynamicTypedBindingError, DynamicTypedFieldType,
        dynamic_typed_field_type, typed_adapter_field_kind_matches,
    };
    use crate::types::EntityTag;
    use icydb_schema::ScalarType;

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
}
