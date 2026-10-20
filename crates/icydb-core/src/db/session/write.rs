//! Module: db::session::write
//! Responsibility: session-owned typed write APIs for insert, replace, update,
//! and structural mutation entrypoints over the shared save pipeline.
//! Does not own: commit staging, mutation execution, or persistence encoding.
//! Boundary: keeps public session write semantics above the executor save surface.

#[cfg(test)]
mod identity_pre_key_tests;
#[cfg(test)]
mod key_handoff_tests;
#[cfg(test)]
mod mixed_relation_batch_tests;
#[cfg(test)]
mod output_handoff_tests;
#[cfg(test)]
mod targeted_rule_mutation_tests;
#[cfg(test)]
mod typed_adapter_tests;

#[cfg(test)]
use crate::db::executor::mutation::structural::{
    AcceptedStructuralMutationPacking, AcceptedStructuralMutationStagedAdmission,
    MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES, add_structural_mutation_staged_bytes,
    admit_structural_mutation_staged_charge, data_key_from_validated_reader,
    insert_key_exists_after_generation, into_mutation_output_values,
    structural_mutation_staged_charge,
};
use crate::db::executor::{
    AcceptedSchemaCatalogContext,
    mutation::structural::{
        AcceptedStructuralMutation, AcceptedStructuralMutationBatchItem,
        AcceptedStructuralMutationCommitDirective, AcceptedStructuralMutationCommitOptions,
        AcceptedStructuralMutationRow, AcceptedStructuralMutationTarget,
        MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES, MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
        execute_accepted_structural_mutation_batch, mutation_diagnostic_context,
        validate_structural_mutation_result_bytes,
    },
};
#[cfg(any(feature = "sql", test))]
use crate::value::Value;

#[cfg(test)]
use crate::db::{
    data::{RawRow, StructuralRowContract, StructuralSlotReader},
    executor::mutation::structural::{
        MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES, checked_pre_key_candidate_count,
    },
    integrity::MutationProgressRecordOp,
    schema::FieldId,
};
use crate::{
    db::{
        DbSession, DynamicMutation, DynamicMutationResult, DynamicStructuralPatch,
        DynamicTypedBindingError, DynamicTypedEntityBinding, DynamicTypedMutation,
        DynamicTypedStructuralPatch, DynamicWriteCell, TypedEntityDescriptor, TypedFieldType,
        commit::database_incarnation_id,
        data::{AcceptedMutationIntentPatch, DecodedDataStoreKey, FieldSlot},
        schema::{
            AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
            lower_field_type, output_value_from_runtime,
        },
        write_context::MutationMode,
    },
    error::{InternalError, MutationDiagnosticContext},
    traits::CanisterKind,
    types::{CurrentTimestamp, Timestamp},
    value::InputValue,
};
use icydb_schema::{
    EntitySourceKey, FieldSourceKey, FieldType, SchemaContractError, TypeSourceKey,
};

fn dynamic_key(
    entity_tag: crate::types::EntityTag,
    key: InputValue,
) -> Result<DecodedDataStoreKey, InternalError> {
    let value = key
        .try_into_runtime_non_enum()
        .ok_or_else(InternalError::executor_unsupported)?;
    DecodedDataStoreKey::try_from_structural_key(entity_tag, &value)
}

fn lower_resolved_write_cell(
    lowered: AcceptedMutationIntentPatch,
    field: &AcceptedRowLayoutRuntimeField<'_>,
    cell: DynamicWriteCell,
    mode: MutationMode,
    mutation_context: MutationDiagnosticContext,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    // Insert DEFAULT requests select accepted database policy rather than
    // authoring a value. Keyed writes retain their existing ownership rules.
    if (matches!(cell, DynamicWriteCell::Null | DynamicWriteCell::Value(_))
        || (matches!(cell, DynamicWriteCell::Default) && !matches!(mode, MutationMode::Insert)))
        && (field.write_policy().insert_generation().is_some()
            || field.write_policy().write_management().is_some())
    {
        return Err(InternalError::mutation_database_owned_field_explicit(
            mutation_context,
            field.field_id().get(),
        ));
    }

    let slot = FieldSlot::from_validated_index(usize::from(field.slot().get()));
    Ok(match cell {
        DynamicWriteCell::Omitted => lowered,
        DynamicWriteCell::Default => match mode {
            MutationMode::Insert | MutationMode::Replace => {
                lowered.set_explicit_insert_default(slot)
            }
            MutationMode::Update => lowered.set_explicit_update_default(slot),
        },
        DynamicWriteCell::Null => lowered.set_authored(slot, InputValue::null()),
        DynamicWriteCell::Value(value) => lowered.set_authored(slot, value),
    })
}

fn lower_dynamic_patch(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: DynamicStructuralPatch,
    mode: MutationMode,
    mutation_context: MutationDiagnosticContext,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    let mut lowered = AcceptedMutationIntentPatch::new();
    for (field_name, cell) in patch.into_fields() {
        let slot = descriptor
            .field_slot_index_by_name(&field_name)
            .ok_or_else(InternalError::executor_unsupported)?;
        let field = descriptor
            .field_for_slot_index(slot)
            .ok_or_else(InternalError::executor_invariant)?;
        lowered = lower_resolved_write_cell(lowered, field, cell, mode, mutation_context)?;
    }
    Ok(lowered)
}

fn lower_dynamic_save_intent(
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: DynamicStructuralPatch,
    mode: MutationMode,
    target: AcceptedStructuralMutationTarget,
    batch_position: u32,
) -> Result<AcceptedStructuralMutation, InternalError> {
    Ok(AcceptedStructuralMutation::save(
        mode,
        target,
        lower_dynamic_patch(
            descriptor,
            patch,
            mode,
            mutation_diagnostic_context(catalog, mode, batch_position),
        )?,
    ))
}

fn lower_dynamic_mutation_intent(
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    request: DynamicMutation,
    batch_position: u32,
) -> Result<AcceptedStructuralMutation, InternalError> {
    let entity_tag = catalog.identity().entity_tag();
    match request {
        DynamicMutation::Insert { patch, .. } => lower_dynamic_save_intent(
            catalog,
            descriptor,
            patch,
            MutationMode::Insert,
            AcceptedStructuralMutationTarget::ResolveFromAfterImage,
            batch_position,
        ),
        DynamicMutation::Update { key, patch, .. } => lower_dynamic_save_intent(
            catalog,
            descriptor,
            patch,
            MutationMode::Update,
            AcceptedStructuralMutationTarget::expected(dynamic_key(entity_tag, key)?),
            batch_position,
        ),
        DynamicMutation::Replace { key, patch, .. } => lower_dynamic_save_intent(
            catalog,
            descriptor,
            patch,
            MutationMode::Replace,
            AcceptedStructuralMutationTarget::expected(dynamic_key(entity_tag, key)?),
            batch_position,
        ),
        DynamicMutation::Delete { key, .. } => Ok(AcceptedStructuralMutation::delete(dynamic_key(
            entity_tag, key,
        )?)),
    }
}

fn lower_typed_patch(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    binding: &DynamicTypedEntityBinding,
    patch: DynamicTypedStructuralPatch,
    mode: MutationMode,
    mutation_context: MutationDiagnosticContext,
) -> Result<AcceptedMutationIntentPatch, InternalError> {
    let mut lowered = AcceptedMutationIntentPatch::new();
    for (descriptor_ordinal, cell) in patch.into_fields() {
        let (field_id, slot) = binding
            .field_identity_binding(descriptor_ordinal)
            .ok_or_else(InternalError::store_invariant)?;
        let slot_index = usize::from(slot);
        let field = descriptor
            .field_for_slot_index(slot_index)
            .ok_or_else(InternalError::store_invariant)?;
        if field.field_id().get() != field_id {
            return Err(InternalError::store_invariant());
        }
        lowered = lower_resolved_write_cell(lowered, field, cell, mode, mutation_context)?;
    }
    Ok(lowered)
}

fn lower_typed_mutation_intent(
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    binding: &DynamicTypedEntityBinding,
    request: DynamicTypedMutation,
    batch_position: u32,
) -> Result<Option<AcceptedStructuralMutation>, InternalError> {
    let entity_tag = catalog.identity().entity_tag();
    let (mode, target, patch) = match request {
        DynamicTypedMutation::Insert { patch } => (
            MutationMode::Insert,
            AcceptedStructuralMutationTarget::ResolveFromAfterImage,
            patch,
        ),
        DynamicTypedMutation::Update { key, patch } => (
            MutationMode::Update,
            AcceptedStructuralMutationTarget::expected(dynamic_key(entity_tag, key)?),
            patch,
        ),
        DynamicTypedMutation::Replace { key, patch } => (
            MutationMode::Replace,
            AcceptedStructuralMutationTarget::expected(dynamic_key(entity_tag, key)?),
            patch,
        ),
        DynamicTypedMutation::Delete { key } => {
            return Ok(Some(AcceptedStructuralMutation::delete(dynamic_key(
                entity_tag, key,
            )?)));
        }
    };
    if !patch.is_bound_to(binding) {
        return Ok(None);
    }
    let patch = lower_typed_patch(
        descriptor,
        binding,
        patch,
        mode,
        mutation_diagnostic_context(catalog, mode, batch_position),
    )?;
    Ok(Some(AcceptedStructuralMutation::save(mode, target, patch)))
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
                .into_iter()
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
        let encoded = icydb_schema::wire::encode_one(&result)
            .map_err(|_| InternalError::executor_invariant())?;
        validate_structural_mutation_result_bytes(encoded.len())?;
    }
    Ok(result)
}

fn typed_descriptor_field_type(
    field_type: TypedFieldType,
) -> Result<FieldType, SchemaContractError> {
    match field_type {
        TypedFieldType::Scalar(scalar) => Ok(FieldType::Scalar(scalar)),
        TypedFieldType::List(item) => Ok(FieldType::List(Box::new(typed_descriptor_field_type(
            *item,
        )?))),
        TypedFieldType::Named(source_key) => {
            TypeSourceKey::try_new(source_key.to_string()).map(FieldType::Named)
        }
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
        descriptor: &TypedEntityDescriptor,
    ) -> Result<DynamicTypedEntityBinding, DynamicTypedBindingError> {
        let unavailable = |field_source| {
            DynamicTypedBindingError::source_unavailable(descriptor.entity_source_key, field_source)
        };
        let entity_source = EntitySourceKey::try_new(descriptor.entity_source_key)
            .map_err(|_| unavailable(None))?;
        let catalog = self
            .find_accepted_schema_catalog_context_for_entity_source_key(entity_source.as_str())?
            .ok_or_else(|| unavailable(None))?;
        let identity = catalog.identity();
        if identity.entity_path() != entity_source.as_str() {
            return Err(InternalError::store_invariant().into());
        }
        let store = self.db.recovered_store(identity.store_path())?;
        // Binding issuance only projects owned adapter data. Borrow the verified
        // authority in place instead of cloning every entity's schema bundle;
        // release the borrow before the returned binding can execute or mutate.
        store.with_schema(|schema| {
            let bundle = schema
                .borrow_accepted_schema_bundle_for_authority(
                    catalog.value_catalog_handle().authority(),
                )?
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
            if descriptor.primary_key_source_keys.len() != snapshot.primary_key_field_ids().len() {
                return Err(DynamicTypedBindingError::IncompatibleField);
            }
            for (source_key, accepted_field_id) in descriptor
                .primary_key_source_keys
                .iter()
                .zip(snapshot.primary_key_field_ids())
            {
                let source = FieldSourceKey::try_new((*source_key).to_string())
                    .map_err(|_| unavailable(Some(*source_key)))?;
                let descriptor_field_id = bundle
                    .source_bindings()
                    .field(entity_tag, &source)
                    .ok_or_else(|| unavailable(Some(*source_key)))?;
                if descriptor_field_id != *accepted_field_id {
                    return Err(DynamicTypedBindingError::IncompatibleField);
                }
            }
            let row_contract = catalog.inspection_plan().row_contract();
            let mut fields = Vec::with_capacity(descriptor.fields.len());
            for field_descriptor in descriptor.fields {
                let source = FieldSourceKey::try_new(field_descriptor.source_key.to_string())
                    .map_err(|_| unavailable(Some(field_descriptor.source_key)))?;
                let field_id = bundle
                    .source_bindings()
                    .field(entity_tag, &source)
                    .ok_or_else(|| unavailable(Some(field_descriptor.source_key)))?;
                let field = snapshot
                    .fields()
                    .iter()
                    .find(|field| field.id() == field_id)
                    .ok_or_else(InternalError::store_invariant)?;
                let runtime_field = row_contract
                    .required_accepted_field_contract(usize::from(field.slot().get()))?;
                if runtime_field.field_id() != field_id {
                    return Err(InternalError::store_invariant().into());
                }
                let field_type = typed_descriptor_field_type(field_descriptor.field_type)
                    .map_err(|_| unavailable(Some(field_descriptor.source_key)))?;
                let expected_kind = lower_field_type(&field_type, bundle.source_bindings())
                    .map_err(|_| DynamicTypedBindingError::IncompatibleField)?;
                if field.nullable() != field_descriptor.nullable
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
        })
    }

    pub(in crate::db::session) fn current_typed_entity_binding_catalog(
        &self,
        binding: &DynamicTypedEntityBinding,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        // Select this session's commit domain before inspecting its identity.
        // The checked value is local to this synchronous validation, not the
        // binding lifetime; catalog lookup and matching retain their live checks.
        self.db.ensure_recovered_state()?;
        let incarnation = database_incarnation_id()?.to_bytes();
        if incarnation != binding.database_incarnation {
            return Ok(None);
        }
        let Some(catalog) = self.find_accepted_schema_catalog_context_for_entity_source_key(
            binding.entity_source.as_str(),
        )?
        else {
            return Ok(None);
        };
        self.typed_entity_binding_matches_catalog(binding, &catalog, incarnation)
            .map(|current| current.then_some(catalog))
    }

    fn typed_entity_binding_matches_catalog(
        &self,
        binding: &DynamicTypedEntityBinding,
        catalog: &AcceptedSchemaCatalogContext,
        incarnation: [u8; 16],
    ) -> Result<bool, InternalError> {
        if incarnation != binding.database_incarnation {
            return Ok(false);
        }
        let row_contract = catalog.inspection_plan().row_contract();
        let identity = catalog.identity();
        if identity.entity_path() != binding.entity_source.as_str()
            || identity.entity_tag().value() != binding.entity_tag
            || catalog.revision().get() != binding.accepted_revision
            || catalog.fingerprint() != binding.accepted_fingerprint
            || row_contract.current_layout_version().get() != binding.entity_generation
        {
            return Ok(false);
        }
        let entity_source = EntitySourceKey::try_new(binding.entity_source.clone())
            .map_err(|_| InternalError::store_invariant())?;
        let store = self.db.recovered_store(identity.store_path())?;
        // Only inspect the bundle here; keep its schema-owned validation and
        // release the borrow before the caller can prepare or commit writes.
        store.with_schema(|schema| {
            let bundle = schema
                .borrow_accepted_schema_bundle_for_authority(
                    catalog.value_catalog_handle().authority(),
                )?
                .ok_or_else(InternalError::store_invariant)?;
            if bundle.revision() != catalog.revision()
                || bundle.source_bindings().entity(&entity_source) != Some(identity.entity_tag())
            {
                return Ok(false);
            }
            let snapshot = bundle
                .entity_snapshots()
                .get(&identity.entity_tag())
                .ok_or_else(InternalError::store_invariant)?;
            for (source_key, expected_field_id, expected_slot) in binding.field_identity_bindings()
            {
                let source = FieldSourceKey::try_new(source_key)
                    .map_err(|_| InternalError::store_invariant())?;
                let Some(field_id) = bundle
                    .source_bindings()
                    .field(identity.entity_tag(), &source)
                else {
                    return Ok(false);
                };
                let Some(field) = snapshot
                    .fields()
                    .iter()
                    .find(|field| field.id() == field_id)
                else {
                    return Err(InternalError::store_invariant());
                };
                if field_id.get() != expected_field_id || field.slot().get() != expected_slot {
                    return Ok(false);
                }
            }

            Ok(true)
        })
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
        capture_output_values: bool,
        keys: Vec<DecodedDataStoreKey>,
        precommit_validation: impl FnOnce(&[Vec<Value>]) -> Result<(), InternalError>,
    ) -> Result<Vec<Vec<Value>>, InternalError> {
        let mutations = keys
            .into_iter()
            .map(AcceptedStructuralMutation::delete)
            .collect::<Vec<_>>();
        let mutation_capacity = mutations.len();
        let mut mutations = mutations.into_iter();
        execute_accepted_structural_mutation_batch(
            &self.db,
            catalog,
            mutation_capacity,
            0,
            || {
                Ok(mutations
                    .next()
                    .map(|mutation| AcceptedStructuralMutationBatchItem {
                        catalog: catalog.clone(),
                        mutation,
                    }))
            },
            Timestamp::now(),
            AcceptedStructuralMutationCommitOptions::standard(capture_output_values),
            |rows, _report| {
                let rows = rows
                    .into_iter()
                    .map(AcceptedStructuralMutationRow::into_values)
                    .collect::<Vec<_>>();
                precommit_validation(rows.as_slice())?;
                Ok((rows, AcceptedStructuralMutationCommitDirective::Standard))
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
    /// Count-only callers omit result values, never row validation or constraints.
    pub(in crate::db::session) fn execute_accepted_structural_save_batch<T>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        capture_output_values: bool,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        precommit_preparation: impl FnOnce(
            Vec<AcceptedStructuralMutationRow>,
        ) -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        let mutation_capacity = mutations.len();
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
        let mut mutations = mutations.into_iter();
        execute_accepted_structural_mutation_batch(
            &self.db,
            catalog,
            mutation_capacity,
            identity_candidate_count,
            || {
                Ok(mutations
                    .next()
                    .map(|mutation| AcceptedStructuralMutationBatchItem {
                        catalog: catalog.clone(),
                        mutation,
                    }))
            },
            operation_timestamp,
            AcceptedStructuralMutationCommitOptions::standard(capture_output_values),
            |rows, _report| {
                precommit_preparation(rows).map(|prepared| {
                    (
                        prepared,
                        AcceptedStructuralMutationCommitDirective::Standard,
                    )
                })
            },
        )
    }

    /// Commit one complete accepted update page and its exact durable progress successor.
    #[cfg(test)]
    pub(in crate::db::session) fn execute_accepted_structural_update_with_mutation_progress(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        mutations: Vec<AcceptedStructuralMutation>,
        operation_timestamp: Timestamp,
        mutation_progress: MutationProgressRecordOp,
    ) -> Result<usize, InternalError> {
        let mutation_capacity = mutations.len();
        let mut mutations = mutations.into_iter();
        execute_accepted_structural_mutation_batch(
            &self.db,
            catalog,
            mutation_capacity,
            0,
            || {
                Ok(mutations
                    .next()
                    .map(|mutation| AcceptedStructuralMutationBatchItem {
                        catalog: catalog.clone(),
                        mutation,
                    }))
            },
            operation_timestamp,
            AcceptedStructuralMutationCommitOptions::with_mutation_progress(),
            |rows, _report| {
                Ok((
                    rows.len(),
                    AcceptedStructuralMutationCommitDirective::WithMutationProgress(
                        mutation_progress,
                    ),
                ))
            },
        )
    }

    fn execute_lowered_dynamic_mutation_batch(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        mutations: Vec<AcceptedStructuralMutation>,
        enforce_mixed_batch_result_bound: bool,
    ) -> Result<DynamicMutationResult, InternalError> {
        self.execute_accepted_structural_save_batch(
            catalog,
            true,
            mutations,
            Timestamp::now(),
            |rows| {
                prepare_dynamic_mutation_result(
                    catalog,
                    descriptor,
                    rows,
                    enforce_mixed_batch_result_bound,
                )
            },
        )
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
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let mutation = lower_dynamic_mutation_intent(&catalog, &descriptor, request.clone(), 0)?;

        self.execute_lowered_dynamic_mutation_batch(&catalog, &descriptor, vec![mutation], false)
    }

    /// Execute one bounded same-store structural mutation batch atomically.
    ///
    /// Every item resolves from one captured accepted root and store, shares
    /// one operation timestamp, and is projected to its public result before
    /// the commit marker can be published.
    pub fn execute_trusted_dynamic_mutation_batch(
        &self,
        requests: Vec<DynamicMutation>,
    ) -> Result<Vec<DynamicMutationResult>, InternalError> {
        self.execute_trusted_dynamic_mutation_batch_mixed(requests)
    }

    fn execute_trusted_dynamic_mutation_batch_mixed(
        &self,
        requests: Vec<DynamicMutation>,
    ) -> Result<Vec<DynamicMutationResult>, InternalError> {
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
        let anchor_catalog =
            self.accepted_schema_catalog_context_for_entity_name(Some(first.entity()))?;
        let anchor_identity = anchor_catalog.identity();
        let mut entity_tags = std::collections::BTreeSet::new();
        let mut items = Vec::with_capacity(requests.len());
        let mut result_catalogs = Vec::with_capacity(requests.len());
        let mut identity_candidate_count = 0_usize;

        let request_count = requests.len();
        for (batch_position, request) in requests.into_iter().enumerate() {
            let batch_position = u32::try_from(batch_position).map_err(|_| {
                InternalError::mutation_batch_too_many_items(
                    request_count,
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
                )
            })?;
            if request.entity().is_empty() {
                return Err(InternalError::executor_unsupported());
            }
            let item_catalog = anchor_catalog
                .for_entity_name(request.entity())
                .ok_or_else(|| InternalError::unsupported_entity_path(request.entity()))?;
            let item_identity = item_catalog.identity();
            if item_identity.store_path() != anchor_identity.store_path() {
                return Err(InternalError::mutation_batch_store_mismatch(
                    batch_position,
                    anchor_identity.entity_tag().value(),
                    item_identity.entity_tag().value(),
                ));
            }
            entity_tags.insert(item_identity.entity_tag());
            if entity_tags.len() > MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES {
                return Err(InternalError::mutation_batch_too_many_entities(
                    entity_tags.len(),
                    MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES,
                ));
            }
            let descriptor =
                AcceptedRowLayoutRuntimeContract::from_accepted_schema(item_catalog.snapshot())?;
            let mutation =
                lower_dynamic_mutation_intent(&item_catalog, &descriptor, request, batch_position)?;
            if matches!(
                mutation,
                AcceptedStructuralMutation::Save {
                    mode: MutationMode::Insert,
                    target: AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                    ..
                }
            ) {
                identity_candidate_count = identity_candidate_count.saturating_add(1);
            }
            result_catalogs.push(item_catalog.clone());
            items.push(AcceptedStructuralMutationBatchItem {
                catalog: item_catalog,
                mutation,
            });
        }

        self.execute_lowered_mixed_mutation_batch(
            &anchor_catalog,
            items,
            result_catalogs,
            identity_candidate_count,
        )
    }

    fn execute_lowered_mixed_mutation_batch(
        &self,
        anchor_catalog: &AcceptedSchemaCatalogContext,
        items: Vec<AcceptedStructuralMutationBatchItem>,
        result_catalogs: Vec<AcceptedSchemaCatalogContext>,
        identity_candidate_count: usize,
    ) -> Result<Vec<DynamicMutationResult>, InternalError> {
        if items.len() != result_catalogs.len() {
            return Err(InternalError::executor_invariant());
        }
        let mutation_count = items.len();
        let mut items = items.into_iter();
        execute_accepted_structural_mutation_batch(
            &self.db,
            anchor_catalog,
            mutation_count,
            identity_candidate_count,
            || Ok(items.next()),
            Timestamp::now(),
            AcceptedStructuralMutationCommitOptions::standard(true),
            |rows, _report| {
                if rows.len() != result_catalogs.len() {
                    return Err(InternalError::executor_invariant());
                }
                let mut results = Vec::with_capacity(rows.len());
                for (row, catalog) in rows.into_iter().zip(result_catalogs.iter()) {
                    let descriptor =
                        AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
                    results.push(prepare_dynamic_mutation_result(
                        catalog,
                        &descriptor,
                        vec![row],
                        false,
                    )?);
                }
                let encoded = icydb_schema::wire::encode_one(&results)
                    .map_err(|_| InternalError::executor_invariant())?;
                validate_structural_mutation_result_bytes(encoded.len())?;
                Ok((results, AcceptedStructuralMutationCommitDirective::Standard))
            },
        )
    }

    /// Execute one generated typed write through immutable accepted entity and
    /// field identities. `None` means the opaque binding is stale.
    #[doc(hidden)]
    pub fn execute_trusted_typed_mutation(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: DynamicTypedMutation,
    ) -> Result<Option<DynamicMutationResult>, InternalError> {
        let Some(catalog) = self.current_typed_entity_binding_catalog(binding)? else {
            return Ok(None);
        };
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let Some(mutation) =
            lower_typed_mutation_intent(&catalog, &descriptor, binding, request, 0)?
        else {
            return Ok(None);
        };
        self.execute_lowered_dynamic_mutation_batch(&catalog, &descriptor, vec![mutation], false)
            .map(Some)
    }

    /// Execute one bounded same-entity generated typed-write batch through one
    /// exact current binding. `None` means the binding or a patch is stale or
    /// mismatched.
    #[doc(hidden)]
    pub fn execute_trusted_same_entity_typed_mutation_batch(
        &self,
        binding: &DynamicTypedEntityBinding,
        requests: Vec<DynamicTypedMutation>,
    ) -> Result<Option<DynamicMutationResult>, InternalError> {
        if requests.is_empty() {
            return Err(InternalError::mutation_batch_empty());
        }
        if requests.len() > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items(
                requests.len(),
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
            ));
        }
        let Some(catalog) = self.current_typed_entity_binding_catalog(binding)? else {
            return Ok(None);
        };
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let mut mutations = Vec::with_capacity(requests.len());
        let request_count = requests.len();
        for (batch_position, request) in requests.into_iter().enumerate() {
            let batch_position = u32::try_from(batch_position).map_err(|_| {
                InternalError::mutation_batch_too_many_items(
                    request_count,
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
                )
            })?;
            let Some(mutation) = lower_typed_mutation_intent(
                &catalog,
                &descriptor,
                binding,
                request,
                batch_position,
            )?
            else {
                return Ok(None);
            };
            mutations.push(mutation);
        }

        self.execute_lowered_dynamic_mutation_batch(&catalog, &descriptor, mutations, true)
            .map(Some)
    }

    /// Execute one bounded generated typed-write batch atomically through
    /// exact current same-store bindings. `None` means a binding or patch is
    /// stale or mismatched.
    #[doc(hidden)]
    pub fn execute_trusted_typed_mutation_batch(
        &self,
        requests: Vec<(DynamicTypedEntityBinding, DynamicTypedMutation)>,
    ) -> Result<Option<Vec<DynamicMutationResult>>, InternalError> {
        if requests.is_empty() {
            return Err(InternalError::mutation_batch_empty());
        }
        if requests.len() > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items(
                requests.len(),
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
            ));
        }
        let first_binding = requests
            .first()
            .map(|(binding, _)| binding)
            .ok_or_else(InternalError::mutation_batch_empty)?;
        let Some(catalog) = self.current_typed_entity_binding_catalog(first_binding)? else {
            return Ok(None);
        };
        let anchor_identity = catalog.identity();
        let mut entity_tags = std::collections::BTreeSet::new();
        let mut items = Vec::with_capacity(requests.len());
        let mut result_catalogs = Vec::with_capacity(requests.len());
        let mut identity_candidate_count = 0_usize;

        let request_count = requests.len();
        for (batch_position, (binding, request)) in requests.into_iter().enumerate() {
            let batch_position = u32::try_from(batch_position).map_err(|_| {
                InternalError::mutation_batch_too_many_items(
                    request_count,
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
                )
            })?;
            let Some(item_catalog) = catalog.for_entity_path(binding.entity_source.as_str()) else {
                return Ok(None);
            };
            if !self.typed_entity_binding_matches_catalog(
                &binding,
                &item_catalog,
                database_incarnation_id()?.to_bytes(),
            )? {
                return Ok(None);
            }
            let item_identity = item_catalog.identity();
            if item_identity.store_path() != anchor_identity.store_path() {
                return Err(InternalError::mutation_batch_store_mismatch(
                    batch_position,
                    anchor_identity.entity_tag().value(),
                    item_identity.entity_tag().value(),
                ));
            }
            entity_tags.insert(item_identity.entity_tag());
            if entity_tags.len() > MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES {
                return Err(InternalError::mutation_batch_too_many_entities(
                    entity_tags.len(),
                    MAX_STRUCTURAL_MUTATION_BATCH_ENTITIES,
                ));
            }
            let descriptor =
                AcceptedRowLayoutRuntimeContract::from_accepted_schema(item_catalog.snapshot())?;
            let Some(mutation) = lower_typed_mutation_intent(
                &item_catalog,
                &descriptor,
                &binding,
                request,
                batch_position,
            )?
            else {
                return Ok(None);
            };
            if matches!(
                mutation,
                AcceptedStructuralMutation::Save {
                    mode: MutationMode::Insert,
                    target: AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                    ..
                }
            ) {
                identity_candidate_count = identity_candidate_count.saturating_add(1);
            }
            result_catalogs.push(item_catalog.clone());
            items.push(AcceptedStructuralMutationBatchItem {
                catalog: item_catalog,
                mutation,
            });
        }

        self.execute_lowered_mixed_mutation_batch(
            &catalog,
            items,
            result_catalogs,
            identity_candidate_count,
        )
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
        let patch_count = patches.len();
        if patch_count == 0 {
            return Err(InternalError::mutation_batch_empty());
        }
        if patch_count > MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS {
            return Err(InternalError::mutation_batch_too_many_items(
                patch_count,
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
            ));
        }
        if entity.is_empty() {
            return Err(InternalError::executor_unsupported());
        }
        let catalog = self.accepted_schema_catalog_context_for_entity_name(Some(entity))?;
        let descriptor =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())?;
        let mut mutations = Vec::with_capacity(patch_count);
        // The batch already names one entity. Lower each patch against that
        // captured authority without constructing or resolving per-row names.
        for (batch_position, patch) in patches.into_iter().enumerate() {
            let batch_position = u32::try_from(batch_position).map_err(|_| {
                InternalError::mutation_batch_too_many_items(
                    patch_count,
                    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS,
                )
            })?;
            mutations.push(lower_dynamic_save_intent(
                &catalog,
                &descriptor,
                patch,
                MutationMode::Insert,
                AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                batch_position,
            )?);
        }

        self.execute_lowered_dynamic_mutation_batch(&catalog, &descriptor, mutations, false)
    }
}
