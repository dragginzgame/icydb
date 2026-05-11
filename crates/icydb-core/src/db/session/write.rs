//! Module: db::session::write
//! Responsibility: session-owned typed write APIs for insert, replace, update,
//! and structural mutation entrypoints over the shared save pipeline.
//! Does not own: commit staging, mutation execution, or persistence encoding.
//! Boundary: keeps public session write semantics above the executor save surface.

use crate::{
    db::{
        DbSession, PersistedRow, WriteBatchResponse,
        data::{FieldSlot, StructuralPatch},
        executor::MutationMode,
        schema::{
            AcceptedFieldAbsencePolicy, AcceptedRowLayoutRuntimeDescriptor, SchemaInfo,
            accepted_commit_schema_fingerprint,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityCreateInput, EntityValue},
    value::Value,
};

// Append one session-resolved structural field update. The caller passes the
// accepted runtime descriptor that already crossed schema reconciliation, so
// field-name lookup follows persisted row-layout metadata rather than generated
// declaration order.
fn append_accepted_structural_patch_field(
    entity_path: &'static str,
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    patch: StructuralPatch,
    field_name: &str,
    value: Value,
) -> Result<StructuralPatch, InternalError> {
    let slot = descriptor
        .field_slot_index_by_name(field_name)
        .ok_or_else(|| InternalError::mutation_structural_field_unknown(entity_path, field_name))?;

    Ok(patch.set(FieldSlot::from_validated_index(slot), value))
}

// Enforce public structural patch policy before the executor materializes an
// entity through generated derive code. This keeps database write ownership and
// absence/default policy owned by accepted schema metadata instead of
// accidentally relying on executor-local generated field metadata, Rust
// construction defaults, or derive-local missing slot behavior.
fn validate_structural_patch_schema_policy<E>(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    patch: &StructuralPatch,
    mode: MutationMode,
) -> Result<(), InternalError>
where
    E: PersistedRow + EntityValue,
{
    reject_explicit_generated_fields_from_accepted_patch::<E>(descriptor, patch)?;

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

    // Every omitted field must be allowed by accepted schema absence policy.
    // Future database defaults should extend `AcceptedFieldAbsencePolicy`; this
    // check must not inspect `Default` impls or generated construction values.
    for field in descriptor.fields() {
        let slot = usize::from(field.slot().get());
        if provided_slots.get(slot).copied().unwrap_or(false) {
            continue;
        }

        if matches!(field.absence_policy(), AcceptedFieldAbsencePolicy::Required) {
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

// Preserve generated-field ownership diagnostics ahead of sparse-patch
// required-field diagnostics. Public structural writes must not author fields
// whose values are owned by accepted schema write policy, except for the
// redundant primary-key slot because the structural API already carries the
// authoritative key separately.
fn reject_explicit_generated_fields_from_accepted_patch<E>(
    descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>,
    patch: &StructuralPatch,
) -> Result<(), InternalError>
where
    E: PersistedRow + EntityValue,
{
    for entry in patch.entries() {
        let slot = entry.slot().index();
        let Some(accepted_field) = descriptor.field_for_slot_index(slot) else {
            continue;
        };
        let write_policy = accepted_field.write_policy();

        if write_policy.insert_generation().is_some()
            && accepted_field.name() != descriptor.primary_key_name()
        {
            return Err(InternalError::mutation_generated_field_explicit(
                E::PATH,
                accepted_field.name(),
            ));
        }
    }

    Ok(())
}

impl<C: CanisterKind> DbSession<C> {
    /// Insert one entity row.
    pub fn insert<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.insert(entity))
    }

    /// Insert one authored typed input.
    pub fn create<I>(&self, input: I) -> Result<I::Entity, InternalError>
    where
        I: EntityCreateInput,
        I::Entity: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.create(input))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_atomic(entities))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_non_atomic(entities))
    }

    /// Replace one existing entity row.
    pub fn replace<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.replace(entity))
    }

    /// Apply one structural mutation under one explicit write-mode contract.
    ///
    /// This is the public core session boundary for structural writes:
    /// callers provide the key, field patch, and intended mutation mode, and
    /// the session routes that through the shared structural mutation pipeline.
    pub fn mutate_structural<E>(
        &self,
        key: E::Key,
        patch: StructuralPatch,
        mode: MutationMode,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let accepted_schema = self.ensure_accepted_schema_snapshot::<E>()?;
        let (descriptor, _) = AcceptedRowLayoutRuntimeDescriptor::from_generated_compatible_schema(
            &accepted_schema,
            E::MODEL,
        )?;
        validate_structural_patch_schema_policy::<E>(&descriptor, &patch, mode)?;
        let accepted_schema_info =
            SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, &accepted_schema);
        let accepted_schema_fingerprint = accepted_commit_schema_fingerprint(&accepted_schema)?;

        let row_decode_contract = descriptor.row_decode_contract();
        let mutation_row_decode_contract = row_decode_contract.clone();

        self.execute_save_with_checked_accepted_row_contract(
            row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
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
    pub fn structural_patch<E, I, S>(&self, fields: I) -> Result<StructuralPatch, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
        I: IntoIterator<Item = (S, Value)>,
        S: AsRef<str>,
    {
        let accepted_schema = self.ensure_accepted_schema_snapshot::<E>()?;
        let (descriptor, _) = AcceptedRowLayoutRuntimeDescriptor::from_generated_compatible_schema(
            &accepted_schema,
            E::MODEL,
        )?;
        let mut patch = StructuralPatch::new();

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
                value,
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
        patch: StructuralPatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.mutate_structural(key, patch, MutationMode::Replace)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_atomic(entities))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_non_atomic(entities))
    }

    /// Update one existing entity row.
    pub fn update<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
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
        patch: StructuralPatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
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
        patch: StructuralPatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.mutate_structural(key, patch, MutationMode::Update)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_atomic(entities))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_non_atomic(entities))
    }
}
