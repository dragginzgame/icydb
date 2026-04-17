use super::{MutationMode, SaveExecutor};

use crate::{
    db::{
        commit::{
            CommitRowOp,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        },
        data::{CanonicalRow, DataKey, PersistedRow, RawRow, SerializedUpdatePatch, UpdatePatch},
        executor::mutation::{MutationInput, emit_index_delta_metrics, mutation_write_context},
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    sanitize::SanitizeWriteContext,
    traits::{EntityValue, FieldValue, Storable},
    types::Timestamp,
};

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Build one canonical write preflight context for one structural save mode.
    const fn structural_write_context(mode: MutationMode, now: Timestamp) -> SanitizeWriteContext {
        SanitizeWriteContext::new(mode.sanitize_write_mode(), now)
    }

    // Run one structural key + patch mutation under one explicit save-mode contract.
    pub(in crate::db) fn apply_structural_mutation(
        &self,
        mode: MutationMode,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError> {
        let write_context = Self::structural_write_context(mode, Timestamp::now());

        self.apply_structural_mutation_with_write_context(mode, key, patch, write_context)
    }

    pub(in crate::db) fn apply_structural_mutation_with_write_context(
        &self,
        mode: MutationMode,
        key: E::Key,
        patch: UpdatePatch,
        write_context: SanitizeWriteContext,
    ) -> Result<E, InternalError> {
        let mutation = MutationInput::from_update_patch::<E>(key, &patch)?;

        self.save_structural_mutation(mode, mutation, Some(&patch), write_context)
    }

    // Apply one structurally staged mutation whose patch was synthesized by an
    // internal write lane instead of authored directly by a public caller.
    pub(in crate::db) fn apply_internal_structural_mutation_with_write_context(
        &self,
        mode: MutationMode,
        key: E::Key,
        patch: UpdatePatch,
        write_context: SanitizeWriteContext,
    ) -> Result<E, InternalError> {
        let mutation = MutationInput::from_update_patch::<E>(key, &patch)?;

        self.save_structural_mutation(mode, mutation, None, write_context)
    }

    fn save_structural_mutation(
        &self,
        mode: MutationMode,
        mutation: MutationInput,
        authored_patch: Option<&UpdatePatch>,
        write_context: SanitizeWriteContext,
    ) -> Result<E, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = mutation_write_context::<E>(&self.db)?;
        let data_key = mutation.data_key().clone();
        let old_raw = Self::resolve_existing_row_for_rule(&ctx, &data_key, mode.save_rule())?;

        // Phase 0: reject authored values for insert-generated fields on every
        // public structural lane. The one structural exception is the primary
        // key slot: public structural writes already carry the authoritative
        // key out of band, so a matching generated primary-key payload in the
        // patch is redundant identity wiring rather than a second generated
        // value source.
        if let Some(authored_patch) = authored_patch {
            Self::reject_explicit_generated_fields(authored_patch)?;
        }

        // Phase 1: materialize and preflight the structural after-image under
        // the same save contract as typed writes.
        let entity = match mode {
            MutationMode::Update => {
                let raw_after_image =
                    Self::build_structural_after_image_row(mode, &mutation, old_raw.as_ref())?;
                self.validate_structural_after_image(&data_key, &raw_after_image, write_context)?
            }
            MutationMode::Insert | MutationMode::Replace => self
                .validate_structural_after_image_from_patch(
                    &data_key,
                    mutation.serialized_slots(),
                    write_context,
                )?,
        };

        // Phase 2: restage the normalized typed entity as one complete slot
        // image so commit preparation still sees the final canonical row.
        let normalized_mutation = MutationInput::from_entity(&entity)?;
        let row_bytes =
            Self::build_structural_after_image_row(mode, &normalized_mutation, old_raw.as_ref())?;
        let row_bytes = row_bytes.into_raw_row().into_bytes();
        let before_bytes = old_raw.map(<RawRow as Storable>::into_bytes);
        let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
        let marker_row_op = CommitRowOp::new(
            E::PATH,
            data_key.to_raw()?,
            before_bytes,
            Some(row_bytes),
            schema_fingerprint,
        );
        let prepared_row_op =
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
                &self.db,
                &marker_row_op,
                &ctx,
                &ctx,
                schema_fingerprint,
            )?;

        Self::commit_prepared_single_row(
            &self.db,
            marker_row_op,
            prepared_row_op,
            |delta| emit_index_delta_metrics::<E>(delta),
            || {
                span.set_rows(1);
            },
        )?;

        Ok(entity)
    }

    // Reject structural patches that try to author schema insert-generated
    // fields directly. Public structural writes must not bypass system-owned
    // generation on create or later rewrites, except for the redundant primary
    // key slot because the structural API already carries the authoritative
    // key separately.
    fn reject_explicit_generated_fields(patch: &UpdatePatch) -> Result<(), InternalError> {
        for entry in patch.entries() {
            let field = &E::MODEL.fields()[entry.slot().index()];
            if field.insert_generation().is_some() && field.name() != E::MODEL.primary_key.name() {
                return Err(InternalError::mutation_generated_field_explicit(
                    E::PATH,
                    field.name(),
                ));
            }
        }

        Ok(())
    }

    // Build the final persisted after-image under one explicit structural mode.
    // Sparse insert/replace no longer routes through this helper before
    // preflight; only the final normalized row image crosses here.
    fn build_structural_after_image_row(
        mode: MutationMode,
        mutation: &MutationInput,
        old_row: Option<&RawRow>,
    ) -> Result<CanonicalRow, InternalError> {
        match mode {
            MutationMode::Update => {
                let Some(old_row) = old_row else {
                    return Err(InternalError::executor_invariant(
                        "structural update staging requires an existing baseline row",
                    ));
                };

                old_row.apply_serialized_update_patch(E::MODEL, mutation.serialized_slots())
            }
            MutationMode::Insert | MutationMode::Replace => {
                RawRow::from_complete_serialized_update_patch(E::MODEL, mutation.serialized_slots())
            }
        }
    }

    // Validate one structurally patched after-image by decoding it against the
    // target key and reusing the existing typed save preflight rules.
    fn validate_structural_after_image(
        &self,
        data_key: &DataKey,
        row: &RawRow,
        write_context: SanitizeWriteContext,
    ) -> Result<E, InternalError> {
        let expected_key = data_key.try_key::<E>()?;
        let mut entity = row.try_decode::<E>().map_err(|err| {
            InternalError::mutation_structural_after_image_invalid(
                E::PATH,
                data_key,
                err.to_string(),
            )
        })?;
        let identity_key = entity.id().key();
        if identity_key != expected_key {
            let field_name = E::MODEL.primary_key().name();
            let field_value = FieldValue::to_value(&identity_key);
            let identity_value = FieldValue::to_value(&expected_key);

            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                field_name,
                &field_value,
                &identity_value,
            ));
        }

        self.preflight_entity(&mut entity, write_context)?;

        Ok(entity)
    }

    // Validate one structural insert/replace after-image by materializing the
    // sparse patch directly so derive-owned missing-slot semantics run before
    // save preflight emits the final dense row image.
    fn validate_structural_after_image_from_patch(
        &self,
        data_key: &DataKey,
        patch: &SerializedUpdatePatch,
        write_context: SanitizeWriteContext,
    ) -> Result<E, InternalError> {
        let expected_key = data_key.try_key::<E>()?;
        let mut entity = crate::db::data::materialize_entity_from_serialized_update_patch::<E>(
            patch,
        )
        .map_err(|err| {
            InternalError::mutation_structural_after_image_invalid(
                E::PATH,
                data_key,
                err.to_string(),
            )
        })?;
        let identity_key = entity.id().key();
        if identity_key != expected_key {
            let field_name = E::MODEL.primary_key().name();
            let field_value = FieldValue::to_value(&identity_key);
            let identity_value = FieldValue::to_value(&expected_key);

            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                field_name,
                &field_value,
                &identity_value,
            ));
        }

        self.preflight_entity(&mut entity, write_context)?;

        Ok(entity)
    }
}
