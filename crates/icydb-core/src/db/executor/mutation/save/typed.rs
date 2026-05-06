use super::{SaveExecutor, SaveMode, SavePreflightInputs, SaveRule};

use crate::{
    db::{
        commit::{
            CommitRowOp,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        },
        data::{
            CanonicalRow, DataKey, PersistedRow, RawRow, StructuralRowContract,
            canonical_row_from_raw_row_with_structural_contract,
        },
        executor::{
            Context,
            mutation::{emit_index_delta_metrics, mutation_write_context},
        },
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    sanitize::SanitizeWriteContext,
    traits::{EntityCreateInput, EntityValue, Storable},
    types::Timestamp,
};

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Create one authored typed input after materializing its typed entity and
    // authored-slot provenance.
    pub(super) fn save_typed_create_input<I>(&self, input: I) -> Result<E, InternalError>
    where
        I: EntityCreateInput<Entity = E>,
    {
        let materialized = input.materialize_create();
        let authored_create_slots = materialized.authored_slots().to_vec();
        let entity = materialized.into_entity();
        let ctx = mutation_write_context::<E>(&self.db)?;
        let preflight = SavePreflightInputs {
            schema: Self::schema_info(),
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            validate_relations: E::MODEL.has_any_strong_relations(),
            write_context: Self::save_write_context(SaveMode::Insert, Timestamp::now()),
            authored_create_slots: Some(authored_create_slots.as_slice()),
        };

        self.save_entity_with_context_and_schema(
            &ctx,
            SaveRule::from_mode(SaveMode::Insert),
            preflight,
            entity,
        )
    }

    // Build one logical row operation from a full typed after-image.
    pub(super) fn prepare_typed_entity_row_op(
        &self,
        ctx: &Context<'_, E>,
        save_rule: SaveRule,
        entity: &E,
        schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    ) -> Result<CommitRowOp, InternalError> {
        // Phase 1: resolve key + current-store baseline from the canonical save rule.
        let data_key = DataKey::try_new::<E>(entity.id().key())?;
        let raw_key = data_key.to_raw()?;
        let old_raw = Self::resolve_existing_row_for_rule(
            ctx,
            &data_key,
            save_rule,
            self.accepted_row_decode_contract.as_ref(),
        )?;

        // Phase 2: typed save lanes already own a complete after-image, so
        // emit the canonical row directly instead of replaying a dense slot
        // patch back into the same full row image.
        let row_bytes = CanonicalRow::from_entity(entity)?
            .into_raw_row()
            .into_bytes();
        let before_bytes = old_raw
            .map(|old_raw| self.build_typed_before_image_bytes(old_raw))
            .transpose()?;
        let row_op = CommitRowOp::new(
            E::PATH,
            raw_key,
            before_bytes,
            Some(row_bytes),
            schema_fingerprint,
        );

        Ok(row_op)
    }

    // Build the commit-marker before image for typed saves. Accepted-schema
    // updates must not pass old short rows into commit preflight because index
    // and relation delta planning consume generated-compatible dense rows.
    fn build_typed_before_image_bytes(&self, old_row: RawRow) -> Result<Vec<u8>, InternalError> {
        let Some(accepted_row_decode_contract) = &self.accepted_row_decode_contract else {
            return Ok(old_row.into_bytes());
        };
        let contract = StructuralRowContract::from_model_with_accepted_decode_contract(
            E::MODEL,
            accepted_row_decode_contract.clone(),
        );
        let canonical =
            canonical_row_from_raw_row_with_structural_contract(E::MODEL, &old_row, contract)?;

        Ok(canonical.into_raw_row().into_bytes())
    }

    pub(super) fn save_entity(&self, mode: SaveMode, entity: E) -> Result<E, InternalError> {
        let ctx = mutation_write_context::<E>(&self.db)?;
        let save_rule = SaveRule::from_mode(mode);
        let write_context = Self::save_write_context(mode, Timestamp::now());

        self.save_entity_with_context(&ctx, save_rule, write_context, entity)
    }

    // Run one typed save against an already-resolved write context so batch
    // non-atomic lanes do not rebuild the same store authority for every row.
    pub(super) fn save_entity_with_context(
        &self,
        ctx: &Context<'_, E>,
        save_rule: SaveRule,
        write_context: SanitizeWriteContext,
        entity: E,
    ) -> Result<E, InternalError> {
        let preflight = SavePreflightInputs {
            schema: Self::schema_info(),
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            validate_relations: E::MODEL.has_any_strong_relations(),
            write_context,
            authored_create_slots: None,
        };

        self.save_entity_with_context_and_schema(ctx, save_rule, preflight, entity)
    }

    // Run one typed save against an already-resolved write context and
    // preflight schema metadata so batch lanes do not repay cache lookups.
    pub(super) fn save_entity_with_context_and_schema(
        &self,
        ctx: &Context<'_, E>,
        save_rule: SaveRule,
        preflight: SavePreflightInputs<'_>,
        entity: E,
    ) -> Result<E, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let result = (|| {
            let (entity, marker_row_op) =
                self.prepare_entity_save_row_op(ctx, save_rule, preflight, entity)?;
            let prepared_row_op =
                prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<E>(
                    &self.db,
                    &marker_row_op,
                    ctx,
                    ctx,
                    preflight.schema_fingerprint,
                )?;

            // Phase 1: persist/apply one single-row commit through the shared
            // commit-window path under the normal single-save metrics contract.
            Self::commit_prepared_single_row(
                &self.db,
                marker_row_op,
                prepared_row_op,
                |delta| emit_index_delta_metrics::<E>(delta),
                || {
                    span.set_rows(1);
                },
            )?;
            Self::record_save_mutation(save_rule.save_mutation_kind(), 1);

            Ok(entity)
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }

    // Prepare one typed save row op after canonical entity preflight so both
    // single-row and batched non-atomic lanes share the same validation path.
    pub(super) fn prepare_entity_save_row_op(
        &self,
        ctx: &Context<'_, E>,
        save_rule: SaveRule,
        preflight: SavePreflightInputs<'_>,
        entity: E,
    ) -> Result<(E, CommitRowOp), InternalError> {
        let mut entity = entity;

        // Phase 1: run canonical save preflight before key extraction so
        // typed validation still owns the write contract.
        self.preflight_entity_with_cached_schema(
            &mut entity,
            preflight.schema,
            preflight.validate_relations,
            preflight.write_context,
            preflight.authored_create_slots,
        )?;
        let marker_row_op = self.prepare_typed_entity_row_op(
            ctx,
            save_rule,
            &entity,
            preflight.schema_fingerprint,
        )?;

        Ok((entity, marker_row_op))
    }
}
