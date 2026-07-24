use super::{
    MutationMode, SaveExecutor, SaveMode, SavePreflightInputs, SaveRule,
    structural::{StructuralMutationRequest, StructuralMutationTargetKey},
};

use crate::{
    db::{
        commit::{
            CommitRowOp,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        },
        data::{AcceptedMutationIntentPatch, AuthoredStructuralPatch, FieldSlot, PersistedRow},
        executor::{
            Context,
            mutation::{emit_index_delta_metrics, mutation_write_context},
        },
    },
    entity::EntityCreateInput,
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    sanitize::SanitizeWriteContext,
    traits::AuthoredFieldProjection,
    types::{CurrentTimestamp, Timestamp},
};

impl<E: PersistedRow> SaveExecutor<E> {
    // Create one authored typed input after materializing its typed entity and
    // authored-slot provenance.
    pub(super) fn save_typed_create_input<I>(&self, input: I) -> Result<E, InternalError>
    where
        I: EntityCreateInput<Entity = E>,
    {
        let authored_fields = input.into_authored_fields();
        let mut patch = AuthoredStructuralPatch::new();
        for field in authored_fields {
            patch = patch.set(
                FieldSlot::from_validated_index(field.slot()),
                field.into_value(),
            );
        }
        let write_context = Self::save_write_context(SaveMode::Insert, Timestamp::now());
        let mut span = Span::<E>::new(ExecKind::Save);
        let result =
            (|| {
                let ctx = mutation_write_context::<E>(&self.db)?;
                let schema = self.accepted_schema_info();
                let schema_fingerprint = self.accepted_schema_fingerprint();
                let request = StructuralMutationRequest::accepted_lowered(
                    MutationMode::Insert,
                    StructuralMutationTargetKey::ResolveFromAfterImage,
                    AcceptedMutationIntentPatch::from_authored(patch),
                    write_context,
                    self.accepted_row_decode_contract().clone(),
                );
                let (entity, marker_row_op) = self.prepare_structural_mutation_row_op(
                    &ctx,
                    schema,
                    schema_fingerprint,
                    schema.has_any_relations(),
                    request,
                )?;
                let prepared_row_op =
                    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<
                        E,
                    >(&self.db, &marker_row_op, &ctx, &ctx, schema_fingerprint)?;
                Self::commit_prepared_single_row(
                    &self.db,
                    marker_row_op,
                    prepared_row_op,
                    |delta| emit_index_delta_metrics::<E>(delta),
                    || span.set_rows(1),
                )?;
                Self::record_save_mutation(SaveRule::RequireAbsent.save_mutation_kind(), 1);

                Ok(entity)
            })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
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
        let schema = self.accepted_schema_info();
        let preflight = SavePreflightInputs {
            schema,
            schema_fingerprint: self.accepted_schema_fingerprint(),
            validate_relations: schema.has_any_relations(),
            write_context,
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
        let patch = self.typed_entity_authored_patch(&entity)?;
        let mode = save_rule.mutation_mode();
        let target_key = match mode {
            MutationMode::Insert => StructuralMutationTargetKey::ResolveFromAfterImage,
            MutationMode::Update | MutationMode::Replace => {
                StructuralMutationTargetKey::Expected(entity.id().key())
            }
        };
        let request = StructuralMutationRequest::accepted_lowered(
            mode,
            target_key,
            AcceptedMutationIntentPatch::from_authored(patch),
            preflight.write_context,
            self.accepted_row_decode_contract().clone(),
        );

        self.prepare_structural_mutation_row_op(
            ctx,
            preflight.schema,
            preflight.schema_fingerprint,
            preflight.validate_relations,
            request,
        )
    }

    // Lower one full typed value into exact authored accepted inputs. Ordinary
    // generated-schema fields remain fully authored, including values produced
    // by Rust `Default`; accepted generation, management, and DDL-only fields
    // stay absent so the canonical mutation resolver retains their authority.
    fn typed_entity_authored_patch(
        &self,
        entity: &E,
    ) -> Result<AuthoredStructuralPatch, InternalError> {
        let accepted = self.accepted_row_decode_contract();
        let mut patch = AuthoredStructuralPatch::new();
        for slot in 0..accepted.required_slot_count() {
            let Some(field) = accepted.field_for_slot(slot) else {
                continue;
            };
            let write_policy = field.write_policy();
            if !field.generated()
                || write_policy.insert_generation().is_some()
                || write_policy.write_management().is_some()
            {
                continue;
            }
            let value = AuthoredFieldProjection::get_input_value_by_index(entity, slot)
                .ok_or_else(InternalError::executor_invariant)?;
            patch = patch.set(FieldSlot::from_validated_index(slot), value);
        }

        Ok(patch)
    }
}
