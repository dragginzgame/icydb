use crate::{
    db::{
        commit::{
            CommitRowOp, CommitSchemaFingerprint,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        },
        data::{
            CanonicalRow, DataKey, PersistedRow, RawRow, SerializedStructuralPatch, StructuralPatch,
        },
        executor::{
            Context,
            mutation::{
                MutationInput, emit_index_delta_metrics, mutation_write_context,
                save::{MutationMode, SaveExecutor},
            },
        },
        schema::{SchemaInfo, commit_schema_fingerprint_for_entity},
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    sanitize::SanitizeWriteContext,
    traits::{EntityValue, KeyValueCodec, Storable},
    types::Timestamp,
};
use std::collections::HashSet;

///
/// StructuralPatchOrigin
///
/// StructuralPatchOrigin records whether one structural patch was authored
/// through the public field-patch surface or synthesized by an internal
/// lowering lane. Save preflight uses this to enforce generated-field
/// authorship policy without encoding policy into the patch container itself.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StructuralPatchOrigin {
    PublicAuthored,
    InternalLowered,
}

impl StructuralPatchOrigin {
    // Public authored patches must not provide generated field payloads because
    // generated/managed values belong to write-boundary materialization, not to
    // caller-authored sparse field patches.
    const fn rejects_explicit_generated_fields(self) -> bool {
        matches!(self, Self::PublicAuthored)
    }
}

///
/// StructuralMutationRequest
///
/// StructuralMutationRequest is the internal save-executor handoff for one
/// structural mutation before persisted-row serialization. It keeps mode,
/// target key, patch payload, write context, and authored-origin policy in one
/// request so helper signatures do not use loose tuples or option flags for
/// mutation semantics.
///

struct StructuralMutationRequest<E: PersistedRow + EntityValue> {
    mode: MutationMode,
    key: E::Key,
    patch: StructuralPatch,
    write_context: SanitizeWriteContext,
    origin: StructuralPatchOrigin,
}

///
/// StructuralMutationBatchItem
///
/// One internally lowered structural mutation staged by a batch write caller.
/// SQL INSERT/UPDATE uses this private executor boundary after SQL-facing
/// admission has already rejected generated and managed field ownership escapes.
///

pub(in crate::db) struct StructuralMutationBatchItem<E: PersistedRow + EntityValue> {
    key: E::Key,
    patch: StructuralPatch,
}

impl<E: PersistedRow + EntityValue> StructuralMutationBatchItem<E> {
    /// Build one internally lowered structural batch item.
    #[must_use]
    pub(in crate::db) const fn internal_lowered(key: E::Key, patch: StructuralPatch) -> Self {
        Self { key, patch }
    }
}

impl<E: PersistedRow + EntityValue> StructuralMutationRequest<E> {
    // Build one request from a public structural patch authored by a caller.
    const fn public_authored(
        mode: MutationMode,
        key: E::Key,
        patch: StructuralPatch,
        write_context: SanitizeWriteContext,
    ) -> Self {
        Self {
            mode,
            key,
            patch,
            write_context,
            origin: StructuralPatchOrigin::PublicAuthored,
        }
    }

    // Build one request from an internally lowered structural patch, such as a
    // SQL INSERT/UPDATE assignment set that has already crossed its own syntax
    // and generated-field policy boundary.
    const fn internal_lowered(
        mode: MutationMode,
        key: E::Key,
        patch: StructuralPatch,
        write_context: SanitizeWriteContext,
    ) -> Self {
        Self {
            mode,
            key,
            patch,
            write_context,
            origin: StructuralPatchOrigin::InternalLowered,
        }
    }
}

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
        patch: StructuralPatch,
    ) -> Result<E, InternalError> {
        let write_context = Self::structural_write_context(mode, Timestamp::now());
        let request = StructuralMutationRequest::public_authored(mode, key, patch, write_context);

        self.save_structural_mutation(request)
    }

    // Apply one internally lowered structural batch in a single commit window.
    //
    // Strong relation validation intentionally remains committed-store-only here:
    // same-statement relation targets are not visible until the relation domain
    // grows an overlay-aware reader seam of its own.
    pub(in crate::db) fn apply_internal_structural_mutation_batch(
        &self,
        mode: MutationMode,
        items: Vec<StructuralMutationBatchItem<E>>,
        write_context: SanitizeWriteContext,
    ) -> Result<Vec<E>, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = mutation_write_context::<E>(&self.db)?;
        let schema = Self::schema_info();
        let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
        let validate_relations = E::MODEL.has_any_strong_relations();
        let mut entities = Vec::with_capacity(items.len());
        let mut marker_row_ops = Vec::with_capacity(items.len());
        let mut seen_row_keys = HashSet::with_capacity(items.len());

        // Phase 1: lower, materialize, and validate every structural after-image
        // before the shared commit-window helper can persist a marker.
        for item in items {
            let request = StructuralMutationRequest::internal_lowered(
                mode,
                item.key,
                item.patch,
                write_context,
            );
            let (entity, marker_row_op) = self.prepare_structural_mutation_row_op(
                &ctx,
                schema,
                schema_fingerprint,
                validate_relations,
                request,
            )?;
            if !seen_row_keys.insert(marker_row_op.key) {
                let data_key = DataKey::try_new::<E>(entity.id().key())?;
                return Err(InternalError::mutation_atomic_save_duplicate_key(
                    E::PATH,
                    data_key,
                ));
            }
            marker_row_ops.push(marker_row_op);
            entities.push(entity);
        }

        if marker_row_ops.is_empty() {
            return Ok(entities);
        }

        // Phase 2: open one marker/control-slot window and let commit preflight
        // simulate index/data overlay state across the staged row ops.
        Self::commit_atomic_batch(&self.db, marker_row_ops, &mut span)?;

        Ok(entities)
    }

    fn save_structural_mutation(
        &self,
        request: StructuralMutationRequest<E>,
    ) -> Result<E, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = mutation_write_context::<E>(&self.db)?;
        let schema = Self::schema_info();
        let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
        let validate_relations = E::MODEL.has_any_strong_relations();
        let (entity, marker_row_op) = self.prepare_structural_mutation_row_op(
            &ctx,
            schema,
            schema_fingerprint,
            validate_relations,
            request,
        )?;
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

    // Prepare one structural mutation into a normalized row operation without
    // opening a commit window. Both the single-row and SQL batch lanes share this
    // so structural validation and after-image materialization cannot drift.
    fn prepare_structural_mutation_row_op(
        &self,
        ctx: &Context<'_, E>,
        schema: &SchemaInfo,
        schema_fingerprint: CommitSchemaFingerprint,
        validate_relations: bool,
        request: StructuralMutationRequest<E>,
    ) -> Result<(E, CommitRowOp), InternalError> {
        let StructuralMutationRequest {
            mode,
            key,
            patch,
            write_context,
            origin,
        } = request;

        // Phase 0: reject authored values for insert-generated fields on every
        // public structural lane. The one structural exception is the primary
        // key slot: public structural writes already carry the authoritative
        // key out of band, so a matching generated primary-key payload in the
        // patch is redundant identity wiring rather than a second generated
        // value source.
        if origin.rejects_explicit_generated_fields() {
            Self::reject_explicit_generated_fields(&patch)?;
        }

        Self::validate_structural_patch_write_bounds(&patch)?;

        let mutation = MutationInput::from_structural_patch::<E>(key, &patch)?;
        let data_key = mutation.data_key().clone();
        let old_raw = Self::resolve_existing_row_for_rule(ctx, &data_key, mode.save_rule())?;

        // Phase 1: materialize and preflight the structural after-image under
        // the same save contract as typed writes.
        let entity = match mode {
            MutationMode::Update => {
                let raw_after_image =
                    Self::build_structural_after_image_row(mode, &mutation, old_raw.as_ref())?;
                self.validate_structural_after_image(
                    &data_key,
                    &raw_after_image,
                    schema,
                    validate_relations,
                    write_context,
                )?
            }
            MutationMode::Insert | MutationMode::Replace => self
                .validate_structural_after_image_from_patch(
                    &data_key,
                    mutation.serialized_slots(),
                    schema,
                    validate_relations,
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
        let marker_row_op = CommitRowOp::new(
            E::PATH,
            data_key.to_raw()?,
            before_bytes,
            Some(row_bytes),
            schema_fingerprint,
        );

        Ok((entity, marker_row_op))
    }

    // Reject structural patches that try to author schema insert-generated
    // fields directly. Public structural writes must not bypass system-owned
    // generation on create or later rewrites, except for the redundant primary
    // key slot because the structural API already carries the authoritative
    // key separately.
    fn reject_explicit_generated_fields(patch: &StructuralPatch) -> Result<(), InternalError> {
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

                old_row.apply_serialized_structural_patch(E::MODEL, mutation.serialized_slots())
            }
            MutationMode::Insert | MutationMode::Replace => {
                RawRow::from_complete_serialized_structural_patch(
                    E::MODEL,
                    mutation.serialized_slots(),
                )
            }
        }
    }

    // Validate one structurally patched after-image by decoding it against the
    // target key and reusing the existing typed save preflight rules.
    fn validate_structural_after_image(
        &self,
        data_key: &DataKey,
        row: &RawRow,
        schema: &SchemaInfo,
        validate_relations: bool,
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
            let field_value = KeyValueCodec::to_key_value(&identity_key);
            let identity_value = KeyValueCodec::to_key_value(&expected_key);

            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                field_name,
                &field_value,
                &identity_value,
            ));
        }

        self.preflight_entity_with_cached_schema(
            &mut entity,
            schema,
            validate_relations,
            write_context,
            None,
        )?;

        Ok(entity)
    }

    // Validate one structural insert/replace after-image by materializing the
    // sparse patch directly so derive-owned missing-slot semantics run before
    // save preflight emits the final dense row image.
    fn validate_structural_after_image_from_patch(
        &self,
        data_key: &DataKey,
        patch: &SerializedStructuralPatch,
        schema: &SchemaInfo,
        validate_relations: bool,
        write_context: SanitizeWriteContext,
    ) -> Result<E, InternalError> {
        let expected_key = data_key.try_key::<E>()?;
        let mut entity =
            crate::db::data::materialize_entity_from_serialized_structural_patch::<E>(patch)
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
            let field_value = KeyValueCodec::to_key_value(&identity_key);
            let identity_value = KeyValueCodec::to_key_value(&expected_key);

            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                field_name,
                &field_value,
                &identity_value,
            ));
        }

        self.preflight_entity_with_cached_schema(
            &mut entity,
            schema,
            validate_relations,
            write_context,
            None,
        )?;

        Ok(entity)
    }
}
