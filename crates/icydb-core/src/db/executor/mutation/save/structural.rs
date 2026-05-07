use crate::{
    db::{
        commit::{
            CommitRowOp, CommitSchemaFingerprint,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        },
        data::{
            CanonicalRow, DataKey, PersistedRow, RawRow, SerializedStructuralPatch,
            StructuralPatch, StructuralRowContract,
            apply_serialized_structural_patch_to_raw_row_with_accepted_contract,
            canonical_row_from_raw_row_with_structural_contract,
        },
        executor::{
            Context,
            mutation::{
                MutationInput, emit_index_delta_metrics, mutation_write_context,
                save::{MutationMode, SaveExecutor},
            },
        },
        schema::{AcceptedRowDecodeContract, SchemaInfo, commit_schema_fingerprint_for_entity},
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    sanitize::SanitizeWriteContext,
    traits::{EntityValue, KeyValueCodec, Storable},
    types::Timestamp,
};
use std::collections::HashSet;

///
/// StructuralMutationRequest
///
/// StructuralMutationRequest is the internal save-executor handoff for one
/// structural mutation before persisted-row serialization. It keeps mode, target
/// key, patch payload, and write context in one request so helper signatures do
/// not use loose tuples for mutation semantics.
///

struct StructuralMutationRequest<E: PersistedRow + EntityValue> {
    mode: MutationMode,
    key: E::Key,
    patch: StructuralPatch,
    write_context: SanitizeWriteContext,
    accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
}

///
/// StructuralMutationBatchItem
///
/// One internally lowered structural mutation staged by a batch write caller.
/// SQL INSERT/UPDATE uses this private executor boundary after SQL-facing
/// admission has already rejected generated and managed field ownership escapes.
///

struct StructuralMutationBatchItem<E: PersistedRow + EntityValue> {
    key: E::Key,
    patch: StructuralPatch,
}

impl<E: PersistedRow + EntityValue> StructuralMutationBatchItem<E> {
    // Build one internally lowered structural batch item after the caller has
    // crossed its admission boundary and selected the batch mutation mode.
    const fn internal_lowered(key: E::Key, patch: StructuralPatch) -> Self {
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
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Self {
        Self {
            mode,
            key,
            patch,
            write_context,
            accepted_row_decode_contract,
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
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Self {
        Self {
            mode,
            key,
            patch,
            write_context,
            accepted_row_decode_contract,
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
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Result<E, InternalError> {
        let write_context = Self::structural_write_context(mode, Timestamp::now());
        let request = StructuralMutationRequest::public_authored(
            mode,
            key,
            patch,
            write_context,
            accepted_row_decode_contract,
        );

        self.save_structural_mutation(request)
    }

    // Apply one internally lowered structural batch in a single commit window.
    //
    // Strong relation validation intentionally remains committed-store-only here:
    // same-statement relation targets are not visible until the relation domain
    // grows an overlay-aware reader seam of its own.
    pub(in crate::db) fn apply_internal_lowered_structural_mutation_batch(
        &self,
        mode: MutationMode,
        rows: Vec<(E::Key, StructuralPatch)>,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Result<Vec<E>, InternalError> {
        let items = rows
            .into_iter()
            .map(|(key, patch)| StructuralMutationBatchItem::internal_lowered(key, patch))
            .collect();

        self.apply_internal_structural_mutation_batch(
            mode,
            items,
            write_context,
            accepted_row_decode_contract,
        )
    }

    // Prepare and commit one executor-owned batch of internal structural
    // mutation items. Keeping the item type private prevents SQL/session code
    // from depending on mutation staging internals.
    fn apply_internal_structural_mutation_batch(
        &self,
        mode: MutationMode,
        items: Vec<StructuralMutationBatchItem<E>>,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Result<Vec<E>, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let result = (|| {
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
                    accepted_row_decode_contract.clone(),
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
            Self::record_save_mutation(
                mode.save_mutation_kind(),
                u64::try_from(entities.len()).unwrap_or(u64::MAX),
            );

            Ok(entities)
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }

    fn save_structural_mutation(
        &self,
        request: StructuralMutationRequest<E>,
    ) -> Result<E, InternalError> {
        let mutation_kind = request.mode.save_mutation_kind();
        let mut span = Span::<E>::new(ExecKind::Save);
        let result =
            (|| {
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
                    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<
                        E,
                    >(&self.db, &marker_row_op, &ctx, &ctx, schema_fingerprint)?;

                Self::commit_prepared_single_row(
                    &self.db,
                    marker_row_op,
                    prepared_row_op,
                    |delta| emit_index_delta_metrics::<E>(delta),
                    || {
                        span.set_rows(1);
                    },
                )?;
                Self::record_save_mutation(mutation_kind, 1);

                Ok(entity)
            })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
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
            accepted_row_decode_contract,
        } = request;

        Self::validate_structural_patch_write_bounds(
            &patch,
            accepted_row_decode_contract.as_ref(),
        )?;

        let complete_after_image = matches!(mode, MutationMode::Insert | MutationMode::Replace);
        let mutation = MutationInput::from_structural_patch::<E>(
            key,
            &patch,
            accepted_row_decode_contract.clone(),
            complete_after_image,
        )?;
        let data_key = mutation.data_key().clone();
        let old_raw = Self::resolve_existing_row_for_rule(
            ctx,
            &data_key,
            mode.save_rule(),
            accepted_row_decode_contract.as_ref(),
        )?;

        // Phase 1: materialize and preflight the structural after-image under
        // the same save contract as typed writes.
        let entity = match mode {
            MutationMode::Update => {
                let raw_after_image = Self::build_structural_update_after_image_row(
                    &mutation,
                    old_raw.as_ref(),
                    accepted_row_decode_contract.clone(),
                )?;
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
        let row_bytes = Self::build_normalized_structural_after_image_row(&normalized_mutation)?;
        let row_bytes = row_bytes.into_raw_row().into_bytes();
        let before_bytes = old_raw
            .as_ref()
            .map(|row| {
                Self::build_structural_before_image_bytes(
                    row,
                    accepted_row_decode_contract.as_ref(),
                )
            })
            .transpose()?;
        let marker_row_op = CommitRowOp::new(
            E::PATH,
            data_key.to_raw()?,
            before_bytes,
            Some(row_bytes),
            schema_fingerprint,
        );

        Ok((entity, marker_row_op))
    }

    // Build a sparse structural update after-image over the existing row. When
    // an accepted decode contract is present, old short physical rows first
    // materialize missing nullable slots before patch overlay.
    fn build_structural_update_after_image_row(
        mutation: &MutationInput,
        old_row: Option<&RawRow>,
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Result<CanonicalRow, InternalError> {
        let Some(old_row) = old_row else {
            return Err(InternalError::executor_invariant(
                "structural update staging requires an existing baseline row",
            ));
        };

        if let Some(accepted_row_decode_contract) = accepted_row_decode_contract {
            return Self::build_structural_update_after_image_row_with_accepted_contract(
                mutation,
                old_row,
                accepted_row_decode_contract,
            );
        }

        Self::build_structural_update_after_image_row_with_generated_contract(mutation, old_row)
    }

    // Build a sparse structural update after-image through the accepted row
    // contract selected for the mutation. Older short physical rows materialize
    // missing accepted slots before the sparse patch overlay is applied.
    fn build_structural_update_after_image_row_with_accepted_contract(
        mutation: &MutationInput,
        old_row: &RawRow,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
    ) -> Result<CanonicalRow, InternalError> {
        apply_serialized_structural_patch_to_raw_row_with_accepted_contract(
            E::MODEL,
            accepted_row_decode_contract,
            old_row,
            mutation.serialized_slots(),
        )
    }

    // Build a sparse structural update after-image through the generated row
    // contract used by compatibility callers without accepted schema metadata.
    fn build_structural_update_after_image_row_with_generated_contract(
        mutation: &MutationInput,
        old_row: &RawRow,
    ) -> Result<CanonicalRow, InternalError> {
        old_row.apply_serialized_structural_patch(E::MODEL, mutation.serialized_slots())
    }

    // Build the final persisted after-image from a normalized typed entity.
    // The normalized mutation is dense by construction, so this emits the
    // current generated row layout and never preserves an old short row shape.
    fn build_normalized_structural_after_image_row(
        mutation: &MutationInput,
    ) -> Result<CanonicalRow, InternalError> {
        RawRow::from_complete_serialized_structural_patch(E::MODEL, mutation.serialized_slots())
    }

    // Build the commit-marker before image. Accepted-schema updates must not
    // hand old short rows to commit preflight because index and relation delta
    // planning still consume generated-compatible dense row images.
    fn build_structural_before_image_bytes(
        old_row: &RawRow,
        accepted_row_decode_contract: Option<&AcceptedRowDecodeContract>,
    ) -> Result<Vec<u8>, InternalError> {
        if let Some(accepted_row_decode_contract) = accepted_row_decode_contract {
            return Self::build_structural_before_image_bytes_with_accepted_contract(
                old_row,
                accepted_row_decode_contract,
            );
        }

        Ok(Self::build_structural_before_image_bytes_with_generated_contract(old_row))
    }

    // Build one generated-layout before image for commit markers. Generated
    // callers can reuse the raw row bytes directly because they already match
    // the current generated row contract.
    fn build_structural_before_image_bytes_with_generated_contract(old_row: &RawRow) -> Vec<u8> {
        old_row.clone().into_bytes()
    }

    // Build one accepted-layout before image for commit markers. Accepted
    // callers normalize older physical rows into the current generated-compatible
    // dense layout before commit preflight sees them.
    fn build_structural_before_image_bytes_with_accepted_contract(
        old_row: &RawRow,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
    ) -> Result<Vec<u8>, InternalError> {
        let contract = StructuralRowContract::from_model_with_accepted_decode_contract(
            E::MODEL,
            accepted_row_decode_contract.clone(),
        );
        let canonical = canonical_row_from_raw_row_with_structural_contract(old_row, contract)?;

        Ok(canonical.into_raw_row().into_bytes())
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
