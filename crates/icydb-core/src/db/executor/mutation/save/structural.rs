use crate::{
    db::{
        KeyValueCodec,
        commit::{
            CommitRowOp, CommitSchemaFingerprint,
            prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        },
        data::{
            AcceptedMutationIntentPatch, AuthoredStructuralPatch, DecodedDataStoreKey,
            PersistedRow, RawRow, ResolvedAcceptedMutationRow, StructuralRowContract,
            StructuralSlotReader, canonical_row_from_raw_row_with_accepted_decode_contract,
            merge_non_generated_slots_into_canonical_row_with_accepted_contract,
            resolve_insert_structural_patch_with_accepted_contract,
            resolve_update_structural_patch_with_accepted_contract,
        },
        executor::{
            Context,
            mutation::{
                emit_index_delta_metrics, mutation_write_context,
                save::{MutationMode, SaveExecutor},
            },
        },
        schema::{AcceptedRowDecodeContract, SchemaInfo},
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    sanitize::SanitizeWriteContext,
    types::Timestamp,
};
use ic_stable_structures::Storable;
#[cfg(feature = "sql")]
use std::collections::HashSet;

///
/// StructuralMutationRequest
///
/// StructuralMutationRequest is the internal save-executor handoff for one
/// structural mutation before persisted-row serialization. It keeps mode, target
/// key, patch payload, and write context in one request so helper signatures do
/// not use loose tuples for mutation semantics.
///

pub(super) struct StructuralMutationRequest<E: PersistedRow> {
    mode: MutationMode,
    target_key: StructuralMutationTargetKey<E::Key>,
    patch: AcceptedMutationIntentPatch,
    write_context: SanitizeWriteContext,
    accepted_row_decode_contract: AcceptedRowDecodeContract,
}

///
/// StructuralMutationBatchItem
///
/// One internally lowered structural mutation staged by a batch write caller.
/// SQL INSERT/UPDATE uses this private executor boundary after SQL-facing
/// admission has already rejected generated and managed field ownership escapes.
///

#[cfg(feature = "sql")]
struct StructuralMutationBatchItem<E: PersistedRow> {
    target_key: StructuralMutationTargetKey<E::Key>,
    patch: AcceptedMutationIntentPatch,
}

/// Row-identity request carried by one structural mutation ingress.
///
/// SQL inserts whose accepted policy generates a primary key resolve identity
/// from the canonical after-image. Keyed mutation lanes carry an exact expected
/// identity and reject any materialized mismatch.
pub(in crate::db) enum StructuralMutationTargetKey<K> {
    /// Derive row identity from the accepted insert after-image.
    ResolveFromAfterImage,
    /// Require the accepted after-image to match this caller-selected key.
    Expected(K),
}

impl<K> StructuralMutationTargetKey<K> {
    /// Build one keyless insert identity request.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn resolve_from_after_image() -> Self {
        Self::ResolveFromAfterImage
    }

    /// Build one exact keyed mutation identity request.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn expected(key: K) -> Self {
        Self::Expected(key)
    }
}

#[cfg(feature = "sql")]
impl<E: PersistedRow> StructuralMutationBatchItem<E> {
    // Build one internally lowered structural batch item after the caller has
    // crossed its admission boundary and selected the batch mutation mode.
    const fn internal_lowered(
        target_key: StructuralMutationTargetKey<E::Key>,
        patch: AcceptedMutationIntentPatch,
    ) -> Self {
        Self { target_key, patch }
    }
}

impl<E: PersistedRow> StructuralMutationRequest<E> {
    // Build one accepted mutation request after a typed or internally lowered
    // frontend has frozen exact authored field provenance.
    pub(super) const fn accepted_lowered(
        mode: MutationMode,
        target_key: StructuralMutationTargetKey<E::Key>,
        patch: AcceptedMutationIntentPatch,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        Self {
            mode,
            target_key,
            patch,
            write_context,
            accepted_row_decode_contract,
        }
    }

    // Build one request from a public structural patch authored by a caller.
    fn public_authored(
        mode: MutationMode,
        key: E::Key,
        patch: AuthoredStructuralPatch,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        Self::accepted_lowered(
            mode,
            StructuralMutationTargetKey::Expected(key),
            AcceptedMutationIntentPatch::from_authored(patch),
            write_context,
            accepted_row_decode_contract,
        )
    }

    // Build one request from an internally lowered structural patch, such as a
    // SQL INSERT/UPDATE assignment set that has already crossed its own syntax
    // and generated-field policy boundary.
    #[cfg(feature = "sql")]
    const fn internal_lowered(
        mode: MutationMode,
        target_key: StructuralMutationTargetKey<E::Key>,
        patch: AcceptedMutationIntentPatch,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        Self::accepted_lowered(
            mode,
            target_key,
            patch,
            write_context,
            accepted_row_decode_contract,
        )
    }
}

impl<E: PersistedRow> SaveExecutor<E> {
    // Build one canonical write preflight context for one structural save mode.
    const fn structural_write_context(mode: MutationMode, now: Timestamp) -> SanitizeWriteContext {
        SanitizeWriteContext::new(mode.sanitize_write_mode(), now)
    }

    // Run one structural key + patch mutation under one explicit save-mode contract.
    pub(in crate::db) fn apply_structural_mutation(
        &self,
        mode: MutationMode,
        key: E::Key,
        patch: AuthoredStructuralPatch,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
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

    // Apply one internally lowered structural batch after giving the caller a
    // final chance to inspect validated after-images before commit publication.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn apply_internal_lowered_structural_mutation_batch_with_precommit<F>(
        &self,
        mode: MutationMode,
        rows: Vec<(
            StructuralMutationTargetKey<E::Key>,
            AcceptedMutationIntentPatch,
        )>,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
        precommit: F,
    ) -> Result<Vec<E>, InternalError>
    where
        F: FnOnce(&[E]) -> Result<(), InternalError>,
    {
        let items = rows
            .into_iter()
            .map(|(target_key, patch)| {
                StructuralMutationBatchItem::internal_lowered(target_key, patch)
            })
            .collect();

        self.apply_internal_structural_mutation_batch(
            mode,
            items,
            write_context,
            accepted_row_decode_contract,
            precommit,
        )
    }

    // Prepare and commit one executor-owned batch of internal structural
    // mutation items. Keeping the item type private prevents SQL/session code
    // from depending on mutation staging internals.
    #[cfg(feature = "sql")]
    fn apply_internal_structural_mutation_batch<F>(
        &self,
        mode: MutationMode,
        items: Vec<StructuralMutationBatchItem<E>>,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
        precommit: F,
    ) -> Result<Vec<E>, InternalError>
    where
        F: FnOnce(&[E]) -> Result<(), InternalError>,
    {
        let mut span = Span::<E>::new(ExecKind::Save);
        let result = (|| {
            let ctx = mutation_write_context::<E>(&self.db)?;
            let schema = self.accepted_schema_info();
            let schema_fingerprint = self.accepted_schema_fingerprint();
            let validate_relations = schema.has_any_relations();
            let mut entities = Vec::with_capacity(items.len());
            let mut marker_row_ops = Vec::with_capacity(items.len());
            let mut seen_row_keys = HashSet::with_capacity(items.len());

            // Phase 1: lower, materialize, and validate every structural after-image
            // before the shared commit-window helper can persist a marker.
            for item in items {
                let request = StructuralMutationRequest::internal_lowered(
                    mode,
                    item.target_key,
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
                if !seen_row_keys.insert(marker_row_op.key.clone()) {
                    let data_key = DecodedDataStoreKey::try_new::<E>(entity.id().key())?;
                    return Err(InternalError::mutation_atomic_save_duplicate_key(
                        E::PATH,
                        data_key,
                    ));
                }
                marker_row_ops.push(marker_row_op);
                entities.push(entity);
            }

            precommit(entities.as_slice())?;

            if marker_row_ops.is_empty() {
                return Ok(entities);
            }

            // Phase 2: open one marker/control-slot window and let commit preflight
            // simulate index/data overlay state across the staged row ops.
            Self::commit_atomic_batch(&self.db, marker_row_ops, schema_fingerprint, &mut span)?;
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
                let schema = self.accepted_schema_info();
                let schema_fingerprint = self.accepted_schema_fingerprint();
                let validate_relations = schema.has_any_relations();
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
    pub(super) fn prepare_structural_mutation_row_op(
        &self,
        ctx: &Context<'_, E>,
        schema: &SchemaInfo,
        schema_fingerprint: CommitSchemaFingerprint,
        validate_relations: bool,
        request: StructuralMutationRequest<E>,
    ) -> Result<(E, CommitRowOp), InternalError> {
        let StructuralMutationRequest {
            mode,
            target_key,
            patch,
            write_context,
            accepted_row_decode_contract,
        } = request;

        Self::validate_structural_patch_write_bounds_with_accepted_contract(
            &patch,
            &accepted_row_decode_contract,
        )?;

        let (resolved, mut old_raw) = self.resolve_accepted_mutation_after_image(
            ctx,
            mode,
            &target_key,
            &patch,
            write_context,
            &accepted_row_decode_contract,
        )?;
        let (structural_after_image, provenance) = resolved.into_parts();
        let mut entity = Self::materialize_structural_after_image(
            structural_after_image.as_raw_row(),
            accepted_row_decode_contract.clone(),
        )?;
        let data_key = DecodedDataStoreKey::try_new::<E>(entity.id().key())?;
        if let StructuralMutationTargetKey::Expected(expected) = target_key
            && entity.id().key() != expected
        {
            let field_name = Self::primary_key_label_from_schema(schema)?;
            let field_value = KeyValueCodec::to_key_value(&entity.id().key());
            let identity_value = KeyValueCodec::to_key_value(&expected);
            return Err(InternalError::mutation_entity_primary_key_mismatch(
                E::PATH,
                field_name.as_str(),
                &field_value,
                &identity_value,
            ));
        }
        if !matches!(mode, MutationMode::Update) {
            old_raw = Self::resolve_existing_row_for_rule_with_accepted_contract(
                ctx,
                &data_key,
                mode.save_rule(),
                &accepted_row_decode_contract,
                self.accepted_schema_info(),
            )?;
        }
        let normalized_entity_row = self.preflight_resolved_entity_with_provenance(
            &mut entity,
            structural_after_image.as_raw_row(),
            provenance.as_slice(),
            schema,
            validate_relations,
            write_context,
        )?;

        // Phase 2: retain normalized generated fields from typed preflight and
        // accepted non-generated slots from the structural after-image. DDL
        // fields have no Rust entity slot and must not collapse back to their
        // missing/default policy during re-emission.
        let row_bytes = merge_non_generated_slots_into_canonical_row_with_accepted_contract(
            E::PATH,
            accepted_row_decode_contract.clone(),
            normalized_entity_row.as_raw_row(),
            structural_after_image.as_raw_row(),
        )?;
        let row_bytes = row_bytes.into_raw_row().into_bytes();
        let before_bytes = old_raw
            .as_ref()
            .map(|row| {
                Self::build_structural_before_image_bytes_with_accepted_contract(
                    row,
                    &accepted_row_decode_contract,
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

    // Resolve accepted insertion or preservation policy before typed
    // materialization. Only updates need a pre-resolution baseline; insert and
    // replace identity is derived from the completed accepted after-image.
    fn resolve_accepted_mutation_after_image(
        &self,
        ctx: &Context<'_, E>,
        mode: MutationMode,
        target_key: &StructuralMutationTargetKey<E::Key>,
        patch: &AcceptedMutationIntentPatch,
        write_context: SanitizeWriteContext,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
    ) -> Result<(ResolvedAcceptedMutationRow, Option<RawRow>), InternalError> {
        match mode {
            MutationMode::Update => {
                let StructuralMutationTargetKey::Expected(key) = target_key else {
                    return Err(InternalError::executor_invariant());
                };
                let data_key = DecodedDataStoreKey::try_new::<E>(*key)?;
                let old_raw = Self::resolve_existing_row_for_rule_with_accepted_contract(
                    ctx,
                    &data_key,
                    mode.save_rule(),
                    accepted_row_decode_contract,
                    self.accepted_schema_info(),
                )?;
                let baseline_row = Self::structural_update_baseline_row(old_raw.as_ref())?;
                let resolved = resolve_update_structural_patch_with_accepted_contract(
                    E::PATH,
                    accepted_row_decode_contract.clone(),
                    baseline_row,
                    patch,
                    write_context,
                )?;

                Ok((resolved, old_raw))
            }
            MutationMode::Insert => Ok((
                resolve_insert_structural_patch_with_accepted_contract(
                    E::PATH,
                    accepted_row_decode_contract.clone(),
                    patch,
                    write_context,
                )?,
                None,
            )),
            MutationMode::Replace => {
                let StructuralMutationTargetKey::Expected(key) = target_key else {
                    return Err(InternalError::executor_invariant());
                };
                let patch = Self::preserve_database_owned_replacement_identity(
                    patch,
                    *key,
                    accepted_row_decode_contract,
                )?;
                Ok((
                    resolve_insert_structural_patch_with_accepted_contract(
                        E::PATH,
                        accepted_row_decode_contract.clone(),
                        &patch,
                        write_context,
                    )?,
                    None,
                ))
            }
        }
    }

    // Carry keyed replacement identity through insert-policy resolution for
    // database-owned primary-key fields. Explicit field intents remain in
    // place so the accepted resolver still rejects attempts to author a
    // generated or managed key.
    fn preserve_database_owned_replacement_identity(
        patch: &AcceptedMutationIntentPatch,
        expected_key: E::Key,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
    ) -> Result<AcceptedMutationIntentPatch, InternalError> {
        let primary_key_slots = accepted_row_decode_contract.primary_key_slot_indices();
        let key_value = KeyValueCodec::to_key_value(&expected_key);
        let key_components: Vec<_> = match key_value {
            crate::value::Value::List(values) if primary_key_slots.len() > 1 => values,
            value if primary_key_slots.len() == 1 => vec![value],
            _ => return Err(InternalError::executor_invariant()),
        };
        if key_components.len() != primary_key_slots.len() {
            return Err(InternalError::executor_invariant());
        }

        let mut resolved_patch = patch.clone();
        for (slot, value) in primary_key_slots.iter().copied().zip(key_components) {
            let field = accepted_row_decode_contract.required_field_for_slot(E::PATH, slot)?;
            let write_policy = field.write_policy();
            let has_explicit_intent = patch
                .entries()
                .iter()
                .any(|entry| entry.slot().index() == slot);
            if has_explicit_intent
                || (write_policy.insert_generation().is_none()
                    && write_policy.write_management().is_none())
            {
                continue;
            }
            resolved_patch = resolved_patch.set_preserved_replacement_identity(
                crate::db::data::FieldSlot::from_validated_index(slot),
                crate::value::InputValue::try_from_runtime_non_enum(&value)
                    .ok_or_else(InternalError::executor_invariant)?,
            );
        }

        Ok(resolved_patch)
    }

    // Require the baseline row needed by structural updates before accepted
    // patch replay. Keeping this check separate lets the accepted replay helper
    // assume an existing raw row.
    fn structural_update_baseline_row(old_row: Option<&RawRow>) -> Result<&RawRow, InternalError> {
        let Some(old_row) = old_row else {
            return Err(InternalError::executor_invariant());
        };

        Ok(old_row)
    }

    // Build one accepted-layout before image for commit markers. Accepted
    // callers normalize older physical rows into the current generated-compatible
    // dense layout before commit preflight sees them.
    fn build_structural_before_image_bytes_with_accepted_contract(
        old_row: &RawRow,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
    ) -> Result<Vec<u8>, InternalError> {
        let canonical = canonical_row_from_raw_row_with_accepted_decode_contract(
            E::PATH,
            accepted_row_decode_contract.clone(),
            old_row,
        )?;

        Ok(canonical.into_raw_row().into_bytes())
    }

    // Validate one structurally patched after-image by decoding it against the
    // target key and reusing the existing typed save preflight rules.
    fn materialize_structural_after_image(
        row: &RawRow,
        accepted_row_decode_contract: AcceptedRowDecodeContract,
    ) -> Result<E, InternalError> {
        let contract = StructuralRowContract::from_accepted_decode_contract(
            E::PATH,
            accepted_row_decode_contract,
        );
        let mut slots = StructuralSlotReader::from_raw_row_with_validated_contract(row, contract)
            .map_err(|err| {
            InternalError::mutation_structural_after_image_invalid(E::PATH, (), err)
        })?;
        E::materialize_from_slots(&mut slots)
            .map_err(|err| InternalError::mutation_structural_after_image_invalid(E::PATH, (), err))
    }

    // Resolve the primary-key field label from the schema boundary for
    // structural after-image diagnostics. Composite keys use the full ordered
    // field list so diagnostics do not fall back to scalar-only metadata.
    fn primary_key_label_from_schema(schema: &SchemaInfo) -> Result<String, InternalError> {
        let primary_key_names = schema.primary_key_names();
        if primary_key_names.is_empty() {
            return Err(InternalError::executor_invariant());
        }

        if let Some(name) = schema.scalar_primary_key_name() {
            return Ok(name.to_string());
        }

        Ok(primary_key_names.join(", "))
    }
}
