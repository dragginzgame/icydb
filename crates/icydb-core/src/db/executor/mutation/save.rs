//! Module: executor::mutation::save
//! Responsibility: save-mode execution (`insert`/`update`/`replace`) and batch lanes.
//! Does not own: relation-domain validation semantics or commit marker protocol internals.
//! Boundary: save preflight + commit-window handoff for one entity type.

use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{
            CanonicalRow, DataKey, PersistedRow, RawRow, UpdatePatch, decode_raw_row_for_entity_key,
        },
        executor::{
            Context, ExecutorError,
            mutation::{MutationInput, commit_save_row_ops_with_window, mutation_write_context},
        },
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    metrics::sink::{ExecKind, MetricsEvent, Span, record},
    traits::{EntityValue, FieldValue},
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

// Debug assertions below are diagnostic sentinels; correctness is enforced by
// runtime validation earlier in the pipeline.

///
/// SaveMode
///
/// Create  : will only insert a row if it's empty
/// Replace : will change the row regardless of what was there
/// Update  : will only change an existing row
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Display, Serialize)]
enum SaveMode {
    #[default]
    Insert,
    Replace,
    Update,
}

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub(in crate::db) struct SaveExecutor<E: PersistedRow + EntityValue> {
    pub(in crate::db::executor::mutation) db: Db<E::Canister>,
}

///
/// SaveRule
///
/// Canonical save precondition for resolving the current row baseline.
///
#[derive(Clone, Copy)]
enum SaveRule {
    RequireAbsent,
    RequirePresent,
    AllowAny,
}

impl SaveRule {
    const fn from_mode(mode: SaveMode) -> Self {
        match mode {
            SaveMode::Insert => Self::RequireAbsent,
            SaveMode::Update => Self::RequirePresent,
            SaveMode::Replace => Self::AllowAny,
        }
    }
}

///
/// MutationMode
///
/// MutationMode
///
/// MutationMode makes the structural patch path spell out the same
/// row-existence contract the typed save surface already owns.
/// This keeps future structural callers from smuggling write-mode meaning
/// through ad hoc helper choice once the seam moves beyond `icydb-core`.
///

#[derive(Clone, Copy)]
pub enum MutationMode {
    #[allow(dead_code)]
    Insert,
    #[allow(dead_code)]
    Replace,
    Update,
}

impl MutationMode {
    const fn save_rule(self) -> SaveRule {
        match self {
            Self::Insert => SaveRule::RequireAbsent,
            Self::Replace => SaveRule::AllowAny,
            Self::Update => SaveRule::RequirePresent,
        }
    }
}

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // ======================================================================
    // Construction & configuration
    // ======================================================================

    /// Construct one save executor bound to a database handle.
    #[must_use]
    pub(in crate::db) const fn new(db: Db<E::Canister>, _debug: bool) -> Self {
        Self { db }
    }

    // ======================================================================
    // Single-entity save operations
    // ======================================================================

    /// Insert a brand-new entity (errors if the key already exists).
    pub(crate) fn insert(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Update an existing entity (errors if it does not exist).
    pub(crate) fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Apply one structural field patch to an existing entity row.
    ///
    /// This entrypoint is intentionally staged ahead of the higher-level API
    /// layer so the executor boundary can lock its invariants first.
    #[allow(dead_code)]
    pub(in crate::db) fn insert_structural(
        &self,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError> {
        self.apply_structural_mutation(MutationMode::Insert, key, patch)
    }

    /// Apply one structural full-row replacement, inserting if missing.
    ///
    /// Replace semantics deliberately rebuild the after-image from an empty row
    /// layout so absent fields do not inherit old-row values implicitly.
    #[allow(dead_code)]
    pub(in crate::db) fn replace_structural(
        &self,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError> {
        self.apply_structural_mutation(MutationMode::Replace, key, patch)
    }

    /// Apply one structural field patch to an existing entity row.
    ///
    /// This entrypoint is intentionally staged ahead of the higher-level API
    /// layer so the executor boundary can lock its invariants first.
    #[allow(dead_code)]
    pub(in crate::db) fn update_structural(
        &self,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError> {
        self.apply_structural_mutation(MutationMode::Update, key, patch)
    }

    /// Replace an entity, inserting if missing.
    pub(crate) fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }

    // ======================================================================
    // Batch save operations (explicit atomic and non-atomic lanes)
    // ======================================================================

    /// Save a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: this helper is fail-fast and non-atomic. If one element fails,
    /// earlier elements in the batch remain committed.
    fn save_batch_non_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        for entity in iter {
            match self.save_entity(mode, entity) {
                Ok(saved) => out.push(saved),
                Err(err) => {
                    if !out.is_empty() {
                        record(MetricsEvent::NonAtomicPartialCommit {
                            entity_path: E::PATH,
                            committed_rows: u64::try_from(out.len()).unwrap_or(u64::MAX),
                        });
                    }

                    return Err(err);
                }
            }
        }

        Ok(out)
    }

    /// Save a single-entity-type batch atomically in a single commit window.
    ///
    /// All entities are prevalidated first; if any entity fails pre-commit validation,
    /// no row in this batch is persisted.
    ///
    /// This is not a multi-entity transaction surface.
    fn save_batch_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let entities: Vec<E> = entities.into_iter().collect();

        self.save_batch_atomic_impl(SaveRule::from_mode(mode), entities)
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub(crate) fn insert_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Insert, entities)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub(crate) fn update_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Update, entities)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub(crate) fn replace_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Replace, entities)
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub(crate) fn insert_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Insert, entities)
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub(crate) fn update_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Update, entities)
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub(crate) fn replace_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Replace, entities)
    }

    // Keep the atomic batch body out of the iterator-generic wrapper so mode
    // wrappers do not each own one copy of the full staging pipeline.
    #[inline(never)]
    fn save_batch_atomic_impl(
        &self,
        save_rule: SaveRule,
        entities: Vec<E>,
    ) -> Result<Vec<E>, InternalError> {
        // Phase 1: validate + stage all row ops before opening the commit window.
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = mutation_write_context::<E>(&self.db)?;
        let mut out = Vec::with_capacity(entities.len());
        let mut marker_row_ops = Vec::with_capacity(entities.len());
        let mut seen_row_keys = BTreeSet::<Vec<u8>>::new();

        // Validate and stage all row ops before opening the commit window.
        for mut entity in entities {
            self.preflight_entity(&mut entity)?;
            let mutation = MutationInput::from_entity(&entity)?;

            let (marker_row_op, data_key) =
                Self::prepare_logical_row_op(&ctx, save_rule, &mutation)?;
            if !seen_row_keys.insert(marker_row_op.key.clone()) {
                return Err(InternalError::mutation_atomic_save_duplicate_key(
                    E::PATH,
                    data_key,
                ));
            }
            marker_row_ops.push(marker_row_op);
            out.push(entity);
        }

        if marker_row_ops.is_empty() {
            return Ok(out);
        }

        // Phase 2: enter commit window and apply staged row ops atomically.
        Self::commit_atomic_batch(&self.db, marker_row_ops, &mut span)?;

        Ok(out)
    }

    // Build one logical row operation from the save rule and current entity.
    fn prepare_logical_row_op(
        ctx: &Context<'_, E>,
        save_rule: SaveRule,
        mutation: &MutationInput,
    ) -> Result<(CommitRowOp, DataKey), InternalError> {
        // Phase 1: resolve key + current-store baseline from the canonical save rule.
        let data_key = mutation.data_key().clone();
        let raw_key = data_key.to_raw()?;
        let old_raw = Self::resolve_existing_row_for_rule(ctx, &data_key, save_rule)?;
        let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();

        // Phase 2: build the after-image through the structural row boundary.
        let row = Self::build_after_image_row(mutation, old_raw.as_ref())?;
        let row_op = CommitRowOp::new(
            E::PATH,
            raw_key.as_bytes().to_vec(),
            old_raw.as_ref().map(|item| item.as_bytes().to_vec()),
            Some(row.as_bytes().to_vec()),
            schema_fingerprint,
        );

        Ok((row_op, data_key))
    }

    // Build the persisted after-image under one explicit structural mode.
    fn build_after_image_row(
        mutation: &MutationInput,
        old_row: Option<&RawRow>,
    ) -> Result<CanonicalRow, InternalError> {
        let Some(old_row) = old_row else {
            return RawRow::from_serialized_update_patch(E::MODEL, mutation.serialized_patch());
        };

        old_row.apply_serialized_update_patch(E::MODEL, mutation.serialized_patch())
    }

    // Build the persisted after-image under one explicit structural mode.
    fn build_structural_after_image_row(
        mode: MutationMode,
        mutation: &MutationInput,
        old_row: Option<&RawRow>,
    ) -> Result<CanonicalRow, InternalError> {
        match mode {
            MutationMode::Update => {
                let Some(old_row) = old_row else {
                    return RawRow::from_serialized_update_patch(
                        E::MODEL,
                        mutation.serialized_patch(),
                    );
                };

                old_row.apply_serialized_update_patch(E::MODEL, mutation.serialized_patch())
            }
            MutationMode::Insert | MutationMode::Replace => {
                RawRow::from_serialized_update_patch(E::MODEL, mutation.serialized_patch())
            }
        }
    }

    // Resolve the "before" row according to one canonical save rule.
    fn resolve_existing_row_for_rule(
        ctx: &Context<'_, E>,
        data_key: &DataKey,
        save_rule: SaveRule,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = data_key.to_raw()?;

        match save_rule {
            SaveRule::RequireAbsent => {
                if let Some(existing) = ctx.with_store(|store| store.get(&raw_key))? {
                    Self::validate_existing_row_identity(data_key, &existing)?;
                    return Err(ExecutorError::KeyExists(data_key.clone()).into());
                }

                Ok(None)
            }
            SaveRule::RequirePresent => {
                let old_row = ctx
                    .with_store(|store| store.get(&raw_key))?
                    .ok_or_else(|| InternalError::store_not_found(data_key.to_string()))?;
                Self::validate_existing_row_identity(data_key, &old_row)?;

                Ok(Some(old_row))
            }
            SaveRule::AllowAny => {
                let old_row = ctx.with_store(|store| store.get(&raw_key))?;
                if let Some(old) = old_row.as_ref() {
                    Self::validate_existing_row_identity(data_key, old)?;
                }

                Ok(old_row)
            }
        }
    }

    // Decode an existing row and verify it is consistent with the target data key.
    fn validate_existing_row_identity(
        data_key: &DataKey,
        row: &RawRow,
    ) -> Result<(), InternalError> {
        let (_, decoded) = decode_raw_row_for_entity_key::<E>(data_key, row)?;
        Self::ensure_entity_invariants(&decoded).map_err(|err| {
            InternalError::from(ExecutorError::persisted_row_invariant_violation(
                data_key,
                &err.message,
            ))
        })?;

        Ok(())
    }

    fn save_entity(&self, mode: SaveMode, entity: E) -> Result<E, InternalError> {
        let mut entity = entity;
        (|| {
            let mut span = Span::<E>::new(ExecKind::Save);
            let ctx = mutation_write_context::<E>(&self.db)?;
            let save_rule = SaveRule::from_mode(mode);

            // Run the canonical save preflight before key extraction.
            self.preflight_entity(&mut entity)?;
            let mutation = MutationInput::from_entity(&entity)?;

            let (marker_row_op, _data_key) =
                Self::prepare_logical_row_op(&ctx, save_rule, &mutation)?;

            // Stage-2 commit protocol:
            // - preflight row-op preparation before persisting the marker
            // - then apply prepared row ops mechanically.
            // Durable correctness is marker + recovery owned. Apply guard rollback
            // here is best-effort, in-process cleanup only.
            Self::commit_single_row(&self.db, marker_row_op, &mut span)?;

            Ok(entity)
        })()
    }

    // Run one structural key + patch mutation under one explicit save-mode contract.
    #[allow(dead_code)]
    pub(in crate::db) fn apply_structural_mutation(
        &self,
        mode: MutationMode,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError> {
        let mutation = MutationInput::from_update_patch::<E>(key, &patch)?;

        self.save_structural_mutation(mode, mutation)
    }

    #[allow(dead_code)]
    fn save_structural_mutation(
        &self,
        mode: MutationMode,
        mutation: MutationInput,
    ) -> Result<E, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = mutation_write_context::<E>(&self.db)?;
        let data_key = mutation.data_key().clone();
        let old_raw = Self::resolve_existing_row_for_rule(&ctx, &data_key, mode.save_rule())?;
        let raw_after_image =
            Self::build_structural_after_image_row(mode, &mutation, old_raw.as_ref())?;
        let entity = self.validate_structural_after_image(&data_key, &raw_after_image)?;
        let normalized_mutation = MutationInput::from_entity(&entity)?;
        let row =
            Self::build_structural_after_image_row(mode, &normalized_mutation, old_raw.as_ref())?;
        let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
        let marker_row_op = CommitRowOp::new(
            E::PATH,
            data_key.to_raw()?.as_bytes().to_vec(),
            old_raw.as_ref().map(|item| item.as_bytes().to_vec()),
            Some(row.as_bytes().to_vec()),
            schema_fingerprint,
        );

        Self::commit_single_row(&self.db, marker_row_op, &mut span)?;

        Ok(entity)
    }

    // Validate one structurally patched after-image by decoding it against the
    // target key and reusing the existing typed save preflight rules.
    #[allow(dead_code)]
    fn validate_structural_after_image(
        &self,
        data_key: &DataKey,
        row: &RawRow,
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

        self.preflight_entity(&mut entity)?;

        Ok(entity)
    }

    // Open + apply commit mechanics for one logical row operation.
    fn commit_single_row(
        db: &Db<E::Canister>,
        marker_row_op: CommitRowOp,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        // FIRST STABLE WRITE: commit marker is persisted before any mutations.
        commit_save_row_ops_with_window::<E>(db, vec![marker_row_op], "save_row_apply", || {
            span.set_rows(1);
        })?;

        Ok(())
    }

    // Open + apply commit mechanics for an atomic staged row-op batch.
    fn commit_atomic_batch(
        db: &Db<E::Canister>,
        marker_row_ops: Vec<CommitRowOp>,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        let rows_touched = u64::try_from(marker_row_ops.len()).unwrap_or(u64::MAX);
        commit_save_row_ops_with_window::<E>(
            db,
            marker_row_ops,
            "save_batch_atomic_row_apply",
            || {
                span.set_rows(rows_touched);
            },
        )?;

        Ok(())
    }
}
