use crate::{
    db::{
        Db,
        commit::{CommitRowOp, ensure_recovered_for_write},
        data::{DataKey, RawRow, decode_and_validate_entity_key},
        executor::{
            Context, ExecutorError,
            mutation::commit_window::{
                OpenCommitWindow, apply_prepared_row_ops, emit_prepared_row_op_delta_metrics,
                open_commit_window,
            },
        },
    },
    error::InternalError,
    obs::sink::{ExecKind, MetricsEvent, Span, record},
    serialize::serialize,
    traits::{EntityKind, EntityValue},
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, marker::PhantomData};

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
pub(crate) enum SaveMode {
    #[default]
    Insert,
    Replace,
    Update,
}

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub(crate) struct SaveExecutor<E: EntityKind + EntityValue> {
    pub(in crate::db::executor::mutation) db: Db<E::Canister>,
    _marker: PhantomData<E>,
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

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    // ======================================================================
    // Construction & configuration
    // ======================================================================

    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, _debug: bool) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    // ======================================================================
    // Single-entity save operations
    // ======================================================================

    /// Insert a brand-new entity (errors if the key already exists).
    pub(crate) fn insert(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Insert a new view, returning the stored view.
    pub(crate) fn insert_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Insert, view)
    }

    /// Update an existing entity (errors if it does not exist).
    pub(crate) fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Update an existing view (errors if it does not exist).
    pub(crate) fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Update, view)
    }

    /// Replace an entity, inserting if missing.
    pub(crate) fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }

    /// Replace a view, inserting if missing.
    pub(crate) fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Replace, view)
    }

    // Shared wrapper for view-based save operations.
    fn save_view(&self, mode: SaveMode, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);

        Ok(self.save_entity(mode, entity)?.as_view())
    }

    // ======================================================================
    // Batch save operations (explicit atomic and non-atomic lanes)
    // ======================================================================

    /// Save a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: this helper is fail-fast and non-atomic. If one element fails,
    /// earlier elements in the batch remain committed.
    pub(crate) fn save_batch_non_atomic(
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
    pub(crate) fn save_batch_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        (|| {
            let mut span = Span::<E>::new(ExecKind::Save);
            let ctx = self.db.context::<E>();
            let save_rule = SaveRule::from_mode(mode);
            let iter = entities.into_iter();
            let mut out = Vec::with_capacity(iter.size_hint().0);
            let mut marker_row_ops = Vec::with_capacity(iter.size_hint().0);
            let mut seen_row_keys = BTreeSet::<Vec<u8>>::new();

            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered_for_write(&self.db)?;

            // Validate and stage all row ops before opening the commit window.
            for mut entity in iter {
                self.preflight_entity(&mut entity)?;

                let (marker_row_op, data_key) =
                    Self::prepare_logical_row_op(&ctx, save_rule, &entity)?;
                if !seen_row_keys.insert(marker_row_op.key.clone()) {
                    return Err(InternalError::executor_unsupported(format!(
                        "atomic save batch rejected duplicate key: entity={} key={data_key}",
                        E::PATH,
                    )));
                }
                marker_row_ops.push(marker_row_op);
                out.push(entity);
            }

            if marker_row_ops.is_empty() {
                return Ok(out);
            }

            Self::commit_atomic_batch(&self.db, marker_row_ops, &mut span)?;

            Ok(out)
        })()
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

    // Build one logical row operation from the save rule and current entity.
    fn prepare_logical_row_op(
        ctx: &Context<'_, E>,
        save_rule: SaveRule,
        entity: &E,
    ) -> Result<(CommitRowOp, DataKey), InternalError> {
        // Phase 1: resolve key + current-store baseline from the canonical save rule.
        let key = entity.id().key();
        let data_key = DataKey::try_new::<E>(key)?;
        let raw_key = data_key.to_raw()?;
        let old_raw = Self::resolve_existing_row_for_rule(ctx, &data_key, save_rule)?;

        // Phase 2: encode the after-image and build a marker row op.
        let bytes = serialize(entity)?;
        let row = RawRow::try_new(bytes)?;
        let row_op = CommitRowOp::new(
            E::PATH,
            raw_key.as_bytes().to_vec(),
            old_raw.as_ref().map(|item| item.as_bytes().to_vec()),
            Some(row.as_bytes().to_vec()),
        );

        Ok((row_op, data_key))
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
        let expected = data_key.try_key::<E>()?;
        let decoded = decode_and_validate_entity_key::<E, _, _, _, _>(
            expected,
            || row.try_decode::<E>(),
            |err| {
                ExecutorError::serialize_corruption(format!(
                    "failed to deserialize row: {data_key} ({err})"
                ))
                .into()
            },
            |expected, actual| {
                ExecutorError::store_corruption(format!(
                    "row key mismatch: expected {expected:?}, found {actual:?}"
                ))
                .into()
            },
        )?;
        Self::ensure_entity_invariants(&decoded).map_err(|err| {
            InternalError::from(ExecutorError::store_corruption(format!(
                "persisted row invariant violation: {data_key} ({})",
                err.message
            )))
        })?;

        Ok(())
    }

    fn save_entity(&self, mode: SaveMode, entity: E) -> Result<E, InternalError> {
        let mut entity = entity;
        (|| {
            let mut span = Span::<E>::new(ExecKind::Save);
            let ctx = self.db.context::<E>();
            let save_rule = SaveRule::from_mode(mode);

            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered_for_write(&self.db)?;

            // Run the canonical save preflight before key extraction.
            self.preflight_entity(&mut entity)?;

            let (marker_row_op, _data_key) =
                Self::prepare_logical_row_op(&ctx, save_rule, &entity)?;

            // Preflight data store availability before index mutations.
            ctx.with_store(|_| ())?;

            // Stage-2 commit protocol:
            // - preflight row-op preparation before persisting the marker
            // - then apply prepared row ops mechanically.
            // Durable correctness is marker + recovery owned. Apply guard rollback
            // here is best-effort, in-process cleanup only.
            Self::commit_single_row(&self.db, marker_row_op, &mut span)?;

            Ok(entity)
        })()
    }

    // Open + apply commit mechanics for one logical row operation.
    fn commit_single_row(
        db: &Db<E::Canister>,
        marker_row_op: CommitRowOp,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        let marker_row_ops = vec![marker_row_op];
        let OpenCommitWindow {
            commit,
            prepared_row_ops,
            index_store_guards,
            delta,
        } = open_commit_window::<E>(db, marker_row_ops)?;

        // FIRST STABLE WRITE: commit marker is persisted before any mutations.
        apply_prepared_row_ops(
            commit,
            "save_row_apply",
            prepared_row_ops,
            index_store_guards,
            || {
                emit_prepared_row_op_delta_metrics::<E>(&delta);
            },
            || {
                span.set_rows(1);
            },
        )?;

        Ok(())
    }

    // Open + apply commit mechanics for an atomic staged row-op batch.
    fn commit_atomic_batch(
        db: &Db<E::Canister>,
        marker_row_ops: Vec<CommitRowOp>,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        let OpenCommitWindow {
            commit,
            prepared_row_ops,
            index_store_guards,
            delta,
        } = open_commit_window::<E>(db, marker_row_ops)?;

        let rows_touched = u64::try_from(delta.rows_touched).unwrap_or(u64::MAX);
        apply_prepared_row_ops(
            commit,
            "save_batch_atomic_row_apply",
            prepared_row_ops,
            index_store_guards,
            || {
                emit_prepared_row_op_delta_metrics::<E>(&delta);
            },
            || {
                span.set_rows(rows_touched);
            },
        )?;

        Ok(())
    }
}
