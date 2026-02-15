mod invariants;
mod relations;
#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::value::Value;
use crate::{
    db::{
        CommitMarker, CommitRowOp, Db, begin_commit, ensure_recovered_for_write,
        executor::{
            Context, ExecutorError,
            mutation::{apply_prepared_row_ops, preflight_prepare_row_ops},
            trace::{QueryTraceSink, TraceExecutorKind, start_exec_trace},
        },
        query::SaveMode,
        store::{DataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::serialize,
    traits::{EntityKind, EntityValue},
    validate::validate,
};
use std::{collections::BTreeSet, marker::PhantomData};

// Debug assertions below are diagnostic sentinels; correctness is enforced by
// runtime validation earlier in the pipeline.

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub struct SaveExecutor<E: EntityKind + EntityValue> {
    db: Db<E::Canister>,
    debug: bool,
    trace: Option<&'static dyn QueryTraceSink>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    // ======================================================================
    // Construction & configuration
    // ======================================================================

    // Debug is session-scoped via DbSession and propagated into executors;
    // executors do not expose independent debug control.
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            trace: None,
            _marker: PhantomData,
        }
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("[debug] {}", s.into());
        }
    }

    // ======================================================================
    // Single-entity save operations
    // ======================================================================

    /// Insert a brand-new entity (errors if the key already exists).
    pub fn insert(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Insert a new view, returning the stored view.
    pub fn insert_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Insert, view)
    }

    /// Update an existing entity (errors if it does not exist).
    pub fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Update an existing view (errors if it does not exist).
    pub fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        self.save_view(SaveMode::Update, view)
    }

    /// Replace an entity, inserting if missing.
    pub fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }

    /// Replace a view, inserting if missing.
    pub fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
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
    pub fn save_batch_non_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);
        let mut batch_index = 0usize;

        for entity in iter {
            batch_index = batch_index.saturating_add(1);
            match self.save_entity(mode, entity) {
                Ok(saved) => out.push(saved),
                Err(err) => {
                    if !out.is_empty() {
                        // Batch writes are intentionally non-atomic; surface partial commits loudly.
                        println!(
                            "[warn] icydb non-atomic batch partial commit: mode={mode:?} entity={} committed={} failed_at_item={} error={err}",
                            E::PATH,
                            out.len(),
                            batch_index,
                        );
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
    pub fn save_batch_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let mut commit_started = false;
        let trace = start_exec_trace(
            self.trace,
            TraceExecutorKind::Save,
            E::PATH,
            None,
            Some(save_mode_tag(mode)),
        );
        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Save);
            let ctx = self.db.context::<E>();
            let iter = entities.into_iter();
            let mut out = Vec::with_capacity(iter.size_hint().0);
            let mut marker_row_ops = Vec::with_capacity(iter.size_hint().0);
            let mut seen_row_keys = BTreeSet::<Vec<u8>>::new();

            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered_for_write(&self.db)?;

            // Validate and stage all row ops before opening the commit window.
            for mut entity in iter {
                sanitize(&mut entity)?;
                validate(&entity)?;
                Self::ensure_entity_invariants(&entity)?;
                self.validate_strong_relations(&entity)?;

                let (marker_row_op, data_key) = Self::prepare_marker_row_op(&ctx, mode, &entity)?;
                if !seen_row_keys.insert(marker_row_op.key.clone()) {
                    return Err(InternalError::new(
                        ErrorClass::Unsupported,
                        ErrorOrigin::Executor,
                        format!(
                            "atomic save batch rejected duplicate key: entity={} key={data_key}",
                            E::PATH,
                        ),
                    ));
                }
                marker_row_ops.push(marker_row_op);
                out.push(entity);
            }

            if marker_row_ops.is_empty() {
                return Ok(out);
            }

            // Stage-2 commit protocol:
            // - preflight row-op preparation before persisting the marker
            // - then apply prepared row ops mechanically.
            let prepared_row_ops = preflight_prepare_row_ops::<E>(&self.db, &marker_row_ops)?;
            let marker = CommitMarker::new(marker_row_ops)?;
            let index_removes = prepared_row_ops
                .iter()
                .fold(0usize, |acc, op| acc.saturating_add(op.index_remove_count));
            let index_inserts = prepared_row_ops
                .iter()
                .fold(0usize, |acc, op| acc.saturating_add(op.index_insert_count));
            let reverse_index_removes = prepared_row_ops.iter().fold(0usize, |acc, op| {
                acc.saturating_add(op.reverse_index_remove_count)
            });
            let reverse_index_inserts = prepared_row_ops.iter().fold(0usize, |acc, op| {
                acc.saturating_add(op.reverse_index_insert_count)
            });
            let rows_touched = u64::try_from(prepared_row_ops.len()).unwrap_or(u64::MAX);

            let commit = begin_commit(marker)?;
            commit_started = true;
            self.debug_log("Atomic save batch commit window opened");

            // FIRST STABLE WRITE: commit marker is persisted before any mutations.
            apply_prepared_row_ops(
                commit,
                "save_batch_atomic_row_apply",
                prepared_row_ops,
                || {
                    // NOTE: Trace metrics saturate on overflow; diagnostics only.
                    let removes = u64::try_from(index_removes).unwrap_or(u64::MAX);
                    let inserts = u64::try_from(index_inserts).unwrap_or(u64::MAX);
                    sink::record(MetricsEvent::IndexDelta {
                        entity_path: E::PATH,
                        inserts,
                        removes,
                    });

                    let reverse_removes = u64::try_from(reverse_index_removes).unwrap_or(u64::MAX);
                    let reverse_inserts = u64::try_from(reverse_index_inserts).unwrap_or(u64::MAX);
                    sink::record(MetricsEvent::ReverseIndexDelta {
                        entity_path: E::PATH,
                        inserts: reverse_inserts,
                        removes: reverse_removes,
                    });
                },
                || {
                    span.set_rows(rows_touched);
                },
            )?;
            self.debug_log("Atomic save batch committed");

            Ok(out)
        })();

        if commit_started && result.is_err() {
            self.debug_log("Atomic save batch failed during marker apply; cleanup attempted");
        }

        if let Some(trace) = trace {
            match &result {
                Ok(saved) => {
                    let rows = u64::try_from(saved.len()).unwrap_or(u64::MAX);
                    trace.finish(rows);
                }
                Err(err) => trace.error(err),
            }
        }

        result
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Insert, entities)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Update, entities)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Replace, entities)
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Insert, entities)
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Update, entities)
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Replace, entities)
    }

    // Prepare one row operation for marker-based apply without mutating stores.
    fn prepare_marker_row_op(
        ctx: &Context<'_, E>,
        mode: SaveMode,
        entity: &E,
    ) -> Result<(CommitRowOp, DataKey), InternalError> {
        // Phase 1: resolve key + current-store baseline for requested save mode.
        let key = entity.id().key();
        let data_key = DataKey::try_new::<E>(key)?;
        let raw_key = data_key.to_raw()?;

        let old_raw = match mode {
            SaveMode::Insert => {
                // Inserts must not load or decode existing rows; absence is expected.
                if let Some(existing) = ctx.with_store(|store| store.get(&raw_key))? {
                    Self::validate_existing_row_identity(&data_key, &existing)?;
                    return Err(ExecutorError::KeyExists(data_key).into());
                }

                None
            }
            SaveMode::Update => {
                let old_row = ctx
                    .with_store(|store| store.get(&raw_key))?
                    .ok_or_else(|| InternalError::store_not_found(data_key.to_string()))?;
                Self::validate_existing_row_identity(&data_key, &old_row)?;

                Some(old_row)
            }
            SaveMode::Replace => {
                let old_row = ctx.with_store(|store| store.get(&raw_key))?;
                if let Some(old) = old_row.as_ref() {
                    Self::validate_existing_row_identity(&data_key, old)?;
                }

                old_row
            }
        };

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

    // Decode an existing row and verify it is consistent with the target data key.
    fn validate_existing_row_identity(
        data_key: &DataKey,
        row: &RawRow,
    ) -> Result<(), InternalError> {
        let decoded = row.try_decode::<E>().map_err(|err| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {data_key} ({err})"),
            )
        })?;
        let expected = data_key.try_key::<E>()?;
        let actual = decoded.id().key();
        if expected != actual {
            return Err(ExecutorError::corruption(
                ErrorOrigin::Store,
                format!("row key mismatch: expected {expected:?}, found {actual:?}"),
            )
            .into());
        }

        Ok(())
    }

    fn save_entity(&self, mode: SaveMode, mut entity: E) -> Result<E, InternalError> {
        let mut commit_started = false;
        let trace = start_exec_trace(
            self.trace,
            TraceExecutorKind::Save,
            E::PATH,
            None,
            Some(save_mode_tag(mode)),
        );
        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Save);
            let ctx = self.db.context::<E>();

            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered_for_write(&self.db)?;

            // Sanitize & validate before key extraction in case PK fields are normalized
            sanitize(&mut entity)?;
            validate(&entity)?;
            Self::ensure_entity_invariants(&entity)?;

            // Enforce explicit strong relations before commit planning.
            self.validate_strong_relations(&entity)?;

            let (marker_row_op, data_key) = Self::prepare_marker_row_op(&ctx, mode, &entity)?;
            self.debug_log(format!("save {:?} on {} (key={})", mode, E::PATH, data_key));

            // Preflight data store availability before index mutations.
            ctx.with_store(|_| ())?;

            // Stage-2 commit protocol:
            // - preflight row-op preparation before persisting the marker
            // - then apply prepared row ops mechanically.
            // Durable correctness is marker + recovery owned. Apply guard rollback
            // here is best-effort, in-process cleanup only.
            let marker_row_ops = vec![marker_row_op];
            let prepared_row_ops = preflight_prepare_row_ops::<E>(&self.db, &marker_row_ops)?;
            let marker = CommitMarker::new(marker_row_ops)?;
            let index_removes = prepared_row_ops
                .iter()
                .fold(0usize, |acc, op| acc.saturating_add(op.index_remove_count));
            let index_inserts = prepared_row_ops
                .iter()
                .fold(0usize, |acc, op| acc.saturating_add(op.index_insert_count));
            let reverse_index_removes = prepared_row_ops.iter().fold(0usize, |acc, op| {
                acc.saturating_add(op.reverse_index_remove_count)
            });
            let reverse_index_inserts = prepared_row_ops.iter().fold(0usize, |acc, op| {
                acc.saturating_add(op.reverse_index_insert_count)
            });
            let commit = begin_commit(marker)?;
            commit_started = true;
            self.debug_log("Save commit window opened");

            // FIRST STABLE WRITE: commit marker is persisted before any mutations.
            apply_prepared_row_ops(
                commit,
                "save_row_apply",
                prepared_row_ops,
                || {
                    // NOTE: Trace metrics saturate on overflow; diagnostics only.
                    let removes = u64::try_from(index_removes).unwrap_or(u64::MAX);
                    let inserts = u64::try_from(index_inserts).unwrap_or(u64::MAX);
                    sink::record(MetricsEvent::IndexDelta {
                        entity_path: E::PATH,
                        inserts,
                        removes,
                    });

                    let reverse_removes = u64::try_from(reverse_index_removes).unwrap_or(u64::MAX);
                    let reverse_inserts = u64::try_from(reverse_index_inserts).unwrap_or(u64::MAX);
                    sink::record(MetricsEvent::ReverseIndexDelta {
                        entity_path: E::PATH,
                        inserts: reverse_inserts,
                        removes: reverse_removes,
                    });
                },
                || {
                    span.set_rows(1);
                },
            )?;

            self.debug_log("Save committed");

            Ok(entity)
        })();

        if commit_started && result.is_err() {
            self.debug_log("Save failed during marker apply; best-effort cleanup attempted");
        }

        if let Some(trace) = trace {
            match &result {
                Ok(_) => trace.finish(1),
                Err(err) => trace.error(err),
            }
        }

        result
    }
}

const fn save_mode_tag(mode: SaveMode) -> &'static str {
    match mode {
        SaveMode::Insert => "insert",
        SaveMode::Update => "update",
        SaveMode::Replace => "replace",
    }
}
