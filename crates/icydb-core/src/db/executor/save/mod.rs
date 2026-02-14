mod invariants;
mod relations;
#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::value::Value;
use crate::{
    db::{
        CommitKind, CommitMarker, CommitRowOp, Db, begin_commit, ensure_recovered_for_write,
        executor::{
            ExecutorError,
            mutation::{apply_prepared_row_ops, preflight_prepare_row_ops},
            trace::{QueryTraceSink, TraceExecutorKind, start_exec_trace},
        },
        query::SaveMode,
        store::{DataKey, RawRow},
    },
    error::{ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::serialize,
    traits::{EntityKind, EntityValue},
    validate::validate,
};
use std::marker::PhantomData;

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
    // Batch save operations (fail-fast, non-atomic)
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

    #[expect(clippy::too_many_lines)]
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

            let key = entity.id().key();
            let data_key = DataKey::try_new::<E>(key)?;
            let raw_key = data_key.to_raw()?;

            self.debug_log(format!("save {:?} on {} (key={})", mode, E::PATH, data_key));
            let (_old, old_raw) = match mode {
                SaveMode::Insert => {
                    // Inserts must not load or decode existing rows; absence is expected.
                    if let Some(existing) = ctx.with_store(|store| store.get(&raw_key))? {
                        let stored = existing.try_decode::<E>().map_err(|err| {
                            ExecutorError::corruption(
                                ErrorOrigin::Serialize,
                                format!("failed to deserialize row: {data_key} ({err})"),
                            )
                        })?;

                        let expected = data_key.try_key::<E>()?;
                        let actual = stored.id().key();
                        if expected != actual {
                            return Err(ExecutorError::corruption(
                                ErrorOrigin::Store,
                                format!(
                                    "row key mismatch: expected {expected:?}, found {actual:?}",
                                ),
                            )
                            .into());
                        }

                        return Err(ExecutorError::KeyExists(data_key).into());
                    }

                    (None, None)
                }
                SaveMode::Update => {
                    let Some(old_row) = ctx.with_store(|store| store.get(&raw_key))? else {
                        return Err(InternalError::store_not_found(data_key.to_string()));
                    };
                    let old = old_row.try_decode::<E>().map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Serialize,
                            format!("failed to deserialize row: {data_key} ({err})"),
                        )
                    })?;
                    let expected = data_key.try_key::<E>()?;
                    let actual = old.id().key();
                    if expected != actual {
                        return Err(ExecutorError::corruption(
                            ErrorOrigin::Store,
                            format!("row key mismatch: expected {expected:?}, found {actual:?}",),
                        )
                        .into());
                    }
                    (Some(old), Some(old_row))
                }
                SaveMode::Replace => {
                    let old_row = ctx.with_store(|store| store.get(&raw_key))?;
                    let old = old_row
                        .as_ref()
                        .map(|row| {
                            row.try_decode::<E>().map_err(|err| {
                                ExecutorError::corruption(
                                    ErrorOrigin::Serialize,
                                    format!("failed to deserialize row: {data_key} ({err})"),
                                )
                            })
                        })
                        .transpose()?;
                    if let Some(old) = old.as_ref() {
                        let expected = data_key.try_key::<E>()?;
                        let actual = old.id().key();
                        if expected != actual {
                            return Err(ExecutorError::corruption(
                                ErrorOrigin::Store,
                                format!(
                                    "row key mismatch: expected {expected:?}, found {actual:?}",
                                ),
                            )
                            .into());
                        }
                    }
                    (old, old_row)
                }
            };

            let bytes = serialize(&entity)?;
            let row = RawRow::try_new(bytes)?;

            // Preflight data store availability before index mutations.
            ctx.with_store(|_| ())?;

            // Stage-2 commit protocol:
            // - preflight row-op preparation before persisting the marker
            // - then apply prepared row ops mechanically.
            // Durable correctness is marker + recovery owned. Apply guard rollback
            // here is best-effort, in-process cleanup only.
            let after_bytes = row.as_bytes().to_vec();
            let marker_row_ops = vec![CommitRowOp::new(
                E::PATH,
                raw_key.as_bytes().to_vec(),
                old_raw.as_ref().map(|row| row.as_bytes().to_vec()),
                Some(after_bytes),
            )];
            let prepared_row_ops = preflight_prepare_row_ops::<E>(&self.db, &marker_row_ops)?;
            let marker = CommitMarker::new(CommitKind::Save, marker_row_ops)?;
            let index_removes = prepared_row_ops
                .iter()
                .fold(0usize, |acc, op| acc.saturating_add(op.index_remove_count));
            let index_inserts = prepared_row_ops
                .iter()
                .fold(0usize, |acc, op| acc.saturating_add(op.index_insert_count));
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
