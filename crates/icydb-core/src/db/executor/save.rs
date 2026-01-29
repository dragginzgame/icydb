use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, WriteUnit, begin_commit,
        ensure_recovered,
        executor::{
            ExecutorError,
            trace::{QueryTraceSink, TraceExecutorKind, start_exec_trace},
        },
        finish_commit,
        index::{
            IndexKey, IndexStore, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey,
            plan::{IndexApplyPlan, plan_index_mutation_for_entity},
        },
        query::SaveMode,
        store::{DataKey, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::serialize,
    traits::{EntityKind, Path, Storable},
    validate::validate,
};
use std::{
    borrow::Cow, cell::RefCell, collections::BTreeMap, marker::PhantomData, thread::LocalKey,
};

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub struct SaveExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    trace: Option<&'static dyn QueryTraceSink>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> SaveExecutor<E> {
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

    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn with_trace_sink(
        mut self,
        sink: Option<&'static dyn QueryTraceSink>,
    ) -> Self {
        self.trace = sink;
        self
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
        let entity = E::from_view(view);
        Ok(self.insert(entity)?.to_view())
    }

    /// Update an existing entity (errors if it does not exist).
    pub fn update(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Update an existing view (errors if it does not exist).
    pub fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);

        Ok(self.update(entity)?.to_view())
    }

    /// Replace an entity, inserting if missing.
    pub fn replace(&self, entity: E) -> Result<E, InternalError> {
        self.save_entity(SaveMode::Replace, entity)
    }

    /// Replace a view, inserting if missing.
    pub fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, InternalError> {
        let entity = E::from_view(view);

        Ok(self.replace(entity)?.to_view())
    }

    // ======================================================================
    // Batch save operations (fail-fast, non-atomic)
    // ======================================================================

    pub fn insert_many(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        // Batch semantics: fail-fast and non-atomic; partial successes remain.
        // Retry-safe only with caller idempotency and conflict handling.
        for entity in iter {
            out.push(self.insert(entity)?);
        }

        Ok(out)
    }

    pub fn update_many(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        // Batch semantics: fail-fast and non-atomic; partial successes remain.
        // Retry-safe only if the caller tolerates already-updated rows.
        for entity in iter {
            out.push(self.update(entity)?);
        }

        Ok(out)
    }

    pub fn replace_many(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);

        // Batch semantics: fail-fast and non-atomic; partial successes remain.
        // Retry-safe only with caller idempotency and conflict handling.
        for entity in iter {
            out.push(self.replace(entity)?);
        }

        Ok(out)
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

            // Recovery is mutation-only to keep read paths side-effect free.
            ensure_recovered(&self.db)?;

            // Sanitize & validate before key extraction in case PK fields are normalized
            sanitize(&mut entity)?;
            validate(&entity)?;

            let key = entity.key();
            let data_key = DataKey::new::<E>(key);
            let raw_key = data_key.to_raw()?;

            self.debug_log(format!("save {:?} on {} (key={})", mode, E::PATH, data_key));
            let (old, old_raw) = match mode {
                SaveMode::Insert => {
                    // Inserts must not load or decode existing rows; absence is expected.
                    if ctx.with_store(|store| store.contains_key(&raw_key))? {
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
                    (old, old_row)
                }
            };

            let bytes = serialize(&entity)?;
            let row = RawRow::try_new(bytes)?;

            // Preflight data store availability before index mutations.
            ctx.with_store(|_| ())?;

            // Stage-2 atomicity:
            // Prevalidate index/data mutations before the commit marker is written.
            // After the marker is persisted, mutations run inside a WriteUnit so
            // failures roll back before the marker is cleared.
            let index_plan =
                plan_index_mutation_for_entity::<E>(&self.db, old.as_ref(), Some(&entity))?;
            let data_op = CommitDataOp {
                store: E::Store::PATH.to_string(),
                key: raw_key.as_bytes().to_vec(),
                value: Some(row.as_bytes().to_vec()),
            };
            let marker = CommitMarker::new(CommitKind::Save, index_plan.commit_ops, vec![data_op])?;
            let (index_apply_stores, index_rollback_ops) =
                Self::prepare_index_save_ops(&index_plan.apply, &marker.index_ops)?;
            let (index_removes, index_inserts) = Self::plan_index_metrics(old.as_ref(), &entity)?;
            let data_rollback_ops = Self::prepare_data_save_ops(&marker.data_ops, old_raw)?;
            let commit = begin_commit(marker)?;
            commit_started = true;
            self.debug_log("Save commit window opened");

            // FIRST STABLE WRITE: commit marker is persisted before any mutations.
            finish_commit(commit, |guard| {
                let mut unit = WriteUnit::new("save_entity_atomic");
                let index_rollback_ops = index_rollback_ops;
                unit.record_rollback(move || Self::apply_index_rollbacks(index_rollback_ops));
                Self::apply_marker_index_ops(&guard.marker.index_ops, index_apply_stores);
                for _ in 0..index_removes {
                    sink::record(MetricsEvent::IndexRemove {
                        entity_path: E::PATH,
                    });
                }
                for _ in 0..index_inserts {
                    sink::record(MetricsEvent::IndexInsert {
                        entity_path: E::PATH,
                    });
                }

                unit.checkpoint("save_entity_after_indexes")?;

                let data_rollback_ops = data_rollback_ops;
                let db = self.db;
                unit.record_rollback(move || Self::apply_data_rollbacks(db, data_rollback_ops));
                unit.run(|| Self::apply_marker_data_ops(&guard.marker.data_ops, &ctx))?;

                span.set_rows(1);
                unit.commit();
                Ok(())
            })?;

            self.debug_log("Save committed");

            Ok(entity)
        })();

        if commit_started && result.is_err() {
            self.debug_log("Save failed; rollback applied");
        }

        if let Some(trace) = trace {
            match &result {
                Ok(_) => trace.finish(1),
                Err(err) => trace.error(err),
            }
        }

        result
    }

    // ======================================================================
    // Commit-marker apply (mechanical)
    // ======================================================================

    /// Precompute index mutation metrics before the commit marker is persisted.
    fn plan_index_metrics(old: Option<&E>, new: &E) -> Result<(usize, usize), InternalError> {
        let mut removes = 0usize;
        let mut inserts = 0usize;

        for index in E::INDEXES {
            if let Some(old) = old
                && IndexKey::new(old, index)?.is_some()
            {
                removes = removes.saturating_add(1);
            }
            if IndexKey::new(new, index)?.is_some() {
                inserts = inserts.saturating_add(1);
            }
        }

        Ok((removes, inserts))
    }

    /// Resolve commit index ops into stores and capture rollback entries.
    #[allow(clippy::type_complexity)]
    fn prepare_index_save_ops(
        plans: &[IndexApplyPlan],
        ops: &[CommitIndexOp],
    ) -> Result<
        (
            Vec<&'static LocalKey<RefCell<IndexStore>>>,
            Vec<PreparedIndexRollback>,
        ),
        InternalError,
    > {
        // Phase 1: map index store paths to store handles.
        let mut stores = BTreeMap::new();
        for plan in plans {
            stores.insert(plan.index.store, plan.store);
        }

        let mut apply_stores = Vec::with_capacity(ops.len());
        let mut rollbacks = Vec::with_capacity(ops.len());

        // Phase 2: validate marker ops and snapshot current entries for rollback.
        for op in ops {
            let store = stores.get(op.store.as_str()).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker references unknown index store '{}' ({})",
                        op.store,
                        E::PATH
                    ),
                )
            })?;
            if op.key.len() != IndexKey::STORED_SIZE as usize {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index key length {} does not match {} ({})",
                        op.key.len(),
                        IndexKey::STORED_SIZE,
                        E::PATH
                    ),
                ));
            }
            if let Some(value) = &op.value
                && value.len() > MAX_INDEX_ENTRY_BYTES as usize
            {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index entry exceeds max size: {} bytes ({})",
                        value.len(),
                        E::PATH
                    ),
                ));
            }

            let raw_key = RawIndexKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            let existing = store.with_borrow(|s| s.get(&raw_key));
            if op.value.is_none() && existing.is_none() {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index op missing entry before save: {} ({})",
                        op.store,
                        E::PATH
                    ),
                ));
            }

            apply_stores.push(*store);
            rollbacks.push(PreparedIndexRollback {
                store,
                key: raw_key,
                value: existing,
            });
        }

        Ok((apply_stores, rollbacks))
    }

    /// Validate commit data ops and prepare rollback rows for the save.
    fn prepare_data_save_ops(
        ops: &[CommitDataOp],
        old_row: Option<RawRow>,
    ) -> Result<Vec<PreparedDataRollback>, InternalError> {
        if ops.len() != 1 {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker save expects 1 data op, found {} ({})",
                    ops.len(),
                    E::PATH
                ),
            ));
        }

        let op = &ops[0];
        if op.store != E::Store::PATH {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker references unexpected data store '{}' ({})",
                    op.store,
                    E::PATH
                ),
            ));
        }
        if op.key.len() != DataKey::STORED_SIZE as usize {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker data key length {} does not match {} ({})",
                    op.key.len(),
                    DataKey::STORED_SIZE,
                    E::PATH
                ),
            ));
        }
        let Some(value) = &op.value else {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!("commit marker save missing data payload ({})", E::PATH),
            ));
        };
        if value.len() > crate::db::store::MAX_ROW_BYTES as usize {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!(
                    "commit marker data payload exceeds max size: {} bytes ({})",
                    value.len(),
                    E::PATH
                ),
            ));
        }

        let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
        Ok(vec![PreparedDataRollback {
            key: raw_key,
            value: old_row,
        }])
    }

    /// Apply commit marker index ops using pre-resolved stores.
    fn apply_marker_index_ops(
        ops: &[CommitIndexOp],
        stores: Vec<&'static LocalKey<RefCell<IndexStore>>>,
    ) {
        debug_assert_eq!(
            ops.len(),
            stores.len(),
            "commit marker index ops length mismatch"
        );

        for (op, store) in ops.iter().zip(stores.into_iter()) {
            debug_assert_eq!(op.key.len(), IndexKey::STORED_SIZE as usize);
            let raw_key = RawIndexKey::from_bytes(Cow::Borrowed(op.key.as_slice()));

            store.with_borrow_mut(|s| {
                if let Some(value) = &op.value {
                    debug_assert!(value.len() <= MAX_INDEX_ENTRY_BYTES as usize);
                    let raw_entry = RawIndexEntry::from_bytes(Cow::Borrowed(value.as_slice()));
                    s.insert(raw_key, raw_entry);
                } else {
                    s.remove(&raw_key);
                }
            });
        }
    }

    /// Apply rollback mutations for index entries using raw bytes.
    fn apply_index_rollbacks(ops: Vec<PreparedIndexRollback>) {
        for op in ops {
            op.store.with_borrow_mut(|s| {
                if let Some(value) = op.value {
                    s.insert(op.key, value);
                } else {
                    s.remove(&op.key);
                }
            });
        }
    }

    /// Apply commit marker data ops to the data store.
    fn apply_marker_data_ops(
        ops: &[CommitDataOp],
        ctx: &crate::db::executor::Context<'_, E>,
    ) -> Result<(), InternalError> {
        for op in ops {
            debug_assert!(op.value.is_some());
            let Some(value) = op.value.as_ref() else {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker save missing data payload ({})", E::PATH),
                ));
            };
            let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            let raw_value = RawRow::from_bytes(Cow::Borrowed(value.as_slice()));
            ctx.with_store_mut(|s| s.insert(raw_key, raw_value))?;
        }
        Ok(())
    }

    /// Apply rollback mutations for saved rows.
    fn apply_data_rollbacks(db: Db<E::Canister>, ops: Vec<PreparedDataRollback>) {
        let ctx = db.context::<E>();
        for op in ops {
            let _ = ctx.with_store_mut(|s| {
                if let Some(value) = op.value {
                    s.insert(op.key, value);
                } else {
                    s.remove(&op.key);
                }
            });
        }
    }
}

const fn save_mode_tag(mode: SaveMode) -> &'static str {
    match mode {
        SaveMode::Insert => "insert",
        SaveMode::Update => "update",
        SaveMode::Replace => "replace",
    }
}

/// Rollback descriptor for index mutations recorded in a commit marker.
struct PreparedIndexRollback {
    store: &'static LocalKey<RefCell<IndexStore>>,
    key: RawIndexKey,
    value: Option<RawIndexEntry>,
}

/// Rollback descriptor for data mutations recorded in a commit marker.
struct PreparedDataRollback {
    key: RawDataKey,
    value: Option<RawRow>,
}
