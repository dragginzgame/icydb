use crate::{
    db::{
        CommitDataOp, CommitKind, CommitMarker, Db, WriteUnit, begin_commit, ensure_recovered,
        executor::ExecutorError,
        finish_commit,
        index::{
            IndexInsertError, IndexInsertOutcome, IndexRemoveOutcome,
            plan::{IndexApplyPlan, plan_index_mutation_for_entity},
        },
        query::{SaveMode, SaveQuery},
        store::{DataKey, RawDataKey, RawRow},
    },
    error::{ErrorOrigin, InternalError},
    model::index::IndexModel,
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::{deserialize, serialize},
    traits::{EntityKind, Path},
    validate::validate,
};
use std::marker::PhantomData;

///
/// SaveExecutor
///

#[derive(Clone, Copy)]
pub struct SaveExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> SaveExecutor<E> {
    // ======================================================================
    // Construction & configuration
    // ======================================================================

    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("{}", s.into());
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

    // ======================================================================
    // Low-level execution
    // ======================================================================

    /// Execute a serialized save query.
    ///
    /// NOTE: Deserialization here is over user-supplied bytes. Failures are
    /// considered invalid input rather than storage corruption.
    pub fn execute(&self, query: SaveQuery) -> Result<E, InternalError> {
        let entity: E = deserialize(&query.bytes)?;
        self.save_entity(query.mode, entity)
    }

    fn save_entity(&self, mode: SaveMode, mut entity: E) -> Result<E, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Save);
        let ctx = self.db.context::<E>();

        // Recovery is mutation-only to keep read paths side-effect free.
        ensure_recovered(&self.db)?;

        // Sanitize & validate before key extraction in case PK fields are normalized
        sanitize(&mut entity)?;
        validate(&entity)?;

        let key = entity.key();
        let data_key = DataKey::new::<E>(key);
        let raw_key = data_key.to_raw();

        self.debug_log(format!(
            "[debug] save {:?} on {} (key={})",
            mode,
            E::PATH,
            data_key
        ));
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
        let commit = begin_commit(marker)?;

        // FIRST STABLE WRITE: commit marker is persisted before any mutations.
        finish_commit(commit, |guard| {
            let mut unit = WriteUnit::new("save_entity_atomic");
            Self::apply_indexes(&index_plan.apply, old.as_ref(), &entity, &mut unit)?;
            unit.checkpoint("save_entity_after_indexes")?;
            guard.mark_index_written();
            Self::apply_data(self.db, raw_key, row, old_raw, &mut unit, &mut span)?;
            unit.commit();
            Ok(())
        })?;

        Ok(entity)
    }

    // ======================================================================
    // Index maintenance
    // ======================================================================

    /// Apply index mutations from a prevalidated plan, registering rollbacks on change.
    fn apply_indexes(
        plans: &[IndexApplyPlan],
        old: Option<&E>,
        new: &E,
        unit: &mut WriteUnit,
    ) -> Result<(), InternalError> {
        for plan in plans {
            let fields = plan.index.fields.join(", ");

            let (removed, inserted) = unit.run(|| {
                plan.store.with_borrow_mut(|s| {
                    let removed = if let Some(old) = old {
                        s.remove_index_entry(old, plan.index).map_err(|err| {
                            ExecutorError::corruption(
                                ErrorOrigin::Index,
                                format!("index corrupted: {} ({fields}) -> {err}", E::PATH),
                            )
                        })? == IndexRemoveOutcome::Removed
                    } else {
                        false
                    };

                    let inserted = matches!(
                        s.insert_index_entry(new, plan.index).map_err(|err| {
                            Self::map_index_insert_error(plan.index, &fields, err)
                        })?,
                        IndexInsertOutcome::Inserted
                    );

                    Ok((removed, inserted))
                })
            })?;

            if removed {
                if let Some(old) = old.cloned() {
                    let store = plan.store;
                    let index = plan.index;
                    unit.record_rollback(move || {
                        let _ = store.with_borrow_mut(|s| s.insert_index_entry(&old, index));
                    });
                }

                sink::record(MetricsEvent::IndexRemove {
                    entity_path: E::PATH,
                });
            }

            if inserted {
                let store = plan.store;
                let index = plan.index;
                let new = new.clone();
                unit.record_rollback(move || {
                    let _ = store.with_borrow_mut(|s| s.remove_index_entry(&new, index));
                });

                sink::record(MetricsEvent::IndexInsert {
                    entity_path: E::PATH,
                });
            }
        }

        Ok(())
    }

    fn apply_data(
        db: Db<E::Canister>,
        raw_key: RawDataKey,
        row: RawRow,
        old_row: Option<RawRow>,
        unit: &mut WriteUnit,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        unit.run(|| {
            db.context::<E>().with_store_mut(|store| {
                store.insert(raw_key, row);
            })
        })?;

        span.set_rows(1);

        match old_row {
            Some(old_row) => {
                unit.record_rollback(move || {
                    let ctx = db.context::<E>();
                    let _ = ctx.with_store_mut(|store| {
                        store.insert(raw_key, old_row);
                    });
                });
            }
            None => {
                unit.record_rollback(move || {
                    let ctx = db.context::<E>();
                    let _ = ctx.with_store_mut(|store| {
                        store.remove(&raw_key);
                    });
                });
            }
        }

        Ok(())
    }

    fn map_index_insert_error(
        index: &IndexModel,
        fields: &str,
        err: IndexInsertError,
    ) -> InternalError {
        match err {
            IndexInsertError::UniqueViolation => {
                ExecutorError::index_violation(E::PATH, index.fields).into()
            }
            IndexInsertError::CorruptedEntry(err) => ExecutorError::corruption(
                ErrorOrigin::Index,
                format!("index corrupted: {} ({fields}) -> {err}", E::PATH),
            )
            .into(),
            IndexInsertError::EntryTooLarge { keys } => ExecutorError::corruption(
                ErrorOrigin::Index,
                format!(
                    "index entry exceeds max keys: {} ({fields}) -> {keys}",
                    E::PATH
                ),
            )
            .into(),
        }
    }
}
