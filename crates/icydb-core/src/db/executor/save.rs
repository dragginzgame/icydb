use crate::{
    db::{
        CommitDataOp, CommitKind, CommitMarker, Db, begin_commit, ensure_recovered,
        executor::{ExecutorError, WriteUnit},
        finish_commit,
        index::{
            IndexInsertOutcome, IndexRemoveOutcome,
            plan::{IndexApplyPlan, plan_index_mutation_for_entity},
        },
        query::{SaveMode, SaveQuery},
        store::{DataKey, RawRow},
    },
    error::{ErrorOrigin, InternalError},
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
        let _unit = WriteUnit::new("save_entity_stage2_atomic");

        // Recovery is mutation-only to keep read paths side-effect free.
        ensure_recovered(&self.db)?;

        // Sanitize & validate before key extraction in case PK fields are normalized
        sanitize(&mut entity)?;
        validate(&entity)?;

        let key = entity.key();
        let data_key = DataKey::new::<E>(key);
        let raw_key = data_key.to_raw();
        let old_result = ctx.with_store(|store| store.get(&raw_key))?;

        let old = match (mode, old_result) {
            (SaveMode::Insert | SaveMode::Replace, None) => None,
            (SaveMode::Update | SaveMode::Replace, Some(old_row)) => {
                Some(old_row.try_decode::<E>().map_err(|err| {
                    ExecutorError::corruption(
                        ErrorOrigin::Serialize,
                        format!("failed to deserialize row: {data_key} ({err})"),
                    )
                })?)
            }
            (SaveMode::Insert, Some(_)) => return Err(ExecutorError::KeyExists(data_key).into()),
            (SaveMode::Update, None) => return Err(ExecutorError::KeyNotFound(data_key).into()),
        };

        let bytes = serialize(&entity)?;
        let row = RawRow::try_new(bytes)?;

        // Preflight data store availability before index mutations.
        ctx.with_store(|_| ())?;

        // Stage-2 atomicity:
        // Prevalidate index/data mutations before the commit marker is written.
        // After the marker is persisted, only infallible operations or traps remain.
        let index_plan =
            plan_index_mutation_for_entity::<E>(&self.db, old.as_ref(), Some(&entity))?;
        let data_op = CommitDataOp {
            store: E::Store::PATH.to_string(),
            key: raw_key.as_bytes().to_vec(),
            value: Some(row.as_bytes().to_vec()),
        };
        let marker = CommitMarker::new(CommitKind::Save, index_plan.commit_ops, vec![data_op])?;
        let commit = begin_commit(marker)?;

        // FIRST STABLE WRITE: commit marker is persisted; apply phase is infallible or traps.
        finish_commit(
            commit,
            || Self::apply_indexes(&index_plan.apply, old.as_ref(), &entity),
            || {
                ctx.with_store_mut(|store| store.insert(raw_key, row))
                    .expect("data store missing after preflight");
                span.set_rows(1);
            },
        );

        Ok(entity)
    }

    // ======================================================================
    // Index maintenance
    // ======================================================================

    /// Apply index mutations using an infallible (prevalidated) plan.
    fn apply_indexes(plans: &[IndexApplyPlan], old: Option<&E>, new: &E) {
        // Prevalidation guarantees these mutations cannot fail except by trap.
        for plan in plans {
            let mut removed = false;
            let mut inserted = false;

            plan.store.with_borrow_mut(|s| {
                if let Some(old) = old {
                    let outcome = s
                        .remove_index_entry(old, plan.index)
                        .expect("index remove failed after prevalidation");
                    if outcome == IndexRemoveOutcome::Removed {
                        removed = true;
                    }
                }

                let outcome = s
                    .insert_index_entry(new, plan.index)
                    .expect("index insert failed after prevalidation");
                if outcome == IndexInsertOutcome::Inserted {
                    inserted = true;
                }
            });

            if removed {
                sink::record(MetricsEvent::IndexRemove {
                    entity_path: E::PATH,
                });
            }

            if inserted {
                sink::record(MetricsEvent::IndexInsert {
                    entity_path: E::PATH,
                });
            }
        }
    }
}
