use crate::{
    db::{
        Db,
        executor::{ExecutorError, WriteUnit},
        query::{SaveMode, SaveQuery},
        store::{DataKey, IndexInsertError, IndexInsertOutcome, IndexRemoveOutcome},
    },
    error::InternalError,
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    sanitize::sanitize,
    serialize::{deserialize, serialize},
    traits::EntityKind,
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
        let _unit = WriteUnit::new("save_entity_non_atomic");

        // Sanitize & validate before key extraction in case PK fields are normalized
        sanitize(&mut entity)?;
        validate(&entity)?;

        let key = entity.key();
        let data_key = DataKey::new::<E>(key);
        let old_result = ctx.with_store(|store| store.get(&data_key))?;

        let old = match (mode, old_result) {
            (SaveMode::Insert | SaveMode::Replace, None) => None,
            (SaveMode::Update | SaveMode::Replace, Some(old_bytes)) => {
                Some(deserialize::<E>(&old_bytes)?)
            }
            (SaveMode::Insert, Some(_)) => return Err(ExecutorError::KeyExists(data_key).into()),
            (SaveMode::Update, None) => return Err(ExecutorError::KeyNotFound(data_key).into()),
        };

        let bytes = serialize(&entity)?;

        // Partial-write window:
        // - Phase 1 uniqueness checks are safe (no mutation, retry-safe).
        // - Phase 2 mutates indexes; failures here can leave index divergence.
        // - Data write happens after index updates; failures can orphan indexes.
        // Corruption risk exists if failures occur after index mutation.
        self.replace_indexes(old.as_ref(), &entity)?;

        ctx.with_store_mut(|store| store.insert(data_key.clone(), bytes))?;
        span.set_rows(1);

        Ok(entity)
    }

    // ======================================================================
    // Index maintenance
    // ======================================================================

    /// Replace index entries using a two-phase (validate, then mutate) approach
    /// to avoid partial updates on uniqueness violations.
    fn replace_indexes(&self, old: Option<&E>, new: &E) -> Result<(), InternalError> {
        use crate::db::store::IndexKey;

        // Phase 1: validate uniqueness constraints without mutating.
        for index in E::INDEXES {
            if index.unique
                && let Some(new_idx_key) = IndexKey::new(new, index)
            {
                let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
                let violates = store.with_borrow(|s| {
                    if let Some(existing) = s.get(&new_idx_key) {
                        let new_entity_key = new.key();
                        !existing.contains(&new_entity_key) && !existing.is_empty()
                    } else {
                        false
                    }
                });

                if violates {
                    sink::record(MetricsEvent::UniqueViolation {
                        entity_path: E::PATH,
                    });

                    return Err(ExecutorError::index_violation(E::PATH, index.fields).into());
                }
            }
        }

        // Phase 2: apply mutations.
        // Failure here can leave partial index updates (corruption risk).
        for index in E::INDEXES {
            let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
            let mut removed = false;
            let mut inserted = false;
            store.with_borrow_mut(|s| {
                if let Some(old) = old
                    && s.remove_index_entry(old, index) == IndexRemoveOutcome::Removed
                {
                    removed = true;
                }
                match s.insert_index_entry(new, index) {
                    Ok(IndexInsertOutcome::Inserted) => {
                        inserted = true;
                    }
                    Ok(IndexInsertOutcome::Skipped) => {}
                    Err(IndexInsertError::UniqueViolation) => {
                        sink::record(MetricsEvent::UniqueViolation {
                            entity_path: E::PATH,
                        });
                        return Err(ExecutorError::index_violation(E::PATH, index.fields).into());
                    }
                }
                Ok::<(), InternalError>(())
            })?;

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

        Ok(())
    }
}
