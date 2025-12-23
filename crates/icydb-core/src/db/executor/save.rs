use crate::{
    Error,
    db::{
        Db,
        executor::ExecutorError,
        query::{SaveMode, SaveQuery},
        store::DataKey,
    },
    deserialize,
    obs::metrics,
    serialize,
    traits::EntityKind,
    visitor::{sanitize, validate},
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
    pub fn insert(&self, entity: E) -> Result<E, Error> {
        self.save_entity(SaveMode::Insert, entity)
    }

    /// Insert a new view, returning the stored view.
    pub fn insert_view(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        Ok(self.insert(entity)?.to_view())
    }

    /// Update an existing entity (errors if it does not exist).
    pub fn update(&self, entity: E) -> Result<E, Error> {
        self.save_entity(SaveMode::Update, entity)
    }

    /// Update an existing view (errors if it does not exist).
    pub fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        Ok(self.update(entity)?.to_view())
    }

    /// Replace an entity, inserting if missing.
    pub fn replace(&self, entity: E) -> Result<E, Error> {
        self.save_entity(SaveMode::Replace, entity)
    }

    /// Replace a view, inserting if missing.
    pub fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        Ok(self.replace(entity)?.to_view())
    }

    // ======================================================================
    // Batch save operations (fail-fast, non-atomic)
    // ======================================================================

    pub fn insert_many(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);
        for entity in iter {
            out.push(self.insert(entity)?);
        }
        Ok(out)
    }

    pub fn update_many(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);
        for entity in iter {
            out.push(self.update(entity)?);
        }
        Ok(out)
    }

    pub fn replace_many(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);
        for entity in iter {
            out.push(self.replace(entity)?);
        }
        Ok(out)
    }

    // ======================================================================
    // Low-level execution
    // ======================================================================

    /// Execute a serialized save query.
    pub fn execute(&self, query: SaveQuery) -> Result<E, Error> {
        let entity: E = deserialize(&query.bytes)?;
        self.save_entity(query.mode, entity)
    }

    fn save_entity(&self, mode: SaveMode, mut entity: E) -> Result<E, Error> {
        let mut span = metrics::Span::<E>::new(metrics::ExecKind::Save);
        let ctx = self.db.context::<E>();

        // sanitize & validate before key extraction in case PK fields are normalized
        sanitize(&mut entity);
        validate(&entity)?;

        // match save mode
        let key = entity.key();
        let data_key = DataKey::new::<E>(key);
        let old_result = ctx.with_store(|store| store.get(&data_key))?;

        let old = match (mode, old_result) {
            (SaveMode::Insert | SaveMode::Replace, None) => None,

            (SaveMode::Update | SaveMode::Replace, Some(old_bytes)) => {
                Some(deserialize::<E>(&old_bytes)?)
            }

            (SaveMode::Insert, Some(_)) => return Err(ExecutorError::KeyExists(data_key))?,
            (SaveMode::Update, None) => return Err(ExecutorError::KeyNotFound(data_key))?,
        };

        // serialize new entity
        let bytes = serialize(&entity)?;

        // update indexes (two-phase)
        self.replace_indexes(old.as_ref(), &entity)?;

        // write data row
        ctx.with_store_mut(|store| store.insert(data_key.clone(), bytes))?;
        span.set_rows(1);

        Ok(entity)
    }

    // ======================================================================
    // Index maintenance
    // ======================================================================

    /// Replace index entries using a two-phase (validate, then mutate) approach
    /// to avoid partial updates on uniqueness violations.
    fn replace_indexes(&self, old: Option<&E>, new: &E) -> Result<(), Error> {
        use crate::db::store::IndexKey;

        // Phase 1: validate uniqueness constraints without mutating
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
                    metrics::with_state_mut(|m| {
                        metrics::record_unique_violation_for::<E>(m);
                    });

                    return Err(ExecutorError::index_violation(E::PATH, index.fields).into());
                }
            }
        }

        // Phase 2: apply mutations
        for index in E::INDEXES {
            let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
            store.with_borrow_mut(|s| {
                if let Some(old) = old {
                    s.remove_index_entry(old, index);
                }
                s.insert_index_entry(new, index)?;
                Ok::<(), Error>(())
            })?;
        }

        Ok(())
    }
}
