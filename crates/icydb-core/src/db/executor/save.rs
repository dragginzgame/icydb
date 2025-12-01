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
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    // debug
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    ///
    /// EXECUTION METHODS
    ///

    pub fn insert(&self, entity: E) -> Result<E, Error> {
        let entity = self.save_entity(SaveMode::Insert, entity)?;

        Ok(entity)
    }

    pub fn insert_view<V>(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        let saved_view = self.insert(entity)?.to_view();

        Ok(saved_view)
    }

    pub fn update(&self, entity: E) -> Result<E, Error> {
        let entity = self.save_entity(SaveMode::Update, entity)?;

        Ok(entity)
    }

    pub fn update_view<V>(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        let saved_view = self.update(entity)?.to_view();

        Ok(saved_view)
    }

    pub fn replace(&self, entity: E) -> Result<E, Error> {
        let entity = self.save_entity(SaveMode::Replace, entity)?;

        Ok(entity)
    }

    pub fn replace_view<V>(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        let entity = E::from_view(view);
        let saved_view = self.replace(entity)?.to_view();

        Ok(saved_view)
    }

    // execute
    // serializes the save query to pass to save_entity
    pub fn execute(&self, query: SaveQuery) -> Result<E, Error> {
        let e: E = deserialize(&query.bytes)?;
        let entity = self.save_entity(query.mode, e)?;

        Ok(entity)
    }

    // save_entity
    fn save_entity(&self, mode: SaveMode, mut entity: E) -> Result<E, Error> {
        let mut span = metrics::Span::<E>::new(metrics::ExecKind::Save);
        let key = entity.key();
        let ctx = self.db.context::<E>();

        // sanitize & validate
        sanitize(&mut entity);
        validate(&entity)?;

        // debug
        //   debug!(self.debug, "query.{mode}: {} ({key:?}) ", E::PATH);

        //
        // match save mode
        // on Update and Replace compare old and new data
        //

        let data_key = DataKey::new::<E>(key);
        let old_result = ctx.with_store(|store| store.get(&data_key))?;

        // did anything change?
        let old = match (mode, old_result) {
            (SaveMode::Insert | SaveMode::Replace, None) => None,

            (SaveMode::Update | SaveMode::Replace, Some(old_bytes)) => {
                let old = deserialize::<E>(&old_bytes)?;
                Some(old)
            }

            // invalid
            (SaveMode::Insert, Some(_)) => return Err(ExecutorError::KeyExists(data_key))?,
            (SaveMode::Update, None) => return Err(ExecutorError::KeyNotFound(data_key))?,
        };

        // now we can serialize
        let bytes = serialize(&entity)?;

        // replace indexes, fail if there are any unique violations
        self.replace_indexes(old.as_ref(), &entity)?;

        // insert data row
        ctx.with_store_mut(|store| store.insert(data_key.clone(), bytes))?;
        span.set_rows(1);

        Ok(entity)
    }

    // replace_indexes: two-phase (validate, then mutate) to avoid partial updates
    fn replace_indexes(&self, old: Option<&E>, new: &E) -> Result<(), Error> {
        use crate::db::store::IndexKey;

        // Phase 1: validate uniqueness for all indexes without mutating
        for index in E::INDEXES {
            // Only check when we can compute the new key and the index is unique
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
                    // Count the unique violation just like the store-level check would have
                    metrics::with_state_mut(|m| {
                        metrics::record_unique_violation_for::<E>(m);
                    });

                    return Err(ExecutorError::index_violation(E::PATH, index.fields).into());
                }
            }
        }

        // Phase 2: apply changes (remove old, insert new) for each index
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
