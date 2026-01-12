use crate::{
    Error,
    db::{map_runtime, query::SaveQuery},
    traits::EntityKind,
};
use icydb_core::{self as core};

///
/// SaveExecutor
///

pub struct SaveExecutor<E: EntityKind> {
    inner: core::db::executor::SaveExecutor<E>,
}

impl<E: EntityKind> SaveExecutor<E> {
    pub(crate) const fn from_core(inner: core::db::executor::SaveExecutor<E>) -> Self {
        Self { inner }
    }

    /// Insert a brand-new entity (errors if the key already exists).
    pub fn insert(&self, entity: E) -> Result<E, Error> {
        map_runtime(self.inner.insert(entity))
    }

    /// Insert a new view, returning the stored view.
    pub fn insert_view(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        map_runtime(self.inner.insert_view(view))
    }

    /// Update an existing entity (errors if it does not exist).
    pub fn update(&self, entity: E) -> Result<E, Error> {
        map_runtime(self.inner.update(entity))
    }

    /// Update an existing view (errors if it does not exist).
    pub fn update_view(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        map_runtime(self.inner.update_view(view))
    }

    /// Replace an entity, inserting if missing.
    pub fn replace(&self, entity: E) -> Result<E, Error> {
        map_runtime(self.inner.replace(entity))
    }

    /// Replace a view, inserting if missing.
    pub fn replace_view(&self, view: E::ViewType) -> Result<E::ViewType, Error> {
        map_runtime(self.inner.replace_view(view))
    }

    pub fn insert_many(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error> {
        map_runtime(self.inner.insert_many(entities))
    }

    pub fn update_many(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error> {
        map_runtime(self.inner.update_many(entities))
    }

    pub fn replace_many(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error> {
        map_runtime(self.inner.replace_many(entities))
    }

    /// Execute a serialized save query.
    pub fn execute(&self, query: SaveQuery) -> Result<E, Error> {
        map_runtime(self.inner.execute(query.into()))
    }

    #[must_use]
    pub const fn debug(self) -> Self {
        Self {
            inner: self.inner.debug(),
        }
    }
}
