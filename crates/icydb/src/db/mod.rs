use crate::{
    Error,
    traits::{CanisterKind, EntityKind},
};
use core::obs::sink::MetricsSink;
use icydb_core::{self as core, error::InternalError};

///
/// Re-exports
///
pub mod query;
pub mod response;

///
/// Helpers
///

fn map_runtime<T>(res: Result<T, InternalError>) -> Result<T, Error> {
    res.map_err(Error::from)
}

///
/// DbSession
/// Database handle plus a debug flag that controls query verbosity.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    #[doc(hidden)]
    pub(crate) const fn from_core(db: core::db::Db<C>) -> Self {
        Self {
            inner: core::db::DbSession::new(db),
        }
    }

    #[must_use]
    /// Enable debug logging for subsequent queries in this session.
    pub const fn debug(self) -> Self {
        Self {
            inner: self.inner.debug(),
        }
    }

    #[must_use]
    /// Override the metrics sink for operations executed through this session.
    pub const fn metrics_sink(self, sink: &'static dyn MetricsSink) -> Self {
        Self {
            inner: self.inner.metrics_sink(sink),
        }
    }

    //
    // High-level save shortcuts
    //

    /// Insert a new entity, returning the stored value.
    pub fn insert<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.insert(entity))
    }

    /// Insert multiple entities, returning stored values (best-effort, non-atomic).
    ///
    /// Individual inserts are atomic, but the batch may partially succeed.
    pub fn insert_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.insert_many(entities))
    }

    /// Replace an existing entity or insert it if it does not yet exist.
    pub fn replace<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.replace(entity))
    }

    /// Replace multiple entities, inserting if missing (best-effort, non-atomic).
    ///
    /// Individual replaces are atomic, but the batch may partially succeed.
    pub fn replace_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.replace_many(entities))
    }

    /// Partially update an existing entity.
    pub fn update<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.update(entity))
    }

    /// Partially update multiple existing entities (best-effort, non-atomic).
    ///
    /// Individual updates are atomic, but the batch may partially succeed.
    pub fn update_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.update_many(entities))
    }

    /// Insert a new view value for an entity.
    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.insert_view::<E>(view))
    }

    /// Replace an existing view or insert it if it does not yet exist.
    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.replace_view::<E>(view))
    }

    /// Partially update an existing view.
    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        map_runtime(self.inner.update_view::<E>(view))
    }
}
