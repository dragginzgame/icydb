use crate::{
    Error,
    db::{
        executor::{
            delete::DeleteExecutor, load::LoadExecutor, save::SaveExecutor, upsert::UpsertExecutor,
        },
        response::Response,
    },
    traits::{CanisterKind, EntityKind, FromKey},
};
use core::obs::sink::MetricsSink;
use icydb_core::{self as core, error::InternalError, model::index::IndexModel};

///
/// Re-exports
///

pub mod primitives {
    pub use icydb_core::db::primitives::*;
}
pub mod executor;
pub mod query;
pub mod response;

///
/// Helpers
///

fn map_runtime<T>(res: Result<T, InternalError>) -> Result<T, Error> {
    res.map_err(Error::from)
}

fn map_response<E: EntityKind>(
    res: Result<core::db::response::Response<E>, InternalError>,
) -> Result<Response<E>, Error> {
    map_runtime(res).map(Response::from)
}

///
/// DbSession
/// Database handle plus a debug flag that controls executor verbosity.
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
    // Low-level executors
    //

    /// Get a [`LoadExecutor`] for building and executing queries that read entities.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn load<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        LoadExecutor::from_core(self.inner.load::<E>())
    }

    /// Get a [`SaveExecutor`] for inserting or updating entities.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn save<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        SaveExecutor::from_core(self.inner.save::<E>())
    }

    /// Get an [`UpsertExecutor`] for inserting or updating by a unique index.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn upsert<E>(&self) -> UpsertExecutor<E>
    where
        E: EntityKind<Canister = C>,
        E::PrimaryKey: FromKey,
    {
        UpsertExecutor::from_core(self.inner.upsert::<E>())
    }

    /// Get a [`DeleteExecutor`] for deleting entities by key or query.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn delete<E>(&self) -> DeleteExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        DeleteExecutor::from_core(self.inner.delete::<E>())
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

    /// Insert multiple entities, returning stored values.
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

    /// Replace multiple entities, inserting if missing.
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

    /// Partially update multiple existing entities.
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

///
/// UniqueIndexHandle
/// Validated handle to a unique index for an entity type.
///

#[derive(Clone, Copy)]
pub struct UniqueIndexHandle {
    inner: core::db::executor::UniqueIndexHandle,
}

impl UniqueIndexHandle {
    #[must_use]
    /// Return the underlying index specification.
    pub const fn index(&self) -> &'static IndexModel {
        self.inner.index()
    }

    /// Wrap a unique index for the given entity type.
    pub fn new<E: EntityKind>(index: &'static IndexModel) -> Result<Self, Error> {
        core::db::executor::UniqueIndexHandle::new::<E>(index)
            .map(|inner| Self { inner })
            .map_err(Error::from)
    }

    /// Resolve a unique index by its field list for the given entity type.
    pub fn for_fields<E: EntityKind>(fields: &[&str]) -> Result<Self, Error> {
        core::db::executor::UniqueIndexHandle::for_fields::<E>(fields)
            .map(|inner| Self { inner })
            .map_err(Error::from)
    }

    pub(crate) const fn as_core(self) -> core::db::executor::UniqueIndexHandle {
        self.inner
    }
}
