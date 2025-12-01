pub mod executor;
pub mod primitives;
pub mod query;
pub mod response;
pub mod store;

use crate::{
    Error,
    db::{
        executor::{Context, DeleteExecutor, ExecutorError, LoadExecutor, SaveExecutor},
        query::QueryError,
        response::ResponseError,
        store::{DataStoreRegistry, IndexStoreRegistry, StoreError},
    },
    serialize::SerializeError,
    traits::{CanisterKind, EntityKind},
    visitor::VisitorError,
};
use std::{marker::PhantomData, thread::LocalKey};
use thiserror::Error as ThisError;

///
/// DbError
///

#[derive(Debug, ThisError)]
pub enum DbError {
    #[error(transparent)]
    ExecutorError(#[from] ExecutorError),

    #[error(transparent)]
    QueryError(#[from] QueryError),

    #[error(transparent)]
    ResponseError(#[from] ResponseError),

    #[error(transparent)]
    SerializeError(#[from] SerializeError),

    #[error(transparent)]
    StoreError(#[from] StoreError),

    #[error(transparent)]
    VisitorError(#[from] VisitorError),
}

///
/// Db
///
/// A handle to the set of stores registered for a specific canister domain.
///
/// - `C` is the [`CanisterKind`] (schema canister marker).
///
/// The `Db` acts as the entry point for querying, saving, and deleting entities
/// within a single canister's store registry.
///

pub struct Db<C: CanisterKind> {
    data: &'static LocalKey<DataStoreRegistry>,
    index: &'static LocalKey<IndexStoreRegistry>,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    #[must_use]
    pub const fn new(
        data: &'static LocalKey<DataStoreRegistry>,
        index: &'static LocalKey<IndexStoreRegistry>,
    ) -> Self {
        Self {
            data,
            index,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub const fn context<E>(&self) -> Context<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        Context::new(self)
    }

    pub fn with_data<R>(&self, f: impl FnOnce(&DataStoreRegistry) -> R) -> R {
        self.data.with(|reg| f(reg))
    }

    pub fn with_index<R>(&self, f: impl FnOnce(&IndexStoreRegistry) -> R) -> R {
        self.index.with(|reg| f(reg))
    }
}

// Manual Copy + Clone implementations.
// Safe because Db only contains &'static LocalKey<_> handles,
// duplicating them does not duplicate the contents.
impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}

///
/// DbSession
/// database plus a debug boolean, as we don't want to store the bool
/// inside the database handle
///

pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
}

impl<C: CanisterKind> DbSession<C> {
    #[must_use]
    pub const fn new(db: Db<C>) -> Self {
        Self { db, debug: false }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    //
    // Low-level executors
    //

    /// Get a [`LoadExecutor`] for building and executing queries that read entities.
    #[must_use]
    pub const fn load<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        LoadExecutor::new(self.db, self.debug)
    }

    /// Get a [`SaveExecutor`] for inserting or updating entities.
    ///
    /// Normally you will use the higher-level `create/replace/update` shortcuts instead.
    #[must_use]
    pub const fn save<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        SaveExecutor::new(self.db, self.debug)
    }

    /// Get a [`DeleteExecutor`] for deleting entities by key or query.
    #[must_use]
    pub const fn delete<E>(&self) -> DeleteExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        DeleteExecutor::new(self.db, self.debug)
    }

    //
    // High-level save shortcuts
    //

    pub fn insert<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        self.save::<E>().insert(entity)
    }

    pub fn replace<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        self.save::<E>().replace(entity)
    }

    pub fn update<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        self.save::<E>().update(entity)
    }

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        self.save::<E>().insert_view::<E::ViewType>(view)
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        self.save::<E>().replace_view::<E::ViewType>(view)
    }

    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        self.save::<E>().update_view::<E::ViewType>(view)
    }
}
