use crate::{
    db::{
        query::{Query, predicate::Predicate},
        response::Response,
    },
    error::Error,
    key::Key,
    traits::{CanisterKind, EntityKind},
    view::View,
};
use icydb_core as core;

///
/// SessionLoadQuery
///
/// Session-bound fluent wrapper for load queries.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    pub(crate) inner: core::db::query::SessionLoadQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'_, C, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access
    // ------------------------------------------------------------------

    #[must_use]
    pub fn key(mut self, key: impl Into<Key>) -> Self {
        self.inner = self.inner.key(key.into());
        self
    }

    /// Load multiple entities by primary key.
    ///
    /// Uses key-based access only.
    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::PrimaryKey>,
    {
        self.inner = self.inner.many(keys);
        self
    }

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by_desc(field);
        self
    }

    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    #[must_use]
    pub fn offset(mut self, offset: u64) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    // ------------------------------------------------------------------
    // Execution terminals
    // ------------------------------------------------------------------

    pub fn exists(&self) -> Result<bool, Error> {
        Ok(self.inner.exists()?)
    }

    pub fn count(&self) -> Result<u64, Error> {
        Ok(self.inner.count()?)
    }

    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    pub fn execute(&self) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.execute()?))
    }

    pub fn all(&self) -> Result<Vec<E>, Error> {
        self.execute().map(Response::entities)
    }

    pub fn views(&self) -> Result<Vec<View<E>>, Error> {
        self.execute().map(|r| r.views())
    }

    pub fn one(&self) -> Result<E, Error> {
        self.execute()?.entity()
    }

    pub fn one_opt(&self) -> Result<Option<E>, Error> {
        self.execute()?.try_entity()
    }

    pub fn view(&self) -> Result<View<E>, Error> {
        self.execute()?.view()
    }

    pub fn view_opt(&self) -> Result<Option<View<E>>, Error> {
        self.execute()?.view_opt()
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = ()>> SessionLoadQuery<'_, C, E> {
    /// Load the singleton entity identified by `()`.
    #[must_use]
    pub fn only(mut self) -> Self {
        self.inner = self.inner.only();
        self
    }
}
