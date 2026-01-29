use crate::{
    db::{
        query::{Query, predicate::Predicate},
        response::Response,
    },
    error::Error,
    key::Key,
    traits::{CanisterKind, EntityKind},
};
use icydb_core as core;

///
/// SessionDeleteQuery
///
/// Session-bound fluent wrapper for delete queries.
///

pub struct SessionDeleteQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    pub(crate) inner: core::db::query::SessionDeleteQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionDeleteQuery<'_, C, E> {
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

    /// Delete multiple entities by primary key.
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

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    /// Execute the delete and return affected rows.
    pub fn execute(&self) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.execute()?))
    }

    /// Execute the delete and return affected rows (explicit form).
    pub fn delete_rows(&self) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.delete_rows()?))
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = ()>> SessionDeleteQuery<'_, C, E> {
    /// Delete the singleton entity identified by `()`.
    #[must_use]
    pub fn only(mut self) -> Self {
        self.inner = self.inner.only();
        self
    }
}
