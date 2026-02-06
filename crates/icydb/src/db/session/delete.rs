use crate::{
    db::{
        Row,
        query::{
            Query,
            expr::{FilterExpr, SortExpr},
            predicate::Predicate,
        },
        response::{Response, map_response_error},
    },
    error::Error,
    traits::{CanisterKind, EntityKind, EntityValue, SingletonEntity},
    types::Id,
    view::View,
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
    // Primary-key access (semantic)
    // ------------------------------------------------------------------

    /// Set the access path to a single entity identity.
    #[must_use]
    pub fn by_id(mut self, id: Id<E>) -> Self {
        self.inner = self.inner.by_id(id);
        self
    }

    /// Set the access path to multiple entity identities.
    #[must_use]
    pub fn by_ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = Id<E>>,
    {
        self.inner = self.inner.by_ids(ids);
        self
    }

    // ------------------------------------------------------------------
    // Query Refinement
    // ------------------------------------------------------------------

    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, Error> {
        let core_expr = expr.lower::<E>()?;
        self.inner = self.inner.filter_expr(core_expr)?;
        Ok(self)
    }

    pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, Error> {
        let core_expr = expr.lower();
        self.inner = self.inner.sort_expr(core_expr)?;
        Ok(self)
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

    pub fn execute(&self) -> Result<Response<E>, Error>
    where
        E: EntityValue,
    {
        Ok(Response::from_core(self.inner.execute()?))
    }

    pub fn is_empty(&self) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.is_empty())
    }

    pub fn count(&self) -> Result<u32, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.count())
    }

    pub fn require_one(&self) -> Result<(), Error>
    where
        E: EntityValue,
    {
        self.inner
            .execute()?
            .require_one()
            .map_err(map_response_error)
    }

    pub fn require_some(&self) -> Result<(), Error>
    where
        E: EntityValue,
    {
        self.inner
            .execute()?
            .require_some()
            .map_err(map_response_error)
    }

    pub fn row(&self) -> Result<Row<E>, Error>
    where
        E: EntityValue,
    {
        self.inner.execute()?.row().map_err(map_response_error)
    }

    pub fn try_row(&self) -> Result<Option<Row<E>>, Error>
    where
        E: EntityValue,
    {
        self.inner.execute()?.try_row().map_err(map_response_error)
    }

    pub fn rows(&self) -> Result<Vec<Row<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.rows())
    }

    pub fn entity(&self) -> Result<E, Error>
    where
        E: EntityValue,
    {
        self.inner.execute()?.entity().map_err(map_response_error)
    }

    pub fn try_entity(&self) -> Result<Option<E>, Error>
    where
        E: EntityValue,
    {
        self.inner
            .execute()?
            .try_entity()
            .map_err(map_response_error)
    }

    pub fn entities(&self) -> Result<Vec<E>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.entities())
    }

    // ------------------------------------------------------------------
    // Primary-key results (semantic)
    // ------------------------------------------------------------------

    pub fn key(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.id())
    }

    pub fn require_key(&self) -> Result<Id<E>, Error>
    where
        E: EntityValue,
    {
        self.inner
            .execute()?
            .require_id()
            .map_err(map_response_error)
    }

    pub fn try_key(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        self.inner
            .execute()?
            .try_row()
            .map(|row| row.map(|(id, _)| id))
            .map_err(map_response_error)
    }

    pub fn keys(&self) -> Result<Vec<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.ids())
    }

    pub fn contains_key(&self, id: &Id<E>) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.contains_id(id))
    }

    pub fn view(&self) -> Result<View<E>, Error>
    where
        E: EntityValue,
    {
        self.inner.execute()?.view().map_err(map_response_error)
    }

    pub fn view_opt(&self) -> Result<Option<View<E>>, Error>
    where
        E: EntityValue,
    {
        self.inner.execute()?.view_opt().map_err(map_response_error)
    }

    pub fn views(&self) -> Result<Vec<View<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute()?.views())
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C> + SingletonEntity> SessionDeleteQuery<'_, C, E> {
    /// Delete the singleton entity.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}
