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
    traits::{CanisterKind, EntityKind, SingletonEntity},
    view::View,
};
use icydb_core as core;
use std::borrow::Borrow;

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

    /// Delete by primary key.
    #[must_use]
    pub fn by_key(mut self, id: E::Id) -> Self {
        self.inner = self.inner.by_key(id);
        self
    }

    /// Delete multiple entities by primary key.
    ///
    /// Uses key-based access only.
    #[must_use]
    pub fn many<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<E::Id>,
    {
        self.inner = self.inner.many(ids.into_iter().map(|id| *id.borrow()));
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

    pub fn execute(&self) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.execute()?))
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.inner.execute()?.is_empty())
    }

    pub fn count(&self) -> Result<u32, Error> {
        Ok(self.inner.execute()?.count())
    }

    pub fn require_one(&self) -> Result<(), Error> {
        self.inner
            .execute()?
            .require_one()
            .map_err(map_response_error)
    }

    pub fn require_some(&self) -> Result<(), Error> {
        self.inner
            .execute()?
            .require_some()
            .map_err(map_response_error)
    }

    pub fn row(&self) -> Result<Row<E>, Error> {
        self.inner.execute()?.row().map_err(map_response_error)
    }

    pub fn try_row(&self) -> Result<Option<Row<E>>, Error> {
        self.inner.execute()?.try_row().map_err(map_response_error)
    }

    pub fn rows(&self) -> Result<Vec<Row<E>>, Error> {
        Ok(self.inner.execute()?.rows())
    }

    pub fn entity(&self) -> Result<E, Error> {
        self.inner.execute()?.entity().map_err(map_response_error)
    }

    pub fn try_entity(&self) -> Result<Option<E>, Error> {
        self.inner
            .execute()?
            .try_entity()
            .map_err(map_response_error)
    }

    pub fn entities(&self) -> Result<Vec<E>, Error> {
        Ok(self.inner.execute()?.entities())
    }

    // ------------------------------------------------------------------
    // Primary-key results (semantic)
    // ------------------------------------------------------------------

    pub fn key(&self) -> Result<Option<E::Id>, Error> {
        Ok(self.inner.execute()?.id())
    }

    pub fn key_strict(&self) -> Result<E::Id, Error> {
        self.inner
            .execute()?
            .id_strict()
            .map_err(map_response_error)
    }

    pub fn try_key(&self) -> Result<Option<E::Id>, Error> {
        self.inner
            .execute()?
            .try_row()
            .map(|row| row.map(|(id, _)| id))
            .map_err(map_response_error)
    }

    pub fn keys(&self) -> Result<Vec<E::Id>, Error> {
        Ok(self.inner.execute()?.ids())
    }

    pub fn contains_key(&self, id: &E::Id) -> Result<bool, Error> {
        Ok(self.inner.execute()?.contains_id(id))
    }

    pub fn view(&self) -> Result<View<E>, Error> {
        self.inner.execute()?.view().map_err(map_response_error)
    }

    pub fn view_opt(&self) -> Result<Option<View<E>>, Error> {
        self.inner.execute()?.view_opt().map_err(map_response_error)
    }

    pub fn views(&self) -> Result<Vec<View<E>>, Error> {
        Ok(self.inner.execute()?.views())
    }
}

impl<C: CanisterKind, E: SingletonEntity<Canister = C>> SessionDeleteQuery<'_, C, E> {
    /// Delete the singleton entity identified by an explicit ID.
    #[must_use]
    pub fn only(mut self, id: E::Id) -> Self {
        self.inner = self.inner.only(id);
        self
    }
}
