use crate::{
    db::{
        Row,
        query::{FilterExpr, Predicate, Query, SortExpr},
        response::{Response, map_response_error},
    },
    error::Error,
    traits::{CanisterKind, EntityKind, UnitKey},
    types::Ref,
    view::View,
};
use icydb_core as core;
use std::{borrow::Borrow, collections::HashMap, hash::Hash};

///
/// SessionLoadQuery
///
/// Session-bound fluent wrapper for load queries.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = Ref<E>>> {
    pub(crate) inner: core::db::query::SessionLoadQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = Ref<E>>> SessionLoadQuery<'_, C, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    /// Return a reference to the underlying query intent.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access
    // ------------------------------------------------------------------

    /// Filter by primary key.
    #[must_use]
    pub fn by_key(mut self, key: E::PrimaryKey) -> Self {
        self.inner = self.inner.by_key(key);
        self
    }

    /// Load multiple entities by primary key.
    ///
    /// Uses key-based access only.
    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<E::PrimaryKey>,
    {
        self.inner = self.inner.many(keys.into_iter().map(|k| *k.borrow()));

        self
    }

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    /// Apply a dynamic filter expression.
    pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, Error> {
        let core_expr = expr.lower::<E>()?;
        self.inner = self.inner.filter_expr(core_expr)?;

        Ok(self)
    }

    /// Apply a dynamic sort expression.
    pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, Error> {
        let core_expr = expr.lower();
        self.inner = self.inner.sort_expr(core_expr)?;

        Ok(self)
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by_desc(field);
        self
    }

    /// Apply a load limit to bound result size.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// Apply a load offset.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    // ------------------------------------------------------------------
    // Execution terminals
    // ------------------------------------------------------------------

    /// Execute and return whether the response is empty.
    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.inner.is_empty()?)
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, Error> {
        Ok(self.inner.count()?)
    }

    /// Explain this query without executing it.
    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    /// Execute this query and return the facade response.
    pub fn execute(&self) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.execute()?))
    }

    /// Execute and require exactly one row.
    pub fn require_one(&self) -> Result<(), Error> {
        self.inner
            .execute()?
            .require_one()
            .map_err(map_response_error)
    }

    /// Execute and require at least one row.
    pub fn require_some(&self) -> Result<(), Error> {
        self.inner
            .execute()?
            .require_some()
            .map_err(map_response_error)
    }

    /// Execute and return the single row.
    pub fn row(&self) -> Result<Row<E>, Error> {
        self.inner.execute()?.row().map_err(map_response_error)
    }

    /// Execute and return zero or one row.
    pub fn try_row(&self) -> Result<Option<Row<E>>, Error> {
        self.inner.execute()?.try_row().map_err(map_response_error)
    }

    /// Execute and return all rows.
    pub fn rows(&self) -> Result<Vec<Row<E>>, Error> {
        Ok(self.inner.execute()?.rows())
    }

    /// Execute and return the single entity.
    pub fn entity(&self) -> Result<E, Error> {
        self.inner.execute()?.entity().map_err(map_response_error)
    }

    /// Execute and return zero or one entity.
    pub fn try_entity(&self) -> Result<Option<E>, Error> {
        self.inner
            .execute()?
            .try_entity()
            .map_err(map_response_error)
    }

    /// Execute and return all entities.
    pub fn entities(&self) -> Result<Vec<E>, Error> {
        Ok(self.inner.execute()?.entities())
    }

    /// Materialize all entities and count them grouped by a derived key.
    ///
    /// This is a facade-level helper that performs in-memory aggregation
    /// over the query result. It does not affect query planning or execution.
    pub fn group_count_by<K>(self, key: impl Fn(&E) -> K) -> Result<HashMap<K, u32>, Error>
    where
        K: Eq + Hash,
    {
        let entities = self.inner.execute()?.entities();

        let mut counts = HashMap::new();
        for entity in entities {
            *counts.entry(key(&entity)).or_insert(0) += 1;
        }

        Ok(counts)
    }

    /// Execute and return the first primary key, if any.
    pub fn key(&self) -> Result<Option<E::PrimaryKey>, Error> {
        Ok(self.inner.execute()?.key())
    }

    /// Execute and require exactly one primary key.
    pub fn key_strict(&self) -> Result<E::PrimaryKey, Error> {
        self.inner
            .execute()?
            .key_strict()
            .map_err(map_response_error)
    }

    /// Execute and return zero or one primary key.
    pub fn try_key(&self) -> Result<Option<E::PrimaryKey>, Error> {
        self.inner.execute()?.try_key().map_err(map_response_error)
    }

    /// Execute and return all primary keys.
    pub fn keys(&self) -> Result<Vec<E::PrimaryKey>, Error> {
        Ok(self.inner.execute()?.keys())
    }

    /// Execute and check whether the response contains the provided key.
    pub fn contains_key(&self, key: &E::PrimaryKey) -> Result<bool, Error> {
        Ok(self.inner.execute()?.contains_key(key))
    }

    /// Execute and require exactly one primary key.
    pub fn primary_key(&self) -> Result<E::PrimaryKey, Error> {
        self.inner
            .execute()?
            .primary_key()
            .map_err(map_response_error)
    }

    /// Execute and return zero or one primary key.
    pub fn try_primary_key(&self) -> Result<Option<E::PrimaryKey>, Error> {
        self.inner
            .execute()?
            .try_primary_key()
            .map_err(map_response_error)
    }

    /// Execute and return all primary keys.
    pub fn primary_keys(&self) -> Result<Vec<E::PrimaryKey>, Error> {
        Ok(self.inner.execute()?.primary_keys())
    }

    /// Execute and return all entities.
    pub fn all(&self) -> Result<Vec<E>, Error> {
        self.entities()
    }

    /// Execute and return all results as views.
    pub fn views(&self) -> Result<Vec<View<E>>, Error> {
        Ok(self.inner.execute()?.views())
    }

    /// Execute and require exactly one entity.
    pub fn one(&self) -> Result<E, Error> {
        self.entity()
    }

    /// Execute and return zero or one entity.
    pub fn one_opt(&self) -> Result<Option<E>, Error> {
        self.try_entity()
    }

    /// Execute and require exactly one view.
    pub fn view(&self) -> Result<View<E>, Error> {
        self.inner.execute()?.view().map_err(map_response_error)
    }

    /// Execute and return zero or one view.
    pub fn view_opt(&self) -> Result<Option<View<E>>, Error> {
        self.inner.execute()?.view_opt().map_err(map_response_error)
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = Ref<E>> + UnitKey>
    SessionLoadQuery<'_, C, E>
{
    /// Load the singleton entity identified by `()`.
    #[must_use]
    pub fn only(mut self) -> Self {
        self.inner = self.inner.only();
        self
    }
}
