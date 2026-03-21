use crate::{
    db::{
        PersistedRow, Row,
        query::{AggregateExpr, FilterExpr, Predicate, Query, QueryTracePlan, SortExpr},
        response::{PagedGroupedResponse, PagedResponse, Response},
        session::macros::{impl_session_materialization_methods, impl_session_query_shape_methods},
    },
    error::Error,
    traits::{EntityValue, SingletonEntity},
    types::Id,
    value::Value,
};
use icydb_core as core;

///
/// FluentLoadQuery
///
/// Session-bound fluent wrapper for load queries.
///

pub struct FluentLoadQuery<'a, E: PersistedRow> {
    pub(crate) inner: core::db::FluentLoadQuery<'a, E>,
}

impl<'a, E: PersistedRow> FluentLoadQuery<'a, E> {
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

    impl_session_query_shape_methods!();

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    /// Skip a number of rows in the ordered result stream.
    ///
    /// Scalar pagination requires explicit ordering; combine `offset` and/or
    /// `limit` with `order_by(...)` or planning fails for scalar loads.
    /// GROUP BY pagination uses canonical grouped-key order by default.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    /// Attach an opaque cursor token for continuation pagination.
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.inner = self.inner.cursor(token);
        self
    }

    /// Add one grouped key field.
    pub fn group_by(mut self, field: impl AsRef<str>) -> Result<Self, Error> {
        self.inner = self.inner.group_by(field)?;
        Ok(self)
    }

    /// Add one grouped aggregate terminal.
    #[must_use]
    pub fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.inner = self.inner.aggregate(aggregate);
        self
    }

    /// Override grouped hard limits for grouped execution budget enforcement.
    #[must_use]
    pub fn grouped_limits(mut self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.inner = self.inner.grouped_limits(max_groups, max_group_bytes);
        self
    }

    // ------------------------------------------------------------------
    // Execution primitives
    // ------------------------------------------------------------------
    impl_session_materialization_methods!();

    /// Enter typed cursor-pagination mode for this query.
    ///
    /// Cursor pagination requires explicit ordering and limit, and disallows offset.
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn page(self) -> Result<PagedLoadQuery<'a, E>, Error> {
        Ok(PagedLoadQuery {
            inner: self.inner.page()?,
        })
    }

    /// Execute as cursor pagination, returning entities plus an opaque continuation token.
    pub fn execute_paged(self) -> Result<PagedResponse<E>, Error>
    where
        E: EntityValue,
    {
        self.page()?.execute()
    }

    /// Execute one grouped query page with optional continuation cursor.
    ///
    /// Grouped rows are returned as grouped key/value vectors to preserve
    /// grouped response fidelity.
    pub fn execute_grouped(self) -> Result<PagedGroupedResponse, Error>
    where
        E: EntityValue,
    {
        let execution = self.inner.execute_grouped()?;
        let next_cursor = execution.continuation_cursor().map(core::db::encode_cursor);

        Ok(PagedGroupedResponse::new(
            execution.rows().to_vec(),
            next_cursor,
            execution.execution_trace().copied(),
        ))
    }

    /// Return the stable plan hash for this query.
    pub fn plan_hash_hex(&self) -> Result<String, Error> {
        Ok(self.inner.plan_hash_hex()?)
    }

    /// Build one trace payload without executing the query.
    pub fn trace(&self) -> Result<QueryTracePlan, Error> {
        Ok(self.inner.trace()?)
    }

    // ------------------------------------------------------------------
    // Aggregation helpers
    // ------------------------------------------------------------------

    /// Return whether at least one matching row exists.
    pub fn exists(&self) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.exists()?)
    }

    /// Return whether no matching row exists.
    pub fn not_exists(&self) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.not_exists()?)
    }

    /// Return the first matching identifier in response order.
    pub fn first(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.first()?)
    }

    /// Return total persisted payload bytes for the effective result window.
    pub fn bytes(&self) -> Result<u64, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.bytes()?)
    }

    /// Return the last matching identifier in response order.
    pub fn last(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.last()?)
    }

    /// Return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.values_by(field)?)
    }

    /// Return distinct projected field values for the effective result window.
    ///
    /// Value order preserves first observation in effective response order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.distinct_values_by(field)?)
    }

    /// Return projected field values paired with row ids for the effective
    /// result window.
    pub fn values_by_with_ids(&self, field: impl AsRef<str>) -> Result<Vec<(Id<E>, Value)>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.values_by_with_ids(field)?)
    }

    /// Return the first projected field value in effective response order.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.first_value_by(field)?)
    }

    /// Return the last projected field value in effective response order.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.last_value_by(field)?)
    }

    // ------------------------------------------------------------------
    // Convenience aliases (semantic sugar)
    // ------------------------------------------------------------------

    pub fn one(&self) -> Result<E, Error>
    where
        E: EntityValue,
    {
        self.entity()
    }

    pub fn one_opt(&self) -> Result<Option<E>, Error>
    where
        E: EntityValue,
    {
        self.try_entity()
    }

    pub fn all(&self) -> Result<Vec<E>, Error>
    where
        E: EntityValue,
    {
        self.entities()
    }
}

impl<E: PersistedRow + SingletonEntity> FluentLoadQuery<'_, E> {
    /// Load the singleton entity.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}

///
/// PagedLoadQuery
///
/// Facade wrapper for cursor-pagination mode.
/// Returns typed entity items plus an opaque continuation cursor.
///

pub struct PagedLoadQuery<'a, E: PersistedRow> {
    pub(crate) inner: core::db::PagedLoadQuery<'a, E>,
}

impl<E: PersistedRow> PagedLoadQuery<'_, E> {
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    /// Attach an opaque continuation cursor token for the next page.
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.inner = self.inner.cursor(token);
        self
    }

    /// Execute in cursor-pagination mode.
    ///
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn execute(self) -> Result<PagedResponse<E>, Error>
    where
        E: EntityValue,
    {
        let execution = self.inner.execute()?;
        let (response, continuation_cursor) = execution.into_parts();
        let next_cursor = continuation_cursor.as_deref().map(core::db::encode_cursor);

        Ok(PagedResponse::new(response.entities(), next_cursor))
    }
}
