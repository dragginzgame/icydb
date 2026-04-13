use crate::{
    db::{
        ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor, PersistedRow, Row,
        query::{
            AggregateExpr, CompareOp, CompiledQuery, ExplainPlan, FilterExpr, PlannedQuery,
            Predicate, Query, QueryTracePlan, SortExpr,
        },
        response::{PagedResponse, QueryResponse, Response},
        session::macros::{impl_session_materialization_methods, impl_session_query_shape_methods},
    },
    error::Error,
    traits::{EntityValue, SingletonEntity},
    types::{Decimal, Id},
    value::Value,
};
use icydb_core as core;

type MinMaxIds<E> = Option<(Id<E>, Id<E>)>;

///
/// FluentLoadQuery
///
/// Session-bound fluent wrapper for typed load queries.
/// This facade keeps query shaping and execution on the public `icydb`
/// surface while delegating planning and execution to `icydb-core`.
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

    /// Add one grouped HAVING compare clause over one grouped key field.
    pub fn having_group(
        mut self,
        field: impl AsRef<str>,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, Error> {
        self.inner = self.inner.having_group(field, op, value)?;
        Ok(self)
    }

    /// Add one grouped HAVING compare clause over one grouped aggregate output.
    pub fn having_aggregate(
        mut self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, Error> {
        self.inner = self.inner.having_aggregate(aggregate_index, op, value)?;
        Ok(self)
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

    /// Return the stable plan hash for this query.
    pub fn plan_hash_hex(&self) -> Result<String, Error> {
        Ok(self.inner.plan_hash_hex()?)
    }

    /// Build one trace payload without executing the query.
    pub fn trace(&self) -> Result<QueryTracePlan, Error> {
        Ok(self.inner.trace()?)
    }

    /// Build the validated logical plan without compiling execution details.
    pub fn planned(&self) -> Result<PlannedQuery<E>, Error> {
        Ok(self.inner.planned()?)
    }

    /// Build the compiled executable plan for this query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, Error> {
        Ok(self.inner.plan()?)
    }

    /// Build logical explain metadata for the current query.
    pub fn explain(&self) -> Result<ExplainPlan, Error> {
        Ok(self.inner.explain()?)
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

    /// Explain scalar `exists()` routing without executing the terminal.
    pub fn explain_exists(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_exists()?)
    }

    /// Return whether no matching row exists.
    pub fn not_exists(&self) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.not_exists()?)
    }

    /// Explain scalar `not_exists()` routing without executing the terminal.
    pub fn explain_not_exists(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_not_exists()?)
    }

    /// Explain the execution shape without executing the query.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_execution()?)
    }

    /// Render execution explain output as a compact text tree.
    pub fn explain_execution_text(&self) -> Result<String, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_execution_text()?)
    }

    /// Render execution explain output as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_execution_json()?)
    }

    /// Render execution explain output as a verbose text tree.
    pub fn explain_execution_verbose(&self) -> Result<String, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_execution_verbose()?)
    }

    /// Return total persisted payload bytes for the effective result window.
    pub fn bytes(&self) -> Result<u64, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.bytes()?)
    }

    /// Return total serialized bytes for one projected field over the effective result window.
    pub fn bytes_by(&self, field: impl AsRef<str>) -> Result<u64, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.bytes_by(field)?)
    }

    /// Explain `bytes_by(field)` routing without executing the terminal.
    pub fn explain_bytes_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_bytes_by(field)?)
    }

    /// Return the minimum identifier under deterministic response ordering.
    pub fn min(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.min()?)
    }

    /// Explain scalar `min()` routing without executing the terminal.
    pub fn explain_min(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_min()?)
    }

    /// Return the identifier with the minimum `field` value.
    pub fn min_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.min_by(field)?)
    }

    /// Return the maximum identifier under deterministic response ordering.
    pub fn max(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.max()?)
    }

    /// Explain scalar `max()` routing without executing the terminal.
    pub fn explain_max(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_max()?)
    }

    /// Return the identifier with the maximum `field` value.
    pub fn max_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.max_by(field)?)
    }

    /// Return the `nth` identifier by deterministic `(field asc, id asc)` ordering.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.nth_by(field, nth)?)
    }

    /// Return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.sum_by(field)?)
    }

    /// Explain scalar `sum_by(field)` routing without executing the terminal.
    pub fn explain_sum_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_sum_by(field)?)
    }

    /// Return the sum of distinct `field` values.
    pub fn sum_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.sum_distinct_by(field)?)
    }

    /// Explain scalar `sum(distinct field)` routing without executing the terminal.
    pub fn explain_sum_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_sum_distinct_by(field)?)
    }

    /// Return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.avg_by(field)?)
    }

    /// Explain scalar `avg_by(field)` routing without executing the terminal.
    pub fn explain_avg_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_avg_by(field)?)
    }

    /// Return the average of distinct `field` values.
    pub fn avg_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.avg_distinct_by(field)?)
    }

    /// Explain scalar `avg(distinct field)` routing without executing the terminal.
    pub fn explain_avg_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_avg_distinct_by(field)?)
    }

    /// Return the median identifier by deterministic `(field asc, id asc)` ordering.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.median_by(field)?)
    }

    /// Return the distinct value count for `field` over the effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.count_distinct_by(field)?)
    }

    /// Explain `count_distinct_by(field)` routing without executing the terminal.
    pub fn explain_count_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_count_distinct_by(field)?)
    }

    /// Return both `(min_by(field), max_by(field))` in one terminal.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxIds<E>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.min_max_by(field)?)
    }

    /// Return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.values_by(field)?)
    }

    /// Explain `values_by(field)` routing without executing the terminal.
    pub fn explain_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_values_by(field)?)
    }

    /// Return the first `k` rows from the effective result window.
    pub fn take(&self, take_count: u32) -> Result<Response<E>, Error>
    where
        E: EntityValue,
    {
        Ok(Response::from_core(self.inner.take(take_count)?))
    }

    /// Return the top `k` rows by deterministic `(field desc, id asc)` ordering.
    pub fn top_k_by(&self, field: impl AsRef<str>, take_count: u32) -> Result<Response<E>, Error>
    where
        E: EntityValue,
    {
        Ok(Response::from_core(self.inner.top_k_by(field, take_count)?))
    }

    /// Return the bottom `k` rows by deterministic `(field asc, id asc)` ordering.
    pub fn bottom_k_by(&self, field: impl AsRef<str>, take_count: u32) -> Result<Response<E>, Error>
    where
        E: EntityValue,
    {
        Ok(Response::from_core(
            self.inner.bottom_k_by(field, take_count)?,
        ))
    }

    /// Return projected values for the top `k` rows by `field`.
    pub fn top_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.top_k_by_values(field, take_count)?)
    }

    /// Return projected values for the bottom `k` rows by `field`.
    pub fn bottom_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.bottom_k_by_values(field, take_count)?)
    }

    /// Return projected id/value pairs for the top `k` rows by `field`.
    pub fn top_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.top_k_by_with_ids(field, take_count)?)
    }

    /// Return projected id/value pairs for the bottom `k` rows by `field`.
    pub fn bottom_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.bottom_k_by_with_ids(field, take_count)?)
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

    /// Explain `distinct_values_by(field)` routing without executing the terminal.
    pub fn explain_distinct_values_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_distinct_values_by(field)?)
    }

    /// Return projected field values paired with row ids for the effective result window.
    pub fn values_by_with_ids(&self, field: impl AsRef<str>) -> Result<Vec<(Id<E>, Value)>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.values_by_with_ids(field)?)
    }

    /// Explain `values_by_with_ids(field)` routing without executing the terminal.
    pub fn explain_values_by_with_ids(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_values_by_with_ids(field)?)
    }

    /// Return the first projected field value in effective response order.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.first_value_by(field)?)
    }

    /// Explain `first_value_by(field)` routing without executing the terminal.
    pub fn explain_first_value_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_first_value_by(field)?)
    }

    /// Return the last projected field value in effective response order.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.last_value_by(field)?)
    }

    /// Explain `last_value_by(field)` routing without executing the terminal.
    pub fn explain_last_value_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_last_value_by(field)?)
    }

    /// Return the first matching identifier in response order.
    pub fn first(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.first()?)
    }

    /// Explain scalar `first()` routing without executing the terminal.
    pub fn explain_first(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_first()?)
    }

    /// Return the last matching identifier in response order.
    pub fn last(&self) -> Result<Option<Id<E>>, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.last()?)
    }

    /// Explain scalar `last()` routing without executing the terminal.
    pub fn explain_last(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.explain_last()?)
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
/// Facade wrapper for cursor-pagination mode over typed load queries.
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
