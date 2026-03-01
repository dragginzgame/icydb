//! Module: query::fluent::load
//! Responsibility: fluent load-query builder, pagination, and execution routing.
//! Does not own: planner semantics or row-level predicate evaluation.
//! Boundary: session API facade over query intent/planning/execution.

use crate::{
    db::{
        DbSession, PagedGroupedExecutionWithTrace, PagedLoadExecution, PagedLoadExecutionWithTrace,
        predicate::Predicate,
        query::{
            explain::ExplainPlan,
            expr::{FilterExpr, SortExpr},
            intent::{CompiledQuery, IntentError, PlannedQuery, Query, QueryError},
            policy,
        },
        response::Response,
    },
    traits::{EntityKind, EntityValue, SingletonEntity},
    types::{Decimal, Id},
    value::Value,
};

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

///
/// FluentLoadQuery
///
/// Session-bound load query wrapper.
/// Owns intent construction and execution routing only.
/// All result inspection and projection is performed on `Response<E>`.
///

pub struct FluentLoadQuery<'a, E>
where
    E: EntityKind,
{
    session: &'a DbSession<E::Canister>,
    query: Query<E>,
    cursor_token: Option<String>,
}

impl<'a, E> FluentLoadQuery<'a, E>
where
    E: EntityKind,
{
    pub(crate) const fn new(session: &'a DbSession<E::Canister>, query: Query<E>) -> Self {
        Self {
            session,
            query,
            cursor_token: None,
        }
    }

    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
    }

    fn map_query(mut self, map: impl FnOnce(Query<E>) -> Query<E>) -> Self {
        self.query = map(self.query);
        self
    }

    fn try_map_query(
        mut self,
        map: impl FnOnce(Query<E>) -> Result<Query<E>, QueryError>,
    ) -> Result<Self, QueryError> {
        self.query = map(self.query)?;
        Ok(self)
    }

    // ------------------------------------------------------------------
    // Intent builders (pure)
    // ------------------------------------------------------------------

    /// Set the access path to a single typed primary-key value.
    ///
    /// `Id<E>` is treated as a plain query input value here. It does not grant access.
    #[must_use]
    pub fn by_id(mut self, id: Id<E>) -> Self {
        self.query = self.query.by_id(id.key());
        self
    }

    /// Set the access path to multiple typed primary-key values.
    ///
    /// IDs are public and may come from untrusted input sources.
    #[must_use]
    pub fn by_ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = Id<E>>,
    {
        self.query = self.query.by_ids(ids.into_iter().map(|id| id.key()));
        self
    }

    // ------------------------------------------------------------------
    // Query Refinement
    // ------------------------------------------------------------------

    #[must_use]
    pub fn filter(self, predicate: Predicate) -> Self {
        self.map_query(|query| query.filter(predicate))
    }

    pub fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        self.try_map_query(|query| query.filter_expr(expr))
    }

    pub fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        self.try_map_query(|query| query.sort_expr(expr))
    }

    #[must_use]
    pub fn order_by(self, field: impl AsRef<str>) -> Self {
        self.map_query(|query| query.order_by(field))
    }

    #[must_use]
    pub fn order_by_desc(self, field: impl AsRef<str>) -> Self {
        self.map_query(|query| query.order_by_desc(field))
    }

    /// Add one grouped key field.
    pub fn group_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let Self {
            session,
            query,
            cursor_token,
        } = self;
        let query = query.group_by(field)?;

        Ok(Self {
            session,
            query,
            cursor_token,
        })
    }

    /// Add one grouped `count(*)` terminal.
    #[must_use]
    pub fn group_count(self) -> Self {
        self.map_query(Query::group_count)
    }

    /// Add one grouped `exists` terminal.
    #[must_use]
    pub fn group_exists(self) -> Self {
        self.map_query(Query::group_exists)
    }

    /// Add one grouped `first` terminal.
    #[must_use]
    pub fn group_first(self) -> Self {
        self.map_query(Query::group_first)
    }

    /// Add one grouped `last` terminal.
    #[must_use]
    pub fn group_last(self) -> Self {
        self.map_query(Query::group_last)
    }

    /// Add one grouped `min` terminal (id extrema).
    #[must_use]
    pub fn group_min(self) -> Self {
        self.map_query(Query::group_min)
    }

    /// Add one grouped `max` terminal (id extrema).
    #[must_use]
    pub fn group_max(self) -> Self {
        self.map_query(Query::group_max)
    }

    /// Add one grouped `min(field)` terminal.
    pub fn group_min_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let Self {
            session,
            query,
            cursor_token,
        } = self;
        let query = query.group_min_by(field)?;

        Ok(Self {
            session,
            query,
            cursor_token,
        })
    }

    /// Add one grouped `max(field)` terminal.
    pub fn group_max_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let Self {
            session,
            query,
            cursor_token,
        } = self;
        let query = query.group_max_by(field)?;

        Ok(Self {
            session,
            query,
            cursor_token,
        })
    }

    /// Override grouped hard limits for grouped execution budget enforcement.
    #[must_use]
    pub fn grouped_limits(self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.map_query(|query| query.grouped_limits(max_groups, max_group_bytes))
    }

    /// Bound the number of returned rows.
    ///
    /// Scalar pagination requires explicit ordering; combine `limit` and/or
    /// `offset` with `order_by(...)` or planning fails for scalar loads.
    /// GROUP BY pagination uses canonical grouped-key order by default.
    #[must_use]
    pub fn limit(self, limit: u32) -> Self {
        self.map_query(|query| query.limit(limit))
    }

    /// Skip a number of rows in the ordered result stream.
    ///
    /// Scalar pagination requires explicit ordering; combine `offset` and/or
    /// `limit` with `order_by(...)` or planning fails for scalar loads.
    /// GROUP BY pagination uses canonical grouped-key order by default.
    #[must_use]
    pub fn offset(self, offset: u32) -> Self {
        self.map_query(|query| query.offset(offset))
    }

    /// Attach an opaque cursor token for continuation pagination.
    ///
    /// Cursor-mode invariants are checked before planning/execution:
    /// - explicit `order_by(...)` is required
    /// - explicit `limit(...)` is required
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.cursor_token = Some(token.into());
        self
    }

    // ------------------------------------------------------------------
    // Planning / diagnostics
    // ------------------------------------------------------------------

    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    pub fn planned(&self) -> Result<PlannedQuery<E>, QueryError> {
        if let Some(err) = self.cursor_intent_error() {
            return Err(QueryError::Intent(err));
        }

        self.query.planned()
    }

    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        if let Some(err) = self.cursor_intent_error() {
            return Err(QueryError::Intent(err));
        }

        self.query.plan()
    }

    // ------------------------------------------------------------------
    // Execution (single semantic boundary)
    // ------------------------------------------------------------------

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session.execute_query(self.query())
    }

    /// Enter typed cursor-pagination mode for this query.
    ///
    /// Cursor pagination requires:
    /// - explicit `order_by(...)`
    /// - explicit `limit(...)`
    ///
    /// Requests are deterministic under canonical ordering, but continuation is
    /// best-effort and forward-only over live state.
    /// No snapshot/version is pinned across requests, so concurrent writes may
    /// shift page boundaries.
    pub fn page(self) -> Result<PagedLoadQuery<'a, E>, QueryError> {
        self.ensure_paged_mode_ready()?;

        Ok(PagedLoadQuery { inner: self })
    }

    /// Execute this query as cursor pagination and return items + next cursor.
    ///
    /// The returned cursor token is opaque and must be passed back via `.cursor(...)`.
    pub fn execute_paged(self) -> Result<PagedLoadExecution<E>, QueryError>
    where
        E: EntityValue,
    {
        self.page()?.execute()
    }

    /// Execute one grouped query page with optional grouped continuation cursor.
    ///
    /// This grouped entrypoint is intentionally separate from scalar load
    /// execution to keep grouped response shape explicit.
    pub fn execute_grouped(self) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: EntityValue,
    {
        self.session
            .execute_grouped(self.query(), self.cursor_token.as_deref())
    }

    // ------------------------------------------------------------------
    // Execution terminals — semantic only
    // ------------------------------------------------------------------

    /// Execute and return whether the result set is empty.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(!self.exists()?)
    }

    /// Execute and return whether at least one matching row exists.
    pub fn exists(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_exists(plan))
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_count(plan))
    }

    /// Execute and return the smallest matching identifier, if any.
    pub fn min(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_min(plan))
    }

    /// Execute and return the id of the row with the smallest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn min_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_min_by(plan, field.as_ref())
            })
    }

    /// Execute and return the largest matching identifier, if any.
    pub fn max(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_max(plan))
    }

    /// Execute and return the id of the row with the largest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn max_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_max_by(plan, field.as_ref())
            })
    }

    /// Execute and return the id at zero-based ordinal `nth` when rows are
    /// ordered by `field` ascending, with primary-key ascending tie-breaks.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_nth_by(plan, field.as_ref(), nth)
            })
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_sum_by(plan, field.as_ref())
            })
    }

    /// Execute and return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_avg_by(plan, field.as_ref())
            })
    }

    /// Execute and return the median id by `field` using deterministic ordering
    /// `(field asc, primary key asc)`.
    ///
    /// Even-length windows select the lower median.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_median_by(plan, field.as_ref())
            })
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_count_distinct_by(plan, field.as_ref())
            })
    }

    /// Execute and return both `(min_by(field), max_by(field))` in one terminal.
    ///
    /// Tie handling is deterministic for both extrema: primary key ascending.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxByIds<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.aggregate_min_max_by(plan, field.as_ref())
            })
    }

    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.values_by(plan, field.as_ref())
            })
    }

    /// Execute and return the first `k` rows from the effective response window.
    pub fn take(&self, take_count: u32) -> Result<Response<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.take(plan, take_count))
    }

    /// Execute and return the top `k` rows by `field` under deterministic
    /// ordering `(field desc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_by(...)` row order in the returned rows. For `k = 1`, this
    /// matches `max_by(field)` selection semantics.
    pub fn top_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Response<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.top_k_by(plan, field.as_ref(), take_count)
            })
    }

    /// Execute and return the bottom `k` rows by `field` under deterministic
    /// ordering `(field asc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_by(...)` row order in the returned rows. For `k = 1`, this
    /// matches `min_by(field)` selection semantics.
    pub fn bottom_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Response<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.bottom_k_by(plan, field.as_ref(), take_count)
            })
    }

    /// Execute and return projected values for the top `k` rows by `field`
    /// under deterministic ordering `(field desc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one value.
    pub fn top_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.top_k_by_values(plan, field.as_ref(), take_count)
            })
    }

    /// Execute and return projected values for the bottom `k` rows by `field`
    /// under deterministic ordering `(field asc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one value.
    pub fn bottom_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.bottom_k_by_values(plan, field.as_ref(), take_count)
            })
    }

    /// Execute and return projected id/value pairs for the top `k` rows by
    /// `field` under deterministic ordering `(field desc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one `(id, value)` pair.
    pub fn top_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.top_k_by_with_ids(plan, field.as_ref(), take_count)
            })
    }

    /// Execute and return projected id/value pairs for the bottom `k` rows by
    /// `field` under deterministic ordering `(field asc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one `(id, value)` pair.
    pub fn bottom_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.bottom_k_by_with_ids(plan, field.as_ref(), take_count)
            })
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.distinct_values_by(plan, field.as_ref())
            })
    }

    /// Execute and return projected field values paired with row ids for the
    /// effective result window.
    pub fn values_by_with_ids(
        &self,
        field: impl AsRef<str>,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.values_by_with_ids(plan, field.as_ref())
            })
    }

    /// Execute and return the first projected field value in effective response
    /// order, if any.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.first_value_by(plan, field.as_ref())
            })
    }

    /// Execute and return the last projected field value in effective response
    /// order, if any.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| {
                load.last_value_by(plan, field.as_ref())
            })
    }

    /// Execute and return the first matching identifier in response order, if any.
    pub fn first(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_first(plan))
    }

    /// Execute and return the last matching identifier in response order, if any.
    pub fn last(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_last(plan))
    }

    /// Execute and require exactly one matching row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_one()?;
        Ok(())
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_some()?;
        Ok(())
    }
}

impl<E> FluentLoadQuery<'_, E>
where
    E: EntityKind,
{
    const fn non_paged_intent_error(&self) -> Option<IntentError> {
        if self.cursor_token.is_some() {
            return Some(IntentError::CursorRequiresPagedExecution);
        }
        if self.query.has_grouping() {
            return Some(IntentError::GroupedRequiresExecuteGrouped);
        }

        None
    }

    fn cursor_intent_error(&self) -> Option<IntentError> {
        self.cursor_token
            .as_ref()
            .and_then(|_| self.paged_intent_error())
    }

    fn paged_intent_error(&self) -> Option<IntentError> {
        if self.query.has_grouping() {
            return Some(IntentError::GroupedRequiresExecuteGrouped);
        }

        let spec = self.query.load_spec()?;

        policy::validate_cursor_paging_requirements(self.query.has_explicit_order(), spec)
            .err()
            .map(IntentError::from)
    }

    fn ensure_paged_mode_ready(&self) -> Result<(), QueryError> {
        if let Some(err) = self.paged_intent_error() {
            return Err(QueryError::Intent(err));
        }

        Ok(())
    }

    const fn ensure_non_paged_mode_ready(&self) -> Result<(), QueryError> {
        if let Some(err) = self.non_paged_intent_error() {
            return Err(QueryError::Intent(err));
        }

        Ok(())
    }
}

impl<E> FluentLoadQuery<'_, E>
where
    E: EntityKind + SingletonEntity,
    E::Key: Default,
{
    #[must_use]
    pub fn only(self) -> Self {
        self.map_query(Query::only)
    }
}

///
/// PagedLoadQuery
///
/// Session-bound cursor pagination wrapper.
/// This wrapper only exposes cursor continuation and paged execution.
///

pub struct PagedLoadQuery<'a, E>
where
    E: EntityKind,
{
    inner: FluentLoadQuery<'a, E>,
}

impl<E> PagedLoadQuery<'_, E>
where
    E: EntityKind,
{
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Cursor continuation
    // ------------------------------------------------------------------

    /// Attach an opaque continuation token for the next page.
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.inner = self.inner.cursor(token);
        self
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    /// Execute in cursor-pagination mode and return items + next cursor.
    ///
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn execute(self) -> Result<PagedLoadExecution<E>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_with_trace()
            .map(PagedLoadExecutionWithTrace::into_execution)
    }

    /// Execute in cursor-pagination mode and return items, next cursor,
    /// and optional execution trace details when session debug mode is enabled.
    ///
    /// Trace collection is opt-in via `DbSession::debug()` and does not
    /// change query planning or result semantics.
    pub fn execute_with_trace(self) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: EntityValue,
    {
        self.inner.ensure_paged_mode_ready()?;

        self.inner.session.execute_load_query_paged_with_trace(
            self.inner.query(),
            self.inner.cursor_token.as_deref(),
        )
    }
}
