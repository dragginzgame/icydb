//! Module: query::fluent::load::builder
//! Responsibility: fluent load-query builder surface and immutable query-intent mutation API.
//! Does not own: planner semantic validation or runtime execution dispatch.
//! Boundary: accumulates typed load intent and delegates planning/execution to session/query APIs.

use crate::{
    db::{
        DbSession,
        predicate::{CompareOp, Predicate},
        query::{
            builder::aggregate::AggregateExpr,
            explain::ExplainPlan,
            expr::{FilterExpr, SortExpr},
            intent::{CompiledQuery, PlannedQuery, Query, QueryError},
            trace::QueryTracePlan,
        },
    },
    traits::{EntityKind, SingletonEntity},
    types::Id,
    value::Value,
};

///
/// FluentLoadQuery
///
/// Session-bound load query wrapper.
/// Owns intent construction and execution routing only.
/// Result inspection is provided by query API extension traits over `EntityResponse<E>`.
///

pub struct FluentLoadQuery<'a, E>
where
    E: EntityKind,
{
    pub(super) session: &'a DbSession<E::Canister>,
    pub(super) query: Query<E>,
    pub(super) cursor_token: Option<String>,
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

    /// Borrow the current immutable query intent.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
    }

    pub(super) fn map_query(mut self, map: impl FnOnce(Query<E>) -> Query<E>) -> Self {
        self.query = map(self.query);
        self
    }

    pub(super) fn try_map_query(
        mut self,
        map: impl FnOnce(Query<E>) -> Result<Query<E>, QueryError>,
    ) -> Result<Self, QueryError> {
        self.query = map(self.query)?;
        Ok(self)
    }

    // Run one read-only session/query projection without mutating the fluent
    // builder shell so diagnostic and planning surfaces share one handoff
    // shape from the builder boundary into the session/query layer.
    fn map_session_query_output<T>(
        &self,
        map: impl FnOnce(&DbSession<E::Canister>, &Query<E>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        map(self.session, self.query())
    }

    // ------------------------------------------------------------------
    // Intent builders (pure)
    // ------------------------------------------------------------------

    /// Set the access path to a single typed primary-key value.
    ///
    /// `Id<E>` is treated as a plain query input value here. It does not grant access.
    #[must_use]
    pub fn by_id(self, id: Id<E>) -> Self {
        self.map_query(|query| query.by_id(id.key()))
    }

    /// Set the access path to multiple typed primary-key values.
    ///
    /// IDs are public and may come from untrusted input sources.
    #[must_use]
    pub fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = Id<E>>,
    {
        self.map_query(|query| query.by_ids(ids.into_iter().map(|id| id.key())))
    }

    // ------------------------------------------------------------------
    // Query Refinement
    // ------------------------------------------------------------------

    /// Add a typed predicate expression directly.
    #[must_use]
    pub fn filter(self, predicate: Predicate) -> Self {
        self.map_query(|query| query.filter(predicate))
    }

    /// Add a serialized filter expression after lowering and validation.
    pub fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        self.try_map_query(|query| query.filter_expr(expr))
    }

    /// Add sort clauses from a serialized sort expression.
    pub fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        self.try_map_query(|query| query.sort_expr(expr))
    }

    /// Append ascending order for one field.
    #[must_use]
    pub fn order_by(self, field: impl AsRef<str>) -> Self {
        self.map_query(|query| query.order_by(field))
    }

    /// Append descending order for one field.
    #[must_use]
    pub fn order_by_desc(self, field: impl AsRef<str>) -> Self {
        self.map_query(|query| query.order_by_desc(field))
    }

    /// Add one grouped key field.
    pub fn group_by(self, field: impl AsRef<str>) -> Result<Self, QueryError> {
        let field = field.as_ref().to_owned();
        self.try_map_query(|query| query.group_by(&field))
    }

    /// Add one aggregate terminal via composable aggregate expression.
    #[must_use]
    pub fn aggregate(self, aggregate: AggregateExpr) -> Self {
        self.map_query(|query| query.aggregate(aggregate))
    }

    /// Override grouped hard limits for grouped execution budget enforcement.
    #[must_use]
    pub fn grouped_limits(self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.map_query(|query| query.grouped_limits(max_groups, max_group_bytes))
    }

    /// Add one grouped HAVING compare clause over one grouped key field.
    pub fn having_group(
        self,
        field: impl AsRef<str>,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let field = field.as_ref().to_owned();
        self.try_map_query(|query| query.having_group(&field, op, value))
    }

    /// Add one grouped HAVING compare clause over one grouped aggregate output.
    pub fn having_aggregate(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        self.try_map_query(|query| query.having_aggregate(aggregate_index, op, value))
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

    /// Build explain metadata for the current query.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.map_session_query_output(DbSession::explain_query_with_visible_indexes)
    }

    /// Return the stable plan hash for this query.
    pub fn plan_hash_hex(&self) -> Result<String, QueryError> {
        self.map_session_query_output(DbSession::query_plan_hash_hex_with_visible_indexes)
    }

    /// Build one trace payload without executing the query.
    pub fn trace(&self) -> Result<QueryTracePlan, QueryError> {
        self.map_session_query_output(DbSession::trace_query)
    }

    /// Build the validated logical plan without compiling execution details.
    pub fn planned(&self) -> Result<PlannedQuery<E>, QueryError> {
        self.ensure_cursor_mode_ready()?;
        self.map_session_query_output(DbSession::planned_query_with_visible_indexes)
    }

    /// Build the compiled executable plan for this query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        self.ensure_cursor_mode_ready()?;
        self.map_session_query_output(DbSession::compile_query_with_visible_indexes)
    }
}

impl<E> FluentLoadQuery<'_, E>
where
    E: EntityKind + SingletonEntity,
    E::Key: Default,
{
    /// Constrain this query to the singleton entity row.
    #[must_use]
    pub fn only(self) -> Self {
        self.map_query(Query::only)
    }
}
