//! Module: query::fluent::delete
//! Responsibility: fluent delete-query builder and execution routing.
//! Does not own: query semantic validation or response projection.
//! Boundary: session API facade over query intent/planning/execution.

use crate::{
    db::{
        DbSession, EntityResponse, PersistedRow,
        predicate::Predicate,
        query::{
            explain::ExplainPlan,
            expr::{FilterExpr, SortExpr},
            intent::{CompiledQuery, PlannedQuery, Query, QueryError},
            trace::QueryTracePlan,
        },
        response::ResponseError,
    },
    traits::{EntityKind, EntityValue, SingletonEntity},
    types::Id,
};

///
/// FluentDeleteQuery
///
/// Session-bound delete query wrapper.
/// This type owns *intent construction* and *execution routing only*.
/// Delete execution follows the same traditional mutation contract as the
/// unified SQL write lane: bare execution returns affected-row count.
///

pub struct FluentDeleteQuery<'a, E>
where
    E: EntityKind,
{
    session: &'a DbSession<E::Canister>,
    query: Query<E>,
}

impl<'a, E> FluentDeleteQuery<'a, E>
where
    E: PersistedRow,
{
    pub(crate) const fn new(session: &'a DbSession<E::Canister>, query: Query<E>) -> Self {
        Self { session, query }
    }

    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    /// Borrow the current immutable query intent.
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

    // Run one read-only session/query projection without mutating the delete
    // builder shell so diagnostics and planning surfaces share one handoff
    // shape from the fluent delete boundary into the session/query layer.
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

    /// Bound the number of rows affected by this delete.
    #[must_use]
    pub fn limit(self, limit: u32) -> Self {
        self.map_query(|query| query.limit(limit))
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
        self.map_session_query_output(DbSession::planned_query_with_visible_indexes)
    }

    /// Build the compiled executable plan for this query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        self.map_session_query_output(DbSession::compile_query_with_visible_indexes)
    }

    // ------------------------------------------------------------------
    // Execution (minimal core surface)
    // ------------------------------------------------------------------

    /// Execute this delete and return the affected-row count.
    pub fn execute(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.session.execute_delete_count(self.query())
    }

    /// Execute this delete and materialize deleted rows for one explicit
    /// row-returning surface.
    pub fn execute_rows(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.session.execute_query(self.query())
    }

    /// Execute and return whether any rows were affected.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()? == 0)
    }

    /// Execute and return the number of affected rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute()
    }

    /// Execute and require exactly one affected row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        match self.execute()? {
            1 => Ok(()),
            0 => Err(ResponseError::not_found(E::PATH).into()),
            count => Err(ResponseError::not_unique(E::PATH, count).into()),
        }
    }

    /// Execute and require at least one affected row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        if self.execute()? == 0 {
            return Err(ResponseError::not_found(E::PATH).into());
        }

        Ok(())
    }
}

impl<E> FluentDeleteQuery<'_, E>
where
    E: PersistedRow + SingletonEntity,
    E::Key: Default,
{
    /// Delete the singleton entity.
    #[must_use]
    pub fn only(self) -> Self {
        self.map_query(Query::only)
    }
}
