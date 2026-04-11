//! Module: query::fluent::delete
//! Responsibility: fluent delete-query builder and execution routing.
//! Does not own: query semantic validation or response projection.
//! Boundary: session API facade over query intent/planning/execution.

use crate::{
    db::{
        DbSession, PersistedRow,
        predicate::Predicate,
        query::{
            api::ResponseCardinalityExt,
            explain::ExplainPlan,
            expr::{FilterExpr, SortExpr},
            intent::{CompiledQuery, PlannedQuery, Query, QueryError},
            trace::QueryTracePlan,
        },
        response::EntityResponse,
    },
    traits::{EntityKind, EntityValue, SingletonEntity},
    types::Id,
};

///
/// FluentDeleteQuery
///
/// Session-bound delete query wrapper.
/// This type owns *intent construction* and *execution routing only*.
/// Result inspection is provided by query API extension traits over `EntityResponse<E>`.
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
        self.session
            .explain_query_with_visible_indexes(self.query())
    }

    /// Return the stable plan hash for this query.
    pub fn plan_hash_hex(&self) -> Result<String, QueryError> {
        self.session
            .query_plan_hash_hex_with_visible_indexes(self.query())
    }

    /// Build one trace payload without executing the query.
    pub fn trace(&self) -> Result<QueryTracePlan, QueryError> {
        self.session.trace_query(self.query())
    }

    /// Build the validated logical plan without compiling execution details.
    pub fn planned(&self) -> Result<PlannedQuery<E>, QueryError> {
        self.session
            .planned_query_with_visible_indexes(self.query())
    }

    /// Build the compiled executable plan for this query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, QueryError> {
        self.session
            .compile_query_with_visible_indexes(self.query())
    }

    // ------------------------------------------------------------------
    // Execution (minimal core surface)
    // ------------------------------------------------------------------

    /// Execute this delete using the session's policy settings.
    ///
    /// All result inspection and projection is performed on `EntityResponse<E>`.
    pub fn execute(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.session.execute_query(self.query())
    }

    /// Execute this delete while returning only the affected-row count.
    #[doc(hidden)]
    pub fn execute_count_only(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.session.execute_delete_count(self.query())
    }

    /// Execute and return whether any rows were affected.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.is_empty())
    }

    /// Execute and return the number of affected rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.count())
    }

    /// Execute and require exactly one affected row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_one()?;
        Ok(())
    }

    /// Execute and require at least one affected row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_some()?;
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
