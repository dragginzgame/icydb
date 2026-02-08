use crate::{
    db::{
        DbSession,
        query::{
            Query, QueryError,
            expr::{FilterExpr, SortExpr},
            plan::{ExecutablePlan, ExplainPlan},
            predicate::Predicate,
        },
        response::Response,
    },
    traits::{CanisterKind, EntityKind, EntityValue, SingletonEntity},
    types::Id,
};

///
/// SessionLoadQuery
///
/// Session-bound load query wrapper.
/// Owns intent construction and execution routing only.
/// All result inspection and projection is performed on `Response<E>`.
///

pub struct SessionLoadQuery<'a, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
    session: &'a DbSession<C>,
    query: Query<E>,
}

impl<'a, C, E> SessionLoadQuery<'a, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
    pub(crate) const fn new(session: &'a DbSession<C>, query: Query<E>) -> Self {
        Self { session, query }
    }

    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        &self.query
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
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.query = self.query.filter(predicate);
        self
    }

    pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, QueryError> {
        self.query = self.query.filter_expr(expr)?;
        Ok(self)
    }

    pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, QueryError> {
        self.query = self.query.sort_expr(expr)?;
        Ok(self)
    }

    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by(field);
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by_desc(field);
        self
    }

    /// Bound the number of returned rows.
    ///
    /// Pagination is only valid with explicit ordering; combine `limit` and/or
    /// `offset` with `order_by(...)` or planning fails.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    /// Skip a number of rows in the ordered result stream.
    ///
    /// Pagination is only valid with explicit ordering; combine `offset` and/or
    /// `limit` with `order_by(...)` or planning fails.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    // ------------------------------------------------------------------
    // Planning / diagnostics
    // ------------------------------------------------------------------

    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
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
        self.session.execute_query(self.query())
    }

    // ------------------------------------------------------------------
    // Execution terminals — semantic only
    // ------------------------------------------------------------------

    /// Execute and return whether the result set is empty.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.is_empty())
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.count())
    }

    /// Execute and require exactly one matching row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_one().map_err(QueryError::Response)
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_some().map_err(QueryError::Response)
    }
}

impl<C, E> SessionLoadQuery<'_, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C> + SingletonEntity,
    E::Key: Default,
{
    #[must_use]
    pub fn only(mut self) -> Self {
        self.query = self.query.only();
        self
    }
}
