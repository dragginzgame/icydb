use crate::{
    db::{
        DbSession,
        query::{
            IntentError, Query, QueryError,
            expr::{FilterExpr, SortExpr},
            plan::{ExecutablePlan, ExplainPlan},
            policy,
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
    cursor_token: Option<String>,
    cursor_intent_error: Option<IntentError>,
}

///
/// PagedLoadQuery
///
/// Session-bound cursor pagination wrapper.
/// This wrapper only exposes cursor continuation and paged execution.
///

pub struct PagedLoadQuery<'a, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
    inner: SessionLoadQuery<'a, C, E>,
}

impl<'a, C, E> SessionLoadQuery<'a, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
    pub(crate) const fn new(session: &'a DbSession<C>, query: Query<E>) -> Self {
        Self {
            session,
            query,
            cursor_token: None,
            cursor_intent_error: None,
        }
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
        self.refresh_cursor_intent_error();
        self
    }

    pub fn filter_expr(mut self, expr: FilterExpr) -> Result<Self, QueryError> {
        self.query = self.query.filter_expr(expr)?;
        self.refresh_cursor_intent_error();
        Ok(self)
    }

    pub fn sort_expr(mut self, expr: SortExpr) -> Result<Self, QueryError> {
        self.query = self.query.sort_expr(expr)?;
        self.refresh_cursor_intent_error();
        Ok(self)
    }

    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by(field);
        self.refresh_cursor_intent_error();
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.query = self.query.order_by_desc(field);
        self.refresh_cursor_intent_error();
        self
    }

    /// Bound the number of returned rows.
    ///
    /// Pagination is only valid with explicit ordering; combine `limit` and/or
    /// `offset` with `order_by(...)` or planning fails.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.query = self.query.limit(limit);
        self.refresh_cursor_intent_error();
        self
    }

    /// Skip a number of rows in the ordered result stream.
    ///
    /// Pagination is only valid with explicit ordering; combine `offset` and/or
    /// `limit` with `order_by(...)` or planning fails.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.query = self.query.offset(offset);
        self.refresh_cursor_intent_error();
        self
    }

    /// Attach an opaque cursor token for continuation pagination.
    ///
    /// Cursor-mode invariants are checked before planning/execution:
    /// - explicit `order_by(...)` is required
    /// - explicit `limit(...)` is required
    /// - `offset(...)` is not allowed
    #[must_use]
    pub fn cursor(mut self, token: impl Into<String>) -> Self {
        self.cursor_token = Some(token.into());
        self.refresh_cursor_intent_error();
        self
    }

    // ------------------------------------------------------------------
    // Planning / diagnostics
    // ------------------------------------------------------------------

    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.query.explain()
    }

    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        if let Some(err) = self.cursor_intent_error {
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
        self.session.execute_query(self.query())
    }

    /// Enter typed cursor-pagination mode for this query.
    ///
    /// Cursor pagination requires:
    /// - explicit `order_by(...)`
    /// - explicit `limit(...)`
    /// - no `offset(...)`
    ///
    /// Requests are deterministic under canonical ordering, but continuation is
    /// best-effort and forward-only over live state.
    /// No snapshot/version is pinned across requests, so concurrent writes may
    /// shift page boundaries.
    pub fn page(self) -> Result<PagedLoadQuery<'a, C, E>, QueryError> {
        self.ensure_paged_mode_ready()?;

        Ok(PagedLoadQuery { inner: self })
    }

    /// Execute this query as cursor pagination and return items + next cursor.
    ///
    /// The returned cursor token is opaque and must be passed back via `.cursor(...)`.
    pub fn execute_paged(self) -> Result<(Response<E>, Option<Vec<u8>>), QueryError>
    where
        E: EntityValue,
    {
        self.page()?.execute()
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
    E: EntityKind<Canister = C>,
{
    fn paged_intent_error(&self) -> Option<IntentError> {
        let spec = self.query.load_spec()?;

        policy::validate_cursor_paging_requirements(self.query.has_explicit_order(), spec)
            .err()
            .map(IntentError::from)
    }

    fn refresh_cursor_intent_error(&mut self) {
        self.cursor_intent_error = self
            .cursor_token
            .as_ref()
            .and_then(|_| self.paged_intent_error());
    }

    fn ensure_paged_mode_ready(&self) -> Result<(), QueryError> {
        if let Some(err) = self.paged_intent_error() {
            return Err(QueryError::Intent(err));
        }

        if let Some(err) = self.cursor_intent_error {
            return Err(QueryError::Intent(err));
        }

        Ok(())
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
        self.refresh_cursor_intent_error();
        self
    }
}

impl<C, E> PagedLoadQuery<'_, C, E>
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
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
    pub fn execute(self) -> Result<(Response<E>, Option<Vec<u8>>), QueryError>
    where
        E: EntityValue,
    {
        self.inner.ensure_paged_mode_ready()?;

        self.inner
            .session
            .execute_load_query_paged(self.inner.query(), self.inner.cursor_token.as_deref())
    }
}
