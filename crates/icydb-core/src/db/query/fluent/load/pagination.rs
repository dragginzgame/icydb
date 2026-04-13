//! Module: query::fluent::load::pagination
//! Responsibility: fluent paged-query wrapper APIs and cursor continuation terminals.
//! Does not own: planner semantic validation or runtime execution internals.
//! Boundary: exposes paged execution surfaces over fluent load query contracts.

use crate::{
    db::{
        PagedLoadExecution, PagedLoadExecutionWithTrace, PersistedRow,
        query::fluent::load::FluentLoadQuery,
        query::intent::{Query, QueryError},
    },
    traits::{EntityKind, EntityValue},
};

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

impl<'a, E> FluentLoadQuery<'a, E>
where
    E: PersistedRow,
{
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
        E: PersistedRow + EntityValue,
    {
        self.page()?.execute()
    }
}

impl<E> PagedLoadQuery<'_, E>
where
    E: PersistedRow,
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
        E: PersistedRow + EntityValue,
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
        E: PersistedRow + EntityValue,
    {
        self.inner.ensure_paged_mode_ready()?;

        self.inner.session.execute_load_query_paged_with_trace(
            self.inner.query(),
            self.inner.cursor_token.as_deref(),
        )
    }
}
