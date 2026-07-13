//! Module: query::fluent::load::pagination
//! Responsibility: fluent paged-query wrapper APIs and cursor continuation terminals.
//! Does not own: planner semantic validation or runtime execution internals.
//! Boundary: exposes paged execution surfaces over fluent load query contracts.

use crate::{
    db::{
        PagedLoadExecution, PagedLoadExecutionWithTrace, PersistedRow,
        query::fluent::load::FluentLoadQuery,
        query::{
            intent::{IntentError, QueryError},
            read_intent::{ADMIN_BATCH_ROWS, AdminBatchRequest, PageRequest, ReadIntentKind},
        },
    },
    entity::{EntityKind, EntityValue},
};

struct PagedLoadQuery<'a, E>
where
    E: EntityKind,
{
    inner: FluentLoadQuery<'a, E>,
    read_intent: ReadIntentKind,
}

#[derive(Clone, Copy)]
enum PagedTerminal {
    PublicPage,
    TrustedAdminBatch,
}

impl PagedTerminal {
    const fn raw_limit_error(self) -> IntentError {
        match self {
            Self::PublicPage => IntentError::raw_limit_before_page_terminal(),
            Self::TrustedAdminBatch => IntentError::raw_limit_before_admin_batch_terminal(),
        }
    }

    const fn read_intent(self) -> ReadIntentKind {
        match self {
            Self::PublicPage => ReadIntentKind::PublicPage,
            Self::TrustedAdminBatch => ReadIntentKind::TrustedAdminBatch,
        }
    }

    const fn validate<E>(self, query: &FluentLoadQuery<'_, E>) -> Result<(), QueryError>
    where
        E: EntityKind,
    {
        match self {
            Self::PublicPage => Ok(()),
            Self::TrustedAdminBatch => {
                if !query.trusted_read_unchecked_enabled() {
                    return Err(QueryError::intent(
                        IntentError::admin_batch_requires_trusted_read(),
                    ));
                }

                Ok(())
            }
        }
    }
}

impl<'a, E> FluentLoadQuery<'a, E>
where
    E: PersistedRow,
{
    /// Execute the first typed cursor page with the requested page size.
    ///
    /// Cursor pagination requires:
    /// - explicit `order_term(...)`
    /// - no prior row-window cap
    ///
    /// Results are deterministic under canonical ordering, but continuation is
    /// best-effort and forward-only over live state.
    /// No snapshot/version is pinned across requests, so concurrent writes may
    /// shift page boundaries.
    pub fn page(self, limit: u32) -> Result<PagedLoadExecution<E>, QueryError>
    where
        E: EntityValue,
    {
        self.page_request(PageRequest::first(limit))?.execute()
    }

    /// Execute the next typed cursor page from a previous continuation cursor.
    ///
    /// This is the continuation counterpart to `page(limit)`. The cursor is an
    /// opaque token returned by the previous page response.
    pub fn next_page(
        self,
        limit: u32,
        cursor: impl Into<String>,
    ) -> Result<PagedLoadExecution<E>, QueryError>
    where
        E: EntityValue,
    {
        self.page_request(PageRequest::next(limit, cursor))?
            .execute()
    }

    fn page_request(self, request: PageRequest) -> Result<PagedLoadQuery<'a, E>, QueryError> {
        let limit = request.effective_limit();
        let cursor = request.into_cursor();

        self.into_paged_query(limit, cursor, PagedTerminal::PublicPage)
    }

    /// Execute a trusted/admin cursor batch with an engine-owned batch size.
    ///
    /// This terminal is intentionally unavailable on the normal public read
    /// lane. Callers must opt into `trusted_read_unchecked()` before invoking
    /// it, and a prior row-window cap is rejected because the batch size is
    /// owned by IcyDB.
    pub fn admin_batch(
        self,
        request: AdminBatchRequest,
    ) -> Result<PagedLoadExecution<E>, QueryError>
    where
        E: PersistedRow,
    {
        let cursor = request.into_cursor();
        self.into_paged_query(ADMIN_BATCH_ROWS, cursor, PagedTerminal::TrustedAdminBatch)?
            .execute()
    }

    fn into_paged_query(
        self,
        limit: u32,
        cursor: Option<String>,
        terminal: PagedTerminal,
    ) -> Result<PagedLoadQuery<'a, E>, QueryError> {
        self.ensure_semantic_terminal_owns_limit(terminal.raw_limit_error())?;
        self.ensure_page_request_owns_cursor()?;
        terminal.validate(&self)?;

        let mut inner = self.map_query(|query| query.with_load_limit(limit));
        if let Some(cursor) = cursor {
            inner = inner.with_cursor_token(cursor);
        }

        inner.ensure_paged_mode_ready()?;

        Ok(PagedLoadQuery {
            inner,
            read_intent: terminal.read_intent(),
        })
    }
}

impl<E> PagedLoadQuery<'_, E>
where
    E: PersistedRow,
{
    /// Execute in cursor-pagination mode and return items + next cursor.
    ///
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    fn execute(self) -> Result<PagedLoadExecution<E>, QueryError>
    where
        E: PersistedRow,
    {
        self.execute_with_trace()
            .map(PagedLoadExecutionWithTrace::into_execution)
    }

    /// Execute in cursor-pagination mode and return items, next cursor,
    /// and optional execution trace details when session debug mode is enabled.
    ///
    /// Trace collection is opt-in via `DbSession::debug()` and does not
    /// change query planning or result semantics.
    fn execute_with_trace(self) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: PersistedRow,
    {
        self.inner.ensure_default_read_admission()?;
        self.execute_with_trace_unchecked()
    }

    fn execute_with_trace_unchecked(self) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: PersistedRow,
    {
        // `PagedLoadQuery` is only constructed by page terminals in this module,
        // so paged-mode validation already happened before this wrapper existed.
        let read_intent = self.read_intent;
        self.inner
            .session
            .execute_load_query_paged_with_trace(
                self.inner.query(),
                self.inner.cursor_token.as_deref(),
            )
            .map(|execution| execution.with_read_intent(read_intent))
    }
}
