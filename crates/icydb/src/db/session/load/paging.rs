//! Module: db::session::load::paging
//!
//! Responsibility: public fluent load cursor-page and trusted/admin batch
//! terminals.
//! Does not own: cursor encoding format, read admission, or page execution.
//! Boundary: converts core paged load execution into facade `PagedResponse`.

use crate::{
    db::{AdminBatchRequest, PagedResponse, session::load::FluentLoadQuery},
    error::Error,
    traits::Entity,
};

use icydb_core as core;

impl<E: Entity> FluentLoadQuery<'_, E> {
    /// Execute the first typed cursor page with the requested page size.
    ///
    /// Cursor pagination requires explicit ordering and disallows a prior
    /// `partial_window(...)`. IcyDB clamps the requested page size to the
    /// engine-owned public page cap.
    /// Cursor pagination runs through the default bounded read-admission lane.
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn page(self, limit: u32) -> Result<PagedResponse<E>, Error> {
        Ok(Self::paged_response_from_execution(self.inner.page(limit)?))
    }

    /// Execute the next typed cursor page from a previous continuation cursor.
    ///
    /// This is the continuation counterpart to `page(limit)`. The cursor is an
    /// opaque token returned by the previous page response.
    pub fn next_page(
        self,
        limit: u32,
        cursor: impl Into<String>,
    ) -> Result<PagedResponse<E>, Error> {
        Ok(Self::paged_response_from_execution(
            self.inner.next_page(limit, cursor)?,
        ))
    }

    /// Execute a trusted/admin cursor batch with an engine-owned batch size.
    ///
    /// This terminal is only for reads that have already opted into
    /// `trusted_read_unchecked()`. Application-facing list endpoints should
    /// use `page(limit)` / `next_page(limit, cursor)`.
    pub fn admin_batch(self, request: AdminBatchRequest) -> Result<PagedResponse<E>, Error> {
        Ok(Self::paged_response_from_execution(
            self.inner.admin_batch(request)?,
        ))
    }

    fn paged_response_from_execution(
        execution: core::db::PagedLoadExecution<E>,
    ) -> PagedResponse<E> {
        let read_intent = execution.read_intent();
        let (response, continuation_cursor) = execution.into_response_and_cursor();
        let next_cursor = continuation_cursor
            .as_deref()
            .map(core::db::encode_hex_lower);

        PagedResponse::new(response.entities(), next_cursor, read_intent)
    }
}
