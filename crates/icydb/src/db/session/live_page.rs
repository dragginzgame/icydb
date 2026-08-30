//! Module: db::session::live_page
//! Responsibility: advance non-entity live-page traversal state.
//! Does not own: query construction, row decoding, or unbounded collection.
//! Boundary: one public or trusted page -> moved continuation and bounded step.

use crate::{
    Error, ErrorKind, ErrorOrigin, RuntimeErrorKind,
    db::{DbSession, DynamicQuery, LiveQueryPageOutput},
    traits::CanisterKind,
};

/// One bounded live-page traversal step.
///
/// The returned page never retains its continuation. IcyDB moves that token
/// into the caller-owned traversal state supplied to the advance method, so a
/// consumer can decode or project this page before requesting the next one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LivePageStep {
    page: LiveQueryPageOutput,
    exhausted: bool,
}

impl LivePageStep {
    /// Borrow the page returned by this bounded step.
    #[must_use]
    pub const fn page(&self) -> &LiveQueryPageOutput {
        &self.page
    }

    /// Consume this step and return its page.
    #[must_use]
    pub fn into_page(self) -> LiveQueryPageOutput {
        self.page
    }

    /// Return whether traversal is proven exhausted after this page.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

#[derive(Clone, Copy)]
enum LivePageLane {
    Public,
    Trusted,
}

const fn non_progressing_live_page_error() -> Error {
    Error::from_kind(
        ErrorKind::Runtime(RuntimeErrorKind::InvariantViolation),
        ErrorOrigin::Cursor,
    )
}

fn finish_live_page_step(
    mut page: LiveQueryPageOutput,
    continuation: &mut Option<String>,
) -> Result<LivePageStep, Error> {
    let next = page.continuation.take();
    if next.is_some() && continuation.as_deref() == next.as_deref() {
        return Err(non_progressing_live_page_error());
    }

    let exhausted = next.is_none();
    *continuation = next;
    Ok(LivePageStep { page, exhausted })
}

impl<C: CanisterKind> DbSession<C> {
    /// Advance one caller-authorized public live-page traversal step.
    ///
    /// Start with `None`, process the returned page immediately, and call
    /// again only when [`LivePageStep::is_exhausted`] is false. The returned
    /// continuation is moved into `continuation`; a repeated non-null token
    /// fails with a compact cursor-origin invariant error.
    pub fn advance_live_page(
        &self,
        request: &DynamicQuery,
        continuation: &mut Option<String>,
    ) -> Result<LivePageStep, Error> {
        self.advance_live_page_kernel(request, continuation, LivePageLane::Public)
    }

    /// Advance one explicitly authorized trusted live-page traversal step.
    ///
    /// This bypasses public read admission exactly like
    /// [`DbSession::execute_trusted_live_page`]. Applications must authorize
    /// the surrounding maintenance or administrative operation.
    pub fn advance_trusted_live_page(
        &self,
        request: &DynamicQuery,
        continuation: &mut Option<String>,
    ) -> Result<LivePageStep, Error> {
        self.advance_live_page_kernel(request, continuation, LivePageLane::Trusted)
    }

    #[inline(never)]
    fn advance_live_page_kernel(
        &self,
        request: &DynamicQuery,
        continuation: &mut Option<String>,
        lane: LivePageLane,
    ) -> Result<LivePageStep, Error> {
        let page = match lane {
            LivePageLane::Public => self
                .inner
                .execute_public_live_page(request, continuation.as_deref()),
            LivePageLane::Trusted => self
                .inner
                .execute_trusted_live_page(request, continuation.as_deref()),
        }
        .map_err(Error::from)?;
        finish_live_page_step(page, continuation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(continuation: Option<&str>) -> LiveQueryPageOutput {
        LiveQueryPageOutput {
            entity: "app::User".to_string(),
            columns: vec!["id".to_string()],
            rows: Vec::new(),
            row_count: 0,
            continuation: continuation.map(str::to_string),
            work: crate::db::ScalarPageWork {
                envelope_identity: 1,
                entries_visited: 0,
                result_rows: 0,
            },
        }
    }

    #[test]
    fn live_page_step_moves_progress_and_clears_exhaustion() {
        let mut continuation = None;
        let advanced = finish_live_page_step(page(Some("next")), &mut continuation)
            .expect("a new continuation should advance");
        assert!(!advanced.is_exhausted());
        assert!(advanced.page().continuation.is_none());
        assert_eq!(continuation.as_deref(), Some("next"));

        let exhausted = finish_live_page_step(page(None), &mut continuation)
            .expect("a missing continuation should exhaust traversal");
        assert!(exhausted.is_exhausted());
        assert!(exhausted.page().continuation.is_none());
        assert!(continuation.is_none());
    }

    #[test]
    fn live_page_step_rejects_non_progressing_continuation() {
        let mut continuation = Some("same".to_string());
        let error = finish_live_page_step(page(Some("same")), &mut continuation)
            .expect_err("a repeated continuation must fail");

        assert_eq!(
            error.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
        );
        assert_eq!(error.origin(), ErrorOrigin::Cursor);
        assert_eq!(continuation.as_deref(), Some("same"));
    }
}
