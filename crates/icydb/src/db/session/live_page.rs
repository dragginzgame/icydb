//! Module: db::session::live_page
//! Responsibility: prepare and commit non-entity live-page traversal state.
//! Does not own: query construction, row decoding, or unbounded collection.
//! Boundary: one public or trusted page -> uncommitted bounded step.

use crate::{
    Error, ErrorKind, ErrorOrigin, RuntimeErrorKind,
    db::{DbSession, DynamicQuery, LiveQueryPageOutput},
    traits::CanisterKind,
};

/// One bounded live-page traversal step.
///
/// The returned page retains its continuation until [`LivePageStep::commit`]
/// consumes the step. Decode or project the page first so a failed decode
/// leaves caller-owned traversal state unchanged and the same page retryable.
#[must_use = "decode the page, then commit or explicitly consume the step"]
#[derive(Debug, Eq, PartialEq)]
pub struct LivePageStep {
    page: LiveQueryPageOutput,
}

impl LivePageStep {
    /// Borrow the page returned by this bounded step.
    #[must_use]
    pub const fn page(&self) -> &LiveQueryPageOutput {
        &self.page
    }

    /// Consume this step without committing and return its intact page.
    ///
    /// The returned page retains its continuation for caller-managed paging.
    #[must_use]
    pub fn into_page(self) -> LiveQueryPageOutput {
        self.page
    }

    /// Return whether traversal is proven exhausted after this page.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.page.continuation.is_none()
    }

    /// Commit this successfully processed page to caller-owned traversal state.
    ///
    /// The step is consumed so its continuation can be moved without cloning.
    /// Returns `true` when the committed page proves traversal is exhausted.
    #[must_use]
    pub fn commit(self, continuation: &mut Option<String>) -> bool {
        let next = self.page.continuation;
        let exhausted = next.is_none();
        *continuation = next;
        exhausted
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

pub(super) fn prepare_live_page_step(
    page: LiveQueryPageOutput,
    continuation: Option<&str>,
) -> Result<LivePageStep, Error> {
    if continuation.is_some() && page.continuation.as_deref() == continuation {
        return Err(non_progressing_live_page_error());
    }

    Ok(LivePageStep { page })
}

impl<C: CanisterKind> DbSession<C> {
    /// Advance one caller-authorized public live-page traversal step.
    ///
    /// Start with `None`, process the returned page immediately, and commit the
    /// step only after processing succeeds. Pass the committed continuation to
    /// the next call when [`LivePageStep::is_exhausted`] is false. A repeated
    /// non-null token fails with a compact cursor-origin invariant error.
    pub fn advance_live_page(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
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
        continuation: Option<&str>,
    ) -> Result<LivePageStep, Error> {
        self.advance_live_page_kernel(request, continuation, LivePageLane::Trusted)
    }

    #[inline(never)]
    fn advance_live_page_kernel(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
        lane: LivePageLane,
    ) -> Result<LivePageStep, Error> {
        let page = match lane {
            LivePageLane::Public => self.inner.execute_public_live_page(request, continuation),
            LivePageLane::Trusted => self.inner.execute_trusted_live_page(request, continuation),
        }
        .map_err(Error::from)?;
        prepare_live_page_step(page, continuation)
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
    fn live_page_step_commits_progress_only_after_processing() {
        let mut continuation = Some("prior".to_string());
        let advanced = prepare_live_page_step(page(Some("next")), continuation.as_deref())
            .expect("a new continuation should prepare a step");
        assert!(!advanced.is_exhausted());
        assert_eq!(advanced.page().continuation.as_deref(), Some("next"));
        assert_eq!(continuation.as_deref(), Some("prior"));

        assert!(!advanced.commit(&mut continuation));
        assert_eq!(continuation.as_deref(), Some("next"));

        let exhausted = prepare_live_page_step(page(None), continuation.as_deref())
            .expect("a missing continuation should prepare terminal step");
        assert!(exhausted.is_exhausted());
        assert!(exhausted.page().continuation.is_none());
        assert_eq!(continuation.as_deref(), Some("next"));

        assert!(exhausted.commit(&mut continuation));
        assert!(continuation.is_none());
    }

    #[test]
    fn dropped_live_page_step_preserves_retry_position_and_page_contract() {
        let continuation = Some("prior".to_string());
        let step = prepare_live_page_step(page(Some("next")), continuation.as_deref())
            .expect("a new continuation should prepare a step");

        drop(step);
        assert_eq!(continuation.as_deref(), Some("prior"));

        let retry = prepare_live_page_step(page(Some("next")), continuation.as_deref())
            .expect("the unchanged position should retry the same page");
        let retry_page = retry.into_page();
        assert_eq!(retry_page.continuation.as_deref(), Some("next"));
    }

    #[test]
    fn live_page_step_rejects_non_progressing_continuation_before_commit() {
        let continuation = Some("same".to_string());
        let error = prepare_live_page_step(page(Some("same")), continuation.as_deref())
            .expect_err("a repeated continuation must fail");

        assert_eq!(
            error.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
        );
        assert_eq!(error.origin(), ErrorOrigin::Cursor);
        assert_eq!(continuation.as_deref(), Some("same"));
    }
}
