//! Module: db::session::request
//! Responsibility: guarded public entry into one request-owned execution scope.
//! Does not own: caller authorization or configurable execution-budget profiles.
//! Boundary: sync guards and poll-scoped futures install one root only while code executes.

use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use icydb_core as core;

/// Non-cloneable owner of aggregate IcyDB work for one endpoint request.
///
/// Ordinary entry points use [`with_request_execution`] or
/// [`with_request_execution_async`] and do not need to name this type. A
/// framework that already owns request dispatch may retain this capability
/// and pass it to `db!(&request_root)` as the explicit low-level integration
/// form.
pub struct RequestExecutionRoot {
    inner: Rc<core::db::RequestExecutionRoot>,
}

impl RequestExecutionRoot {
    /// Snapshot this root's aggregate request usage and headroom without charging work.
    #[must_use]
    pub fn request_budget(&self) -> core::db::RequestBudgetSnapshot {
        self.inner.request_budget()
    }

    fn new() -> Self {
        Self {
            inner: Rc::new(core::db::RequestExecutionRoot::__new_or_current_runtime_root()),
        }
    }

    /// Borrow the core capability used by generated session construction.
    #[doc(hidden)]
    #[must_use]
    pub fn __core(&self) -> &core::db::RequestExecutionRoot {
        self.inner.as_ref()
    }

    /// Reject an explicit root that would replace another active request root.
    #[doc(hidden)]
    pub fn __ensure_compatible_with_current(&self) -> Result<(), crate::Error> {
        if self.inner.__is_compatible_with_current() {
            Ok(())
        } else {
            Err(crate::db::__request_execution_root_mismatch())
        }
    }
}

/// Future that installs one request root only while its child is polled.
///
/// The root owns counters across suspension. Ambient state is restored after
/// every poll, including completion and unwinding, so interleaved futures
/// cannot observe or charge each other's request. The already-boxed child is
/// type-erased so this poll state machine is shared across endpoint future
/// types; that retains one allocation and adds one indirect child poll.
///
/// A started future polled beneath a different active request root returns
/// the existing typed root-mismatch error without polling its child. The error
/// type must accept `crate::Error`; ordinary child results pass through unchanged.
#[must_use = "futures do nothing unless awaited or polled"]
pub struct RequestExecutionFuture<'a, T, E = crate::Error> {
    root: Option<RequestExecutionRoot>,
    future: Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>,
}

impl<'a, T, E> RequestExecutionFuture<'a, T, E> {
    fn new(future: impl Future<Output = Result<T, E>> + 'a) -> Self {
        Self {
            root: None,
            future: Box::pin(future),
        }
    }
}

impl<T, E: From<crate::Error>> Future for RequestExecutionFuture<'_, T, E> {
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let root = this.root.get_or_insert_with(RequestExecutionRoot::new);
        if let Err(error) = root.__ensure_compatible_with_current() {
            return Poll::Ready(Err(error.into()));
        }
        let future = &mut this.future;
        root.inner.__with_current_scope(|| future.as_mut().poll(cx))
    }
}

/// Run one synchronous database segment in an endpoint-owned execution scope.
///
/// Zero-argument `db!()` calls made anywhere in `run` reuse this scope.
/// Generated IcyDB endpoints establish this boundary automatically; ordinary
/// manual entries use `#[icydb::request_execution]`.
pub fn with_request_execution<T>(run: impl FnOnce() -> T) -> T {
    let root = RequestExecutionRoot::new();
    root.inner.__with_current_scope(run)
}

/// Run one async entry point under a single aggregate request budget.
///
/// The same root is installed immediately before each poll and removed
/// immediately afterward. Zero-argument `db!()` therefore works before and
/// after `.await` without leaving request state ambient while the future is
/// suspended. Nested wrappers reuse the already-active root.
///
/// The child must return `Result<T, E>` with `E: From<crate::Error>`. If a
/// started future is polled beneath another active root, the boundary returns
/// `RequestExecutionRootMismatch` through that error conversion before any
/// child work. It neither transfers counters nor replaces the active root.
/// Do not share pending request futures across independent request invocations.
pub fn with_request_execution_async<'a, T, E: From<crate::Error>>(
    future: impl Future<Output = Result<T, E>> + 'a,
) -> RequestExecutionFuture<'a, T, E> {
    RequestExecutionFuture::new(future)
}

/// Run a call tree with an explicitly owned request root.
///
/// This is the explicit low-level integration API for a framework that needs
/// to retain and pass request ownership itself. Ordinary sync and async entry
/// points should prefer [`with_request_execution`] or
/// [`with_request_execution_async`] with zero-argument `db!()`.
pub fn with_request_execution_root<T>(run: impl FnOnce(RequestExecutionRoot) -> T) -> T {
    let root = RequestExecutionRoot::new();
    let ambient = Rc::clone(&root.inner);
    ambient.__with_current_scope(|| run(root))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;
