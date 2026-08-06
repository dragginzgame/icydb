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
/// cannot observe or charge each other's request.
#[must_use = "futures do nothing unless awaited or polled"]
pub struct RequestExecutionFuture<F> {
    root: Option<RequestExecutionRoot>,
    future: Pin<Box<F>>,
}

impl<F> RequestExecutionFuture<F> {
    fn new(future: F) -> Self {
        Self {
            root: None,
            future: Box::pin(future),
        }
    }

    #[cfg(test)]
    fn with_root(root: RequestExecutionRoot, future: F) -> Self {
        Self {
            root: Some(root),
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for RequestExecutionFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let root = this.root.get_or_insert_with(RequestExecutionRoot::new);
        assert!(
            root.inner.__is_compatible_with_current(),
            "IcyDB request future moved beneath a different active request root"
        );
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
/// # Panics
///
/// Panics as an internal integration invariant if a future that has already
/// started under one request is later polled beneath a different active
/// request root. Ordinary endpoint executors do not move pending futures
/// between request invocations.
pub fn with_request_execution_async<F>(future: F) -> RequestExecutionFuture<F>
where
    F: Future,
{
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
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        task::Waker,
    };

    use super::*;

    struct YieldOnce {
        yielded: bool,
    }

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let mut cx = Context::from_waker(Waker::noop());
        future.poll(&mut cx)
    }

    #[test]
    fn default_entry_runs_one_synchronous_scoped_callback() {
        let called = with_request_execution(|| 1_u8);

        assert_eq!(called, 1);
    }

    #[test]
    fn explicit_request_root_can_outlive_callback_construction() {
        let retained = with_request_execution_root(std::convert::identity);

        let _retained = std::hint::black_box(retained);
    }

    #[test]
    fn missing_scope_uses_the_actionable_typed_boundary() {
        let err = crate::db::__request_execution_scope_required();

        assert_eq!(
            err.diagnostic().detail().copied(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: icydb_diagnostic_code::RuntimeBoundaryCode::RequestExecutionScopeRequired,
            })
        );
    }

    #[test]
    fn interleaved_futures_install_only_their_own_root_per_poll() {
        let first_root = RequestExecutionRoot::new();
        let first_expected = Rc::clone(&first_root.inner);
        let first_probe = Rc::clone(&first_expected);
        let mut first = Box::pin(RequestExecutionFuture::with_root(first_root, async move {
            assert!(first_probe.__is_current());
            YieldOnce { yielded: false }.await;
            assert!(first_probe.__is_current());
        }));

        let second_root = RequestExecutionRoot::new();
        let second_expected = Rc::clone(&second_root.inner);
        let second_probe = Rc::clone(&second_expected);
        let mut second = Box::pin(RequestExecutionFuture::with_root(second_root, async move {
            assert!(second_probe.__is_current());
            YieldOnce { yielded: false }.await;
            assert!(second_probe.__is_current());
        }));
        assert_eq!(poll_once(first.as_mut()), Poll::Pending);
        assert!(!first_expected.__is_current());
        assert!(!second_expected.__is_current());
        assert_eq!(poll_once(second.as_mut()), Poll::Pending);
        assert!(!first_expected.__is_current());
        assert!(!second_expected.__is_current());
        assert_eq!(poll_once(first.as_mut()), Poll::Ready(()));
        assert_eq!(poll_once(second.as_mut()), Poll::Ready(()));
        assert!(!first_expected.__is_current());
        assert!(!second_expected.__is_current());
    }

    #[test]
    fn nested_async_wrapper_reuses_the_active_root() {
        let root = RequestExecutionRoot::new();
        let expected = Rc::clone(&root.inner);
        let probe = Rc::clone(&expected);
        let nested_probe = Rc::clone(&probe);
        let nested = with_request_execution_async(async move {
            assert!(nested_probe.__is_current());
        });
        let mut future = Box::pin(RequestExecutionFuture::with_root(root, async move {
            assert!(probe.__is_current());
            nested.await;
            assert!(probe.__is_current());
        }));
        assert_eq!(poll_once(future.as_mut()), Poll::Ready(()));
        assert!(!expected.__is_current());
    }

    #[test]
    fn pending_cancellation_leaves_no_ambient_root() {
        let root = RequestExecutionRoot::new();
        let expected = Rc::clone(&root.inner);
        let probe = Rc::clone(&expected);
        let mut future = Box::pin(RequestExecutionFuture::with_root(root, async move {
            assert!(probe.__is_current());
            YieldOnce { yielded: false }.await;
        }));
        assert_eq!(poll_once(future.as_mut()), Poll::Pending);
        assert!(!expected.__is_current());
        drop(future);
        assert!(!expected.__is_current());
    }

    #[test]
    fn started_future_cannot_move_beneath_a_different_request_root() {
        let first_root = RequestExecutionRoot::new();
        let first_expected = Rc::clone(&first_root.inner);
        let mut first = Box::pin(RequestExecutionFuture::with_root(first_root, async move {
            YieldOnce { yielded: false }.await;
        }));
        assert_eq!(poll_once(first.as_mut()), Poll::Pending);

        let second = RequestExecutionRoot::new();
        let second_expected = Rc::clone(&second.inner);
        let panic = second.inner.__with_current_scope(|| {
            let result = catch_unwind(AssertUnwindSafe(|| poll_once(first.as_mut())));
            assert!(second_expected.__is_current());
            result
        });

        assert!(panic.is_err());
        assert!(!first_expected.__is_current());
        assert!(!second_expected.__is_current());
    }

    #[test]
    fn panic_restores_the_previous_ambient_root() {
        let root = RequestExecutionRoot::new();
        let expected = Rc::clone(&root.inner);
        let probe = Rc::clone(&expected);
        let mut future = Box::pin(RequestExecutionFuture::with_root(root, async move {
            assert!(probe.__is_current());
            panic!("poll panic");
        }));
        let panic = catch_unwind(AssertUnwindSafe(|| poll_once(future.as_mut())));

        assert!(panic.is_err());
        assert!(!expected.__is_current());
    }

    #[test]
    fn explicit_root_cannot_replace_a_different_active_root() {
        let first = with_request_execution_root(std::convert::identity);
        let second = with_request_execution_root(std::convert::identity);

        first.inner.__with_current_scope(|| {
            assert!(first.__ensure_compatible_with_current().is_ok());
            let err = second
                .__ensure_compatible_with_current()
                .expect_err("a different active request root must fail closed");

            assert_eq!(
                err.diagnostic().detail().copied(),
                Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                    boundary:
                        icydb_diagnostic_code::RuntimeBoundaryCode::RequestExecutionRootMismatch,
                })
            );
        });
    }
}
