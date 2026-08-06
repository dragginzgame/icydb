//! Module: db::session::request
//! Responsibility: guarded public entry into one request-owned execution scope.
//! Does not own: caller authorization or configurable execution-budget profiles.
//! Boundary: ordinary call trees use ambient synchronous scope; explicit roots cross async work.

use std::rc::Rc;

use icydb_core as core;

/// Non-cloneable owner of aggregate IcyDB work for one endpoint request.
///
/// Obtain this capability through [`with_request_execution_root`] when an
/// endpoint must retain database accounting across an inter-canister `await`.
/// Ordinary synchronous call trees use [`with_request_execution`] and do not
/// need to name this type.
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
}

/// Run one synchronous database segment in an endpoint-owned execution scope.
///
/// Zero-argument `db!()` calls made anywhere in `run` reuse this scope. Enter
/// after the endpoint's final inter-canister `await` when its database work is
/// synchronous. Generated IcyDB endpoints establish this boundary
/// automatically.
///
/// This scope is not retained across an async suspension. Use
/// [`with_request_execution_root`] and `db!(&request_root)` when database work
/// genuinely occurs on both sides of an inter-canister `await`.
pub fn with_request_execution<T>(run: impl FnOnce() -> T) -> T {
    let root = RequestExecutionRoot::new();
    root.inner.__with_current_scope(run)
}

/// Run a call tree with an explicitly owned request root.
///
/// The callback receives the non-cloneable root and may move it into an async
/// future. Use `db!(&request_root)` in code that executes after the callback's
/// synchronous construction, including after an inter-canister `await`.
/// Ordinary synchronous database helpers should prefer
/// [`with_request_execution`] and zero-argument `db!()`.
pub fn with_request_execution_root<T>(run: impl FnOnce(RequestExecutionRoot) -> T) -> T {
    let root = RequestExecutionRoot::new();
    let ambient = Rc::clone(&root.inner);
    ambient.__with_current_scope(|| run(root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_entry_runs_one_synchronous_scoped_callback() {
        let called = with_request_execution(|| 1_u8);

        assert_eq!(called, 1);
    }

    #[test]
    fn explicit_request_root_can_be_retained_by_an_async_endpoint_future() {
        #[expect(
            clippy::future_not_send,
            reason = "canister request futures and their Rc-backed roots stay on one executor"
        )]
        async fn retain_across_await(root: RequestExecutionRoot) -> RequestExecutionRoot {
            std::future::ready(()).await;
            root
        }

        let future = with_request_execution_root(retain_across_await);
        let _future = std::hint::black_box(future);
    }
}
