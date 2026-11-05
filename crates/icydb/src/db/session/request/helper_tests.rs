//! Test-only constructors and adapters for the parent module.

use super::*;

impl<'a, T, E> RequestExecutionFuture<'a, T, E> {
    pub(super) fn with_root(
        root: RequestExecutionRoot,
        future: impl Future<Output = Result<T, E>> + 'a,
    ) -> Self {
        Self {
            root: Some(root),
            future: Box::pin(future),
        }
    }
}
