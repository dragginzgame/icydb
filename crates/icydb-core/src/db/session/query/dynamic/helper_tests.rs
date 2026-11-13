//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db) fn execute_trusted_live_page_with_result_bytes_limit_for_tests(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
        result_bytes_limit: u64,
    ) -> Result<LiveQueryPageOutput, QueryError> {
        let previous = SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE
            .with(|limit| limit.replace(Some(result_bytes_limit)));
        let _guard = core::mem::DropGuard::new(previous, |previous| {
            SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE.with(|limit| limit.set(previous));
        });
        self.execute_trusted_live_page(request, continuation)
    }
}

pub(super) const SCALAR_PAGE_OUTPUT_ROWS: usize = 2;

pub(super) const SCALAR_PAGE_KEY_ENTRIES: u64 = 4;

std::thread_local! {
    pub(super) static SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE: Cell<Option<u64>> = const { Cell::new(None) };
}
