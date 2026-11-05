//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session) fn shared_query_cache_usage_for_tests(&self) -> (usize, usize) {
        self.with_query_plan_cache(|cache| {
            for (key, artifact, charged) in cache.retained_entries() {
                let key_bytes = RetainedBytes::measure(key, usize::MAX).expect("accountable key");
                let value_bytes =
                    RetainedBytes::measure(artifact, usize::MAX).expect("accountable artifact");
                // Independent composition includes the FIFO key copy and the
                // entry's reference-counted bookkeeping after lazy execution.
                let expected = 2 * key_bytes
                    + value_bytes
                    + size_of::<CacheEntryWeight>()
                    + 3 * size_of::<usize>();
                assert!(charged >= expected);
            }
            (cache.len(), cache.retained_weight())
        })
    }

    pub(in crate::db::session) fn clear_shared_query_cache_for_tests(&self, limit: usize) {
        self.with_query_plan_cache(|cache| {
            *cache = QueryPlanCache::new_weighted(SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES, limit);
        });
    }
}
