//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session) fn sql_compiled_cache_contains_for_tests(&self, sql: &str) -> bool {
        self.with_sql_compiled_command_cache(|cache| {
            cache
                .retained_entries()
                .any(|(key, _, _)| key.sql.as_ref() == sql)
        })
    }

    pub(in crate::db::session) fn sql_compiled_cache_len_for_tests(&self) -> usize {
        self.with_sql_compiled_command_cache(|cache| cache.len())
    }

    pub(in crate::db::session) fn sql_compiled_cache_usage_for_tests(&self) -> (usize, usize) {
        self.with_sql_compiled_command_cache(|cache| {
            for (key, command, charged) in cache.retained_entries() {
                let key_bytes = RetainedBytes::measure(key, usize::MAX).expect("accountable key");
                let command_bytes =
                    RetainedBytes::measure(command, usize::MAX).expect("accountable command");
                assert_eq!(
                    charged,
                    2 * key_bytes
                        + command_bytes
                        + size_of::<CacheEntryWeight>()
                        + 3 * size_of::<usize>()
                );
            }
            (cache.len(), cache.retained_weight())
        })
    }

    pub(in crate::db::session) fn clear_sql_compiled_cache_for_tests(&self, capacity: usize) {
        self.with_sql_compiled_command_cache(|cache| {
            *cache = SqlCompiledCommandCache::new_weighted(
                SQL_COMPILED_COMMAND_CACHE_MAX_ENTRIES,
                capacity,
            );
        });
    }
}
