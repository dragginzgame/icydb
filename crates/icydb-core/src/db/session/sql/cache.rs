//! Module: db::session::sql::cache
//! Responsibility: SQL compiled-command cache identity and storage.
//! Does not own: SQL parsing, lowering, execution, or result shaping.
//! Boundary: keeps syntax-bound SQL cache state separate from shared query-plan cache state.

use crate::db::resource::bounded_cache::{BoundedCache, CacheEntryWeight};

use crate::{
    db::{
        DbSession, schema::AcceptedSchemaRuntimeRootIdentity, session::sql::CompiledSqlCommand,
        sql::lowering::CheckedSqlSelectTemplate,
    },
    retained::RetainedBytes,
    traits::CanisterKind,
};
use std::{cell::RefCell, collections::HashMap, mem::size_of, rc::Rc};

// SQL compilation uses exact syntax identity. Semantic plan reuse belongs to
// the separate shared query-plan cache, including grouped canonical identity.
const SQL_COMPILED_COMMAND_CACHE_MAX_ENTRIES: usize = 1024;
const SQL_COMPILED_COMMAND_CACHE_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;

///
/// SqlCompiledCommandSurface
///
/// SqlCompiledCommandSurface separates SQL query and mutation API cache lanes so
/// identical text cannot alias across public session surfaces with different
/// admissible statement families.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db::session::sql) enum SqlCompiledCommandSurface {
    Query,
    Mutation,
}

///
/// SqlCompiledCommandCacheKey
///
/// SqlCompiledCommandCacheKey pins one compiled SQL artifact to the exact
/// session-local semantic boundary that produced it.
/// Exact text and API surface locate an entry before parsing. Its value carries
/// the accepted runtime-root identity; callers must verify that authority before
/// reuse. A schema change replaces the old entry instead of retaining both.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct SqlCompiledCommandCacheKey {
    surface: SqlCompiledCommandSurface,
    sql: Rc<str>,
}

pub(in crate::db) type SqlCompiledCommandCache =
    BoundedCache<SqlCompiledCommandCacheKey, SqlCompiledCommandCacheEntry>;

/// Syntax metadata and its compilation authority, without retaining a runtime
/// root or prepared execution plan. The root identity covers every accepted
/// store/catalog plus database incarnation, including entity renames/removal.
#[derive(Clone, Debug)]
pub(in crate::db::session::sql) struct SqlCompiledCommandCacheEntry {
    pub(in crate::db::session::sql) root: AcceptedSchemaRuntimeRootIdentity,
    pub(in crate::db::session::sql) entity_name: Option<Rc<str>>,
    pub(in crate::db::session::sql) command: SqlCachedCommand,
}

// A template cannot enter execution until its current operands have been bound.
#[derive(Clone, Debug)]
pub(in crate::db::session::sql) enum SqlCachedCommand {
    Literal(CompiledSqlCommand),
    Parameterized(Rc<CheckedSqlSelectTemplate>),
}

thread_local! {
    // Keep SQL-facing caches in canister-lifetime heap state keyed by the
    // store registry identity so state-changing canister calls can warm
    // query-facing SQL reuse without leaking entries across unrelated
    // registries in tests.
    static SQL_COMPILED_COMMAND_CACHES: RefCell<HashMap<usize, SqlCompiledCommandCache>> =
        RefCell::new(HashMap::default());
}

impl SqlCompiledCommandCacheKey {
    pub(in crate::db::session::sql) fn new(surface: SqlCompiledCommandSurface, sql: &str) -> Self {
        Self {
            surface,
            sql: Rc::from(sql),
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    #[cfg(test)]
    pub(in crate::db::session) fn sql_compiled_cache_contains_for_tests(&self, sql: &str) -> bool {
        self.with_sql_compiled_command_cache(|cache| {
            cache
                .retained_entries()
                .any(|(key, _, _)| key.sql.as_ref() == sql)
        })
    }

    #[cfg(test)]
    pub(in crate::db::session) fn sql_compiled_cache_len_for_tests(&self) -> usize {
        self.with_sql_compiled_command_cache(|cache| cache.len())
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub(in crate::db::session) fn clear_sql_compiled_cache_for_tests(&self, capacity: usize) {
        self.with_sql_compiled_command_cache(|cache| {
            *cache = SqlCompiledCommandCache::new_weighted(
                SQL_COMPILED_COMMAND_CACHE_MAX_ENTRIES,
                capacity,
            );
        });
    }

    pub(in crate::db::session::sql) fn with_sql_compiled_command_cache<R>(
        &self,
        f: impl FnOnce(&mut SqlCompiledCommandCache) -> R,
    ) -> R {
        let scope_id = self.db.cache_scope_id();

        SQL_COMPILED_COMMAND_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches.entry(scope_id).or_insert_with(|| {
                SqlCompiledCommandCache::new_weighted(
                    SQL_COMPILED_COMMAND_CACHE_MAX_ENTRIES,
                    SQL_COMPILED_COMMAND_CACHE_MAX_RETAINED_BYTES,
                )
            });

            f(cache)
        })
    }
}

// Map/eviction-queue keys share SQL text. Charge each strong reference conservatively,
// as in the shared plan cache, and include entry bookkeeping.
// Overflow, excessive depth, or an oversized payload skips retention, not execution.
pub(in crate::db::session::sql) fn compiled_command_retained_bytes(
    key: &SqlCompiledCommandCacheKey,
    command: &SqlCompiledCommandCacheEntry,
) -> Option<usize> {
    let mut bytes = RetainedBytes::new(SQL_COMPILED_COMMAND_CACHE_MAX_RETAINED_BYTES);
    bytes.add(
        2 * size_of::<SqlCompiledCommandCacheKey>()
            + size_of::<SqlCompiledCommandCacheEntry>()
            + size_of::<CacheEntryWeight>()
            + 3 * size_of::<usize>(),
    )?;
    let before_key = bytes.total();
    bytes.visit(key)?;
    bytes.add(bytes.total().checked_sub(before_key)?)?;
    bytes.visit(command)?;
    Some(bytes.total())
}

crate::retained::retained_fields!(SqlCompiledCommandCacheKey {
    Self { surface: _, sql } => [sql],
});
crate::retained::retained_fields!(SqlCompiledCommandCacheEntry {
    Self { root: _, entity_name, command } => [entity_name, command],
});

crate::retained::retained_fields!(SqlCachedCommand {
    Self::Literal(command) => [command],
    Self::Parameterized(template) => [template],
});
