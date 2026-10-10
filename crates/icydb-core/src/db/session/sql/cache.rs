//! Module: db::session::sql::cache
//! Responsibility: SQL compiled-command cache identity and storage.
//! Does not own: SQL parsing, lowering, execution, or result shaping.
//! Boundary: keeps syntax-bound SQL cache state separate from shared query-plan cache state.

use crate::{
    db::{
        DbSession,
        schema::{AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity, SchemaVersion},
        session::{
            AcceptedSchemaCatalogContext,
            bounded_cache::{BoundedCache, CacheEntryWeight},
            sql::compiled::{CompiledSqlCommand, SqlCompiledSchemaFingerprint},
        },
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
/// The key is intentionally conservative: surface kind, entity path, schema
/// runtime-root identity, entity schema revision/version, schema fingerprint,
/// and raw SQL text must all match before execution can reuse a prior compile.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct SqlCompiledCommandCacheKey {
    surface: SqlCompiledCommandSurface,
    entity_path: Rc<str>,
    accepted_runtime_root_identity: AcceptedSchemaRuntimeRootIdentity,
    accepted_schema_revision: AcceptedSchemaRevision,
    schema_version: SchemaVersion,
    schema_fingerprint: SqlCompiledSchemaFingerprint,
    sql: Rc<str>,
}

pub(in crate::db) type SqlCompiledCommandCache =
    BoundedCache<SqlCompiledCommandCacheKey, CompiledSqlCommand>;

///
/// SqlCompiledCommandCacheContext
///
/// SqlCompiledCommandCacheContext carries the accepted-schema facts needed by
/// one SQL compile lookup. The cache key uses the accepted schema fingerprint;
/// miss compilation uses the paired `EntityAuthority` and `SchemaInfo` so
/// read-side predicate canonicalization observes the same live schema authority.
///

#[derive(Debug)]
pub(in crate::db::session::sql) struct SqlCompiledCommandCacheContext {
    key: SqlCompiledCommandCacheKey,
    catalog: AcceptedSchemaCatalogContext,
}

impl SqlCompiledCommandCacheContext {
    #[must_use]
    pub(in crate::db::session::sql) fn from_catalog(
        surface: SqlCompiledCommandSurface,
        sql: &str,
        catalog: AcceptedSchemaCatalogContext,
    ) -> Self {
        Self {
            key: SqlCompiledCommandCacheKey::new(
                surface,
                catalog.identity().entity_path(),
                catalog.runtime_root_identity(),
                catalog.revision(),
                catalog.schema_version(),
                SqlCompiledSchemaFingerprint::from_catalog(&catalog),
                sql,
            ),
            catalog,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) fn into_cache_inputs(
        self,
    ) -> (SqlCompiledCommandCacheKey, AcceptedSchemaCatalogContext) {
        (self.key, self.catalog)
    }
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
    fn new(
        surface: SqlCompiledCommandSurface,
        entity_path: impl Into<Rc<str>>,
        accepted_runtime_root_identity: AcceptedSchemaRuntimeRootIdentity,
        accepted_schema_revision: AcceptedSchemaRevision,
        schema_version: SchemaVersion,
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        sql: &str,
    ) -> Self {
        Self {
            surface,
            entity_path: entity_path.into(),
            accepted_runtime_root_identity,
            accepted_schema_revision,
            schema_version,
            schema_fingerprint,
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

// Map/FIFO keys share SQL text. Charge each strong reference conservatively,
// as in the shared plan cache, and include entry bookkeeping.
// Overflow, excessive depth, or an oversized payload skips retention, not execution.
pub(in crate::db::session::sql) fn compiled_command_retained_bytes(
    key: &SqlCompiledCommandCacheKey,
    command: &CompiledSqlCommand,
) -> Option<usize> {
    let mut bytes = RetainedBytes::new(SQL_COMPILED_COMMAND_CACHE_MAX_RETAINED_BYTES);
    bytes.add(
        2 * size_of::<SqlCompiledCommandCacheKey>()
            + size_of::<CompiledSqlCommand>()
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
    Self { surface: _, entity_path, accepted_runtime_root_identity: _, accepted_schema_revision: _, schema_version: _, schema_fingerprint: _, sql }
        => [entity_path, sql],
});
