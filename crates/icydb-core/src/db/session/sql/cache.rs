//! Module: db::session::sql::cache
//! Responsibility: SQL compiled-command cache identity and attribution.
//! Does not own: SQL parsing, lowering, execution, or result shaping.
//! Boundary: keeps syntax-bound SQL cache state separate from shared query-plan cache state.

use crate::{
    db::{
        DbSession, PersistedRow, commit::CommitSchemaFingerprint,
        schema::commit_schema_fingerprint_for_entity,
    },
    traits::{CanisterKind, EntityValue},
};
use std::{cell::RefCell, collections::HashMap};

use crate::db::session::sql::compiled::CompiledSqlCommand;

// Bump these when SQL cache-key meaning changes in a way that must force
// existing in-heap entries to miss instead of aliasing old semantics.
// This cache deliberately stays on syntax-bound SQL statement identity for the
// front-end prepared/template lane. Grouped semantic canonicalization and
// grouped structural/cache identity do not flow into this key.
const SQL_COMPILED_COMMAND_CACHE_METHOD_VERSION: u8 = 1;

///
/// SqlCacheAttribution
///
/// SqlCacheAttribution keeps the surviving SQL-front-end compile cache
/// separate from the shared lower query-plan cache so perf audits can tell
/// which boundary actually produced reuse on one query path.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SqlCacheAttribution {
    pub sql_compiled_command_cache_hits: u64,
    pub sql_compiled_command_cache_misses: u64,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

///
/// SqlCompiledCommandSurface
///
/// SqlCompiledCommandSurface separates SQL read and write API cache lanes so
/// identical text cannot alias across public session surfaces with different
/// admissible statement families.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db::session::sql) enum SqlCompiledCommandSurface {
    Query,
    Update,
}

///
/// SqlCompiledCommandCacheKey
///
/// SqlCompiledCommandCacheKey pins one compiled SQL artifact to the exact
/// session-local semantic boundary that produced it.
/// The key is intentionally conservative: surface kind, entity path, schema
/// fingerprint, and raw SQL text must all match before execution can reuse a
/// prior compile result.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct SqlCompiledCommandCacheKey {
    cache_method_version: u8,
    surface: SqlCompiledCommandSurface,
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    sql: String,
}

pub(in crate::db) type SqlCompiledCommandCache =
    HashMap<SqlCompiledCommandCacheKey, CompiledSqlCommand>;

thread_local! {
    // Keep SQL-facing caches in canister-lifetime heap state keyed by the
    // store registry identity so update calls can warm query-facing SQL reuse
    // without leaking entries across unrelated registries in tests.
    static SQL_COMPILED_COMMAND_CACHES: RefCell<HashMap<usize, SqlCompiledCommandCache>> =
        RefCell::new(HashMap::default());
}

impl SqlCacheAttribution {
    #[must_use]
    pub(in crate::db::session::sql) const fn none() -> Self {
        Self {
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 0,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 0,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) const fn sql_compiled_command_cache_hit() -> Self {
        Self {
            sql_compiled_command_cache_hits: 1,
            ..Self::none()
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) const fn sql_compiled_command_cache_miss() -> Self {
        Self {
            sql_compiled_command_cache_misses: 1,
            ..Self::none()
        }
    }

    #[must_use]
    pub(in crate::db) const fn from_shared_query_plan_cache(
        attribution: crate::db::session::query::QueryPlanCacheAttribution,
    ) -> Self {
        Self {
            shared_query_plan_cache_hits: attribution.hits,
            shared_query_plan_cache_misses: attribution.misses,
            ..Self::none()
        }
    }

    #[cfg(feature = "diagnostics")]
    #[must_use]
    pub(in crate::db::session::sql) const fn merge(self, other: Self) -> Self {
        Self {
            sql_compiled_command_cache_hits: self
                .sql_compiled_command_cache_hits
                .saturating_add(other.sql_compiled_command_cache_hits),
            sql_compiled_command_cache_misses: self
                .sql_compiled_command_cache_misses
                .saturating_add(other.sql_compiled_command_cache_misses),
            shared_query_plan_cache_hits: self
                .shared_query_plan_cache_hits
                .saturating_add(other.shared_query_plan_cache_hits),
            shared_query_plan_cache_misses: self
                .shared_query_plan_cache_misses
                .saturating_add(other.shared_query_plan_cache_misses),
        }
    }
}

impl SqlCompiledCommandCacheKey {
    pub(in crate::db::session::sql) fn for_entity<E>(
        surface: SqlCompiledCommandSurface,
        sql: &str,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self {
            cache_method_version: SQL_COMPILED_COMMAND_CACHE_METHOD_VERSION,
            surface,
            entity_path: E::PATH,
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            sql: sql.to_string(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }
}

#[cfg(test)]
impl SqlCompiledCommandCacheKey {
    pub(in crate::db) fn query_for_entity_with_method_version<E>(
        sql: &str,
        cache_method_version: u8,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self::for_entity_with_method_version::<E>(
            SqlCompiledCommandSurface::Query,
            sql,
            cache_method_version,
        )
    }

    pub(in crate::db) fn update_for_entity_with_method_version<E>(
        sql: &str,
        cache_method_version: u8,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self::for_entity_with_method_version::<E>(
            SqlCompiledCommandSurface::Update,
            sql,
            cache_method_version,
        )
    }

    fn for_entity_with_method_version<E>(
        surface: SqlCompiledCommandSurface,
        sql: &str,
        cache_method_version: u8,
    ) -> Self
    where
        E: PersistedRow + EntityValue,
    {
        Self {
            cache_method_version,
            surface,
            entity_path: E::PATH,
            schema_fingerprint: commit_schema_fingerprint_for_entity::<E>(),
            sql: sql.to_string(),
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql) fn with_sql_compiled_command_cache<R>(
        &self,
        f: impl FnOnce(&mut SqlCompiledCommandCache) -> R,
    ) -> R {
        let scope_id = self.db.cache_scope_id();

        SQL_COMPILED_COMMAND_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches.entry(scope_id).or_default();

            f(cache)
        })
    }

    #[cfg(test)]
    pub(in crate::db) fn sql_compiled_command_cache_len(&self) -> usize {
        self.with_sql_compiled_command_cache(|cache| cache.len())
    }

    #[cfg(test)]
    pub(in crate::db) fn clear_sql_caches_for_tests(&self) {
        self.with_sql_compiled_command_cache(SqlCompiledCommandCache::clear);
    }
}
