//! Module: db::session::sql::cache
//! Responsibility: SQL compiled-command cache identity and attribution.
//! Does not own: SQL parsing, lowering, execution, or result shaping.
//! Boundary: keeps syntax-bound SQL cache state separate from shared query-plan cache state.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        commit::CommitSchemaFingerprint,
        executor::EntityAuthority,
        schema::{SchemaInfo, accepted_schema_cache_fingerprint_for_model},
        session::sql::compiled::CompiledSqlCommand,
    },
    metrics::sink::CacheMissReason,
    traits::{CanisterKind, EntityValue},
};
use std::{cell::RefCell, collections::HashMap};

#[cfg(test)]
use crate::db::schema::commit_schema_fingerprint_for_entity;
#[cfg(test)]
use crate::metrics::sink::{CacheKind, record_cache_entries};

// Bump these when SQL cache-key meaning changes in a way that must force
// existing in-heap entries to miss instead of aliasing superseded cache semantics.
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

// Classify one SQL compiled-command cache miss by comparing the missed key
// against already-warmed entries. The comparison order preserves the most
// actionable drift dimensions before falling back to unrelated query text.
pub(in crate::db::session::sql) fn sql_compiled_command_cache_miss_reason(
    cache: &SqlCompiledCommandCache,
    key: &SqlCompiledCommandCacheKey,
) -> CacheMissReason {
    if cache.is_empty() {
        return CacheMissReason::Cold;
    }

    if cache.keys().any(|candidate| {
        candidate.surface == key.surface
            && candidate.entity_path == key.entity_path
            && candidate.schema_fingerprint == key.schema_fingerprint
            && candidate.sql == key.sql
            && candidate.cache_method_version != key.cache_method_version
    }) {
        return CacheMissReason::MethodVersion;
    }

    if cache.keys().any(|candidate| {
        candidate.surface == key.surface
            && candidate.entity_path == key.entity_path
            && candidate.sql == key.sql
            && candidate.cache_method_version == key.cache_method_version
            && candidate.schema_fingerprint != key.schema_fingerprint
    }) {
        return CacheMissReason::SchemaFingerprint;
    }

    if cache.keys().any(|candidate| {
        candidate.entity_path == key.entity_path
            && candidate.schema_fingerprint == key.schema_fingerprint
            && candidate.sql == key.sql
            && candidate.cache_method_version == key.cache_method_version
            && candidate.surface != key.surface
    }) {
        return CacheMissReason::Surface;
    }

    CacheMissReason::DistinctKey
}

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
    authority: EntityAuthority,
    schema: SchemaInfo,
}

impl SqlCompiledCommandCacheContext {
    #[must_use]
    pub(in crate::db::session::sql) fn into_parts(
        self,
    ) -> (SqlCompiledCommandCacheKey, EntityAuthority, SchemaInfo) {
        (self.key, self.authority, self.schema)
    }
}

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
    fn new(
        surface: SqlCompiledCommandSurface,
        entity_path: &'static str,
        schema_fingerprint: CommitSchemaFingerprint,
        sql: &str,
    ) -> Self {
        Self {
            cache_method_version: SQL_COMPILED_COMMAND_CACHE_METHOD_VERSION,
            surface,
            entity_path,
            schema_fingerprint,
            sql: sql.to_string(),
        }
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
    pub(in crate::db::session::sql) fn sql_compiled_command_cache_context_for_entity<E>(
        &self,
        surface: SqlCompiledCommandSurface,
        sql: &str,
    ) -> Result<SqlCompiledCommandCacheContext, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (accepted_schema, authority) = self
            .ensure_accepted_schema_snapshot_and_authority(EntityAuthority::for_type::<E>())
            .map_err(QueryError::execute)?;
        let schema_fingerprint =
            accepted_schema_cache_fingerprint_for_model(authority.model(), &accepted_schema)
                .map_err(QueryError::execute)?;

        Ok(SqlCompiledCommandCacheContext {
            key: SqlCompiledCommandCacheKey::new(
                surface,
                authority.entity_path(),
                schema_fingerprint,
                sql,
            ),
            authority,
            schema: SchemaInfo::from_accepted_snapshot_for_model(
                authority.model(),
                &accepted_schema,
            ),
        })
    }

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
        let entries = self.with_sql_compiled_command_cache(|cache| {
            cache.clear();
            cache.len()
        });
        record_cache_entries(CacheKind::SqlCompiledCommand, entries);
    }
}
