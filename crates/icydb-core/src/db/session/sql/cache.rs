//! Module: db::session::sql::cache
//! Responsibility: SQL compiled-command cache identity and attribution.
//! Does not own: SQL parsing, lowering, execution, or result shaping.
//! Boundary: keeps syntax-bound SQL cache state separate from shared query-plan cache state.

use crate::{
    db::{
        DbSession, QueryError,
        schema::{AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity, SchemaVersion},
        session::{
            AcceptedSchemaCatalogContext,
            bounded_cache::BoundedCache,
            sql::compiled::{CompiledSqlCommand, SqlCompiledSchemaFingerprint},
        },
    },
    metrics::sink::CacheMissReason,
    traits::CanisterKind,
};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

// This cache deliberately stays on syntax-bound SQL statement identity for the
// front-end prepared/template lane. Grouped semantic canonicalization and
// grouped structural/cache identity do not flow into this key.
const SQL_COMPILED_COMMAND_CACHE_MAX_ENTRIES: usize = 1024;

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
    sql: String,
}

pub(in crate::db) type SqlCompiledCommandCache =
    BoundedCache<SqlCompiledCommandCacheKey, CompiledSqlCommand>;

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
            && (candidate.accepted_runtime_root_identity != key.accepted_runtime_root_identity
                || candidate.accepted_schema_revision != key.accepted_schema_revision
                || candidate.schema_version != key.schema_version)
    }) {
        return CacheMissReason::SchemaVersion;
    }

    if cache.keys().any(|candidate| {
        candidate.surface == key.surface
            && candidate.entity_path == key.entity_path
            && candidate.accepted_runtime_root_identity == key.accepted_runtime_root_identity
            && candidate.accepted_schema_revision == key.accepted_schema_revision
            && candidate.sql == key.sql
            && candidate.schema_fingerprint != key.schema_fingerprint
    }) {
        return CacheMissReason::SchemaFingerprint;
    }

    if cache.keys().any(|candidate| {
        candidate.entity_path == key.entity_path
            && candidate.accepted_runtime_root_identity == key.accepted_runtime_root_identity
            && candidate.accepted_schema_revision == key.accepted_schema_revision
            && candidate.schema_version == key.schema_version
            && candidate.schema_fingerprint == key.schema_fingerprint
            && candidate.sql == key.sql
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
    pub(in crate::db::session::sql) const fn shared_query_plan_cache_hit() -> Self {
        Self {
            shared_query_plan_cache_hits: 1,
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

    pub(in crate::db::session::sql) fn with_default<T>(
        result: Result<T, QueryError>,
    ) -> Result<(T, Self), QueryError> {
        result.map(|result| (result, Self::default()))
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
            let cache = caches.entry(scope_id).or_insert_with(|| {
                SqlCompiledCommandCache::new(SQL_COMPILED_COMMAND_CACHE_MAX_ENTRIES)
            });

            f(cache)
        })
    }
}
