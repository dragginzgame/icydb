//! Module: db::session::query::cache
//! Responsibility: session-owned shared query-plan cache and planner-visibility handoff.
//! Does not own: query planning semantics, execution, or cache-key fingerprint generation.
//! Boundary: resolves store visibility and memoizes prepared plans for typed and SQL callers.

use crate::{
    db::{
        DbSession, Query, QueryError, TraceReuseArtifactClass, TraceReuseEvent,
        commit::CommitSchemaFingerprint,
        executor::{EntityAuthority, PreparedExecutionPlan, SharedPreparedExecutionPlan},
        predicate::predicate_fingerprint_normalized,
        query::{
            intent::StructuralQuery,
            plan::{AccessPlannedQuery, VisibleIndexes},
        },
        schema::{AcceptedSchemaSnapshot, SchemaInfo, accepted_schema_cache_fingerprint},
    },
    metrics::sink::{
        CacheKind, CacheMissReason, CacheOutcome, record_cache_entries,
        record_cache_event_for_path, record_cache_miss_reason_for_path,
    },
    traits::{CanisterKind, EntityKind, Path},
};
use std::{cell::RefCell, collections::HashMap};

// Bump this when the shared lower query-plan cache key meaning changes in a
// way that must force old in-heap entries to miss instead of aliasing.
const SHARED_QUERY_PLAN_CACHE_METHOD_VERSION: u8 = 2;

///
/// QueryPlanVisibility
///
/// QueryPlanVisibility records whether a store's recovered index state can
/// participate in planning-visible secondary index selection.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) enum QueryPlanVisibility {
    StoreNotReady,
    StoreReady,
}

///
/// QueryPlanCacheKey
///
/// QueryPlanCacheKey is the session-level identity for one shared prepared
/// query plan. It includes store visibility and schema identity so cached
/// plans cannot cross lifecycle or schema boundaries.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct QueryPlanCacheKey {
    cache_method_version: u8,
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    visibility: QueryPlanVisibility,
    structural_query: crate::db::query::intent::StructuralQueryCacheKey,
}

///
/// QueryPlanCacheAttribution
///
/// QueryPlanCacheAttribution reports whether one shared query-plan lookup hit
/// or missed without exposing the cache map itself to diagnostics callers.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct QueryPlanCacheAttribution {
    pub hits: u64,
    pub misses: u64,
}

pub(in crate::db) type QueryPlanCache = HashMap<QueryPlanCacheKey, SharedPreparedExecutionPlan>;

// Classify one shared query-plan cache miss by comparing the missed key against
// already-warmed plans. The buckets mirror the identity dimensions that can
// drift independently while keeping query structure and schema hashes private.
fn shared_query_plan_cache_miss_reason(
    cache: &QueryPlanCache,
    key: &QueryPlanCacheKey,
) -> CacheMissReason {
    if cache.is_empty() {
        return CacheMissReason::Cold;
    }

    if cache.keys().any(|candidate| {
        candidate.entity_path == key.entity_path
            && candidate.schema_fingerprint == key.schema_fingerprint
            && candidate.visibility == key.visibility
            && candidate.structural_query == key.structural_query
            && candidate.cache_method_version != key.cache_method_version
    }) {
        return CacheMissReason::MethodVersion;
    }

    if cache.keys().any(|candidate| {
        candidate.entity_path == key.entity_path
            && candidate.visibility == key.visibility
            && candidate.structural_query == key.structural_query
            && candidate.cache_method_version == key.cache_method_version
            && candidate.schema_fingerprint != key.schema_fingerprint
    }) {
        return CacheMissReason::SchemaFingerprint;
    }

    if cache.keys().any(|candidate| {
        candidate.entity_path == key.entity_path
            && candidate.schema_fingerprint == key.schema_fingerprint
            && candidate.structural_query == key.structural_query
            && candidate.cache_method_version == key.cache_method_version
            && candidate.visibility != key.visibility
    }) {
        return CacheMissReason::Visibility;
    }

    CacheMissReason::DistinctKey
}

thread_local! {
    // Keep one in-heap query-plan cache per store registry so fresh `DbSession`
    // facades can share prepared logical plans across update/query calls while
    // tests and multi-registry host processes remain isolated by registry
    // identity.
    static QUERY_PLAN_CACHES: RefCell<HashMap<usize, QueryPlanCache>> =
        RefCell::new(HashMap::default());
}

impl QueryPlanCacheAttribution {
    #[must_use]
    const fn hit() -> Self {
        Self { hits: 1, misses: 0 }
    }

    #[must_use]
    const fn miss() -> Self {
        Self { hits: 0, misses: 1 }
    }
}

// Map one shared query-plan cache attribution outcome onto the explicit reuse
// event shipped in `0.109.0`.
pub(in crate::db::session) const fn query_plan_cache_reuse_event(
    attribution: QueryPlanCacheAttribution,
) -> TraceReuseEvent {
    if attribution.hits > 0 {
        TraceReuseEvent::hit(TraceReuseArtifactClass::SharedPreparedQueryPlan)
    } else {
        TraceReuseEvent::miss(TraceReuseArtifactClass::SharedPreparedQueryPlan)
    }
}

impl<C: CanisterKind> DbSession<C> {
    fn with_query_plan_cache<R>(&self, f: impl FnOnce(&mut QueryPlanCache) -> R) -> R {
        let scope_id = self.db.cache_scope_id();

        QUERY_PLAN_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches.entry(scope_id).or_default();

            f(cache)
        })
    }

    pub(in crate::db::session) fn visible_indexes_for_accepted_schema(
        schema_info: &SchemaInfo,
        visibility: QueryPlanVisibility,
    ) -> VisibleIndexes<'static> {
        match visibility {
            QueryPlanVisibility::StoreReady => {
                let visible_indexes = VisibleIndexes::accepted_schema_visible(schema_info);
                debug_assert!(visible_indexes.accepted_field_path_contracts_are_consistent());
                debug_assert!(visible_indexes.accepted_expression_contracts_are_consistent());
                debug_assert_eq!(
                    visible_indexes.accepted_expression_index_count(),
                    Some(visible_indexes.accepted_expression_indexes().len()),
                );

                visible_indexes
            }
            QueryPlanVisibility::StoreNotReady => VisibleIndexes::none(),
        }
    }

    #[cfg(test)]
    pub(in crate::db) fn query_plan_cache_len(&self) -> usize {
        self.with_query_plan_cache(|cache| cache.len())
    }

    #[cfg(test)]
    pub(in crate::db) fn clear_query_plan_cache_for_tests(&self) {
        let entries = self.with_query_plan_cache(|cache| {
            cache.clear();
            cache.len()
        });
        record_cache_entries(CacheKind::SharedQueryPlan, entries);
    }

    pub(in crate::db) fn query_plan_visibility_for_store_path(
        &self,
        store_path: &'static str,
    ) -> Result<QueryPlanVisibility, QueryError> {
        let store = self
            .db
            .recovered_store(store_path)
            .map_err(QueryError::execute)?;
        let visibility = if store.index_state() == crate::db::IndexState::Ready {
            QueryPlanVisibility::StoreReady
        } else {
            QueryPlanVisibility::StoreNotReady
        };

        Ok(visibility)
    }

    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        query: &StructuralQuery,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema_fingerprint =
            accepted_schema_cache_fingerprint(accepted_schema).map_err(QueryError::execute)?;
        let schema_info = SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(
            authority.model(),
            accepted_schema,
            true,
        );
        if query.trivial_scalar_load_fast_path_eligible() {
            return self.cached_trivial_scalar_load_plan_for_authority(
                authority,
                schema_fingerprint,
                schema_info,
                visibility,
                query,
            );
        }

        let visible_indexes = Self::visible_indexes_for_accepted_schema(&schema_info, visibility);
        let planning_state = query.prepare_scalar_planning_state_with_schema_info(schema_info)?;
        let normalized_predicate_fingerprint = planning_state
            .normalized_predicate()
            .map(predicate_fingerprint_normalized);
        let cache_key =
            QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint_and_method_version(
                authority.clone(),
                schema_fingerprint,
                visibility,
                query,
                normalized_predicate_fingerprint,
                SHARED_QUERY_PLAN_CACHE_METHOD_VERSION,
            );

        let miss_reason = {
            let (cached, entries, miss_reason) = self.with_query_plan_cache(|cache| {
                let cached = cache.get(&cache_key).cloned();
                let miss_reason = cached
                    .is_none()
                    .then(|| shared_query_plan_cache_miss_reason(cache, &cache_key));

                (cached, cache.len(), miss_reason)
            });
            record_cache_entries(CacheKind::SharedQueryPlan, entries);
            if let Some(prepared_plan) = cached {
                record_cache_event_for_path(
                    CacheKind::SharedQueryPlan,
                    CacheOutcome::Hit,
                    authority.entity_path(),
                );
                return Ok((prepared_plan, QueryPlanCacheAttribution::hit()));
            }

            miss_reason
        };
        record_cache_event_for_path(
            CacheKind::SharedQueryPlan,
            CacheOutcome::Miss,
            authority.entity_path(),
        );
        if let Some(reason) = miss_reason {
            record_cache_miss_reason_for_path(
                CacheKind::SharedQueryPlan,
                reason,
                authority.entity_path(),
            );
        }

        let plan = query.build_plan_with_visible_indexes_from_scalar_planning_state(
            &visible_indexes,
            planning_state,
        )?;
        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority.clone(), plan);
        let entries = self.with_query_plan_cache(|cache| {
            cache.insert(cache_key, prepared_plan.clone());
            cache.len()
        });
        record_cache_entries(CacheKind::SharedQueryPlan, entries);
        record_cache_event_for_path(
            CacheKind::SharedQueryPlan,
            CacheOutcome::Insert,
            authority.entity_path(),
        );

        Ok((prepared_plan, QueryPlanCacheAttribution::miss()))
    }

    fn cached_trivial_scalar_load_plan_for_authority(
        &self,
        authority: EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        schema_info: SchemaInfo,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let cache_key =
            QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint_and_method_version(
                authority.clone(),
                schema_fingerprint,
                visibility,
                query,
                None,
                SHARED_QUERY_PLAN_CACHE_METHOD_VERSION,
            );

        let miss_reason = {
            let (cached, entries, miss_reason) = self.with_query_plan_cache(|cache| {
                let cached = cache.get(&cache_key).cloned();
                let miss_reason = cached
                    .is_none()
                    .then(|| shared_query_plan_cache_miss_reason(cache, &cache_key));

                (cached, cache.len(), miss_reason)
            });
            record_cache_entries(CacheKind::SharedQueryPlan, entries);
            if let Some(prepared_plan) = cached {
                record_cache_event_for_path(
                    CacheKind::SharedQueryPlan,
                    CacheOutcome::Hit,
                    authority.entity_path(),
                );
                return Ok((prepared_plan, QueryPlanCacheAttribution::hit()));
            }

            miss_reason
        };
        record_cache_event_for_path(
            CacheKind::SharedQueryPlan,
            CacheOutcome::Miss,
            authority.entity_path(),
        );
        if let Some(reason) = miss_reason {
            record_cache_miss_reason_for_path(
                CacheKind::SharedQueryPlan,
                reason,
                authority.entity_path(),
            );
        }

        let Some(plan) = query.try_build_trivial_scalar_load_plan_with_schema_info(schema_info)?
        else {
            return Err(QueryError::invariant(
                "trivial scalar load fast path lost eligibility during plan build",
            ));
        };
        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority.clone(), plan);
        let entries = self.with_query_plan_cache(|cache| {
            cache.insert(cache_key, prepared_plan.clone());
            cache.len()
        });
        record_cache_entries(CacheKind::SharedQueryPlan, entries);
        record_cache_event_for_path(
            CacheKind::SharedQueryPlan,
            CacheOutcome::Insert,
            authority.entity_path(),
        );

        Ok((prepared_plan, QueryPlanCacheAttribution::miss()))
    }

    #[cfg(test)]
    pub(in crate::db) fn query_plan_cache_key_for_tests(
        authority: EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        cache_method_version: u8,
    ) -> QueryPlanCacheKey {
        QueryPlanCacheKey::for_authority_with_method_version(
            authority,
            schema_fingerprint,
            visibility,
            query,
            cache_method_version,
        )
    }

    // Resolve the planner-visible index slice for one typed query exactly once
    // at the session boundary before handing execution/planning off to query-owned logic.
    pub(in crate::db::session) fn with_query_visible_indexes<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(&Query<E>, &VisibleIndexes<'static>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visibility = self.query_plan_visibility_for_store_path(E::Store::PATH)?;
        let accepted_schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(
            E::MODEL,
            &accepted_schema,
            true,
        );
        let visible_indexes = Self::visible_indexes_for_accepted_schema(&schema_info, visibility);

        op(query, &visible_indexes)
    }

    pub(in crate::db::session) fn cached_prepared_query_plan_for_entity<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(PreparedExecutionPlan<E>, QueryPlanCacheAttribution), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, attribution) = self.cached_shared_query_plan_for_entity::<E>(query)?;

        Ok((prepared_plan.typed_clone::<E>(), attribution))
    }

    // Resolve one typed query through the shared lower query-plan cache using
    // the canonical authority and schema-fingerprint pair for that entity.
    pub(in crate::db::session) fn cached_shared_query_plan_for_entity<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (accepted_schema, authority) = self
            .accepted_entity_authority::<E>()
            .map_err(QueryError::execute)?;

        self.cached_shared_query_plan_for_accepted_authority(
            authority,
            &accepted_schema,
            query.structural(),
        )
    }

    // Borrow one cached shared plan only for derived read-only facts. The helper
    // still clones the cheap shared prepared-plan shell out of the cache map, but
    // it avoids cloning the owned `AccessPlannedQuery` carried inside it.
    pub(in crate::db::session) fn try_map_cached_shared_query_plan_ref_for_entity<E, T>(
        &self,
        query: &Query<E>,
        map: impl FnOnce(&SharedPreparedExecutionPlan) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, _) = self.cached_shared_query_plan_for_entity::<E>(query)?;

        map(&prepared_plan)
    }

    // Map one typed query onto one cached lower prepared plan so session-owned
    // planned and compiled wrappers reuse the same cache lookup while returning
    // query-owned neutral plan DTOs.
    pub(in crate::db::session) fn map_cached_shared_query_plan_for_entity<E, T>(
        &self,
        query: &Query<E>,
        map: impl FnOnce(AccessPlannedQuery) -> T,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, _) = self.cached_shared_query_plan_for_entity::<E>(query)?;

        Ok(map(prepared_plan.logical_plan().clone()))
    }
}

impl QueryPlanCacheKey {
    // Assemble the canonical cache-key shell once so the test and
    // normalized-predicate constructors only decide which structural query key
    // they feed into the shared session cache identity.
    fn from_authority_parts(
        authority: EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        structural_query: crate::db::query::intent::StructuralQueryCacheKey,
        cache_method_version: u8,
    ) -> Self {
        Self {
            cache_method_version,
            entity_path: authority.entity_path(),
            schema_fingerprint,
            visibility,
            structural_query,
        }
    }

    #[cfg(test)]
    fn for_authority_with_method_version(
        authority: EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        cache_method_version: u8,
    ) -> Self {
        Self::from_authority_parts(
            authority,
            schema_fingerprint,
            visibility,
            query.structural_cache_key(),
            cache_method_version,
        )
    }

    fn for_authority_with_normalized_predicate_fingerprint_and_method_version(
        authority: EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        normalized_predicate_fingerprint: Option<[u8; 32]>,
        cache_method_version: u8,
    ) -> Self {
        Self::from_authority_parts(
            authority,
            schema_fingerprint,
            visibility,
            query.structural_cache_key_with_normalized_predicate_fingerprint(
                normalized_predicate_fingerprint,
            ),
            cache_method_version,
        )
    }
}
