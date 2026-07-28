//! Module: db::session::query::cache
//! Responsibility: session-owned shared query-plan cache and planner-visibility handoff.
//! Does not own: query planning semantics, execution, or cache-key fingerprint generation.
//! Boundary: resolves store visibility and memoizes prepared plans for typed and SQL callers.

mod identity;

#[cfg(feature = "sql-explain")]
use crate::db::TraceReuseEvent;
#[cfg(any(test, feature = "sql"))]
use crate::db::commit::CommitSchemaFingerprint;
use crate::{
    db::{
        DbSession, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        predicate::predicate_fingerprint_normalized,
        query::{intent::StructuralQuery, plan::VisibleIndexes},
        schema::{
            AcceptedSchemaSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
            SchemaInfo,
        },
        session::{AcceptedSchemaCatalogContext, bounded_cache::BoundedCache},
    },
    metrics::sink::{
        CacheKind, CacheMissReason, CacheOutcome, record_cache_entries,
        record_cache_event_for_path, record_cache_miss_reason_for_path,
    },
    traits::CanisterKind,
};
use std::{cell::RefCell, collections::HashMap};

#[cfg(feature = "diagnostics")]
pub(in crate::db) use identity::QueryPlanCompilePhaseAttribution;
use identity::{
    QueryPlanAcceptedSchema, QueryPlanCacheKey, QueryPlanCompilePhase,
    QueryPlanCompilePhaseRecorder, SchemaCacheIdentity,
};
pub(in crate::db) use identity::{QueryPlanCacheAttribution, QueryPlanVisibility};

const SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES: usize = 1024;

pub(in crate::db) type QueryPlanCache =
    BoundedCache<QueryPlanCacheKey, SharedPreparedExecutionPlan>;

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

    let mut schema_version_mismatch = false;
    let mut schema_fingerprint_mismatch = false;
    let mut visibility_mismatch = false;

    for candidate in cache.keys() {
        if candidate.entity_path() != key.entity_path()
            || candidate.structural_query() != key.structural_query()
        {
            continue;
        }

        let same_schema_version = candidate
            .schema_identity()
            .same_version(key.schema_identity());
        let same_schema_fingerprint = candidate
            .schema_identity()
            .same_fingerprint(key.schema_identity());
        let same_visibility = candidate.visibility() == key.visibility();

        schema_version_mismatch |=
            same_schema_fingerprint && same_visibility && !same_schema_version;
        schema_fingerprint_mismatch |= same_visibility && !same_schema_fingerprint;
        visibility_mismatch |= same_schema_version && same_schema_fingerprint && !same_visibility;
    }

    if schema_version_mismatch {
        CacheMissReason::SchemaVersion
    } else if schema_fingerprint_mismatch {
        CacheMissReason::SchemaFingerprint
    } else if visibility_mismatch {
        CacheMissReason::Visibility
    } else {
        CacheMissReason::DistinctKey
    }
}

thread_local! {
    // Keep one in-heap query-plan cache per store registry so fresh `DbSession`
    // facades can share prepared logical plans across update/query calls while
    // tests and multi-registry host processes remain isolated by registry
    // identity.
    static QUERY_PLAN_CACHES: RefCell<HashMap<usize, QueryPlanCache>> =
        RefCell::new(HashMap::default());
}

fn schema_info_for_plan_cache_authority(
    authority: &EntityAuthority,
    accepted_schema: &AcceptedSchemaSnapshot,
) -> Result<SchemaInfo, QueryError> {
    if let Some(schema_info) = authority.accepted_schema_info()
        && (!accepted_schema_has_expression_indexes(accepted_schema)
            || !schema_info.expression_indexes().is_empty())
    {
        return Ok(schema_info.clone());
    }

    let enum_catalog = authority
        .accepted_value_catalog_handle()
        .map_err(QueryError::execute)?
        .clone();
    Ok(SchemaInfo::from_accepted_snapshot_and_catalog(
        accepted_schema,
        enum_catalog,
        true,
    ))
}

fn accepted_schema_has_expression_indexes(accepted_schema: &AcceptedSchemaSnapshot) -> bool {
    accepted_schema
        .persisted_snapshot()
        .indexes()
        .iter()
        .any(|index| match index.key() {
            PersistedIndexKeySnapshot::FieldPath(_) => false,
            PersistedIndexKeySnapshot::Items(items) => items
                .iter()
                .any(|item| matches!(item, PersistedIndexKeyItemSnapshot::Expression(_))),
        })
}

// Map one shared query-plan cache attribution outcome onto the explicit reuse
// event owned by the current cache contract.
#[cfg(feature = "sql-explain")]
pub(in crate::db::session) const fn query_plan_cache_reuse_event(
    attribution: QueryPlanCacheAttribution,
) -> TraceReuseEvent {
    if attribution.hits > 0 {
        TraceReuseEvent::Hit
    } else {
        TraceReuseEvent::Miss
    }
}

impl<C: CanisterKind> DbSession<C> {
    fn with_query_plan_cache<R>(&self, f: impl FnOnce(&mut QueryPlanCache) -> R) -> R {
        let scope_id = self.db.cache_scope_id();

        QUERY_PLAN_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches
                .entry(scope_id)
                .or_insert_with(|| QueryPlanCache::new(SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES));

            f(cache)
        })
    }

    fn lookup_shared_query_plan_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        cache_key: &QueryPlanCacheKey,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> (
        Option<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution)>,
        Option<CacheMissReason>,
    ) {
        recorder.measure(QueryPlanCompilePhase::CacheLookup, || {
            let (cached, entries, miss_reason) = self.with_query_plan_cache(|cache| {
                let cached = cache.get(cache_key).cloned();
                let miss_reason = cached
                    .is_none()
                    .then(|| shared_query_plan_cache_miss_reason(cache, cache_key));

                (cached, cache.len(), miss_reason)
            });
            record_cache_entries(CacheKind::SharedQueryPlan, entries);
            if let Some(prepared_plan) = cached {
                record_cache_event_for_path(
                    CacheKind::SharedQueryPlan,
                    CacheOutcome::Hit,
                    authority.entity_path(),
                );
                return (
                    Some((prepared_plan, QueryPlanCacheAttribution::hit())),
                    None,
                );
            }

            (None, miss_reason)
        })
    }

    fn insert_shared_query_plan_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        cache_key: QueryPlanCacheKey,
        prepared_plan: &SharedPreparedExecutionPlan,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) {
        let entries = recorder.measure(QueryPlanCompilePhase::CacheInsert, || {
            self.with_query_plan_cache(|cache| {
                cache.insert(cache_key, prepared_plan.clone());
                cache.len()
            })
        });
        record_cache_entries(CacheKind::SharedQueryPlan, entries);
        record_cache_event_for_path(
            CacheKind::SharedQueryPlan,
            CacheOutcome::Insert,
            authority.entity_path(),
        );
    }

    fn resolve_shared_query_plan_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        cache_key: QueryPlanCacheKey,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
        build_prepared_plan: impl FnOnce() -> Result<SharedPreparedExecutionPlan, QueryError>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let (cached_plan, miss_reason) =
            self.lookup_shared_query_plan_for_authority_recording(authority, &cache_key, recorder);
        if let Some(cached_plan) = cached_plan {
            return Ok(cached_plan);
        }
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

        let prepared_plan =
            recorder.measure(QueryPlanCompilePhase::PlanBuild, build_prepared_plan)?;
        self.insert_shared_query_plan_for_authority_recording(
            authority,
            cache_key,
            &prepared_plan,
            recorder,
        );

        Ok((prepared_plan, QueryPlanCacheAttribution::miss()))
    }

    pub(in crate::db::session) fn visible_indexes_for_accepted_schema(
        schema_info: &SchemaInfo,
        visibility: QueryPlanVisibility,
    ) -> VisibleIndexes {
        match visibility {
            QueryPlanVisibility::StoreReady => {
                let visible_indexes = VisibleIndexes::accepted_schema_visible(schema_info);
                debug_assert!(visible_indexes.accepted_field_path_contracts_are_consistent());
                debug_assert!(visible_indexes.accepted_expression_contracts_are_consistent());
                debug_assert!(visible_indexes.accepted_semantic_contracts_are_consistent());
                debug_assert_eq!(
                    visible_indexes.accepted_expression_index_count(),
                    Some(visible_indexes.accepted_expression_indexes().len()),
                );

                visible_indexes
            }
            #[cfg(feature = "sql")]
            QueryPlanVisibility::PrimaryOnly => {
                VisibleIndexes::accepted_schema_primary_only(schema_info)
            }
            QueryPlanVisibility::StoreNotReady => VisibleIndexes::none(),
        }
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

    #[cfg(feature = "sql")]
    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema = QueryPlanAcceptedSchema::from_accepted_schema_with_fingerprint(
            accepted_schema,
            schema_fingerprint,
        );
        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query,
        )
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let visibility = match self.query_plan_visibility_for_store_path(authority.store_path())? {
            QueryPlanVisibility::StoreReady | QueryPlanVisibility::PrimaryOnly => {
                QueryPlanVisibility::PrimaryOnly
            }
            QueryPlanVisibility::StoreNotReady => QueryPlanVisibility::StoreNotReady,
        };
        let schema = QueryPlanAcceptedSchema::from_accepted_schema_with_fingerprint(
            accepted_schema,
            schema_fingerprint,
        );

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query,
        )
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority_with_catalog(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        query: &StructuralQuery,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema = QueryPlanAcceptedSchema::from_catalog(catalog);

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query,
        )
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        query: &StructuralQuery,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            QueryPlanCacheAttribution,
            QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema = QueryPlanAcceptedSchema::from_catalog(catalog);
        let mut compile_attribution = QueryPlanCompilePhaseAttribution::default();
        let mut recorder = QueryPlanCompilePhaseRecorder::new(&mut compile_attribution);
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility_recording(
                authority,
                schema,
                visibility,
                query,
                &mut recorder,
            )?;

        Ok((prepared_plan, cache_attribution, compile_attribution))
    }

    #[cfg(feature = "sql")]
    fn cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
        &self,
        authority: EntityAuthority,
        schema: QueryPlanAcceptedSchema<'_>,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let mut recorder = QueryPlanCompilePhaseRecorder::none();

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility_recording(
            authority,
            schema,
            visibility,
            query,
            &mut recorder,
        )
    }

    fn cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility_recording(
        &self,
        authority: EntityAuthority,
        schema: QueryPlanAcceptedSchema<'_>,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let schema_identity = schema.identity();
        if let Some(cached) = self.try_cached_filterless_query_plan_for_authority_recording(
            &authority,
            schema_identity,
            visibility,
            query,
            recorder,
        ) {
            return Ok(cached);
        }
        let schema_info = recorder.measure(QueryPlanCompilePhase::SchemaInfo, || {
            schema_info_for_plan_cache_authority(&authority, schema.accepted_schema())
        })?;
        if query.trivial_scalar_load_fast_path_eligible_with_schema(&schema_info) {
            return self.cached_trivial_scalar_load_plan_for_authority_recording(
                authority,
                schema_identity,
                schema_info,
                visibility,
                query,
                recorder,
            );
        }

        let visible_indexes = recorder.measure(QueryPlanCompilePhase::SchemaInfo, || {
            Self::visible_indexes_for_accepted_schema(&schema_info, visibility)
        });
        let planning_state = recorder.measure(QueryPlanCompilePhase::Prepare, || {
            query.prepare_scalar_planning_state_with_schema_info(schema_info)
        })?;
        let normalized_predicate_fingerprint =
            recorder.measure(QueryPlanCompilePhase::Prepare, || {
                planning_state
                    .normalized_predicate()
                    .map(predicate_fingerprint_normalized)
            });
        let cache_key = recorder.measure(QueryPlanCompilePhase::CacheKey, || {
            QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint(
                authority.clone(),
                schema_identity,
                visibility,
                query,
                normalized_predicate_fingerprint,
            )
        });

        self.resolve_shared_query_plan_for_authority_recording(
            &authority,
            cache_key,
            recorder,
            || {
                let plan = query.build_plan_with_visible_indexes_from_scalar_planning_state(
                    &visible_indexes,
                    planning_state,
                )?;

                SharedPreparedExecutionPlan::from_plan(
                    authority.clone(),
                    plan,
                    schema.fingerprint(),
                )
                .map_err(QueryError::execute)
            },
        )
    }

    fn try_cached_filterless_query_plan_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Option<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution)> {
        self.try_cached_filterless_query_plan_for_entity_path_recording(
            authority.entity_path(),
            schema_identity,
            visibility,
            query,
            recorder,
        )
    }

    fn try_cached_filterless_query_plan_for_entity_path_recording(
        &self,
        entity_path: &'static str,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Option<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution)> {
        if query.has_scalar_filter() {
            return None;
        }

        let cache_key = recorder.measure(QueryPlanCompilePhase::CacheKey, || {
            QueryPlanCacheKey::for_entity_path_with_normalized_predicate_fingerprint(
                entity_path,
                schema_identity,
                visibility,
                query,
                None,
            )
        });
        let (cached, entries) = recorder.measure(QueryPlanCompilePhase::CacheLookup, || {
            self.with_query_plan_cache(|cache| {
                let cached = cache.get(&cache_key).cloned();

                (cached, cache.len())
            })
        });
        record_cache_entries(CacheKind::SharedQueryPlan, entries);
        if let Some(prepared_plan) = cached {
            record_cache_event_for_path(CacheKind::SharedQueryPlan, CacheOutcome::Hit, entity_path);
            return Some((prepared_plan, QueryPlanCacheAttribution::hit()));
        }

        None
    }

    fn cached_trivial_scalar_load_plan_for_authority_recording(
        &self,
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        schema_info: SchemaInfo,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let cache_key = recorder.measure(QueryPlanCompilePhase::CacheKey, || {
            QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint(
                authority.clone(),
                schema_identity,
                visibility,
                query,
                None,
            )
        });

        self.resolve_shared_query_plan_for_authority_recording(
            &authority,
            cache_key,
            recorder,
            || {
                let Some(plan) =
                    query.try_build_trivial_scalar_load_plan_with_schema_info(schema_info)?
                else {
                    return Err(QueryError::invariant());
                };

                SharedPreparedExecutionPlan::from_plan(
                    authority.clone(),
                    plan,
                    schema_identity.fingerprint(),
                )
                .map_err(QueryError::execute)
            },
        )
    }
}
