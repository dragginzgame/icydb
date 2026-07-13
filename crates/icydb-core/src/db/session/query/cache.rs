//! Module: db::session::query::cache
//! Responsibility: session-owned shared query-plan cache and planner-visibility handoff.
//! Does not own: query planning semantics, execution, or cache-key fingerprint generation.
//! Boundary: resolves store visibility and memoizes prepared plans for typed and SQL callers.

mod identity;

#[cfg(any(test, feature = "sql"))]
use crate::db::commit::CommitSchemaFingerprint;
#[cfg(test)]
use crate::db::schema::SchemaVersion;
use crate::{
    db::{
        DbSession, Query, QueryError, TraceReuseArtifactClass, TraceReuseEvent,
        executor::{EntityAuthority, PreparedExecutionPlan, SharedPreparedExecutionPlan},
        predicate::predicate_fingerprint_normalized,
        query::{intent::StructuralQuery, plan::VisibleIndexes},
        schema::{
            AcceptedSchemaSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
            SchemaInfo,
        },
        session::{AcceptedSchemaCatalogContext, bounded_cache::BoundedCache},
    },
    entity::EntityKind,
    metrics::sink::{
        CacheKind, CacheMissReason, CacheOutcome, record_cache_entries,
        record_cache_event_for_path, record_cache_miss_reason_for_path,
    },
    traits::{CanisterKind, Path},
};
#[cfg(test)]
use std::cell::Cell;
use std::{cell::RefCell, collections::HashMap};

#[cfg(feature = "diagnostics")]
pub(in crate::db) use identity::QueryPlanCompilePhaseAttribution;
use identity::{
    QueryPlanAcceptedSchema, QueryPlanCacheKey, QueryPlanCompilePhase,
    QueryPlanCompilePhaseRecorder, SHARED_QUERY_PLAN_CACHE_METHOD_VERSION, SchemaCacheIdentity,
};
pub(in crate::db) use identity::{QueryPlanCacheAttribution, QueryPlanVisibility};

const SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES: usize = 1024;

#[cfg(test)]
thread_local! {
    static VISIBLE_INDEX_PROJECTIONS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_visible_index_projection_count_for_tests() {
    VISIBLE_INDEX_PROJECTIONS.with(|projections| projections.set(0));
}

#[cfg(test)]
pub(in crate::db) fn visible_index_projection_count_for_tests() -> u64 {
    VISIBLE_INDEX_PROJECTIONS.with(Cell::get)
}

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

        let same_method_version = candidate.cache_method_version() == key.cache_method_version();
        let same_schema_version = candidate
            .schema_identity()
            .same_version(key.schema_identity());
        let same_schema_fingerprint = candidate
            .schema_identity()
            .same_fingerprint(key.schema_identity());
        let same_visibility = candidate.visibility() == key.visibility();

        if same_schema_version && same_schema_fingerprint && same_visibility && !same_method_version
        {
            return CacheMissReason::MethodVersion;
        }

        schema_version_mismatch |= same_schema_fingerprint
            && same_visibility
            && same_method_version
            && !same_schema_version;
        schema_fingerprint_mismatch |=
            same_visibility && same_method_version && !same_schema_fingerprint;
        visibility_mismatch |= same_schema_version
            && same_schema_fingerprint
            && same_method_version
            && !same_visibility;
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
) -> SchemaInfo {
    if let Some(schema_info) = authority.accepted_schema_info()
        && (!accepted_schema_has_expression_indexes(accepted_schema)
            || !schema_info.expression_indexes().is_empty())
    {
        return schema_info.clone();
    }

    SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(
        authority.model(),
        accepted_schema,
        true,
    )
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
    ) -> VisibleIndexes<'static> {
        #[cfg(test)]
        VISIBLE_INDEX_PROJECTIONS
            .with(|projections| projections.set(projections.get().saturating_add(1)));

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
        });
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
            QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint_and_method_version(
                authority.clone(),
                schema_identity,
                visibility,
                query,
                normalized_predicate_fingerprint,
                SHARED_QUERY_PLAN_CACHE_METHOD_VERSION,
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
            QueryPlanCacheKey::for_entity_path_with_normalized_predicate_fingerprint_and_method_version(
                entity_path,
                schema_identity,
                visibility,
                query,
                None,
                SHARED_QUERY_PLAN_CACHE_METHOD_VERSION,
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
            QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint_and_method_version(
                authority.clone(),
                schema_identity,
                visibility,
                query,
                None,
                SHARED_QUERY_PLAN_CACHE_METHOD_VERSION,
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

    #[cfg(test)]
    pub(in crate::db) fn query_plan_cache_key_for_tests(
        authority: EntityAuthority,
        schema_version: SchemaVersion,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        cache_method_version: u8,
    ) -> QueryPlanCacheKey {
        let schema_identity = SchemaCacheIdentity::new(
            crate::db::schema::AcceptedSchemaRevision::NONE,
            schema_version,
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            schema_fingerprint,
        );
        QueryPlanCacheKey::for_authority_with_method_version(
            authority,
            schema_identity,
            visibility,
            query,
            cache_method_version,
        )
    }

    #[cfg(test)]
    pub(in crate::db) fn query_plan_cache_key_for_tests_with_schema_fingerprint_method_version(
        authority: EntityAuthority,
        schema_version: SchemaVersion,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        cache_method_version: u8,
    ) -> QueryPlanCacheKey {
        let schema_identity = SchemaCacheIdentity::new(
            crate::db::schema::AcceptedSchemaRevision::NONE,
            schema_version,
            schema_fingerprint_method_version,
            schema_fingerprint,
        );
        QueryPlanCacheKey::for_authority_with_method_version(
            authority,
            schema_identity,
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
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = catalog.accepted_schema_info_for::<E>();
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

        Ok((
            prepared_plan
                .typed_clone::<E>()
                .map_err(QueryError::execute)?,
            attribution,
        ))
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db::session) fn cached_prepared_query_plan_for_entity_with_compile_phase_attribution<
        E,
    >(
        &self,
        query: &Query<E>,
    ) -> Result<
        (
            PreparedExecutionPlan<E>,
            QueryPlanCacheAttribution,
            QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, cache_attribution, compile_attribution) =
            self.cached_shared_query_plan_for_entity_with_compile_phase_attribution::<E>(query)?;

        Ok((
            prepared_plan
                .typed_clone::<E>()
                .map_err(QueryError::execute)?,
            cache_attribution,
            compile_attribution,
        ))
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
        let mut recorder = QueryPlanCompilePhaseRecorder::none();

        self.cached_shared_query_plan_for_entity_recording(query, &mut recorder)
    }

    #[cfg(feature = "diagnostics")]
    fn cached_shared_query_plan_for_entity_with_compile_phase_attribution<E>(
        &self,
        query: &Query<E>,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            QueryPlanCacheAttribution,
            QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: EntityKind<Canister = C>,
    {
        let mut compile_attribution = QueryPlanCompilePhaseAttribution::default();
        let mut recorder = QueryPlanCompilePhaseRecorder::new(&mut compile_attribution);
        let (plan, cache_attribution) =
            self.cached_shared_query_plan_for_entity_recording(query, &mut recorder)?;

        Ok((plan, cache_attribution, compile_attribution))
    }

    fn cached_shared_query_plan_for_entity_recording<E>(
        &self,
        query: &Query<E>,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        if !query.structural().has_scalar_filter() {
            let visibility = recorder.measure(QueryPlanCompilePhase::SchemaCatalog, || {
                self.query_plan_visibility_for_store_path(E::Store::PATH)
            })?;
            if let Some(selection) = recorder
                .measure(QueryPlanCompilePhase::SchemaCatalog, || {
                    self.accepted_catalog_snapshot_selection_for_query::<E>()
                })
                .map_err(QueryError::execute)?
            {
                let identity = selection.identity();
                debug_assert_eq!(identity.entity_tag(), E::ENTITY_TAG);
                debug_assert_eq!(identity.entity_path(), E::PATH);
                debug_assert_eq!(identity.store_path(), E::Store::PATH);
                debug_assert_eq!(
                    identity.fingerprint_method_version(),
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                );
                let schema_identity = SchemaCacheIdentity::from_accepted_catalog_identity(identity);
                if let Some(cached) = self
                    .try_cached_filterless_query_plan_for_entity_path_recording(
                        E::PATH,
                        schema_identity,
                        visibility,
                        query.structural(),
                        recorder,
                    )
                {
                    return Ok(cached);
                }

                if let Some(catalog) = recorder
                    .measure(QueryPlanCompilePhase::SchemaCatalog, || {
                        self.accepted_schema_catalog_context_from_selection::<E>(&selection)
                    })
                    .map_err(QueryError::execute)?
                {
                    return self
                        .cached_shared_query_plan_for_entity_with_catalog_and_visibility_recording(
                            query, &catalog, visibility, recorder,
                        );
                }
            }
        }

        let catalog = recorder
            .measure(QueryPlanCompilePhase::SchemaCatalog, || {
                self.accepted_schema_catalog_context_for_query::<E>()
            })
            .map_err(QueryError::execute)?;

        self.cached_shared_query_plan_for_entity_with_catalog_recording(query, &catalog, recorder)
    }

    fn cached_shared_query_plan_for_entity_with_catalog_recording<E>(
        &self,
        query: &Query<E>,
        catalog: &AcceptedSchemaCatalogContext,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visibility = recorder.measure(QueryPlanCompilePhase::SchemaCatalog, || {
            self.query_plan_visibility_for_store_path(E::Store::PATH)
        })?;

        self.cached_shared_query_plan_for_entity_with_catalog_and_visibility_recording(
            query, catalog, visibility, recorder,
        )
    }

    fn cached_shared_query_plan_for_entity_with_catalog_and_visibility_recording<E>(
        &self,
        query: &Query<E>,
        catalog: &AcceptedSchemaCatalogContext,
        visibility: QueryPlanVisibility,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let schema = QueryPlanAcceptedSchema::from_catalog(catalog);
        let schema_identity = schema.identity();
        if let Some(cached) = self.try_cached_filterless_query_plan_for_entity_path_recording(
            E::PATH,
            schema_identity,
            visibility,
            query.structural(),
            recorder,
        ) {
            return Ok(cached);
        }
        let authority = recorder
            .measure(QueryPlanCompilePhase::SchemaCatalog, || {
                catalog.accepted_entity_authority_for::<E>()
            })
            .map_err(QueryError::execute)?;

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility_recording(
            authority,
            schema,
            visibility,
            query.structural(),
            recorder,
        )
    }
}
