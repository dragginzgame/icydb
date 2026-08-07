//! Module: db::session::query::cache
//! Responsibility: session-owned shared query-plan cache and planner-visibility handoff.
//! Does not own: query planning semantics, execution, or cache-key fingerprint generation.
//! Boundary: resolves store visibility and memoizes prepared plans for typed and SQL callers.

mod identity;
mod template;

#[cfg(feature = "sql")]
use crate::db::TraceReuseEvent;
use crate::db::commit::CommitSchemaFingerprint;
use crate::{
    db::{
        DbSession, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            budget::{HardExecutionContext, direct_read_execution_context},
        },
        predicate::predicate_fingerprint_normalized,
        query::{
            intent::StructuralQuery,
            plan::{PreparedQueryParameterContract, VisibleIndexes},
        },
        schema::{
            AcceptedSchemaSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
            SchemaInfo,
        },
        session::{AcceptedSchemaCatalogContext, bounded_cache::BoundedCache},
    },
    error::InternalError,
    metrics::sink::{
        CacheKind, CacheMissReason, CacheOutcome, record_cache_entries,
        record_cache_event_for_path, record_cache_miss_reason_for_path,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionLane};
use std::{cell::RefCell, collections::HashMap};

#[cfg(feature = "diagnostics")]
pub(in crate::db) use identity::QueryPlanCompilePhaseAttribution;
use identity::{
    QueryPlanAcceptedSchema, QueryPlanCacheKey, QueryPlanCompilePhase,
    QueryPlanCompilePhaseRecorder, SchemaCacheIdentity,
};
pub(in crate::db) use identity::{QueryPlanCacheAttribution, QueryPlanVisibility};
use template::PreparedQueryTemplate;

const SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES: usize = 1024;
const SHARED_QUERY_TEMPLATE_CACHE_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;
const REQUEST_PLANNING_SHAPE_DOMAIN: u64 = 0x2210_0006_0000_0001;

type QueryPlanCache = BoundedCache<QueryPlanCacheKey, CachedQueryArtifact>;

#[derive(Clone, Debug)]
enum CachedQueryArtifact {
    PreparedPlan(SharedPreparedExecutionPlan),
    ParameterizedTemplate(PreparedQueryTemplate),
}

impl CachedQueryArtifact {
    const fn prepared_plan(&self) -> Option<&SharedPreparedExecutionPlan> {
        match self {
            Self::PreparedPlan(plan) => Some(plan),
            Self::ParameterizedTemplate(_) => None,
        }
    }

    const fn parameterized_template(&self) -> Option<&PreparedQueryTemplate> {
        match self {
            Self::PreparedPlan(_) => None,
            Self::ParameterizedTemplate(template) => Some(template),
        }
    }

    const fn parameterized_template_mut(&mut self) -> Option<&mut PreparedQueryTemplate> {
        match self {
            Self::PreparedPlan(_) => None,
            Self::ParameterizedTemplate(template) => Some(template),
        }
    }
}

// Classify one shared query-plan cache miss by comparing the missed key against
// already-warmed plans. The buckets mirror the identity dimensions that can
// drift independently while keeping query structure and schema hashes private.
fn shared_query_plan_cache_miss_reason<V>(
    cache: &BoundedCache<QueryPlanCacheKey, V>,
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

#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn shared_query_plan_cache_len_for_tests(cache_scope_id: usize) -> usize {
    QUERY_PLAN_CACHES.with(|caches| {
        caches
            .borrow()
            .get(&cache_scope_id)
            .map_or(0, BoundedCache::len)
    })
}

#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn shared_query_template_cache_len_for_tests(cache_scope_id: usize) -> usize {
    QUERY_PLAN_CACHES.with(|caches| {
        caches.borrow().get(&cache_scope_id).map_or(0, |cache| {
            cache
                .values()
                .filter(|artifact| artifact.parameterized_template().is_some())
                .count()
        })
    })
}

#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
pub(in crate::db) const fn shared_query_template_cache_entry_upper_bound_for_tests() -> usize {
    SHARED_QUERY_TEMPLATE_CACHE_MAX_RETAINED_BYTES
        / PreparedQueryTemplate::estimated_retained_bytes()
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
#[cfg(feature = "sql")]
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
    fn charge_request_planning_resource(
        &self,
        context: HardExecutionContext,
        resource: DiagnosticExecutionBudgetResource,
    ) -> Result<(), QueryError> {
        self.db
            .request_execution_scope()
            .charge(context, resource, 1)
            .map_err(InternalError::from)
            .map_err(QueryError::execute)
    }

    fn with_query_plan_cache<R>(&self, f: impl FnOnce(&mut QueryPlanCache) -> R) -> R {
        let scope_id = self.db.cache_scope_id();

        QUERY_PLAN_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches.entry(scope_id).or_insert_with(|| {
                QueryPlanCache::new_weighted(
                    SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES,
                    SHARED_QUERY_TEMPLATE_CACHE_MAX_RETAINED_BYTES,
                )
            });

            f(cache)
        })
    }

    fn lookup_shared_query_template_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        cache_key: &QueryPlanCacheKey,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> (Option<PreparedQueryTemplate>, Option<CacheMissReason>) {
        recorder.measure(QueryPlanCompilePhase::CacheLookup, || {
            let (cached, entries, miss_reason) = self.with_query_plan_cache(|cache| {
                let cached = cache
                    .get(cache_key)
                    .and_then(CachedQueryArtifact::parameterized_template)
                    .cloned();
                let miss_reason = cached
                    .is_none()
                    .then(|| shared_query_plan_cache_miss_reason(cache, cache_key));

                (cached, cache.len(), miss_reason)
            });
            record_cache_entries(CacheKind::SharedQueryPlan, entries);
            if cached.is_some() {
                record_cache_event_for_path(
                    CacheKind::SharedQueryPlan,
                    CacheOutcome::Hit,
                    authority.entity_path(),
                );
            }

            (cached, miss_reason)
        })
    }

    fn insert_shared_query_template_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        cache_key: QueryPlanCacheKey,
        template: PreparedQueryTemplate,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> QueryPlanCacheAttribution {
        let weight = cache_key
            .estimated_retained_bytes()
            .saturating_add(PreparedQueryTemplate::estimated_retained_bytes());
        let (outcome, entries) = recorder.measure(QueryPlanCompilePhase::CacheInsert, || {
            self.with_query_plan_cache(|cache| {
                let outcome = cache.insert_weighted(
                    cache_key,
                    CachedQueryArtifact::ParameterizedTemplate(template),
                    weight,
                );
                (outcome, cache.len())
            })
        });
        record_cache_entries(CacheKind::SharedQueryPlan, entries);
        if !outcome.rejected_oversize {
            record_cache_event_for_path(
                CacheKind::SharedQueryPlan,
                CacheOutcome::Insert,
                authority.entity_path(),
            );
        }

        QueryPlanCacheAttribution::miss()
            .with_template_insert(outcome.evicted, outcome.rejected_oversize)
    }

    fn remember_shared_query_template_bound_plan_recording(
        &self,
        cache_key: &QueryPlanCacheKey,
        predicate_fingerprint: [u8; 32],
        prepared_plan: SharedPreparedExecutionPlan,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(), QueryError> {
        recorder.measure(QueryPlanCompilePhase::CacheInsert, || {
            self.with_query_plan_cache(|cache| {
                let template = cache
                    .get_mut(cache_key)
                    .and_then(CachedQueryArtifact::parameterized_template_mut)
                    .ok_or_else(QueryError::invariant)?;
                template.remember_bound_plan(predicate_fingerprint, prepared_plan);
                Ok(())
            })
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
                let cached = cache
                    .get(cache_key)
                    .and_then(CachedQueryArtifact::prepared_plan)
                    .cloned();
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
                cache.insert(
                    cache_key,
                    CachedQueryArtifact::PreparedPlan(prepared_plan.clone()),
                );
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
        planning_context: HardExecutionContext,
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

        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanCompilations,
        )?;

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
            QueryPlanVisibility::PrimaryOnly => VisibleIndexes::accepted_schema_primary_only(),
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

    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema = QueryPlanAcceptedSchema::from_accepted_schema_with_fingerprint(
            accepted_schema,
            schema_fingerprint,
            authority.accepted_runtime_root_identity(),
            authority
                .accepted_schema_authority()
                .map_err(QueryError::execute)?
                .revision(),
        );
        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query, lane,
        )
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
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
            authority.accepted_runtime_root_identity(),
            authority
                .accepted_schema_authority()
                .map_err(QueryError::execute)?
                .revision(),
        );

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query, lane,
        )
    }

    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority_with_catalog(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema = QueryPlanAcceptedSchema::from_catalog(catalog);

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query, lane,
        )
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
        &self,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
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
                lane,
                &mut recorder,
            )?;
        if self.db.request_execution_scope().diagnostics_enabled() {
            self.db.request_execution_scope().record_query_plan(
                crate::db::executor::request_query_plan_evidence(&prepared_plan),
                cache_attribution,
            );
        }

        Ok((prepared_plan, cache_attribution, compile_attribution))
    }

    fn cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
        &self,
        authority: EntityAuthority,
        schema: QueryPlanAcceptedSchema<'_>,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let mut recorder = QueryPlanCompilePhaseRecorder::none();

        let prepared = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility_recording(
                authority,
                schema,
                visibility,
                query,
                lane,
                &mut recorder,
            )?;
        #[cfg(feature = "diagnostics")]
        if self.db.request_execution_scope().diagnostics_enabled() {
            self.db.request_execution_scope().record_query_plan(
                crate::db::executor::request_query_plan_evidence(&prepared.0),
                prepared.1,
            );
        }
        Ok(prepared)
    }

    fn cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility_recording(
        &self,
        authority: EntityAuthority,
        schema: QueryPlanAcceptedSchema<'_>,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let planning_context =
            direct_read_execution_context(&authority, lane, REQUEST_PLANNING_SHAPE_DOMAIN);
        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanningSteps,
        )?;
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
                planning_context,
                recorder,
            );
        }

        let planning_state = recorder.measure(QueryPlanCompilePhase::Prepare, || {
            query.prepare_scalar_planning_state_with_schema_info(schema_info)
        })?;
        let parameter_contract = recorder.measure(QueryPlanCompilePhase::Prepare, || {
            query
                .filter_predicate_fully_covers_expression()
                .then(|| planning_state.normalized_predicate())
                .flatten()
                .and_then(PreparedQueryParameterContract::from_normalized_predicate)
        });
        if let Some(parameter_contract) = parameter_contract {
            let bound_predicate_fingerprint = recorder
                .measure(QueryPlanCompilePhase::Prepare, || {
                    planning_state
                        .normalized_predicate()
                        .map(predicate_fingerprint_normalized)
                })
                .ok_or_else(QueryError::invariant)?;

            return self.resolve_parameterized_query_plan_for_authority_recording(
                &authority,
                schema,
                schema_identity,
                visibility,
                query,
                planning_state,
                parameter_contract,
                bound_predicate_fingerprint,
                planning_context,
                recorder,
            );
        }
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
        let visible_indexes = recorder.measure(QueryPlanCompilePhase::SchemaInfo, || {
            Self::visible_indexes_for_accepted_schema(planning_state.schema_info(), visibility)
        });

        self.resolve_shared_query_plan_for_authority_recording(
            &authority,
            cache_key,
            planning_context,
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

    #[expect(
        clippy::too_many_arguments,
        reason = "parameterized cache binding keeps schema, visibility, and current values explicit"
    )]
    fn resolve_parameterized_query_plan_for_authority_recording(
        &self,
        authority: &EntityAuthority,
        schema: QueryPlanAcceptedSchema<'_>,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        planning_state: crate::db::query::plan::PreparedScalarPlanningState<'_>,
        parameter_contract: PreparedQueryParameterContract,
        bound_predicate_fingerprint: [u8; 32],
        planning_context: HardExecutionContext,
        recorder: &mut QueryPlanCompilePhaseRecorder<'_>,
    ) -> Result<(SharedPreparedExecutionPlan, QueryPlanCacheAttribution), QueryError> {
        let cache_key = recorder.measure(QueryPlanCompilePhase::CacheKey, || {
            QueryPlanCacheKey::for_authority_with_parameter_contract(
                authority.clone(),
                schema_identity,
                visibility,
                query,
                parameter_contract.clone(),
            )
        });
        let (cached_template, miss_reason) = self
            .lookup_shared_query_template_for_authority_recording(authority, &cache_key, recorder);
        if let Some(template) = cached_template {
            if let Some(prepared_plan) = template.reused_bound_plan(bound_predicate_fingerprint) {
                return Ok((prepared_plan, QueryPlanCacheAttribution::hit()));
            }
            let bound = recorder.measure(QueryPlanCompilePhase::PlanBuild, || {
                template.bind(query, planning_state)
            })?;
            let prepared_plan = SharedPreparedExecutionPlan::from_plan(
                authority.clone(),
                bound,
                schema.fingerprint(),
            )
            .map_err(QueryError::execute)?;
            self.remember_shared_query_template_bound_plan_recording(
                &cache_key,
                bound_predicate_fingerprint,
                prepared_plan.clone(),
                recorder,
            )?;

            return Ok((prepared_plan, QueryPlanCacheAttribution::hit()));
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

        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanCompilations,
        )?;

        let visible_indexes = recorder.measure(QueryPlanCompilePhase::SchemaInfo, || {
            Self::visible_indexes_for_accepted_schema(planning_state.schema_info(), visibility)
        });
        let plan = recorder.measure(QueryPlanCompilePhase::PlanBuild, || {
            query.build_plan_with_visible_indexes_from_scalar_planning_state(
                &visible_indexes,
                planning_state,
            )
        })?;
        let mut template = PreparedQueryTemplate::from_plan(&plan);
        let prepared_plan =
            SharedPreparedExecutionPlan::from_plan(authority.clone(), plan, schema.fingerprint())
                .map_err(QueryError::execute)?;
        template.remember_bound_plan(bound_predicate_fingerprint, prepared_plan.clone());
        let attribution = self.insert_shared_query_template_for_authority_recording(
            authority, cache_key, template, recorder,
        );

        Ok((prepared_plan, attribution))
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
        entity_path: &str,
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
                let cached = cache
                    .get(&cache_key)
                    .and_then(CachedQueryArtifact::prepared_plan)
                    .cloned();

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

    #[expect(
        clippy::too_many_arguments,
        reason = "accepted planning authority and request-budget attribution stay explicit"
    )]
    fn cached_trivial_scalar_load_plan_for_authority_recording(
        &self,
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        schema_info: SchemaInfo,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        planning_context: HardExecutionContext,
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
            planning_context,
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
