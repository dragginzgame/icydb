//! Module: db::session::query::cache
//! Responsibility: session-owned shared query-plan cache and planner-visibility handoff.
//! Does not own: query planning semantics, execution, or cache-key fingerprint generation.
//! Boundary: resolves store visibility and memoizes prepared plans for typed and SQL callers.

mod identity;
mod template;

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
            plan::{CardinalityTiebreakRoutePin, PreparedQueryParameterContract, VisibleIndexes},
        },
        schema::{
            AcceptedSchemaSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
            SchemaInfo,
        },
        session::{AcceptedSchemaCatalogContext, bounded_cache::BoundedCache},
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionLane};
use std::{cell::RefCell, collections::HashMap};

pub(in crate::db) use identity::QueryPlanVisibility;
use identity::{QueryPlanAcceptedSchema, QueryPlanCacheKey, SchemaCacheIdentity};
use template::PreparedQueryTemplate;

const SHARED_QUERY_PLAN_CACHE_MAX_ENTRIES: usize = 1024;
const SHARED_QUERY_TEMPLATE_CACHE_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;
const REQUEST_PLANNING_SHAPE_DOMAIN: u64 = 0x2210_0006_0000_0001;

type QueryPlanCache = BoundedCache<QueryPlanCacheKey, CachedQueryArtifact>;
type CachedPreparedPlanLookup = Option<(SharedPreparedExecutionPlan, TraceReuseEvent)>;

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

// A compiled front-end artifact may retain an exact or policy-fallback plan,
// but an unavailable-evidence plan must keep flowing through this shared cache
// so the existing lifecycle-stamp check can observe a later Ready transition.
#[cfg(feature = "sql")]
pub(in crate::db::session) fn query_plan_requires_cardinality_lifecycle_recheck(
    prepared_plan: &SharedPreparedExecutionPlan,
) -> bool {
    prepared_plan
        .logical_plan()
        .cardinality_tiebreak()
        .unavailable_stamp()
        .is_some()
}

impl<C: CanisterKind> DbSession<C> {
    fn cached_cardinality_tiebreak_is_current(
        &self,
        authority: &EntityAuthority,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) -> Result<bool, QueryError> {
        let Some(retained_stamp) = prepared_plan
            .logical_plan()
            .cardinality_tiebreak()
            .unavailable_stamp()
        else {
            return Ok(true);
        };
        let store = self
            .db
            .recovered_store(authority.store_path())
            .map_err(QueryError::execute)?;
        let current_stamp = store.exact_user_index_prefix_evidence_lifecycle_stamp();

        Ok(current_stamp == retained_stamp)
    }

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

    fn lookup_shared_query_template_for_authority(
        &self,
        _authority: &EntityAuthority,
        cache_key: &QueryPlanCacheKey,
    ) -> Option<PreparedQueryTemplate> {
        self.with_query_plan_cache(|cache| {
            cache
                .get(cache_key)
                .and_then(CachedQueryArtifact::parameterized_template)
                .cloned()
        })
    }

    fn insert_shared_query_template_for_authority(
        &self,
        _authority: &EntityAuthority,
        cache_key: QueryPlanCacheKey,
        template: PreparedQueryTemplate,
    ) {
        let weight = cache_key
            .estimated_retained_bytes()
            .saturating_add(template.estimated_retained_bytes());
        self.with_query_plan_cache(|cache| {
            cache.insert_weighted(
                cache_key,
                CachedQueryArtifact::ParameterizedTemplate(template),
                weight,
            );
        });
    }

    fn remember_shared_query_template_bound_plan(
        &self,
        cache_key: &QueryPlanCacheKey,
        predicate_fingerprint: [u8; 32],
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Result<(), QueryError> {
        self.with_query_plan_cache(|cache| {
            let template = cache
                .get_mut(cache_key)
                .and_then(CachedQueryArtifact::parameterized_template_mut)
                .ok_or_else(QueryError::invariant)?;
            template.remember_bound_plan(predicate_fingerprint, prepared_plan);
            Ok(())
        })
    }

    fn lookup_shared_query_plan_for_authority(
        &self,
        authority: &EntityAuthority,
        cache_key: &QueryPlanCacheKey,
    ) -> Result<CachedPreparedPlanLookup, QueryError> {
        let cached = self.with_query_plan_cache(|cache| {
            cache
                .get(cache_key)
                .and_then(CachedQueryArtifact::prepared_plan)
                .cloned()
        });
        if let Some(prepared_plan) = cached
            && self.cached_cardinality_tiebreak_is_current(authority, &prepared_plan)?
        {
            return Ok(Some((prepared_plan, TraceReuseEvent::Hit)));
        }

        Ok(None)
    }

    fn insert_shared_query_plan_for_authority(
        &self,
        _authority: &EntityAuthority,
        cache_key: QueryPlanCacheKey,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) {
        self.with_query_plan_cache(|cache| {
            cache.insert(
                cache_key,
                CachedQueryArtifact::PreparedPlan(prepared_plan.clone()),
            );
        });
    }

    fn resolve_shared_query_plan_for_authority(
        &self,
        authority: &EntityAuthority,
        cache_key: QueryPlanCacheKey,
        planning_context: HardExecutionContext,
        build_prepared_plan: impl FnOnce() -> Result<SharedPreparedExecutionPlan, QueryError>,
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
        let cached_plan = self.lookup_shared_query_plan_for_authority(authority, &cache_key)?;
        if let Some(cached_plan) = cached_plan {
            return Ok(cached_plan);
        }

        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanCompilations,
        )?;

        let prepared_plan = build_prepared_plan()?;
        self.insert_shared_query_plan_for_authority(authority, cache_key, &prepared_plan);

        Ok((prepared_plan, TraceReuseEvent::Miss))
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
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
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

    /// Compile one authenticated cardinality-selected continuation route without
    /// consulting current cardinality or replacing the ordinary initial-query cache entry.
    pub(in crate::db) fn shared_query_plan_for_accepted_authority_with_route_pin(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
        route_pin: CardinalityTiebreakRoutePin,
    ) -> Result<Option<(SharedPreparedExecutionPlan, TraceReuseEvent)>, QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let planning_context =
            direct_read_execution_context(&authority, lane, REQUEST_PLANNING_SHAPE_DOMAIN);
        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanningSteps,
        )?;
        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanCompilations,
        )?;
        let schema_info = schema_info_for_plan_cache_authority(&authority, accepted_schema)?;
        let planning_state = query.prepare_scalar_planning_state_with_schema_info(schema_info)?;
        let visible_indexes =
            Self::visible_indexes_for_accepted_schema(planning_state.schema_info(), visibility);
        let plan = query.build_plan_with_visible_indexes_from_scalar_planning_state(
            &visible_indexes,
            planning_state,
        )?;
        let Some(plan) = Self::apply_pinned_cardinality_tiebreak(
            &authority,
            visible_indexes.accepted_semantic_index_contracts(),
            plan,
            route_pin,
        )?
        else {
            return Ok(None);
        };
        let prepared_plan =
            SharedPreparedExecutionPlan::from_plan(authority, plan, schema_fingerprint)
                .map_err(QueryError::execute)?;

        Ok(Some((prepared_plan, TraceReuseEvent::Miss)))
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
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
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let schema = QueryPlanAcceptedSchema::from_catalog(catalog);

        self.cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
            authority, schema, visibility, query, lane,
        )
    }

    fn cached_shared_query_plan_for_accepted_authority_with_schema_and_visibility(
        &self,
        authority: EntityAuthority,
        schema: QueryPlanAcceptedSchema<'_>,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        lane: DiagnosticExecutionLane,
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
        let planning_context =
            direct_read_execution_context(&authority, lane, REQUEST_PLANNING_SHAPE_DOMAIN);
        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanningSteps,
        )?;
        let schema_identity = schema.identity();
        if let Some(cached) = self.try_cached_filterless_query_plan_for_authority(
            &authority,
            schema_identity,
            visibility,
            query,
        ) {
            return Ok(cached);
        }
        let schema_info =
            schema_info_for_plan_cache_authority(&authority, schema.accepted_schema())?;
        if query.trivial_scalar_load_fast_path_eligible_with_schema(&schema_info) {
            return self.cached_trivial_scalar_load_plan_for_authority(
                authority,
                schema_identity,
                schema_info,
                visibility,
                query,
                planning_context,
            );
        }

        let planning_state = query.prepare_scalar_planning_state_with_schema_info(schema_info)?;
        let parameter_contract = query
            .filter_predicate_fully_covers_expression()
            .then(|| planning_state.normalized_predicate())
            .flatten()
            .and_then(PreparedQueryParameterContract::from_normalized_predicate);
        if let Some(parameter_contract) = parameter_contract {
            let bound_predicate_fingerprint = planning_state
                .normalized_predicate()
                .map(predicate_fingerprint_normalized)
                .ok_or_else(QueryError::invariant)?;

            return self.resolve_parameterized_query_plan_for_authority(
                &authority,
                schema,
                schema_identity,
                visibility,
                query,
                planning_state,
                parameter_contract,
                bound_predicate_fingerprint,
                planning_context,
            );
        }
        let normalized_predicate_fingerprint = planning_state
            .normalized_predicate()
            .map(predicate_fingerprint_normalized);
        let cache_key = QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint(
            authority.clone(),
            schema_identity,
            visibility,
            query,
            normalized_predicate_fingerprint,
        );
        let visible_indexes =
            Self::visible_indexes_for_accepted_schema(planning_state.schema_info(), visibility);
        self.resolve_shared_query_plan_for_authority(
            &authority,
            cache_key,
            planning_context,
            || {
                let plan = query.build_plan_with_visible_indexes_from_scalar_planning_state(
                    &visible_indexes,
                    planning_state,
                )?;
                let plan = self.apply_exact_cardinality_tiebreak(
                    &authority,
                    visible_indexes.accepted_semantic_index_contracts(),
                    plan,
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
    fn resolve_parameterized_query_plan_for_authority(
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
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
        let cache_key = QueryPlanCacheKey::for_authority_with_parameter_contract(
            authority.clone(),
            schema_identity,
            visibility,
            query,
            parameter_contract,
        );
        let cached_template =
            self.lookup_shared_query_template_for_authority(authority, &cache_key);
        if let Some(template) = cached_template {
            if let Some(prepared_plan) = template.reused_bound_plan(bound_predicate_fingerprint)
                && self.cached_cardinality_tiebreak_is_current(authority, &prepared_plan)?
            {
                return Ok((prepared_plan, TraceReuseEvent::Hit));
            }
            let bound = template.bind(query, planning_state)?;
            let bound = self.apply_exact_cardinality_tiebreak(
                authority,
                template.candidate_indexes(),
                bound,
            )?;
            let prepared_plan = SharedPreparedExecutionPlan::from_plan(
                authority.clone(),
                bound,
                schema.fingerprint(),
            )
            .map_err(QueryError::execute)?;
            self.remember_shared_query_template_bound_plan(
                &cache_key,
                bound_predicate_fingerprint,
                prepared_plan.clone(),
            )?;

            return Ok((prepared_plan, TraceReuseEvent::Hit));
        }

        self.charge_request_planning_resource(
            planning_context,
            DiagnosticExecutionBudgetResource::PlanCompilations,
        )?;

        let visible_indexes =
            Self::visible_indexes_for_accepted_schema(planning_state.schema_info(), visibility);
        let plan = query.build_plan_with_visible_indexes_from_scalar_planning_state(
            &visible_indexes,
            planning_state,
        )?;
        let plan = self.apply_exact_cardinality_tiebreak(
            authority,
            visible_indexes.accepted_semantic_index_contracts(),
            plan,
        )?;
        let mut template =
            PreparedQueryTemplate::new(visible_indexes.accepted_semantic_index_contracts());
        let prepared_plan =
            SharedPreparedExecutionPlan::from_plan(authority.clone(), plan, schema.fingerprint())
                .map_err(QueryError::execute)?;
        template.remember_bound_plan(bound_predicate_fingerprint, prepared_plan.clone());
        self.insert_shared_query_template_for_authority(authority, cache_key, template);

        Ok((prepared_plan, TraceReuseEvent::Miss))
    }

    fn try_cached_filterless_query_plan_for_authority(
        &self,
        authority: &EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
    ) -> Option<(SharedPreparedExecutionPlan, TraceReuseEvent)> {
        self.try_cached_filterless_query_plan_for_entity_path(
            authority.entity_path(),
            schema_identity,
            visibility,
            query,
        )
    }

    fn try_cached_filterless_query_plan_for_entity_path(
        &self,
        entity_path: &str,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
    ) -> Option<(SharedPreparedExecutionPlan, TraceReuseEvent)> {
        if query.has_scalar_filter() {
            return None;
        }

        let cache_key = QueryPlanCacheKey::for_entity_path_with_normalized_predicate_fingerprint(
            entity_path,
            schema_identity,
            visibility,
            query,
            None,
        );
        let cached = self.with_query_plan_cache(|cache| {
            cache
                .get(&cache_key)
                .and_then(CachedQueryArtifact::prepared_plan)
                .cloned()
        });
        if let Some(prepared_plan) = cached {
            return Some((prepared_plan, TraceReuseEvent::Hit));
        }

        None
    }

    fn cached_trivial_scalar_load_plan_for_authority(
        &self,
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        schema_info: SchemaInfo,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        planning_context: HardExecutionContext,
    ) -> Result<(SharedPreparedExecutionPlan, TraceReuseEvent), QueryError> {
        let cache_key = QueryPlanCacheKey::for_authority_with_normalized_predicate_fingerprint(
            authority.clone(),
            schema_identity,
            visibility,
            query,
            None,
        );

        self.resolve_shared_query_plan_for_authority(
            &authority,
            cache_key,
            planning_context,
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
