//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    GroupedCountAttribution, GroupedExecutePhaseAttribution, ScalarExecutePhaseAttribution,
};
use crate::{
    db::{
        DbSession, EntityResponse, LoadQueryResult, PagedGroupedExecutionWithTrace,
        PagedLoadExecutionWithTrace, PersistedRow, Query, QueryError, QueryTracePlan,
        access::AccessStrategy,
        commit::CommitSchemaFingerprint,
        cursor::{
            CursorPlanError, decode_optional_cursor_token, decode_optional_grouped_cursor_token,
        },
        diagnostics::ExecutionTrace,
        executor::{
            ExecutionFamily, GroupedCursorPage, LoadExecutor, PreparedExecutionPlan,
            SharedPreparedExecutionPlan,
        },
        query::builder::{
            PreparedFluentAggregateExplainStrategy, PreparedFluentProjectionStrategy,
        },
        query::explain::{
            ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor, ExplainPlan,
        },
        query::{
            intent::{CompiledQuery, PlannedQuery, StructuralQuery},
            plan::{AccessPlannedQuery, QueryMode, VisibleIndexes},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
};
#[cfg(feature = "diagnostics")]
use candid::CandidType;
use icydb_utils::Xxh3;
#[cfg(feature = "diagnostics")]
use serde::Deserialize;
use std::{cell::RefCell, collections::HashMap, hash::BuildHasherDefault};

type CacheBuildHasher = BuildHasherDefault<Xxh3>;

// Bump this when the shared lower query-plan cache key meaning changes in a
// way that must force old in-heap entries to miss instead of aliasing.
const SHARED_QUERY_PLAN_CACHE_METHOD_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) enum QueryPlanVisibility {
    StoreNotReady,
    StoreReady,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct QueryPlanCacheKey {
    cache_method_version: u8,
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    visibility: QueryPlanVisibility,
    structural_query: crate::db::query::intent::StructuralQueryCacheKey,
}

#[derive(Clone, Debug)]
pub(in crate::db) struct QueryPlanCacheEntry {
    logical_plan: AccessPlannedQuery,
    prepared_plan: SharedPreparedExecutionPlan,
}

impl QueryPlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        logical_plan: AccessPlannedQuery,
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Self {
        Self {
            logical_plan,
            prepared_plan,
        }
    }

    #[must_use]
    pub(in crate::db) const fn logical_plan(&self) -> &AccessPlannedQuery {
        &self.logical_plan
    }

    #[must_use]
    pub(in crate::db) fn typed_prepared_plan<E: EntityKind>(&self) -> PreparedExecutionPlan<E> {
        self.prepared_plan.typed_clone::<E>()
    }

    #[must_use]
    pub(in crate::db) const fn prepared_plan(&self) -> &SharedPreparedExecutionPlan {
        &self.prepared_plan
    }
}

pub(in crate::db) type QueryPlanCache =
    HashMap<QueryPlanCacheKey, QueryPlanCacheEntry, CacheBuildHasher>;

thread_local! {
    // Keep one in-heap query-plan cache per store registry so fresh `DbSession`
    // facades can share prepared logical plans across update/query calls while
    // tests and multi-registry host processes remain isolated by registry
    // identity.
    static QUERY_PLAN_CACHES: RefCell<HashMap<usize, QueryPlanCache, CacheBuildHasher>> =
        RefCell::new(HashMap::default());
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct QueryPlanCacheAttribution {
    pub hits: u64,
    pub misses: u64,
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

///
/// QueryExecutionAttribution
///
/// QueryExecutionAttribution records the top-level compile/execute split for
/// typed/fluent query execution at the session boundary.
///
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct QueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub runtime_local_instructions: u64,
    pub finalize_local_instructions: u64,
    pub direct_data_row_scan_local_instructions: u64,
    pub direct_data_row_key_stream_local_instructions: u64,
    pub direct_data_row_row_read_local_instructions: u64,
    pub direct_data_row_key_encode_local_instructions: u64,
    pub direct_data_row_store_get_local_instructions: u64,
    pub direct_data_row_order_window_local_instructions: u64,
    pub direct_data_row_page_window_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count_borrowed_hash_computations: u64,
    pub grouped_count_bucket_candidate_checks: u64,
    pub grouped_count_existing_group_hits: u64,
    pub grouped_count_new_group_inserts: u64,
    pub grouped_count_row_materialization_local_instructions: u64,
    pub grouped_count_group_lookup_local_instructions: u64,
    pub grouped_count_existing_group_update_local_instructions: u64,
    pub grouped_count_new_group_insert_local_instructions: u64,
    pub response_decode_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct QueryExecutePhaseAttribution {
    runtime_local_instructions: u64,
    finalize_local_instructions: u64,
    direct_data_row_scan_local_instructions: u64,
    direct_data_row_key_stream_local_instructions: u64,
    direct_data_row_row_read_local_instructions: u64,
    direct_data_row_key_encode_local_instructions: u64,
    direct_data_row_store_get_local_instructions: u64,
    direct_data_row_order_window_local_instructions: u64,
    direct_data_row_page_window_local_instructions: u64,
    grouped_stream_local_instructions: u64,
    grouped_fold_local_instructions: u64,
    grouped_finalize_local_instructions: u64,
    grouped_count: GroupedCountAttribution,
}

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_query_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
fn measure_query_stage<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_query_local_instruction_counter();
    let result = run();
    let delta = read_query_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

impl<C: CanisterKind> DbSession<C> {
    #[cfg(feature = "diagnostics")]
    const fn empty_query_execute_phase_attribution() -> QueryExecutePhaseAttribution {
        QueryExecutePhaseAttribution {
            runtime_local_instructions: 0,
            finalize_local_instructions: 0,
            direct_data_row_scan_local_instructions: 0,
            direct_data_row_key_stream_local_instructions: 0,
            direct_data_row_row_read_local_instructions: 0,
            direct_data_row_key_encode_local_instructions: 0,
            direct_data_row_store_get_local_instructions: 0,
            direct_data_row_order_window_local_instructions: 0,
            direct_data_row_page_window_local_instructions: 0,
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: GroupedCountAttribution::none(),
        }
    }

    #[cfg(feature = "diagnostics")]
    const fn scalar_query_execute_phase_attribution(
        phase: ScalarExecutePhaseAttribution,
    ) -> QueryExecutePhaseAttribution {
        QueryExecutePhaseAttribution {
            runtime_local_instructions: phase.runtime_local_instructions,
            finalize_local_instructions: phase.finalize_local_instructions,
            direct_data_row_scan_local_instructions: phase.direct_data_row_scan_local_instructions,
            direct_data_row_key_stream_local_instructions: phase
                .direct_data_row_key_stream_local_instructions,
            direct_data_row_row_read_local_instructions: phase
                .direct_data_row_row_read_local_instructions,
            direct_data_row_key_encode_local_instructions: phase
                .direct_data_row_key_encode_local_instructions,
            direct_data_row_store_get_local_instructions: phase
                .direct_data_row_store_get_local_instructions,
            direct_data_row_order_window_local_instructions: phase
                .direct_data_row_order_window_local_instructions,
            direct_data_row_page_window_local_instructions: phase
                .direct_data_row_page_window_local_instructions,
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: GroupedCountAttribution::none(),
        }
    }

    #[cfg(feature = "diagnostics")]
    const fn grouped_query_execute_phase_attribution(
        phase: GroupedExecutePhaseAttribution,
    ) -> QueryExecutePhaseAttribution {
        QueryExecutePhaseAttribution {
            runtime_local_instructions: phase
                .stream_local_instructions
                .saturating_add(phase.fold_local_instructions),
            finalize_local_instructions: phase.finalize_local_instructions,
            direct_data_row_scan_local_instructions: 0,
            direct_data_row_key_stream_local_instructions: 0,
            direct_data_row_row_read_local_instructions: 0,
            direct_data_row_key_encode_local_instructions: 0,
            direct_data_row_store_get_local_instructions: 0,
            direct_data_row_order_window_local_instructions: 0,
            direct_data_row_page_window_local_instructions: 0,
            grouped_stream_local_instructions: phase.stream_local_instructions,
            grouped_fold_local_instructions: phase.fold_local_instructions,
            grouped_finalize_local_instructions: phase.finalize_local_instructions,
            grouped_count: phase.grouped_count,
        }
    }

    fn with_query_plan_cache<R>(&self, f: impl FnOnce(&mut QueryPlanCache) -> R) -> R {
        let scope_id = self.db.cache_scope_id();

        QUERY_PLAN_CACHES.with(|caches| {
            let mut caches = caches.borrow_mut();
            let cache = caches.entry(scope_id).or_default();

            f(cache)
        })
    }

    const fn visible_indexes_for_model(
        model: &'static EntityModel,
        visibility: QueryPlanVisibility,
    ) -> VisibleIndexes<'static> {
        match visibility {
            QueryPlanVisibility::StoreReady => VisibleIndexes::planner_visible(model.indexes()),
            QueryPlanVisibility::StoreNotReady => VisibleIndexes::none(),
        }
    }

    #[cfg(test)]
    pub(in crate::db) fn query_plan_cache_len(&self) -> usize {
        self.with_query_plan_cache(|cache| cache.len())
    }

    #[cfg(test)]
    pub(in crate::db) fn clear_query_plan_cache_for_tests(&self) {
        self.with_query_plan_cache(QueryPlanCache::clear);
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

    pub(in crate::db) fn cached_query_plan_entry_for_authority(
        &self,
        authority: crate::db::executor::EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        query: &StructuralQuery,
    ) -> Result<(QueryPlanCacheEntry, QueryPlanCacheAttribution), QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let visible_indexes = Self::visible_indexes_for_model(authority.model(), visibility);
        let normalized_predicate = query.prepare_normalized_scalar_predicate()?;
        let cache_key =
            QueryPlanCacheKey::for_authority_with_normalized_predicate_and_method_version(
                authority,
                schema_fingerprint,
                visibility,
                query,
                normalized_predicate.as_ref(),
                SHARED_QUERY_PLAN_CACHE_METHOD_VERSION,
            );

        {
            let cached = self.with_query_plan_cache(|cache| cache.get(&cache_key).cloned());
            if let Some(entry) = cached {
                return Ok((entry, QueryPlanCacheAttribution::hit()));
            }
        }

        let plan = query.build_plan_with_visible_indexes_from_normalized_predicate(
            &visible_indexes,
            normalized_predicate,
        )?;
        let entry = QueryPlanCacheEntry::new(
            plan.clone(),
            SharedPreparedExecutionPlan::from_plan(authority, plan),
        );
        self.with_query_plan_cache(|cache| {
            cache.insert(cache_key, entry.clone());
        });

        Ok((entry, QueryPlanCacheAttribution::miss()))
    }

    #[cfg(test)]
    pub(in crate::db) fn query_plan_cache_key_for_tests(
        authority: crate::db::executor::EntityAuthority,
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
    fn with_query_visible_indexes<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(
            &Query<E>,
            &crate::db::query::plan::VisibleIndexes<'static>,
        ) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let visibility = self.query_plan_visibility_for_store_path(E::Store::PATH)?;
        let visible_indexes = Self::visible_indexes_for_model(E::MODEL, visibility);

        op(query, &visible_indexes)
    }

    pub(in crate::db::session) fn cached_prepared_query_plan_for_entity<E>(
        &self,
        query: &StructuralQuery,
    ) -> Result<(PreparedExecutionPlan<E>, QueryPlanCacheAttribution), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (entry, attribution) = self.cached_query_plan_entry_for_authority(
            crate::db::executor::EntityAuthority::for_type::<E>(),
            crate::db::schema::commit_schema_fingerprint_for_entity::<E>(),
            query,
        )?;

        Ok((entry.typed_prepared_plan::<E>(), attribution))
    }

    // Compile one typed query using only the indexes currently visible for the
    // query's recovered store.
    pub(in crate::db) fn compile_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<CompiledQuery<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (entry, _) = self.cached_query_plan_entry_for_authority(
            crate::db::executor::EntityAuthority::for_type::<E>(),
            crate::db::schema::commit_schema_fingerprint_for_entity::<E>(),
            query.structural(),
        )?;

        Ok(Query::<E>::compiled_query_from_plan(
            entry.logical_plan().clone(),
        ))
    }

    // Build one logical planned-query shell using only the indexes currently
    // visible for the query's recovered store.
    pub(in crate::db) fn planned_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<PlannedQuery<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (entry, _) = self.cached_query_plan_entry_for_authority(
            crate::db::executor::EntityAuthority::for_type::<E>(),
            crate::db::schema::commit_schema_fingerprint_for_entity::<E>(),
            query.structural(),
        )?;

        Ok(Query::<E>::planned_query_from_plan(
            entry.logical_plan().clone(),
        ))
    }

    // Project one logical explain payload using only planner-visible indexes.
    pub(in crate::db) fn explain_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<ExplainPlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, Query::<E>::explain_with_visible_indexes)
    }

    // Hash one typed query plan using only the indexes currently visible for
    // the query's recovered store.
    pub(in crate::db) fn query_plan_hash_hex_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, Query::<E>::plan_hash_hex_with_visible_indexes)
    }

    // Explain one load execution shape using only planner-visible
    // indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, Query::<E>::explain_execution_with_visible_indexes)
    }

    // Render one load execution descriptor plus route diagnostics using
    // only planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_verbose_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(
            query,
            Query::<E>::explain_execution_verbose_with_visible_indexes,
        )
    }

    // Explain one prepared fluent aggregate terminal using only
    // planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_prepared_aggregate_terminal_with_visible_indexes<E, S>(
        &self,
        query: &Query<E>,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
        S: PreparedFluentAggregateExplainStrategy,
    {
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query
                .explain_prepared_aggregate_terminal_with_visible_indexes(visible_indexes, strategy)
        })
    }

    // Explain one `bytes_by(field)` terminal using only planner-visible
    // indexes from the recovered store state.
    pub(in crate::db) fn explain_query_bytes_by_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_bytes_by_with_visible_indexes(visible_indexes, target_field)
        })
    }

    // Explain one prepared fluent projection terminal using only
    // planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_prepared_projection_terminal_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
        strategy: &PreparedFluentProjectionStrategy,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_prepared_projection_terminal_with_visible_indexes(
                visible_indexes,
                strategy,
            )
        })
    }

    // Validate that one execution strategy is admissible for scalar paged load
    // execution and fail closed on grouped/primary-key-only routes.
    fn ensure_scalar_paged_execution_family(family: ExecutionFamily) -> Result<(), QueryError> {
        match family {
            ExecutionFamily::PrimaryKey => Err(QueryError::invariant(
                CursorPlanError::cursor_requires_explicit_or_grouped_ordering_message(),
            )),
            ExecutionFamily::Ordered => Ok(()),
            ExecutionFamily::Grouped => Err(QueryError::invariant(
                "grouped queries execute via execute(), not page().execute()",
            )),
        }
    }

    // Validate that one execution strategy is admissible for the grouped
    // execution surface.
    fn ensure_grouped_execution_family(family: ExecutionFamily) -> Result<(), QueryError> {
        match family {
            ExecutionFamily::Grouped => Ok(()),
            ExecutionFamily::PrimaryKey | ExecutionFamily::Ordered => Err(QueryError::invariant(
                "grouped execution requires grouped logical plans",
            )),
        }
    }

    // Finalize one grouped cursor page into the outward grouped execution
    // payload so grouped cursor encoding and continuation-shape validation
    // stay owned by the session boundary.
    fn finalize_grouped_execution_page(
        page: GroupedCursorPage,
        trace: Option<ExecutionTrace>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError> {
        let next_cursor = page
            .next_cursor
            .map(|token| {
                let Some(token) = token.as_grouped() else {
                    return Err(QueryError::grouped_paged_emitted_scalar_continuation());
                };

                token.encode().map_err(|err| {
                    QueryError::serialize_internal(format!(
                        "failed to serialize grouped continuation cursor: {err}"
                    ))
                })
            })
            .transpose()?;

        Ok(PagedGroupedExecutionWithTrace::new(
            page.rows,
            next_cursor,
            trace,
        ))
    }

    /// Execute one scalar load/delete query and return materialized response rows.
    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: compile typed intent into one prepared execution-plan contract.
        let mode = query.mode();
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query.structural())?;

        // Phase 2: delegate execution to the shared compiled-plan entry path.
        self.execute_query_dyn(mode, plan)
    }

    /// Execute one typed query while reporting the compile/execute split at
    /// the shared fluent query seam.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_query_result_with_attribution<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(LoadQueryResult<E>, QueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure compile work at the typed/fluent boundary,
        // including the shared lower query-plan cache lookup/build exactly
        // once. This preserves honest hit/miss attribution without
        // double-building plans on one-shot cache misses.
        let (compile_local_instructions, plan_and_cache) = measure_query_stage(|| {
            self.cached_prepared_query_plan_for_entity::<E>(query.structural())
        });
        let (plan, cache_attribution) = plan_and_cache?;

        // Phase 2: execute one query result using the prepared plan produced
        // by the compile/cache boundary above.
        let (execute_local_instructions, result) = measure_query_stage(
            || -> Result<(LoadQueryResult<E>, QueryExecutePhaseAttribution, u64), QueryError> {
                if query.has_grouping() {
                    let (page, trace, phase_attribution) =
                        self.execute_grouped_plan_with(plan, None, |executor, plan, cursor| {
                            executor
                                .execute_grouped_paged_with_cursor_traced_with_phase_attribution(
                                    plan, cursor,
                                )
                        })?;
                    let grouped = Self::finalize_grouped_execution_page(page, trace)?;

                    Ok((
                        LoadQueryResult::Grouped(grouped),
                        Self::grouped_query_execute_phase_attribution(phase_attribution),
                        0,
                    ))
                } else {
                    match query.mode() {
                        QueryMode::Load(_) => {
                            let (rows, phase_attribution, response_decode_local_instructions) =
                                self.load_executor::<E>()
                                    .execute_with_phase_attribution(plan)
                                    .map_err(QueryError::execute)?;

                            Ok((
                                LoadQueryResult::Rows(rows),
                                Self::scalar_query_execute_phase_attribution(phase_attribution),
                                response_decode_local_instructions,
                            ))
                        }
                        QueryMode::Delete(_) => {
                            let result = self.execute_query_dyn(query.mode(), plan)?;

                            Ok((
                                LoadQueryResult::Rows(result),
                                Self::empty_query_execute_phase_attribution(),
                                0,
                            ))
                        }
                    }
                }
            },
        );
        let (result, execute_phase_attribution, response_decode_local_instructions) = result?;
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);

        Ok((
            result,
            QueryExecutionAttribution {
                compile_local_instructions,
                runtime_local_instructions: execute_phase_attribution.runtime_local_instructions,
                finalize_local_instructions: execute_phase_attribution.finalize_local_instructions,
                direct_data_row_scan_local_instructions: execute_phase_attribution
                    .direct_data_row_scan_local_instructions,
                direct_data_row_key_stream_local_instructions: execute_phase_attribution
                    .direct_data_row_key_stream_local_instructions,
                direct_data_row_row_read_local_instructions: execute_phase_attribution
                    .direct_data_row_row_read_local_instructions,
                direct_data_row_key_encode_local_instructions: execute_phase_attribution
                    .direct_data_row_key_encode_local_instructions,
                direct_data_row_store_get_local_instructions: execute_phase_attribution
                    .direct_data_row_store_get_local_instructions,
                direct_data_row_order_window_local_instructions: execute_phase_attribution
                    .direct_data_row_order_window_local_instructions,
                direct_data_row_page_window_local_instructions: execute_phase_attribution
                    .direct_data_row_page_window_local_instructions,
                grouped_stream_local_instructions: execute_phase_attribution
                    .grouped_stream_local_instructions,
                grouped_fold_local_instructions: execute_phase_attribution
                    .grouped_fold_local_instructions,
                grouped_finalize_local_instructions: execute_phase_attribution
                    .grouped_finalize_local_instructions,
                grouped_count_borrowed_hash_computations: execute_phase_attribution
                    .grouped_count
                    .borrowed_hash_computations,
                grouped_count_bucket_candidate_checks: execute_phase_attribution
                    .grouped_count
                    .bucket_candidate_checks,
                grouped_count_existing_group_hits: execute_phase_attribution
                    .grouped_count
                    .existing_group_hits,
                grouped_count_new_group_inserts: execute_phase_attribution
                    .grouped_count
                    .new_group_inserts,
                grouped_count_row_materialization_local_instructions: execute_phase_attribution
                    .grouped_count
                    .row_materialization_local_instructions,
                grouped_count_group_lookup_local_instructions: execute_phase_attribution
                    .grouped_count
                    .group_lookup_local_instructions,
                grouped_count_existing_group_update_local_instructions: execute_phase_attribution
                    .grouped_count
                    .existing_group_update_local_instructions,
                grouped_count_new_group_insert_local_instructions: execute_phase_attribution
                    .grouped_count
                    .new_group_insert_local_instructions,
                response_decode_local_instructions,
                execute_local_instructions,
                total_local_instructions,
                shared_query_plan_cache_hits: cache_attribution.hits,
                shared_query_plan_cache_misses: cache_attribution.misses,
            },
        ))
    }

    // Execute one typed query through the unified row/grouped result surface so
    // higher layers do not need to branch on grouped shape themselves.
    #[doc(hidden)]
    pub fn execute_query_result<E>(
        &self,
        query: &Query<E>,
    ) -> Result<LoadQueryResult<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if query.has_grouping() {
            return self
                .execute_grouped(query, None)
                .map(LoadQueryResult::Grouped);
        }

        self.execute_query(query).map(LoadQueryResult::Rows)
    }

    /// Execute one typed delete query and return only the affected-row count.
    #[doc(hidden)]
    pub fn execute_delete_count<E>(&self, query: &Query<E>) -> Result<u32, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: fail closed if the caller routes a non-delete query here.
        if !query.mode().is_delete() {
            return Err(QueryError::unsupported_query(
                "delete count execution requires delete query mode",
            ));
        }

        // Phase 2: compile typed delete intent into one prepared execution-plan contract.
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_prepared_execution_plan();

        // Phase 3: execute the shared delete core while skipping response-row materialization.
        self.with_metrics(|| self.delete_executor::<E>().execute_count(plan))
            .map_err(QueryError::execute)
    }

    /// Execute one scalar query from one pre-built prepared execution contract.
    ///
    /// This is the shared compiled-plan entry boundary used by the typed
    /// `execute_query(...)` surface and adjacent query execution facades.
    pub(in crate::db) fn execute_query_dyn<E>(
        &self,
        mode: QueryMode,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let result = match mode {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::execute)
    }

    // Shared load-query terminal wrapper: build plan, run under metrics, map
    // execution errors into query-facing errors.
    pub(in crate::db) fn execute_load_query_with<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(LoadExecutor<E>, PreparedExecutionPlan<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query.structural())?;

        self.with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::execute)
    }

    /// Build one trace payload for a query without executing it.
    ///
    /// This lightweight surface is intended for developer diagnostics:
    /// plan hash, access strategy summary, and planner/executor route shape.
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let compiled = self.compile_query_with_visible_indexes(query)?;
        let explain = compiled.explain();
        let plan_hash = compiled.plan_hash_hex();

        let (executable, _) =
            self.cached_prepared_query_plan_for_entity::<E>(query.structural())?;
        let access_strategy = AccessStrategy::from_plan(executable.access()).debug_summary();
        let execution_family = match query.mode() {
            QueryMode::Load(_) => Some(executable.execution_family().map_err(QueryError::execute)?),
            QueryMode::Delete(_) => None,
        };

        Ok(QueryTracePlan::new(
            plan_hash,
            access_strategy,
            execution_family,
            explain,
        ))
    }

    /// Execute one scalar paged load query and return optional continuation cursor plus trace.
    pub(crate) fn execute_load_query_paged_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate prepared execution plan and reject grouped plans.
        let plan = self
            .cached_prepared_query_plan_for_entity::<E>(query.structural())?
            .0;
        Self::ensure_scalar_paged_execution_family(
            plan.execution_family().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external cursor token and validate it against plan surface.
        let cursor_bytes = decode_optional_cursor_token(cursor_token)
            .map_err(QueryError::from_cursor_plan_error)?;
        let cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .map_err(QueryError::from_executor_plan_error)?;

        // Phase 3: execute one traced page and encode outbound continuation token.
        let (page, trace) = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_paged_with_cursor_traced(plan, cursor)
            })
            .map_err(QueryError::execute)?;
        let next_cursor = page
            .next_cursor
            .map(|token| {
                let Some(token) = token.as_scalar() else {
                    return Err(QueryError::scalar_paged_emitted_grouped_continuation());
                };

                token.encode().map_err(|err| {
                    QueryError::serialize_internal(format!(
                        "failed to serialize continuation cursor: {err}"
                    ))
                })
            })
            .transpose()?;

        Ok(PagedLoadExecutionWithTrace::new(
            page.items,
            next_cursor,
            trace,
        ))
    }

    /// Execute one grouped query page with optional grouped continuation cursor.
    ///
    /// This is the explicit grouped execution boundary; scalar load APIs reject
    /// grouped plans to preserve scalar response contracts.
    pub(in crate::db) fn execute_grouped<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build the prepared execution plan once from the typed query.
        let plan = self
            .cached_prepared_query_plan_for_entity::<E>(query.structural())?
            .0;

        // Phase 2: reuse the shared prepared grouped execution path and then
        // finalize the outward grouped payload at the session boundary.
        let (page, trace) = self.execute_grouped_plan_with_trace(plan, cursor_token)?;

        Self::finalize_grouped_execution_page(page, trace)
    }

    // Execute one grouped prepared plan page with optional grouped cursor
    // while letting the caller choose the final grouped-runtime dispatch.
    fn execute_grouped_plan_with<E, T>(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor_token: Option<&str>,
        op: impl FnOnce(
            LoadExecutor<E>,
            PreparedExecutionPlan<E>,
            crate::db::cursor::GroupedPlannedCursor,
        ) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: validate the prepared plan shape before decoding cursors.
        Self::ensure_grouped_execution_family(
            plan.execution_family().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external grouped cursor token and validate against plan.
        let cursor = decode_optional_grouped_cursor_token(cursor_token)
            .map_err(QueryError::from_cursor_plan_error)?;
        let cursor = plan
            .prepare_grouped_cursor_token(cursor)
            .map_err(QueryError::from_executor_plan_error)?;

        // Phase 3: execute one grouped page while preserving the structural
        // grouped cursor payload for whichever outward cursor format the caller needs.
        self.with_metrics(|| op(self.load_executor::<E>(), plan, cursor))
            .map_err(QueryError::execute)
    }

    // Execute one grouped prepared plan page with optional grouped cursor.
    fn execute_grouped_plan_with_trace<E>(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor_token: Option<&str>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_grouped_plan_with(plan, cursor_token, |executor, plan, cursor| {
            executor.execute_grouped_paged_with_cursor_traced(plan, cursor)
        })
    }
}

impl QueryPlanCacheKey {
    // Assemble the canonical cache-key shell once so the test and
    // normalized-predicate constructors only decide which structural query key
    // they feed into the shared session cache identity.
    const fn from_authority_parts(
        authority: crate::db::executor::EntityAuthority,
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
        authority: crate::db::executor::EntityAuthority,
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

    fn for_authority_with_normalized_predicate_and_method_version(
        authority: crate::db::executor::EntityAuthority,
        schema_fingerprint: CommitSchemaFingerprint,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        normalized_predicate: Option<&crate::db::predicate::Predicate>,
        cache_method_version: u8,
    ) -> Self {
        Self::from_authority_parts(
            authority,
            schema_fingerprint,
            visibility,
            query.structural_cache_key_with_normalized_predicate(normalized_predicate),
            cache_method_version,
        )
    }
}
