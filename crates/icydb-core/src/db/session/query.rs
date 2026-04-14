//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

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
        executor::{ExecutionFamily, GroupedCursorPage, LoadExecutor, PreparedExecutionPlan},
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
#[cfg(feature = "perf-attribution")]
use candid::CandidType;
#[cfg(feature = "perf-attribution")]
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) enum QueryPlanVisibility {
    StoreNotReady,
    StoreReady,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct QueryPlanCacheKey {
    entity_path: &'static str,
    schema_fingerprint: CommitSchemaFingerprint,
    visibility: QueryPlanVisibility,
    query_fingerprint: [u8; 32],
}

pub(in crate::db) type QueryPlanCache = HashMap<QueryPlanCacheKey, AccessPlannedQuery>;

///
/// QueryExecutionAttribution
///
/// QueryExecutionAttribution records the top-level compile/execute split for
/// typed/fluent query execution at the session boundary.
///
#[cfg(feature = "perf-attribution")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct QueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
}

#[cfg(feature = "perf-attribution")]
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

#[cfg(feature = "perf-attribution")]
fn measure_query_stage<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_query_local_instruction_counter();
    let result = run();
    let delta = read_query_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

impl<C: CanisterKind> DbSession<C> {
    fn query_plan_cache(&self) -> &std::cell::RefCell<QueryPlanCache> {
        self.query_plan_cache
            .get_or_init(|| std::cell::RefCell::new(QueryPlanCache::new()))
    }

    fn query_plan_visibility_cache(
        &self,
    ) -> &std::cell::RefCell<HashMap<&'static str, QueryPlanVisibility>> {
        self.query_plan_visibility_cache
            .get_or_init(|| std::cell::RefCell::new(HashMap::new()))
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
        self.query_plan_cache().borrow().len()
    }

    pub(in crate::db) fn query_plan_visibility_for_store_path(
        &self,
        store_path: &'static str,
    ) -> Result<QueryPlanVisibility, QueryError> {
        if let Some(visibility) = self
            .query_plan_visibility_cache()
            .borrow()
            .get(store_path)
            .copied()
        {
            return Ok(visibility);
        }

        let store = self
            .db
            .recovered_store(store_path)
            .map_err(QueryError::execute)?;
        let visibility = if store.index_state() == crate::db::IndexState::Ready {
            QueryPlanVisibility::StoreReady
        } else {
            QueryPlanVisibility::StoreNotReady
        };
        self.query_plan_visibility_cache()
            .borrow_mut()
            .insert(store_path, visibility);

        Ok(visibility)
    }

    pub(in crate::db) fn cached_structural_plan_for_authority(
        &self,
        entity_path: &'static str,
        schema_fingerprint: CommitSchemaFingerprint,
        store_path: &'static str,
        model: &'static EntityModel,
        query: &StructuralQuery,
    ) -> Result<AccessPlannedQuery, QueryError> {
        let visibility = self.query_plan_visibility_for_store_path(store_path)?;
        let cache_key = QueryPlanCacheKey {
            entity_path,
            schema_fingerprint,
            visibility,
            query_fingerprint: query.cache_fingerprint(),
        };

        {
            let cache = self.query_plan_cache().borrow();
            if let Some(plan) = cache.get(&cache_key) {
                return Ok(plan.clone());
            }
        }

        let visible_indexes = Self::visible_indexes_for_model(model, visibility);
        let plan = query.build_plan_with_visible_indexes(&visible_indexes)?;
        self.query_plan_cache()
            .borrow_mut()
            .insert(cache_key, plan.clone());

        Ok(plan)
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

    // Resolve one typed structural query onto the shared lower plan cache so
    // typed/fluent callers do not each duplicate the entity metadata plumbing.
    fn cached_structural_plan_for_entity<E>(
        &self,
        query: &StructuralQuery,
    ) -> Result<AccessPlannedQuery, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        self.cached_structural_plan_for_authority(
            E::PATH,
            crate::db::schema::commit_schema_fingerprint_for_entity::<E>(),
            E::Store::PATH,
            E::MODEL,
            query,
        )
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
        let plan = self.cached_structural_plan_for_entity::<E>(query.structural())?;

        Ok(Query::<E>::compiled_query_from_plan(plan))
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
        let plan = self.cached_structural_plan_for_entity::<E>(query.structural())?;

        Ok(Query::<E>::planned_query_from_plan(plan))
    }

    // Project one logical explain payload using only planner-visible indexes.
    pub(in crate::db) fn explain_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<ExplainPlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_with_visible_indexes(visible_indexes)
        })
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
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.plan_hash_hex_with_visible_indexes(visible_indexes)
        })
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
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_execution_with_visible_indexes(visible_indexes)
        })
    }

    // Render one load execution descriptor as deterministic text using
    // only planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_text_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_execution_text_with_visible_indexes(visible_indexes)
        })
    }

    // Render one load execution descriptor as canonical JSON using
    // only planner-visible indexes from the recovered store state.
    pub(in crate::db) fn explain_query_execution_json_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<String, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_execution_json_with_visible_indexes(visible_indexes)
        })
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
        self.with_query_visible_indexes(query, |query, visible_indexes| {
            query.explain_execution_verbose_with_visible_indexes(visible_indexes)
        })
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

    /// Execute one scalar load/delete query and return materialized response rows.
    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: compile typed intent into one prepared execution-plan contract.
        let mode = query.mode();
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_prepared_execution_plan();

        // Phase 2: delegate execution to the shared compiled-plan entry path.
        self.execute_query_dyn(mode, plan)
    }

    /// Execute one typed query while reporting the compile/execute split at
    /// the shared fluent query seam.
    #[cfg(feature = "perf-attribution")]
    #[doc(hidden)]
    pub fn execute_query_result_with_attribution<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(LoadQueryResult<E>, QueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure compile work at the typed/fluent boundary.
        let (compile_local_instructions, compiled) =
            measure_query_stage(|| self.compile_query_with_visible_indexes(query));
        let compiled = compiled?;
        let plan = compiled.into_prepared_execution_plan();

        // Phase 2: measure execute work separately using the already compiled plan.
        let (execute_local_instructions, result) = measure_query_stage(|| {
            if query.has_grouping() {
                self.execute_grouped_plan_with_trace(plan, None)
                    .map(|(page, trace)| {
                        let next_cursor = page
                            .next_cursor
                            .map(|token| {
                                let Some(token) = token.as_grouped() else {
                                    return Err(
                                        QueryError::grouped_paged_emitted_scalar_continuation(),
                                    );
                                };

                                token.encode().map_err(|err| {
                                    QueryError::serialize_internal(format!(
                                        "failed to serialize grouped continuation cursor: {err}"
                                    ))
                                })
                            })
                            .transpose()?;

                        Ok::<LoadQueryResult<E>, QueryError>(LoadQueryResult::Grouped(
                            PagedGroupedExecutionWithTrace::new(page.rows, next_cursor, trace),
                        ))
                    })?
            } else {
                self.execute_query_dyn(query.mode(), plan)
                    .map(LoadQueryResult::Rows)
            }
        });
        let result = result?;
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);

        Ok((
            result,
            QueryExecutionAttribution {
                compile_local_instructions,
                execute_local_instructions,
                total_local_instructions,
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
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_prepared_execution_plan();

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

        let executable = compiled.into_prepared_execution_plan();
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
            .compile_query_with_visible_indexes(query)?
            .into_prepared_execution_plan();
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
        let (page, trace) = self.execute_grouped_page_with_trace(query, cursor_token)?;
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

    // Execute the canonical grouped query core and return the raw grouped page
    // plus optional execution trace before outward cursor formatting.
    fn execute_grouped_page_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build the prepared execution plan once from the typed query.
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_prepared_execution_plan();

        // Phase 2: reuse the shared prepared grouped execution path.
        self.execute_grouped_plan_with_trace(plan, cursor_token)
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
        self.with_metrics(|| {
            self.load_executor::<E>()
                .execute_grouped_paged_with_cursor_traced(plan, cursor)
        })
        .map_err(QueryError::execute)
    }
}
