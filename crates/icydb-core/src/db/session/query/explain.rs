//! Module: db::session::query::explain
//! Responsibility: read-only query explain, trace, and plan-hash surfaces.
//! Does not own: execution, cursor decoding, fluent terminal execution, or diagnostics attribution.
//! Boundary: maps cached session-visible plans into query-facing diagnostic DTOs.

use crate::{
    db::{
        DbSession, Query, QueryError, QueryTracePlan, TraceExecutionFamily,
        access::summarize_executable_access_plan,
        executor::{EntityAuthority, ExecutionFamily},
        query::builder::{AggregateExplain, ProjectionExplain},
        query::explain::{
            ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor, ExplainPlan,
        },
        query::plan::{AccessPlannedQuery, QueryMode, VisibleIndexes},
        schema::SchemaInfo,
        session::query::{QueryPlanCacheAttribution, query_plan_cache_reuse_event},
    },
    traits::{CanisterKind, EntityKind, EntityValue},
};

// Translate executor route-family selection into the query-owned trace label
// at the session boundary so trace DTOs do not depend on executor types.
const fn trace_execution_family_from_executor(family: ExecutionFamily) -> TraceExecutionFamily {
    match family {
        ExecutionFamily::PrimaryKey => TraceExecutionFamily::PrimaryKey,
        ExecutionFamily::Ordered => TraceExecutionFamily::Ordered,
        ExecutionFamily::Grouped => TraceExecutionFamily::Grouped,
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Borrow the cached logical plan for read-only query diagnostics so explain
    // and hash surfaces do not clone the full access-planned query.
    fn try_map_cached_logical_query_plan<E, T>(
        &self,
        query: &Query<E>,
        map: impl FnOnce(&AccessPlannedQuery) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        self.try_map_cached_shared_query_plan_ref_for_entity::<E, T>(query, |prepared_plan| {
            map(prepared_plan.logical_plan())
        })
    }

    // Reuse the same cached logical plan as execution explain, then freeze the
    // explain-only access-choice facts for the effective session-visible index
    // slice before route facts are assembled.
    fn cached_execution_explain_plan<E>(
        &self,
        query: &Query<E>,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<
        (
            AccessPlannedQuery,
            EntityAuthority,
            QueryPlanCacheAttribution,
        ),
        QueryError,
    >
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, cache_attribution) =
            self.cached_shared_query_plan_for_entity::<E>(query)?;
        let mut plan = prepared_plan.logical_plan().clone();
        let accepted_schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = SchemaInfo::from_accepted_snapshot_for_model(
            query.structural().model(),
            &accepted_schema,
        );

        plan.finalize_access_choice_for_model_with_accepted_indexes_and_schema(
            query.structural().model(),
            visible_indexes.generated_static_bridge_indexes(),
            visible_indexes.accepted_field_path_indexes(),
            &schema_info,
        );
        let authority = Self::accepted_entity_authority_for_schema::<E>(&accepted_schema)
            .map_err(QueryError::execute)?;

        Ok((plan, authority, cache_attribution))
    }

    // Project one logical explain payload using only planner-visible indexes.
    pub(in crate::db) fn explain_query_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
    ) -> Result<ExplainPlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        self.try_map_cached_logical_query_plan(query, |plan| Ok(plan.explain()))
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
        self.try_map_cached_logical_query_plan(query, |plan| Ok(plan.fingerprint().to_string()))
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
            let (plan, authority, _) =
                self.cached_execution_explain_plan::<E>(query, visible_indexes)?;

            query
                .structural()
                .explain_execution_descriptor_from_plan_with_authority(&plan, &authority)
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
            let (plan, authority, cache_attribution) =
                self.cached_execution_explain_plan::<E>(query, visible_indexes)?;

            query
                .structural()
                .finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator(
                    &plan,
                    &authority,
                    Some(query_plan_cache_reuse_event(cache_attribution)),
                    |_| {},
                )
                .map(|diagnostics| diagnostics.render_text_verbose())
        })
    }

    // Explain one prepared fluent aggregate terminal from the same cached
    // prepared plan used by execution.
    pub(in crate::db) fn explain_query_prepared_aggregate_terminal_with_visible_indexes<E, S>(
        &self,
        query: &Query<E>,
        strategy: &S,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
        S: AggregateExplain,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        plan.explain_prepared_aggregate_terminal(strategy)
    }

    // Explain one `bytes_by(field)` terminal from the same cached prepared
    // plan used by execution.
    pub(in crate::db) fn explain_query_bytes_by_with_visible_indexes<E>(
        &self,
        query: &Query<E>,
        target_field: &str,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        plan.explain_bytes_by_terminal(target_field)
    }

    // Explain one prepared fluent projection terminal from the same cached
    // prepared plan used by execution.
    pub(in crate::db) fn explain_query_prepared_projection_terminal_with_visible_indexes<E, S>(
        &self,
        query: &Query<E>,
        strategy: &S,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue + EntityKind<Canister = C>,
        S: ProjectionExplain,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        plan.explain_prepared_projection_terminal(strategy)
    }

    /// Build one trace payload for a query without executing it.
    ///
    /// This lightweight surface is intended for developer diagnostics:
    /// plan hash, access strategy summary, and planner/executor route shape.
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let (prepared_plan, cache_attribution) =
            self.cached_prepared_query_plan_for_entity::<E>(query)?;
        let logical_plan = prepared_plan.logical_plan();
        let explain = logical_plan.explain();
        let plan_hash = logical_plan.fingerprint().to_string();
        let executable_access = prepared_plan.access().executable_contract();
        let access_strategy = summarize_executable_access_plan(&executable_access);
        let execution_family = match prepared_plan.mode() {
            QueryMode::Load(_) => Some(trace_execution_family_from_executor(
                prepared_plan
                    .execution_family()
                    .map_err(QueryError::execute)?,
            )),
            QueryMode::Delete(_) => None,
        };
        let reuse = query_plan_cache_reuse_event(cache_attribution);

        Ok(QueryTracePlan::new(
            plan_hash,
            access_strategy,
            execution_family,
            reuse,
            explain,
        ))
    }
}
