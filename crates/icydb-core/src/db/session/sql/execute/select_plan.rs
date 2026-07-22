//! Module: db::session::sql::execute::select_plan
//! Responsibility: SQL SELECT prepared-plan cache and authority resolution.
//! Does not own: SELECT row materialization, grouped execution, or response shaping.
//! Boundary: exposes resolved prepared plans consumed by SELECT execution orchestration.

#[cfg(feature = "diagnostics")]
use crate::db::session::query::QueryPlanCompilePhaseAttribution;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        schema::{AcceptedSchemaSnapshot, accepted_schema_cache_fingerprint},
        session::{
            AcceptedSchemaCatalogContext,
            query::QueryPlanCacheAttribution,
            sql::{
                SqlCacheAttribution, SqlCompiledCommandExecutionContext, SqlProjectionContract,
                projection::projection_contract_from_projection_spec,
            },
        },
    },
    traits::CanisterKind,
};

pub(super) struct ResolvedSelectPreparedPlan {
    prepared_plan: SharedPreparedExecutionPlan,
    projection: SqlProjectionContract,
    cache_attribution: SqlCacheAttribution,
}

impl ResolvedSelectPreparedPlan {
    const fn new(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Self {
        Self {
            prepared_plan,
            projection,
            cache_attribution,
        }
    }

    const fn from_compiled_cache_hit(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
    ) -> Self {
        Self::new(
            prepared_plan,
            projection,
            SqlCacheAttribution::shared_query_plan_cache_hit(),
        )
    }

    const fn from_shared_query_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Self {
        Self::new(prepared_plan, projection, cache_attribution)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        SharedPreparedExecutionPlan,
        SqlProjectionContract,
        SqlCacheAttribution,
    ) {
        (self.prepared_plan, self.projection, self.cache_attribution)
    }

    const fn prepared_plan(&self) -> &SharedPreparedExecutionPlan {
        &self.prepared_plan
    }

    const fn projection(&self) -> &SqlProjectionContract {
        &self.projection
    }
}

fn cached_compiled_select_prepared_plan(
    context: &SqlCompiledCommandExecutionContext,
) -> Option<(SharedPreparedExecutionPlan, SqlProjectionContract)> {
    context
        .command()
        .cached_select_plan(context.compiled_schema_fingerprint())
}

fn cache_compiled_select_prepared_plan(
    context: &SqlCompiledCommandExecutionContext,
    prepared_plan: &SharedPreparedExecutionPlan,
    projection: &SqlProjectionContract,
) {
    context.command().set_cached_select_plan(
        context.compiled_schema_fingerprint(),
        prepared_plan.clone(),
        projection.clone(),
    );
}

impl<C: CanisterKind> DbSession<C> {
    // Resolve one SQL SELECT through a caller-selected accepted authority and
    // accepted schema snapshot. Typed SQL entrypoints use this to avoid passing
    // generated authority through the runtime cache boundary.
    pub(in crate::db::session::sql) fn sql_select_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let schema_fingerprint =
            accepted_schema_cache_fingerprint(accepted_schema).map_err(QueryError::execute)?;

        self.sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
            query,
            authority,
            accepted_schema,
            schema_fingerprint,
        )
    }

    // Resolve one SQL selector through accepted authority while excluding
    // secondary indexes from the cache identity and planner-visible set.
    // Exact mutations use this to make primary-store traversal authoritative.
    pub(in crate::db::session::sql) fn sql_primary_only_select_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let schema_fingerprint =
            accepted_schema_cache_fingerprint(accepted_schema).map_err(QueryError::execute)?;
        let (prepared_plan, cache_attribution) = self
            .cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
            )?;

        Ok(Self::sql_select_projection_from_prepared_plan(
            prepared_plan,
            authority,
            cache_attribution,
        ))
    }

    fn sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
            )?;
        Ok(Self::sql_select_projection_from_prepared_plan(
            prepared_plan,
            authority,
            cache_attribution,
        ))
    }

    fn select_authority_for_context<E>(
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<EntityAuthority, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        context
            .accepted_catalog()
            .accepted_or_provided_entity_authority_for::<E>(context.accepted_authority())
            .map_err(QueryError::execute)
    }

    fn sql_select_prepared_plan_for_accepted_authority_with_catalog(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority.clone(),
                catalog,
                query,
            )?;
        Ok(Self::sql_select_projection_from_prepared_plan(
            prepared_plan,
            authority,
            cache_attribution,
        ))
    }

    #[cfg(feature = "diagnostics")]
    fn sql_select_prepared_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
            QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution, plan_compile_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
                authority.clone(),
                catalog,
                query,
            )?;
        let (prepared_plan, projection, cache_attribution) =
            Self::sql_select_projection_from_prepared_plan(
                prepared_plan,
                authority,
                cache_attribution,
            );

        Ok((
            prepared_plan,
            projection,
            cache_attribution,
            plan_compile_attribution,
        ))
    }

    fn sql_select_projection_from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        authority: EntityAuthority,
        cache_attribution: QueryPlanCacheAttribution,
    ) -> (
        SharedPreparedExecutionPlan,
        SqlProjectionContract,
        SqlCacheAttribution,
    ) {
        let projection_spec = prepared_plan
            .logical_plan()
            .projection_spec(authority.model());
        let projection = projection_contract_from_projection_spec(&projection_spec);

        (
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        )
    }

    pub(super) fn resolve_select_prepared_plan_for_authority_with_catalog(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ResolvedSelectPreparedPlan, QueryError> {
        let (prepared_plan, projection, cache_attribution) = self
            .sql_select_prepared_plan_for_accepted_authority_with_catalog(
                query, authority, catalog,
            )?;

        Ok(ResolvedSelectPreparedPlan::from_shared_query_plan(
            prepared_plan,
            projection,
            cache_attribution,
        ))
    }

    #[cfg(feature = "diagnostics")]
    fn resolve_select_prepared_plan_for_authority_with_catalog_and_compile_phase_attribution(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(ResolvedSelectPreparedPlan, QueryPlanCompilePhaseAttribution), QueryError> {
        let (prepared_plan, projection, cache_attribution, plan_compile_attribution) = self
            .sql_select_prepared_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
                query,
                authority,
                catalog,
            )?;

        Ok((
            ResolvedSelectPreparedPlan::from_shared_query_plan(
                prepared_plan,
                projection,
                cache_attribution,
            ),
            plan_compile_attribution,
        ))
    }

    pub(super) fn resolve_select_prepared_plan_for_context<E>(
        &self,
        query: &StructuralQuery,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<ResolvedSelectPreparedPlan, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        if let Some((prepared_plan, projection)) = cached_compiled_select_prepared_plan(context) {
            return Ok(ResolvedSelectPreparedPlan::from_compiled_cache_hit(
                prepared_plan,
                projection,
            ));
        }

        let authority = Self::select_authority_for_context::<E>(context)?;
        let resolved = self.resolve_select_prepared_plan_for_authority_with_catalog(
            query,
            authority,
            context.accepted_catalog(),
        )?;
        cache_compiled_select_prepared_plan(
            context,
            resolved.prepared_plan(),
            resolved.projection(),
        );

        Ok(resolved)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn resolve_select_prepared_plan_for_context_with_compile_phase_attribution<E>(
        &self,
        query: &StructuralQuery,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(ResolvedSelectPreparedPlan, QueryPlanCompilePhaseAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        if let Some((prepared_plan, projection)) = cached_compiled_select_prepared_plan(context) {
            return Ok((
                ResolvedSelectPreparedPlan::from_compiled_cache_hit(prepared_plan, projection),
                QueryPlanCompilePhaseAttribution::default(),
            ));
        }

        let authority = Self::select_authority_for_context::<E>(context)?;
        let (resolved, plan_compile_attribution) = self
            .resolve_select_prepared_plan_for_authority_with_catalog_and_compile_phase_attribution(
                query,
                authority,
                context.accepted_catalog(),
            )?;
        cache_compiled_select_prepared_plan(
            context,
            resolved.prepared_plan(),
            resolved.projection(),
        );

        Ok((resolved, plan_compile_attribution))
    }
}
