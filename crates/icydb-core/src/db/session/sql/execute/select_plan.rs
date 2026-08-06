//! Module: db::session::sql::execute::select_plan
//! Responsibility: SQL SELECT prepared-plan cache and authority resolution.
//! Does not own: SELECT row materialization, grouped execution, or response shaping.
//! Boundary: exposes resolved prepared plans consumed by SELECT execution orchestration.

#[cfg(feature = "diagnostics")]
use crate::db::session::query::QueryPlanCompilePhaseAttribution;
use crate::{
    db::{
        DbSession, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        schema::AcceptedSchemaSnapshot,
        session::{
            AcceptedSchemaCatalogContext,
            query::{QueryPlanCacheAttribution, StructuralProjectionContract},
            sql::{SqlCacheAttribution, SqlCompiledCommandExecutionContext},
        },
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

pub(super) struct ResolvedSelectPreparedPlan {
    prepared_plan: SharedPreparedExecutionPlan,
    projection: StructuralProjectionContract,
    cache_attribution: SqlCacheAttribution,
}

impl ResolvedSelectPreparedPlan {
    const fn new(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
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
        projection: StructuralProjectionContract,
    ) -> Self {
        Self::new(
            prepared_plan,
            projection,
            SqlCacheAttribution::shared_query_plan_cache_hit(),
        )
    }

    const fn from_shared_query_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Self {
        Self::new(prepared_plan, projection, cache_attribution)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        SharedPreparedExecutionPlan,
        StructuralProjectionContract,
        SqlCacheAttribution,
    ) {
        (self.prepared_plan, self.projection, self.cache_attribution)
    }

    const fn prepared_plan(&self) -> &SharedPreparedExecutionPlan {
        &self.prepared_plan
    }

    const fn projection(&self) -> &StructuralProjectionContract {
        &self.projection
    }
}

fn cached_compiled_select_prepared_plan(
    context: &SqlCompiledCommandExecutionContext,
) -> Option<(SharedPreparedExecutionPlan, StructuralProjectionContract)> {
    context
        .command()
        .cached_select_plan(context.compiled_schema_fingerprint())
}

fn cache_compiled_select_prepared_plan(
    context: &SqlCompiledCommandExecutionContext,
    prepared_plan: &SharedPreparedExecutionPlan,
    projection: &StructuralProjectionContract,
) {
    context.command().set_cached_select_plan(
        context.compiled_schema_fingerprint(),
        prepared_plan.clone(),
        projection.clone(),
    );
}

impl<C: CanisterKind> DbSession<C> {
    #[cfg(all(test, feature = "diagnostics"))]
    pub(in crate::db) fn sql_select_prepared_plan_for_tests(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<SharedPreparedExecutionPlan, QueryError> {
        self.sql_select_prepared_plan_for_accepted_authority(query, authority, accepted_schema)
            .map(|(plan, _, _)| plan)
    }

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
            StructuralProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, projection, cache_attribution) = self
            .structural_projection_prepared_plan_for_accepted_authority(
                query,
                authority,
                accepted_schema,
                DiagnosticExecutionLane::TrustedRead,
            )?;

        Ok((
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
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
            StructuralProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let schema_fingerprint = authority.accepted_schema_fingerprint();
        let (prepared_plan, cache_attribution) = self
            .cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
                DiagnosticExecutionLane::Mutation,
            )?;

        Self::sql_select_projection_from_prepared_plan(prepared_plan, authority, cache_attribution)
    }

    fn select_authority_for_context(
        context: &SqlCompiledCommandExecutionContext,
    ) -> EntityAuthority {
        match context.accepted_authority() {
            Some(authority) => authority.clone(),
            None => context.accepted_catalog().accepted_entity_authority(),
        }
    }

    fn sql_select_prepared_plan_for_accepted_authority_with_catalog(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            StructuralProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority.clone(),
                catalog,
                query,
                DiagnosticExecutionLane::TrustedRead,
            )?;
        Self::sql_select_projection_from_prepared_plan(prepared_plan, authority, cache_attribution)
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
            StructuralProjectionContract,
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
                DiagnosticExecutionLane::TrustedRead,
            )?;
        let (prepared_plan, projection, cache_attribution) =
            Self::sql_select_projection_from_prepared_plan(
                prepared_plan,
                authority,
                cache_attribution,
            )?;

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
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            StructuralProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let projection_spec = prepared_plan.logical_plan().projection_spec_with_schema(
            authority
                .accepted_schema_info()
                .ok_or_else(QueryError::invariant)?,
        );
        let projection = StructuralProjectionContract::from_projection_spec(&projection_spec);

        Ok((
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
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

    pub(super) fn resolve_select_prepared_plan_for_context(
        &self,
        query: &StructuralQuery,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<ResolvedSelectPreparedPlan, QueryError> {
        if let Some((prepared_plan, projection)) = cached_compiled_select_prepared_plan(context) {
            return Ok(ResolvedSelectPreparedPlan::from_compiled_cache_hit(
                prepared_plan,
                projection,
            ));
        }

        let authority = Self::select_authority_for_context(context);
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
    pub(super) fn resolve_select_prepared_plan_for_context_with_compile_phase_attribution(
        &self,
        query: &StructuralQuery,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(ResolvedSelectPreparedPlan, QueryPlanCompilePhaseAttribution), QueryError> {
        if let Some((prepared_plan, projection)) = cached_compiled_select_prepared_plan(context) {
            return Ok((
                ResolvedSelectPreparedPlan::from_compiled_cache_hit(prepared_plan, projection),
                QueryPlanCompilePhaseAttribution::default(),
            ));
        }

        let authority = Self::select_authority_for_context(context);
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
