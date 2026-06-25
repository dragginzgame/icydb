//! Module: db::session::sql::execute::aggregate_plan
//! Responsibility: SQL global aggregate prepared-plan cache and authority resolution.
//! Does not own: aggregate execution, direct count probes, or request construction.
//! Boundary: exposes resolved prepared plans consumed by global aggregate orchestration.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        session::{
            AcceptedSchemaCatalogContext,
            sql::{CompiledSqlCommand, SqlCacheAttribution, SqlCompiledSchemaFingerprint},
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::{CanisterKind, EntityValue},
};

#[cfg(feature = "diagnostics")]
use crate::db::session::query::QueryPlanCompilePhaseAttribution;

pub(super) struct ResolvedGlobalAggregatePreparedPlan {
    prepared_plan: SharedPreparedExecutionPlan,
    cache_attribution: SqlCacheAttribution,
}

pub(super) type PreparedAggregatePlanResolution =
    Result<ResolvedGlobalAggregatePreparedPlan, QueryError>;
#[cfg(feature = "diagnostics")]
pub(super) type MeasuredPreparedAggregatePlanResolution = Result<
    (
        ResolvedGlobalAggregatePreparedPlan,
        QueryPlanCompilePhaseAttribution,
    ),
    QueryError,
>;

impl ResolvedGlobalAggregatePreparedPlan {
    const fn new(
        prepared_plan: SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    ) -> Self {
        Self {
            prepared_plan,
            cache_attribution,
        }
    }

    const fn from_compiled_cache_hit(prepared_plan: SharedPreparedExecutionPlan) -> Self {
        Self::new(
            prepared_plan,
            SqlCacheAttribution::shared_query_plan_cache_hit(),
        )
    }

    const fn from_shared_query_plan_cache(
        prepared_plan: SharedPreparedExecutionPlan,
        cache_attribution: crate::db::session::query::QueryPlanCacheAttribution,
    ) -> Self {
        Self::new(
            prepared_plan,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        )
    }

    pub(super) fn into_parts(self) -> (SharedPreparedExecutionPlan, SqlCacheAttribution) {
        (self.prepared_plan, self.cache_attribution)
    }

    const fn prepared_plan(&self) -> &SharedPreparedExecutionPlan {
        &self.prepared_plan
    }
}

fn cached_compiled_global_aggregate_prepared_plan(
    compiled: &CompiledSqlCommand,
    catalog: &AcceptedSchemaCatalogContext,
) -> Option<SharedPreparedExecutionPlan> {
    compiled.cached_global_aggregate_plan(SqlCompiledSchemaFingerprint::from_catalog(catalog))
}

fn cache_compiled_global_aggregate_prepared_plan(
    compiled: &CompiledSqlCommand,
    catalog: &AcceptedSchemaCatalogContext,
    prepared_plan: &SharedPreparedExecutionPlan,
) {
    compiled.set_cached_global_aggregate_plan(
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        prepared_plan.clone(),
    );
}

impl<C: CanisterKind> DbSession<C> {
    pub(super) fn global_aggregate_prepared_plan_authority<E>(
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> Result<EntityAuthority, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        catalog
            .accepted_or_provided_entity_authority_for::<E>(authority.as_ref())
            .map_err(QueryError::execute)
    }

    pub(super) fn resolve_global_aggregate_prepared_plan_for_authority(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> PreparedAggregatePlanResolution {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority,
                catalog,
                command.query(),
            )?;

        Ok(
            ResolvedGlobalAggregatePreparedPlan::from_shared_query_plan_cache(
                prepared_plan,
                cache_attribution,
            ),
        )
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn resolve_global_aggregate_prepared_plan_for_authority_with_phase_attribution(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> MeasuredPreparedAggregatePlanResolution {
        let (prepared_plan, cache_attribution, plan_compile_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
                authority,
                catalog,
                command.query(),
            )?;

        Ok((
            ResolvedGlobalAggregatePreparedPlan::from_shared_query_plan_cache(
                prepared_plan,
                cache_attribution,
            ),
            plan_compile_attribution,
        ))
    }

    pub(super) fn resolve_compiled_global_aggregate_prepared_plan<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> PreparedAggregatePlanResolution
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(prepared_plan) =
            cached_compiled_global_aggregate_prepared_plan(compiled, catalog)
        {
            return Ok(ResolvedGlobalAggregatePreparedPlan::from_compiled_cache_hit(prepared_plan));
        }

        let authority = Self::global_aggregate_prepared_plan_authority::<E>(catalog, authority)?;
        let resolved =
            self.resolve_global_aggregate_prepared_plan_for_authority(command, catalog, authority)?;
        cache_compiled_global_aggregate_prepared_plan(compiled, catalog, resolved.prepared_plan());

        Ok(resolved)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn resolve_compiled_global_aggregate_prepared_plan_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> MeasuredPreparedAggregatePlanResolution
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(prepared_plan) =
            cached_compiled_global_aggregate_prepared_plan(compiled, catalog)
        {
            return Ok((
                ResolvedGlobalAggregatePreparedPlan::from_compiled_cache_hit(prepared_plan),
                QueryPlanCompilePhaseAttribution::default(),
            ));
        }

        let authority = Self::global_aggregate_prepared_plan_authority::<E>(catalog, authority)?;
        let (resolved, plan_compile_attribution) = self
            .resolve_global_aggregate_prepared_plan_for_authority_with_phase_attribution(
                command, catalog, authority,
            )?;
        cache_compiled_global_aggregate_prepared_plan(compiled, catalog, resolved.prepared_plan());

        Ok((resolved, plan_compile_attribution))
    }
}
