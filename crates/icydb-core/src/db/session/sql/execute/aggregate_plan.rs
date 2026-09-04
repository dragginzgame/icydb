//! Module: db::session::sql::execute::aggregate_plan
//! Responsibility: SQL global aggregate prepared-plan cache and authority resolution.
//! Does not own: aggregate execution, direct count probes, or request construction.
//! Boundary: exposes resolved prepared plans consumed by global aggregate orchestration.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        session::{
            AcceptedSchemaCatalogContext,
            query::query_plan_requires_cardinality_lifecycle_recheck,
            sql::{
                CompiledSqlCommand, SqlCompiledSchemaFingerprint, SqlGlobalAggregateCachedPlan,
                SqlGlobalAggregatePlanCacheEntry,
            },
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;
use std::rc::Rc;

pub(super) type PreparedAggregatePlanResolution = Result<SharedPreparedExecutionPlan, QueryError>;

fn cache_compiled_global_aggregate_prepared_plan(
    compiled: &CompiledSqlCommand,
    catalog: &AcceptedSchemaCatalogContext,
    prepared_plan: &SharedPreparedExecutionPlan,
) {
    if query_plan_requires_cardinality_lifecycle_recheck(prepared_plan) {
        return;
    }
    compiled.set_cached_global_aggregate_plan(Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        SqlGlobalAggregateCachedPlan::prepared(prepared_plan.clone()),
    )));
}

impl<C: CanisterKind> DbSession<C> {
    fn global_aggregate_prepared_plan_authority(
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> EntityAuthority {
        catalog.accepted_or_provided_entity_authority(authority.as_ref())
    }

    fn resolve_global_aggregate_prepared_plan_for_authority(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> PreparedAggregatePlanResolution {
        let prepared_plan = self.cached_shared_query_plan_for_accepted_authority_with_catalog(
            authority,
            catalog,
            command.query(),
            DiagnosticExecutionLane::TrustedRead,
        )?;

        Ok(prepared_plan)
    }

    pub(super) fn resolve_compiled_global_aggregate_prepared_plan(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> PreparedAggregatePlanResolution {
        let authority = Self::global_aggregate_prepared_plan_authority(catalog, authority);
        let prepared_plan =
            self.resolve_global_aggregate_prepared_plan_for_authority(command, catalog, authority)?;
        cache_compiled_global_aggregate_prepared_plan(compiled, catalog, &prepared_plan);

        Ok(prepared_plan)
    }
}
