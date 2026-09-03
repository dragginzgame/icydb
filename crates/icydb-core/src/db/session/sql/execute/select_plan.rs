//! Module: db::session::sql::execute::select_plan
//! Responsibility: SQL SELECT prepared-plan cache and authority resolution.
//! Does not own: SELECT row materialization, grouped execution, or response shaping.
//! Boundary: exposes resolved prepared plans consumed by SELECT execution orchestration.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        schema::AcceptedSchemaSnapshot,
        session::{
            AcceptedSchemaCatalogContext,
            query::{
                StructuralProjectionContract, query_plan_requires_cardinality_lifecycle_recheck,
            },
            sql::SqlCompiledCommandExecutionContext,
        },
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

pub(super) struct ResolvedSelectPreparedPlan {
    prepared_plan: SharedPreparedExecutionPlan,
    projection: StructuralProjectionContract,
}

impl ResolvedSelectPreparedPlan {
    const fn new(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Self {
        Self {
            prepared_plan,
            projection,
        }
    }

    const fn from_compiled_cache_hit(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Self {
        Self::new(prepared_plan, projection)
    }

    const fn from_shared_query_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Self {
        Self::new(prepared_plan, projection)
    }

    pub(super) fn into_parts(self) -> (SharedPreparedExecutionPlan, StructuralProjectionContract) {
        (self.prepared_plan, self.projection)
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
    if query_plan_requires_cardinality_lifecycle_recheck(prepared_plan) {
        return;
    }
    context.command().set_cached_select_plan(
        context.compiled_schema_fingerprint(),
        prepared_plan.clone(),
        projection.clone(),
    );
}

impl<C: CanisterKind> DbSession<C> {
    #[cfg(test)]
    pub(in crate::db) fn sql_select_prepared_plan_for_tests(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<SharedPreparedExecutionPlan, QueryError> {
        self.sql_select_prepared_plan_for_accepted_authority(query, authority, accepted_schema)
            .map(|(plan, _)| plan)
    }

    // Resolve one SQL SELECT through a caller-selected accepted authority and
    // accepted schema snapshot. Typed SQL entrypoints use this to avoid passing
    // generated authority through the runtime cache boundary.
    pub(in crate::db::session::sql) fn sql_select_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<(SharedPreparedExecutionPlan, StructuralProjectionContract), QueryError> {
        let (prepared_plan, projection, _) = self
            .structural_projection_prepared_plan_for_accepted_authority(
                query,
                authority,
                accepted_schema,
                DiagnosticExecutionLane::TrustedRead,
            )?;

        Ok((prepared_plan, projection))
    }

    // Resolve one SQL selector through accepted authority while excluding
    // secondary indexes from the cache identity and planner-visible set.
    // Exact mutations use this to make primary-store traversal authoritative.
    pub(in crate::db::session::sql) fn sql_primary_only_select_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<(SharedPreparedExecutionPlan, StructuralProjectionContract), QueryError> {
        let schema_fingerprint = authority.accepted_schema_fingerprint();
        let (prepared_plan, _) = self
            .cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
                DiagnosticExecutionLane::Mutation,
            )?;

        Self::sql_select_projection_from_prepared_plan(prepared_plan, authority)
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
    ) -> Result<(SharedPreparedExecutionPlan, StructuralProjectionContract), QueryError> {
        let (prepared_plan, _) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority.clone(),
                catalog,
                query,
                DiagnosticExecutionLane::TrustedRead,
            )?;
        Self::sql_select_projection_from_prepared_plan(prepared_plan, authority)
    }

    fn sql_select_projection_from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        authority: EntityAuthority,
    ) -> Result<(SharedPreparedExecutionPlan, StructuralProjectionContract), QueryError> {
        let projection_spec = prepared_plan.logical_plan().projection_spec_with_schema(
            authority
                .accepted_schema_info()
                .ok_or_else(QueryError::invariant)?,
        );
        let projection = StructuralProjectionContract::from_projection_spec(&projection_spec);

        Ok((prepared_plan, projection))
    }

    pub(super) fn resolve_select_prepared_plan_for_authority_with_catalog(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ResolvedSelectPreparedPlan, QueryError> {
        let (prepared_plan, projection) = self
            .sql_select_prepared_plan_for_accepted_authority_with_catalog(
                query, authority, catalog,
            )?;

        Ok(ResolvedSelectPreparedPlan::from_shared_query_plan(
            prepared_plan,
            projection,
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
}
