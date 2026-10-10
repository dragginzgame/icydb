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
            AcceptedSchemaCatalogContext, query::StructuralProjectionContract,
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

    const fn from_shared_query_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Self {
        Self::new(prepared_plan, projection)
    }

    pub(super) fn into_parts(self) -> (SharedPreparedExecutionPlan, StructuralProjectionContract) {
        (self.prepared_plan, self.projection)
    }
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
        let (prepared_plan, projection) = self
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
        let prepared_plan = self
            .cached_primary_only_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority,
                accepted_schema,
                schema_fingerprint,
                query,
                DiagnosticExecutionLane::Mutation,
            )?;

        Self::sql_select_projection_from_prepared_plan(prepared_plan)
    }

    fn sql_select_prepared_plan_for_accepted_authority_with_catalog(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(SharedPreparedExecutionPlan, StructuralProjectionContract), QueryError> {
        let prepared_plan = self.cached_shared_query_plan_for_accepted_authority_with_catalog(
            authority,
            catalog,
            query,
            DiagnosticExecutionLane::TrustedRead,
        )?;
        Self::sql_select_projection_from_prepared_plan(prepared_plan)
    }

    fn sql_select_projection_from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Result<(SharedPreparedExecutionPlan, StructuralProjectionContract), QueryError> {
        let projection = StructuralProjectionContract::from_projection_spec(
            prepared_plan.logical_plan().projection_spec()?,
        );

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
        let authority = context.accepted_catalog().accepted_entity_authority();
        self.resolve_select_prepared_plan_for_authority_with_catalog(
            query,
            authority,
            context.accepted_catalog(),
        )
    }
}
