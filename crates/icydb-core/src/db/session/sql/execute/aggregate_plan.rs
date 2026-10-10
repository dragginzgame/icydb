//! Module: db::session::sql::execute::aggregate_plan
//! Responsibility: SQL global aggregate prepared-plan cache and authority resolution.
//! Does not own: aggregate execution, direct count probes, or request construction.
//! Boundary: exposes resolved prepared plans consumed by global aggregate orchestration.

use crate::{
    db::{
        DbSession, QueryError, executor::SharedPreparedExecutionPlan,
        session::AcceptedSchemaCatalogContext, sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

pub(super) type PreparedAggregatePlanResolution = Result<SharedPreparedExecutionPlan, QueryError>;

impl<C: CanisterKind> DbSession<C> {
    pub(super) fn resolve_compiled_global_aggregate_prepared_plan(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> PreparedAggregatePlanResolution {
        let prepared_plan = self.cached_shared_query_plan_for_accepted_authority_with_catalog(
            catalog.accepted_entity_authority(),
            catalog,
            command.query(),
            DiagnosticExecutionLane::TrustedRead,
        )?;

        Ok(prepared_plan)
    }
}
