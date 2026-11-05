//! Test-only constructors and adapters for the parent module.

use super::*;

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db) fn sql_select_prepared_plan_for_tests(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<SharedPreparedExecutionPlan, QueryError> {
        self.sql_select_prepared_plan_for_accepted_authority(query, authority, accepted_schema)
            .map(|(plan, _)| plan)
    }
}
