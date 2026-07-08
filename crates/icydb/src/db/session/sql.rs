//! Module: db::session::sql
//!
//! Responsibility: public `DbSession` SQL facade methods and SQL perf DTOs.
//! Does not own: SQL lowering, SQL planner semantics, or public read policy.
//! Boundary: wraps core SQL execution with public response conversion.

use crate::{
    db::{session::DbSession, sql::SqlQueryResult},
    error::Error,
    traits::CanisterKind,
};

use icydb_core as core;

/// SQL query attribution envelope used by generated canister endpoints.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlQueryPerfAttribution {
    pub compile_local_instructions: u64,
    pub execution: SqlExecutionPerfAttribution,
    pub pure_covering: Option<SqlPureCoveringPerfAttribution>,
    pub response_decode_local_instructions: u64,
    pub total_local_instructions: u64,
}

/// SQL execution-stage attribution.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlExecutionPerfAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_local_instructions: u64,
}

/// SQL pure-covering attribution.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlPureCoveringPerfAttribution {
    pub decode_local_instructions: u64,
    pub row_assembly_local_instructions: u64,
}

#[cfg(feature = "diagnostics")]
impl From<crate::db::SqlQueryExecutionAttribution> for SqlQueryPerfAttribution {
    fn from(attribution: crate::db::SqlQueryExecutionAttribution) -> Self {
        Self {
            compile_local_instructions: attribution.compile_local_instructions,
            execution: SqlExecutionPerfAttribution {
                planner_local_instructions: attribution.execution.planner_local_instructions,
                store_local_instructions: attribution.execution.store_local_instructions,
                executor_local_instructions: attribution.execution.executor_local_instructions,
            },
            pure_covering: attribution.pure_covering.map(|pure_covering| {
                SqlPureCoveringPerfAttribution {
                    decode_local_instructions: pure_covering.decode_local_instructions,
                    row_assembly_local_instructions: pure_covering.row_assembly_local_instructions,
                }
            }),
            response_decode_local_instructions: attribution.response_decode_local_instructions,
            total_local_instructions: attribution.total_local_instructions,
        }
    }
}

#[cfg(all(feature = "diagnostics", target_arch = "wasm32"))]
fn read_sql_response_decode_local_instruction_counter() -> u64 {
    ic_cdk::api::performance_counter(1)
}

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
const fn read_sql_response_decode_local_instruction_counter() -> u64 {
    0
}

#[cfg(feature = "diagnostics")]
fn measure_sql_response_decode_stage<T>(run: impl FnOnce() -> T) -> (u64, T) {
    let start = read_sql_response_decode_local_instruction_counter();
    let result = run();
    let delta = read_sql_response_decode_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

// Fold the public SQL response-packaging phase onto the outward top-level perf
// contract so shell-facing totals remain exhaustive across compile, planner,
// store, executor, and decode.
#[cfg(feature = "diagnostics")]
const fn finalize_public_sql_query_attribution(
    mut attribution: crate::db::SqlQueryExecutionAttribution,
    response_decode_local_instructions: u64,
) -> crate::db::SqlQueryExecutionAttribution {
    attribution.response_decode_local_instructions = response_decode_local_instructions;
    attribution.execute_local_instructions = attribution
        .execution
        .planner_local_instructions
        .saturating_add(attribution.execution.store_local_instructions)
        .saturating_add(attribution.execution.executor_local_instructions)
        .saturating_add(
            attribution
                .execution
                .response_finalization_local_instructions,
        )
        .saturating_add(response_decode_local_instructions);
    attribution.total_local_instructions = attribution
        .compile_local_instructions
        .saturating_add(attribution.execute_local_instructions);

    attribution
}

impl<C: CanisterKind> DbSession<C> {
    fn sql_query_result_from_statement<E>(statement: core::db::SqlStatementResult) -> SqlQueryResult
    where
        E: crate::traits::EntityFor<C>,
    {
        crate::db::sql::sql_query_result_from_statement(statement, E::MODEL.name().to_string())
    }

    /// Execute one trusted/admin reduced SQL query against one concrete entity type.
    ///
    /// This helper does not make caller-controlled SQL public-safe. Public
    /// endpoints should prefer ordinary typed/fluent reads, or use an
    /// application-owned SQL allowlist before entering this trusted lane.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_query::<E>(sql)?,
        ))
    }

    /// Execute one trusted/admin SQL query and return the shell perf envelope shape.
    ///
    /// This helper is used by generated controller-gated SQL surfaces and keeps
    /// the same trusted-lane caller contract as `execute_sql_query`.
    #[cfg(not(feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn execute_sql_query_with_perf_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, SqlQueryPerfAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok((
            self.execute_sql_query::<E>(sql)?,
            SqlQueryPerfAttribution::default(),
        ))
    }

    /// Execute one trusted/admin SQL query and return the shell perf envelope shape.
    ///
    /// This helper is used by generated controller-gated SQL surfaces and keeps
    /// the same trusted-lane caller contract as `execute_sql_query`.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_sql_query_with_perf_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, SqlQueryPerfAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, attribution) = self.execute_sql_query_with_attribution::<E>(sql)?;

        Ok((result, SqlQueryPerfAttribution::from(attribution)))
    }

    /// Execute one trusted/admin reduced SQL query and report the top-level
    /// compile/execute cost split at the SQL seam.
    ///
    /// This helper keeps the same trusted-lane caller contract as
    /// `execute_sql_query`.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, crate::db::SqlQueryExecutionAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, mut attribution) = self.inner.execute_sql_query_with_attribution::<E>(sql)?;
        let (response_decode_local_instructions, result) =
            measure_sql_response_decode_stage(|| {
                Self::sql_query_result_from_statement::<E>(result)
            });
        attribution =
            finalize_public_sql_query_attribution(attribution, response_decode_local_instructions);

        Ok((result, attribution))
    }

    /// Execute one reduced SQL mutation statement against one concrete entity type.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_update::<E>(sql)?,
        ))
    }

    /// Execute one policy-validated public primary-key SQL `UPDATE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_primary_key_update<E>(
        &self,
        plan: &crate::db::SqlPublicPrimaryKeyUpdatePlan,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner
                .execute_validated_sql_public_primary_key_update::<E>(plan)?,
        ))
    }

    /// Execute one public primary-key-only SQL `UPDATE` against one entity type.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_update<E>(
        &self,
        sql: &str,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_public_primary_key_update::<E>(sql)?,
        ))
    }

    /// Execute one policy-validated bounded deterministic SQL `UPDATE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_bounded_update<E>(
        &self,
        plan: &crate::db::SqlPublicBoundedUpdatePlan,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner
                .execute_validated_sql_public_bounded_update::<E>(plan)?,
        ))
    }

    /// Execute one bounded deterministic public SQL `UPDATE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_update<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_public_bounded_update::<E>(sql)?,
        ))
    }

    /// Execute one policy-validated public primary-key SQL `DELETE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_primary_key_delete<E>(
        &self,
        plan: &crate::db::SqlPublicPrimaryKeyDeletePlan,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner
                .execute_validated_sql_public_primary_key_delete::<E>(plan)?,
        ))
    }

    /// Execute one public primary-key-only SQL `DELETE` against one entity type.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_delete<E>(
        &self,
        sql: &str,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_public_primary_key_delete::<E>(sql)?,
        ))
    }

    /// Execute one policy-validated bounded deterministic SQL `DELETE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_bounded_delete<E>(
        &self,
        plan: &crate::db::SqlPublicBoundedDeletePlan,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner
                .execute_validated_sql_public_bounded_delete::<E>(plan)?,
        ))
    }

    /// Execute one bounded deterministic public SQL `DELETE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_delete<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_public_bounded_delete::<E>(sql)?,
        ))
    }

    /// Execute one supported SQL DDL statement against one concrete entity type.
    pub fn execute_sql_ddl<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_sql_ddl::<E>(sql)?,
        ))
    }
}

#[cfg(all(test, feature = "diagnostics"))]
mod tests {
    use super::finalize_public_sql_query_attribution;
    use crate::db::SqlQueryExecutionAttribution;

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "the public diagnostics DTO test intentionally stays resilient to future attribution fields"
    )]
    fn public_sql_perf_attribution_total_stays_exhaustive_after_decode_finalize() {
        let mut attribution = SqlQueryExecutionAttribution::default();
        attribution.compile_local_instructions = 11;
        attribution.compile.cache_lookup_local_instructions = 1;
        attribution.compile.parse_local_instructions = 2;
        attribution.compile.parse_tokenize_local_instructions = 1;
        attribution.compile.parse_select_local_instructions = 1;
        attribution.compile.prepare_local_instructions = 3;
        attribution.compile.lower_local_instructions = 4;
        attribution.compile.bind_local_instructions = 1;
        attribution.plan_lookup_local_instructions = 13;
        attribution.execution.planner_local_instructions = 13;
        attribution.execution.store_local_instructions = 17;
        attribution.execution.executor_invocation_local_instructions = 17;
        attribution.execution.executor_local_instructions = 17;
        attribution.store_get_calls = 3;
        attribution.execute_local_instructions = 47;
        attribution.total_local_instructions = 58;

        let finalized = finalize_public_sql_query_attribution(attribution, 19);

        assert_eq!(
            finalized.execute_local_instructions,
            finalized
                .execution
                .planner_local_instructions
                .saturating_add(finalized.execution.store_local_instructions)
                .saturating_add(finalized.execution.executor_local_instructions)
                .saturating_add(finalized.execution.response_finalization_local_instructions)
                .saturating_add(finalized.response_decode_local_instructions),
            "public SQL execute totals should include planner, store, executor, and decode work",
        );
        assert_eq!(
            finalized.total_local_instructions,
            finalized
                .compile_local_instructions
                .saturating_add(finalized.execute_local_instructions),
            "public SQL total instructions should remain exhaustive across compiler, planner, store, executor, and decode",
        );
    }
}
