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

// Fold the trusted SQL response-packaging phase onto the outward top-level perf
// contract so shell-facing totals remain exhaustive across compile, planner,
// store, executor, and decode.
#[cfg(feature = "diagnostics")]
const fn finalize_trusted_sql_query_attribution(
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
    pub fn execute_trusted_sql_query<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_trusted_sql_query::<E>(sql)?,
        ))
    }

    /// Execute one trusted/admin SQL query and return the shell perf envelope shape.
    ///
    /// This helper is used by generated controller-gated SQL surfaces and keeps
    /// the same explicit trusted-boundary contract as `execute_trusted_sql_query`.
    #[cfg(not(feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_perf_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, SqlQueryPerfAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok((
            self.execute_trusted_sql_query::<E>(sql)?,
            SqlQueryPerfAttribution::default(),
        ))
    }

    /// Execute one trusted/admin SQL query and return the shell perf envelope shape.
    ///
    /// This helper is used by generated controller-gated SQL surfaces and keeps
    /// the same explicit trusted-boundary contract as `execute_trusted_sql_query`.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_perf_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, SqlQueryPerfAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, attribution) = self.execute_trusted_sql_query_with_attribution::<E>(sql)?;

        Ok((result, SqlQueryPerfAttribution::from(attribution)))
    }

    /// Execute one trusted/admin reduced SQL query and report the top-level
    /// compile/execute cost split at the SQL seam.
    ///
    /// This helper keeps the same explicit trusted-boundary contract as
    /// `execute_trusted_sql_query`.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, crate::db::SqlQueryExecutionAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, mut attribution) = self
            .inner
            .execute_trusted_sql_query_with_attribution::<E>(sql)?;
        let (response_decode_local_instructions, result) =
            measure_sql_response_decode_stage(|| {
                Self::sql_query_result_from_statement::<E>(result)
            });
        attribution =
            finalize_trusted_sql_query_attribution(attribution, response_decode_local_instructions);

        Ok((result, attribution))
    }

    /// Execute one trusted SQL `INSERT` or `DELETE` against one entity type.
    ///
    /// `UPDATE` requires an explicit exact or prefix contract and is rejected
    /// by this broad mutation surface.
    pub fn execute_trusted_sql_mutation<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_trusted_sql_mutation::<E>(sql)?,
        ))
    }

    /// Execute one trusted exact complete-set SQL `UPDATE`.
    ///
    /// `require_affected_at_most` is a positive assertion about the complete
    /// target, not a selection limit. If one extra match exists, the call
    /// rejects before mutation. Exact selection uses authoritative primary-key
    /// traversal. The affected-row and scanned-key ceilings are independently
    /// enforced and are currently 4,096 each.
    pub fn execute_trusted_sql_exact_update<E>(
        &self,
        sql: &str,
        require_affected_at_most: u32,
    ) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner
                .execute_trusted_sql_exact_update::<E>(sql, require_affected_at_most)?,
        ))
    }

    /// Execute one intentional primary-key-ordered prefix SQL `UPDATE`.
    ///
    /// The statement must carry a positive bounded `LIMIT`; only that ordered
    /// prefix is mutated and no complete-target claim is made.
    pub fn execute_trusted_sql_prefix_update<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_trusted_sql_prefix_update::<E>(sql)?,
        ))
    }

    /// Prepare one trusted resumable SQL `UPDATE` without reading or mutating rows.
    ///
    /// The returned proof-bearing continuation must be stored durably outside
    /// the target store before a later resume call. It is not authorization and
    /// must not be accepted through an untrusted public endpoint.
    pub fn prepare_trusted_sql_resumable_update<E>(
        &self,
        operation_id: crate::types::Ulid,
        sql: &str,
    ) -> Result<crate::db::TrustedResumableUpdateContinuation, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self
            .inner
            .prepare_trusted_sql_resumable_update::<E>(operation_id, sql)?)
    }

    /// Resume one trusted resumable SQL `UPDATE` for one bounded engine step.
    ///
    /// The continuation must come from trusted application custody and the SQL
    /// must preserve the exact prepared scope and fixed patch. Forward commits
    /// at most one batch; Verify is read-only and alone may report completion.
    pub fn resume_trusted_sql_resumable_update<E>(
        &self,
        sql: &str,
        continuation: &crate::db::TrustedResumableUpdateContinuation,
    ) -> Result<crate::db::TrustedResumableUpdateReceipt, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self
            .inner
            .resume_trusted_sql_resumable_update::<E>(sql, continuation)?)
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

    /// Execute one administrative SQL DDL statement against one concrete entity type.
    ///
    /// The caller must enforce controller or equivalent administrative
    /// authorization before accepting caller-controlled SQL.
    pub fn execute_admin_sql_ddl<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::sql_query_result_from_statement::<E>(
            self.inner.execute_admin_sql_ddl::<E>(sql)?,
        ))
    }
}

#[cfg(all(test, feature = "diagnostics"))]
mod tests {
    use super::finalize_trusted_sql_query_attribution;
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

        let finalized = finalize_trusted_sql_query_attribution(attribution, 19);

        assert_eq!(
            finalized.execute_local_instructions,
            finalized
                .execution
                .planner_local_instructions
                .saturating_add(finalized.execution.store_local_instructions)
                .saturating_add(finalized.execution.executor_local_instructions)
                .saturating_add(finalized.execution.response_finalization_local_instructions)
                .saturating_add(finalized.response_decode_local_instructions),
            "trusted SQL execute totals should include planner, store, executor, and decode work",
        );
        assert_eq!(
            finalized.total_local_instructions,
            finalized
                .compile_local_instructions
                .saturating_add(finalized.execute_local_instructions),
            "trusted SQL total instructions should remain exhaustive across compiler, planner, store, executor, and decode",
        );
    }
}
