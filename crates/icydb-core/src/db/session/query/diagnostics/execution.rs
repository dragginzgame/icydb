//! Module: db::session::query::diagnostics::execution
//! Responsibility: hidden measured query execution for diagnostics attribution.
//! Does not own: attribution DTO field shape or normal query execution dispatch.
//! Boundary: wraps the existing prepared execution path and projects captured phase counters.

use crate::{
    db::{
        DbSession, LoadQueryResult, PersistedRow, Query, QueryError,
        diagnostics::{
            StoreCounterSnapshot, measure_local_instruction_delta as measure_query_stage,
        },
        session::{
            finalize_structural_grouped_projection_result,
            query::{PreparedQueryExecutionOutcome, PreparedQueryExecutionOutput},
        },
    },
    traits::CanisterKind,
};

use super::model::{
    QueryAttributionCommon, QueryExecutePhaseAttribution, QueryExecutionAttribution,
};

impl<C: CanisterKind> DbSession<C> {
    /// Execute one typed query while reporting the compile/execute split at
    /// the shared fluent query seam.
    #[doc(hidden)]
    pub(in crate::db) fn execute_query_result_with_attribution<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(LoadQueryResult<E>, QueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        // Phase 1: measure compile work at the typed/fluent boundary,
        // including the shared lower query-plan cache lookup/build exactly
        // once. This preserves honest hit/miss attribution without
        // double-building plans on one-shot cache misses.
        let (plan_lookup_local_instructions, plan_and_cache) = measure_query_stage(|| {
            self.cached_prepared_query_plan_for_entity_with_compile_phase_attribution::<E>(query)
        });
        let (plan, cache_attribution, compile_phase_attribution) = plan_and_cache?;

        // Phase 2: execute one prepared plan through the shared execution
        // pipeline, preserving the same outer invocation measurement boundary.
        let store_counters_before = StoreCounterSnapshot::capture();
        let (executor_invocation_local_instructions, outcome) = measure_query_stage(|| {
            self.execute_prepared(plan, true, PreparedQueryExecutionOutput::Rows)
        });
        let outcome = outcome?;
        let store_counters = store_counters_before.delta_since();
        let (result, execute_phase_attribution, response_decode_local_instructions) =
            Self::query_execution_attribution_from_outcome(
                outcome,
                executor_invocation_local_instructions,
            )?;
        let common_attribution = QueryAttributionCommon::new(
            plan_lookup_local_instructions,
            compile_phase_attribution,
            cache_attribution,
            store_counters,
        );

        Ok((
            result,
            QueryExecutionAttribution::from_common(
                common_attribution,
                &execute_phase_attribution,
                response_decode_local_instructions,
            ),
        ))
    }

    // Convert the shared execution outcome into the diagnostics public result
    // and phase-attribution bundle. Grouped response finalization is still
    // measured separately because that counter is part of the public contract.
    fn query_execution_attribution_from_outcome<E>(
        outcome: PreparedQueryExecutionOutcome<E>,
        executor_invocation_local_instructions: u64,
    ) -> Result<(LoadQueryResult<E>, QueryExecutePhaseAttribution, u64), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        match outcome {
            PreparedQueryExecutionOutcome::Scalar {
                rows,
                phase: Some(phase_attribution),
                response_decode_local_instructions,
            } => Ok((
                LoadQueryResult::Rows(rows),
                QueryExecutePhaseAttribution::from_scalar_phase(
                    phase_attribution,
                    executor_invocation_local_instructions,
                ),
                response_decode_local_instructions,
            )),
            PreparedQueryExecutionOutcome::Grouped {
                result,
                trace,
                phase: Some(phase_attribution),
            } => {
                let (response_finalization_local_instructions, grouped) =
                    measure_query_stage(|| {
                        finalize_structural_grouped_projection_result(result, trace)
                    });
                let grouped = grouped?;

                Ok((
                    LoadQueryResult::Grouped(grouped),
                    QueryExecutePhaseAttribution::from_grouped_phase(
                        phase_attribution,
                        executor_invocation_local_instructions,
                        response_finalization_local_instructions,
                    ),
                    0,
                ))
            }
            PreparedQueryExecutionOutcome::Delete { rows } => Ok((
                LoadQueryResult::Rows(rows),
                QueryExecutePhaseAttribution::from_delete(executor_invocation_local_instructions),
                0,
            )),
            PreparedQueryExecutionOutcome::DeleteCount { .. } => Err(QueryError::invariant()),
            PreparedQueryExecutionOutcome::Scalar { phase: None, .. }
            | PreparedQueryExecutionOutcome::Grouped { phase: None, .. } => {
                Err(QueryError::invariant())
            }
        }
    }
}
