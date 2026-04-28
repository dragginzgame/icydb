//! Module: db::session::query::execution
//! Responsibility: canonical query execution dispatch and executor error mapping.
//! Does not own: diagnostics attribution, cursor decoding, fluent adaptation, or explain surfaces.
//! Boundary: maps prepared plans into executor calls and query-facing response/error types.

#[cfg(feature = "diagnostics")]
use crate::db::executor::{GroupedExecutePhaseAttribution, ScalarExecutePhaseAttribution};
use crate::{
    db::{
        DbSession, EntityResponse, LoadQueryResult, PersistedRow, Query, QueryError,
        cursor::CursorPlanError,
        diagnostics::ExecutionTrace,
        executor::{
            ExecutionFamily, ExecutorPlanError, LoadExecutor, PreparedExecutionPlan,
            StructuralGroupedProjectionResult,
        },
        query::plan::QueryMode,
        session::finalize_structural_grouped_projection_result,
    },
    error::InternalError,
    traits::{CanisterKind, EntityValue},
};

///
/// PreparedQueryExecutionOutcome
///
/// PreparedQueryExecutionOutcome is the private shared result shape for one
/// prepared query execution. Normal execution and diagnostics attribution use
/// it to share scalar/grouped/delete dispatch without exposing executor DTOs
/// outside the session query module.
///
#[expect(
    clippy::large_enum_variant,
    reason = "the grouped execution result stays inline to avoid adding a boxed allocation on query execution paths"
)]
pub(in crate::db::session::query) enum PreparedQueryExecutionOutcome<E>
where
    E: PersistedRow,
{
    Scalar {
        rows: EntityResponse<E>,
        #[cfg(feature = "diagnostics")]
        phase: Option<ScalarExecutePhaseAttribution>,
        #[cfg(feature = "diagnostics")]
        response_decode_local_instructions: u64,
    },
    Grouped {
        result: StructuralGroupedProjectionResult,
        trace: Option<ExecutionTrace>,
        #[cfg(feature = "diagnostics")]
        phase: Option<GroupedExecutePhaseAttribution>,
    },
    Delete {
        rows: EntityResponse<E>,
    },
    DeleteCount {
        row_count: u32,
    },
}

///
/// PreparedQueryExecutionOutput
///
/// PreparedQueryExecutionOutput tells the shared prepared-plan seam whether a
/// delete query should materialize deleted rows or use the count-only executor
/// terminal. The mode exists so `execute_delete_count` can share the same
/// session dispatch core without forcing row allocation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::query) enum PreparedQueryExecutionOutput {
    Rows,
    DeleteCount,
}

// Convert executor plan-surface failures at the session boundary so query error
// types do not import executor-owned error enums.
pub(in crate::db::session) fn query_error_from_executor_plan_error(
    err: ExecutorPlanError,
) -> QueryError {
    match err {
        ExecutorPlanError::Cursor(err) => QueryError::from_cursor_plan_error(*err),
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Validate that one execution strategy is admissible for scalar paged load
    // execution and fail closed on grouped/primary-key-only routes.
    pub(in crate::db::session::query) fn ensure_scalar_paged_execution_family(
        family: ExecutionFamily,
    ) -> Result<(), QueryError> {
        match family {
            ExecutionFamily::PrimaryKey => Err(QueryError::invariant(
                CursorPlanError::cursor_requires_explicit_or_grouped_ordering_message(),
            )),
            ExecutionFamily::Ordered => Ok(()),
            ExecutionFamily::Grouped => Err(QueryError::invariant(
                "grouped queries execute via execute(), not page().execute()",
            )),
        }
    }

    // Validate that one execution strategy is admissible for the grouped
    // execution surface.
    pub(in crate::db::session::query) fn ensure_grouped_execution_family(
        family: ExecutionFamily,
    ) -> Result<(), QueryError> {
        match family {
            ExecutionFamily::Grouped => Ok(()),
            ExecutionFamily::PrimaryKey | ExecutionFamily::Ordered => Err(QueryError::invariant(
                "grouped execution requires grouped logical plans",
            )),
        }
    }

    /// Execute one scalar load/delete query and return materialized response rows.
    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_query_result(query)
            .and_then(LoadQueryResult::into_rows)
    }

    // Execute one typed query through the unified row/grouped result surface so
    // higher layers do not need to branch on grouped shape themselves.
    #[doc(hidden)]
    pub fn execute_query_result<E>(
        &self,
        query: &Query<E>,
    ) -> Result<LoadQueryResult<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: compile typed intent into one prepared execution-plan
        // contract shared by scalar, grouped, and delete execution.
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        // Phase 2: execute through the canonical prepared-plan seam and adapt
        // the private executor outcome into the public session result shape.
        self.execute_prepared(query, plan, false, PreparedQueryExecutionOutput::Rows)
            .and_then(Self::load_result_from_prepared_outcome)
    }

    /// Execute one typed delete query and return only the affected-row count.
    #[doc(hidden)]
    pub fn execute_delete_count<E>(&self, query: &Query<E>) -> Result<u32, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: fail closed if the caller routes a non-delete query here.
        if !query.mode().is_delete() {
            return Err(QueryError::unsupported_query(
                "delete count execution requires delete query mode",
            ));
        }

        // Phase 2: resolve one cached prepared execution-plan contract directly
        // from the shared lower boundary instead of rebuilding it through the
        // typed compiled-query wrapper.
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        // Phase 3: execute through the shared prepared-plan seam while keeping
        // the count-only delete terminal that skips response-row materialization.
        match self.execute_prepared(
            query,
            plan,
            false,
            PreparedQueryExecutionOutput::DeleteCount,
        )? {
            PreparedQueryExecutionOutcome::DeleteCount { row_count } => Ok(row_count),
            PreparedQueryExecutionOutcome::Scalar { .. }
            | PreparedQueryExecutionOutcome::Grouped { .. }
            | PreparedQueryExecutionOutcome::Delete { .. } => Err(QueryError::invariant(
                "delete count execution returned non-count result",
            )),
        }
    }

    // Execute one prepared plan through the shared scalar/grouped/delete
    // dispatch. Diagnostics can request phase-attribution executor entrypoints;
    // normal execution keeps the existing non-attribution calls.
    pub(in crate::db::session::query) fn execute_prepared<E>(
        &self,
        query: &Query<E>,
        plan: PreparedExecutionPlan<E>,
        collect_attribution: bool,
        output: PreparedQueryExecutionOutput,
    ) -> Result<PreparedQueryExecutionOutcome<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        #[cfg(not(feature = "diagnostics"))]
        let _ = collect_attribution;

        if query.has_grouping() {
            if output == PreparedQueryExecutionOutput::DeleteCount {
                return Err(QueryError::invariant(
                    "delete count execution requires delete query mode",
                ));
            }

            #[cfg(feature = "diagnostics")]
            if collect_attribution {
                let (result, trace, phase) =
                    self.execute_grouped_with_cursor(plan, None, |executor, plan, cursor| {
                        executor.execute_grouped_paged_with_cursor_traced_with_phase_attribution(
                            plan, cursor,
                        )
                    })?;

                return Ok(PreparedQueryExecutionOutcome::Grouped {
                    result,
                    trace,
                    phase: Some(phase),
                });
            }

            let (result, trace) = self.execute_grouped_with_trace(plan, None)?;

            return Ok(PreparedQueryExecutionOutcome::Grouped {
                result,
                trace,
                #[cfg(feature = "diagnostics")]
                phase: None,
            });
        }

        match query.mode() {
            QueryMode::Load(_) => {
                if output == PreparedQueryExecutionOutput::DeleteCount {
                    return Err(QueryError::invariant(
                        "delete count execution requires delete query mode",
                    ));
                }

                #[cfg(feature = "diagnostics")]
                if collect_attribution {
                    let (rows, phase, response_decode_local_instructions) = self
                        .load_executor::<E>()
                        .execute_with_phase_attribution(plan)
                        .map_err(QueryError::execute)?;

                    return Ok(PreparedQueryExecutionOutcome::Scalar {
                        rows,
                        phase: Some(phase),
                        response_decode_local_instructions,
                    });
                }

                let rows = self
                    .with_metrics(|| self.load_executor::<E>().execute(plan))
                    .map_err(QueryError::execute)?;

                Ok(PreparedQueryExecutionOutcome::Scalar {
                    rows,
                    #[cfg(feature = "diagnostics")]
                    phase: None,
                    #[cfg(feature = "diagnostics")]
                    response_decode_local_instructions: 0,
                })
            }
            QueryMode::Delete(_) => match output {
                PreparedQueryExecutionOutput::Rows => {
                    let rows = self
                        .with_metrics(|| self.delete_executor::<E>().execute(plan))
                        .map_err(QueryError::execute)?;

                    Ok(PreparedQueryExecutionOutcome::Delete { rows })
                }
                PreparedQueryExecutionOutput::DeleteCount => {
                    let row_count = self
                        .with_metrics(|| self.delete_executor::<E>().execute_count(plan))
                        .map_err(QueryError::execute)?;

                    Ok(PreparedQueryExecutionOutcome::DeleteCount { row_count })
                }
            },
        }
    }

    // Adapt the canonical prepared-plan outcome to the public load-query
    // result shape. This is the only non-diagnostics adapter that understands
    // the private scalar/grouped/delete execution outcome variants.
    fn load_result_from_prepared_outcome<E>(
        outcome: PreparedQueryExecutionOutcome<E>,
    ) -> Result<LoadQueryResult<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match outcome {
            PreparedQueryExecutionOutcome::Scalar { rows, .. }
            | PreparedQueryExecutionOutcome::Delete { rows } => Ok(LoadQueryResult::Rows(rows)),
            PreparedQueryExecutionOutcome::Grouped { result, trace, .. } => {
                finalize_structural_grouped_projection_result(result, trace)
                    .map(LoadQueryResult::Grouped)
            }
            PreparedQueryExecutionOutcome::DeleteCount { .. } => Err(QueryError::invariant(
                "delete count result cannot be converted to load query result",
            )),
        }
    }

    // Shared load-query terminal wrapper: build plan, run under metrics, map
    // execution errors into query-facing errors.
    pub(in crate::db) fn execute_with_plan<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(LoadExecutor<E>, PreparedExecutionPlan<E>) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        self.with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::execute)
    }
}
