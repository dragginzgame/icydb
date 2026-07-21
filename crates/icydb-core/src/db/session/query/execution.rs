//! Module: db::session::query::execution
//! Responsibility: canonical query execution dispatch and executor error mapping.
//! Does not own: diagnostics attribution, cursor decoding, fluent adaptation, or explain surfaces.
//! Boundary: maps prepared plans into executor calls and query-facing response/error types.

#[cfg(feature = "diagnostics")]
use crate::db::executor::{GroupedExecutePhaseAttribution, ScalarExecutePhaseAttribution};
use crate::{
    db::{
        DbSession, EntityResponse, LoadQueryResult, PersistedRow, Query, QueryError,
        diagnostics::ExecutionTrace,
        executor::{
            ExecutionFamily, ExecutorPlanError, LoadExecutor, PreparedExecutionPlan,
            StructuralGroupedProjectionResult,
        },
        query::plan::QueryMode,
        schema::AcceptedValueCatalogHandle,
        session::finalize_structural_grouped_projection_result,
    },
    error::InternalError,
    traits::CanisterKind,
    types::Id,
    value::Value,
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
    reason = "the private grouped outcome keeps its execution trace and optional diagnostics attribution inline so attribution does not measure a boundary-only box allocation"
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

/// Runtime output paired with the exact accepted catalog retained by the
/// guarded plan that produced it.
pub(in crate::db) struct AcceptedExecutionOutput<T> {
    value: T,
    value_catalog: AcceptedValueCatalogHandle,
}

pub(in crate::db) type AcceptedValuesOutput = AcceptedExecutionOutput<Vec<Value>>;
pub(in crate::db) type AcceptedIdValuesOutput<E> = AcceptedExecutionOutput<Vec<(Id<E>, Value)>>;
pub(in crate::db) type AcceptedOptionalValueOutput = AcceptedExecutionOutput<Option<Value>>;

impl<T> AcceptedExecutionOutput<T> {
    #[must_use]
    pub(in crate::db) const fn new(value: T, value_catalog: AcceptedValueCatalogHandle) -> Self {
        Self {
            value,
            value_catalog,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (T, AcceptedValueCatalogHandle) {
        (self.value, self.value_catalog)
    }

    #[must_use]
    pub(in crate::db) fn into_value(self) -> T {
        self.value
    }
}

///
/// PreparedQueryExecutionOutput
///
/// PreparedQueryExecutionOutput tells the shared prepared-plan path whether a
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
    // Fail closed before a cached prepared plan reaches row access when its
    // retained catalog authority is no longer the store's current root.
    pub(in crate::db::session) fn ensure_prepared_query_plan_is_current<E>(
        &self,
        plan: &PreparedExecutionPlan<E>,
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let authority = plan
            .accepted_schema_authority()
            .map_err(QueryError::execute)?;

        self.ensure_accepted_schema_authority_is_current::<E>(authority)
            .map_err(QueryError::execute)
    }

    // Validate that one execution strategy is admissible for scalar paged load
    // execution and fail closed on grouped/primary-key-only routes.
    pub(in crate::db::session::query) fn ensure_scalar_paged_execution_family(
        family: ExecutionFamily,
    ) -> Result<(), QueryError> {
        match family {
            ExecutionFamily::Ordered => Ok(()),
            ExecutionFamily::PrimaryKey | ExecutionFamily::Grouped => Err(QueryError::invariant()),
        }
    }

    // Validate that one execution strategy is admissible for the grouped
    // execution surface.
    pub(in crate::db::session::query) fn ensure_grouped_execution_family(
        family: ExecutionFamily,
    ) -> Result<(), QueryError> {
        match family {
            ExecutionFamily::Grouped => Ok(()),
            ExecutionFamily::PrimaryKey | ExecutionFamily::Ordered => Err(QueryError::invariant()),
        }
    }

    /// Execute one scalar load query through a rows-only dispatch path.
    ///
    /// This keeps row-only fluent terminals from retaining grouped and delete
    /// executor branches through the broad `LoadQueryResult` boundary.
    pub fn execute_scalar_query_rows<E>(
        &self,
        query: &Query<E>,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;
        self.ensure_prepared_query_plan_is_current(&plan)?;

        if plan.is_grouped() {
            return Err(QueryError::invariant());
        }

        match plan.mode() {
            QueryMode::Load(_) => self
                .with_metrics(|| self.load_executor::<E>().execute(plan))
                .map_err(QueryError::execute),
            QueryMode::Delete(_) => Err(QueryError::unsupported_query()),
        }
    }

    /// Execute one typed delete query and materialize the deleted rows.
    #[doc(hidden)]
    pub fn execute_delete_rows<E>(&self, query: &Query<E>) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        // Phase 1: fail closed if the caller routes a non-delete query here.
        if !query.mode().is_delete() {
            return Err(QueryError::unsupported_query());
        }

        // Phase 2: resolve one cached prepared execution-plan contract from
        // the shared lower boundary.
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        // Phase 3: execute through the shared prepared-plan path while keeping
        // the row-returning delete terminal explicit.
        match self.execute_prepared(plan, false, PreparedQueryExecutionOutput::Rows)? {
            PreparedQueryExecutionOutcome::Delete { rows } => Ok(rows),
            PreparedQueryExecutionOutcome::Scalar { .. }
            | PreparedQueryExecutionOutcome::Grouped { .. }
            | PreparedQueryExecutionOutcome::DeleteCount { .. } => Err(QueryError::invariant()),
        }
    }

    // Execute one typed query through the unified row/grouped result surface so
    // higher layers do not need to branch on grouped shape themselves.
    #[doc(hidden)]
    pub fn execute_query_result<E>(
        &self,
        query: &Query<E>,
    ) -> Result<LoadQueryResult<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        // Phase 1: compile typed intent into one prepared execution-plan
        // contract shared by scalar, grouped, and delete execution.
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        // Phase 2: execute through the canonical prepared-plan path and adapt
        // the private executor outcome into the public session result shape.
        self.execute_prepared(plan, false, PreparedQueryExecutionOutput::Rows)
            .and_then(Self::load_result_from_prepared_outcome)
    }

    /// Execute one typed delete query and return only the affected-row count.
    #[doc(hidden)]
    pub fn execute_delete_count<E>(&self, query: &Query<E>) -> Result<u32, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        // Phase 1: fail closed if the caller routes a non-delete query here.
        if !query.mode().is_delete() {
            return Err(QueryError::unsupported_query());
        }

        // Phase 2: resolve one cached prepared execution-plan contract directly
        // from the shared lower boundary instead of rebuilding it through the
        // typed compiled-query wrapper.
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;

        // Phase 3: execute through the shared prepared-plan path while keeping
        // the count-only delete terminal that skips response-row materialization.
        match self.execute_prepared(plan, false, PreparedQueryExecutionOutput::DeleteCount)? {
            PreparedQueryExecutionOutcome::DeleteCount { row_count } => Ok(row_count),
            PreparedQueryExecutionOutcome::Scalar { .. }
            | PreparedQueryExecutionOutcome::Grouped { .. }
            | PreparedQueryExecutionOutcome::Delete { .. } => Err(QueryError::invariant()),
        }
    }

    // Execute one prepared plan through the shared scalar/grouped/delete
    // dispatch. Diagnostics can request phase-attribution executor entrypoints;
    // normal execution keeps the existing non-attribution calls.
    pub(in crate::db::session::query) fn execute_prepared<E>(
        &self,
        plan: PreparedExecutionPlan<E>,
        collect_attribution: bool,
        output: PreparedQueryExecutionOutput,
    ) -> Result<PreparedQueryExecutionOutcome<E>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        #[cfg(not(feature = "diagnostics"))]
        let _ = collect_attribution;

        if plan.is_grouped() {
            if output == PreparedQueryExecutionOutput::DeleteCount {
                return Err(QueryError::invariant());
            }

            #[cfg(feature = "diagnostics")]
            if collect_attribution {
                let (result, trace, phase) =
                    self.execute_grouped_with_phase_attribution(plan, None)?;

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

        self.ensure_prepared_query_plan_is_current(&plan)?;

        match plan.mode() {
            QueryMode::Load(_) => {
                if output == PreparedQueryExecutionOutput::DeleteCount {
                    return Err(QueryError::invariant());
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
        E: PersistedRow<Canister = C>,
    {
        match outcome {
            PreparedQueryExecutionOutcome::Scalar { rows, .. }
            | PreparedQueryExecutionOutcome::Delete { rows } => Ok(LoadQueryResult::Rows(rows)),
            PreparedQueryExecutionOutcome::Grouped { result, trace, .. } => {
                finalize_structural_grouped_projection_result(result, trace)
                    .map(LoadQueryResult::Grouped)
            }
            PreparedQueryExecutionOutcome::DeleteCount { .. } => Err(QueryError::invariant()),
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
        E: PersistedRow<Canister = C>,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;
        self.ensure_prepared_query_plan_is_current(&plan)?;

        self.with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::execute)
    }

    // Execute one value-producing operation while retaining the exact catalog
    // handle carried by the guarded plan for later outward rendering.
    pub(in crate::db) fn execute_with_plan_and_catalog<E, T>(
        &self,
        query: &Query<E>,
        op: impl FnOnce(LoadExecutor<E>, PreparedExecutionPlan<E>) -> Result<T, InternalError>,
    ) -> Result<AcceptedExecutionOutput<T>, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(query)?;
        self.ensure_prepared_query_plan_is_current(&plan)?;
        let value_catalog = plan
            .accepted_value_catalog_handle()
            .map_err(QueryError::execute)?
            .clone();
        let value = self
            .with_metrics(|| op(self.load_executor::<E>(), plan))
            .map_err(QueryError::execute)?;

        Ok(AcceptedExecutionOutput::new(value, value_catalog))
    }
}
