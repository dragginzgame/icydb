use crate::{
    db::{
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            ExecutionTrace, LoadCursorInput, ResolvedScalarContinuationContext,
            ScalarRouteContinuationInvariantProjection,
            load::{
                CursorPage, ExecutionInputs, LoadExecutor,
                entrypoints::{LoadExecutionMode, LoadExecutionSurface, LoadTracingMode},
                invariant,
            },
            plan_metrics::record_plan_metrics,
            validate_executor_plan,
        },
        index::IndexCompilePolicy,
        response::EntityResponse,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one unpaged scalar load and materialize rows.
    pub(in crate::db::executor::load) fn execute_load_scalar_rows(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<EntityResponse<E>, InternalError> {
        let surface = self.execute_load(plan, cursor, LoadExecutionMode::scalar_unpaged_rows())?;
        match surface {
            LoadExecutionSurface::ScalarRows(rows) => Ok(rows),
            _ => Err(invariant(
                "scalar rows entrypoint must produce scalar rows surface",
            )),
        }
    }

    // Execute one paged scalar load and materialize page output.
    pub(in crate::db::executor::load) fn execute_load_scalar_page(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<CursorPage<E>, InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Disabled),
        )?;
        match surface {
            LoadExecutionSurface::ScalarPage(page) => Ok(page),
            _ => Err(invariant(
                "scalar page entrypoint must produce scalar page surface",
            )),
        }
    }

    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor::load) fn execute_load_scalar_page_with_trace(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::scalar_paged(LoadTracingMode::Enabled),
        )?;
        match surface {
            LoadExecutionSurface::ScalarPageWithTrace(page, trace) => Ok((page, trace)),
            _ => Err(invariant(
                "scalar traced entrypoint must produce scalar traced page surface",
            )),
        }
    }

    // Scalar execution spine:
    // 1) normalize continuation runtime bindings
    // 2) derive routing and trace contracts
    // 3) execute kernel materialization
    // 4) finalize scalar page + observability
    pub(in crate::db::executor::load) fn execute_scalar_path(
        &self,
        plan: ExecutablePlan<E>,
        resolved_continuation: ResolvedScalarContinuationContext,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        let route_plan = Self::build_execution_route_plan_for_load(
            &logical_plan,
            resolved_continuation.route_context(),
            None,
        )?;
        let continuation = route_plan.continuation();
        let continuation_applied = continuation.applied();
        let continuation_invariants = ScalarRouteContinuationInvariantProjection::new(
            continuation.strict_advance_required_when_applied(),
            continuation.window().effective_offset,
        );
        resolved_continuation
            .debug_assert_route_continuation_invariants(&logical_plan, continuation_invariants);
        let direction = route_plan.direction();
        let continuation_bindings = resolved_continuation.bindings(direction);
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(&logical_plan.access, direction, continuation_applied));
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&logical_plan);

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);

            validate_executor_plan::<E>(&logical_plan)?;
            let ctx = self.db.recovered_context::<E>()?;
            let execution_inputs = ExecutionInputs::new(
                &ctx,
                &logical_plan,
                AccessStreamBindings {
                    index_prefix_specs: index_prefix_specs.as_slice(),
                    index_range_specs: index_range_specs.as_slice(),
                    continuation: resolved_continuation.access_scan_input(direction),
                },
                &execution_preparation,
            );

            record_plan_metrics(&logical_plan.access);
            let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
                &execution_inputs,
                &route_plan,
                continuation_bindings,
                IndexCompilePolicy::ConservativeSubset,
            )?;
            let (page, metrics) = materialized.into_page_and_metrics();

            Ok(Self::finalize_execution(
                page,
                metrics,
                &mut span,
                &mut execution_trace,
            ))
        })();

        result.map(|page| (page, execution_trace))
    }
}
