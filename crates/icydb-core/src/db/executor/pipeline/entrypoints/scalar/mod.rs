//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint orchestration and trace wiring.
//! Does not own: planner semantic ownership or grouped-runtime execution policy.
//! Boundary: executes scalar planned routes through load kernels and continuation inputs.

mod hints;
mod surface;

use crate::{
    db::{
        access::single_path_capabilities,
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPlan,
            ExecutionPreparation, ExecutionTrace, ResolvedScalarContinuationContext,
            ScalarRouteContinuationInvariantProjection,
            pipeline::contracts::{
                CursorPage, ErasedCursorPage, ExecutionInputs, ExecutionOutcomeMetrics,
                ExecutionRuntimeAdapter, LoadExecutor,
            },
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            plan_metrics::record_plan_metrics,
            validate_executor_plan,
        },
        index::IndexCompilePolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::pipeline::entrypoints::scalar::hints::apply_unpaged_top_n_seek_hints;

///
/// ScalarPathRuntime
///
/// ScalarPathRuntime keeps typed scalar execution leaves behind one
/// execution-focused runtime boundary.
/// Shared scalar entrypoint orchestration stays monomorphic by delegating route
/// planning and typed materialization through this trait.
///

trait ScalarPathRuntime {
    /// Build one canonical load route plan for the resolved scalar continuation.
    fn build_execution_route_plan_for_load(
        &self,
        plan: &AccessPlannedQuery,
        resolved_continuation: &ResolvedScalarContinuationContext,
    ) -> Result<ExecutionPlan, InternalError>;

    /// Materialize one scalar route attempt through the typed execution boundary.
    fn materialize_scalar_route(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_specs: &[crate::db::executor::LoweredIndexPrefixSpec],
        index_range_specs: &[crate::db::executor::LoweredIndexRangeSpec],
        route_plan: &ExecutionPlan,
        resolved_continuation: &ResolvedScalarContinuationContext,
    ) -> Result<(ErasedCursorPage, ExecutionOutcomeMetrics), InternalError>;
}

///
/// ScalarPathRuntimeAdapter
///
/// ScalarPathRuntimeAdapter captures one typed executor plus typed access sidecar
/// for scalar execution.
/// This keeps the monomorphic scalar spine free of entity generics while
/// preserving typed leaves for validation, context recovery, and page decode.
///

struct ScalarPathRuntimeAdapter<'a, E>
where
    E: EntityKind + EntityValue,
{
    executor: &'a LoadExecutor<E>,
    typed_access: &'a crate::db::access::AccessPlan<E::Key>,
}

impl<'a, E> ScalarPathRuntimeAdapter<'a, E>
where
    E: EntityKind + EntityValue,
{
    // Bind one typed executor plus typed access sidecar for scalar runtime calls.
    const fn new(
        executor: &'a LoadExecutor<E>,
        typed_access: &'a crate::db::access::AccessPlan<E::Key>,
    ) -> Self {
        Self {
            executor,
            typed_access,
        }
    }
}

///
/// ScalarPathExecution
///
/// ScalarPathExecution is the monomorphic scalar-spine output before typed page
/// downcast and final executor observability.
/// It keeps erased page payload, metrics, and optional trace together so the
/// generic wrapper only performs the typed boundary finish.
///

struct ScalarPathExecution {
    page: ErasedCursorPage,
    metrics: ExecutionOutcomeMetrics,
    trace: Option<ExecutionTrace>,
    execution_time_micros: u64,
}

// Execute one scalar route through the canonical monomorphic scalar spine.
fn execute_scalar_route_path(
    runtime: &dyn ScalarPathRuntime,
    plan: AccessPlannedQuery,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    resolved_continuation: ResolvedScalarContinuationContext,
    unpaged_rows_mode: bool,
    debug: bool,
) -> Result<ScalarPathExecution, InternalError> {
    // Phase 1: derive route/hint state from the structural load plan.
    let top_n_seek_requires_lookahead = plan
        .access_strategy()
        .as_path()
        .map(single_path_capabilities)
        .is_some_and(|capabilities| capabilities.requires_top_n_seek_lookahead());
    let mut route_plan =
        runtime.build_execution_route_plan_for_load(&plan, &resolved_continuation)?;
    apply_unpaged_top_n_seek_hints(
        &resolved_continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        &mut route_plan,
    );

    // Phase 2: project continuation invariants and optional trace setup once.
    let continuation = route_plan.continuation();
    let continuation_capabilities = continuation.capabilities();
    let continuation_applied = continuation_capabilities.applied();
    let continuation_invariants = ScalarRouteContinuationInvariantProjection::new(
        continuation_capabilities.strict_advance_required_when_applied(),
        continuation.effective_offset(),
    );
    resolved_continuation
        .debug_assert_route_continuation_invariants(&plan, continuation_invariants);
    let direction = route_plan.direction();
    let mut execution_trace =
        debug.then(|| ExecutionTrace::new(&plan.access, direction, continuation_applied));
    let execution_started_at = start_execution_timer();

    // Phase 3: materialize one typed page attempt through the runtime leaf.
    let (page, metrics) = runtime.materialize_scalar_route(
        &plan,
        index_prefix_specs.as_slice(),
        index_range_specs.as_slice(),
        &route_plan,
        &resolved_continuation,
    )?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);

    Ok(ScalarPathExecution {
        page,
        metrics,
        trace: execution_trace.take(),
        execution_time_micros,
    })
}

// Downcast one erased scalar page emitted by the scalar runtime boundary.
fn downcast_scalar_cursor_page<E: EntityKind>(
    page: ErasedCursorPage,
) -> Result<CursorPage<E>, InternalError> {
    page.into_typed("scalar runtime returned cursor page with unexpected entity type")
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Scalar execution spine:
    // 1) normalize continuation runtime bindings
    // 2) derive routing and trace contracts
    // 3) execute kernel materialization
    // 4) finalize scalar page + observability
    pub(in crate::db::executor) fn execute_scalar_path(
        &self,
        plan: ExecutablePlan<E>,
        resolved_continuation: ResolvedScalarContinuationContext,
        unpaged_rows_mode: bool,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let typed_access = plan.access().clone();
        let logical_plan = plan.into_inner();
        let runtime = ScalarPathRuntimeAdapter::new(self, &typed_access);
        let mut span = Span::<E>::new(ExecKind::Load);
        let ScalarPathExecution {
            page,
            metrics,
            mut trace,
            execution_time_micros,
        } = execute_scalar_route_path(
            &runtime,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
            resolved_continuation,
            unpaged_rows_mode,
            self.debug,
        )?;
        let page = downcast_scalar_cursor_page::<E>(page)?;
        let page =
            Self::finalize_execution(page, metrics, &mut span, &mut trace, execution_time_micros);

        Ok((page, trace))
    }
}

impl<E> ScalarPathRuntime for ScalarPathRuntimeAdapter<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn build_execution_route_plan_for_load(
        &self,
        plan: &AccessPlannedQuery,
        resolved_continuation: &ResolvedScalarContinuationContext,
    ) -> Result<ExecutionPlan, InternalError> {
        LoadExecutor::<E>::build_execution_route_plan_for_load(
            plan,
            resolved_continuation.route_context(),
            None,
        )
    }

    fn materialize_scalar_route(
        &self,
        plan: &AccessPlannedQuery,
        index_prefix_specs: &[crate::db::executor::LoweredIndexPrefixSpec],
        index_range_specs: &[crate::db::executor::LoweredIndexRangeSpec],
        route_plan: &ExecutionPlan,
        resolved_continuation: &ResolvedScalarContinuationContext,
    ) -> Result<(ErasedCursorPage, ExecutionOutcomeMetrics), InternalError> {
        // Phase 1: recover typed execution helpers once for this scalar attempt.
        validate_executor_plan::<E>(plan)?;
        let ctx = self.executor.db.recovered_context::<E>()?;
        let runtime = ExecutionRuntimeAdapter::new(&ctx, self.typed_access);
        let execution_preparation = ExecutionPreparation::from_plan(
            E::MODEL,
            plan,
            runtime.slot_map().map(<[usize]>::to_vec),
        );
        let direction = route_plan.direction();
        let continuation_bindings = resolved_continuation.bindings(direction);
        let execution_inputs = ExecutionInputs::new(
            &runtime,
            plan,
            AccessStreamBindings {
                index_prefix_specs,
                index_range_specs,
                continuation: resolved_continuation.access_scan_input(direction),
            },
            &execution_preparation,
        );

        // Phase 2: execute the shared kernel and erase the typed page at the boundary.
        record_plan_metrics(&plan.access);
        let materialized = ExecutionKernel::materialize_with_optional_residual_retry::<E>(
            &execution_inputs,
            route_plan,
            continuation_bindings,
            IndexCompilePolicy::ConservativeSubset,
        )?;
        let (page, metrics) = materialized.into_page_and_metrics();

        Ok((ErasedCursorPage::new(page), metrics))
    }
}
