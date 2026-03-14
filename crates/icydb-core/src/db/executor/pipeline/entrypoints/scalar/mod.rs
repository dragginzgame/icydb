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
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            ExecutionTrace, ResolvedScalarContinuationContext,
            ScalarRouteContinuationInvariantProjection,
            pipeline::contracts::{CursorPage, ExecutionInputs, LoadExecutor},
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            plan_metrics::record_plan_metrics,
            validate_executor_plan,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::pipeline::entrypoints::scalar::hints::apply_unpaged_top_n_seek_hints;

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
        let logical_plan = plan.into_inner();
        let top_n_seek_requires_lookahead = logical_plan
            .access_strategy()
            .as_path()
            .map(single_path_capabilities)
            .is_some_and(|capabilities| capabilities.requires_top_n_seek_lookahead());
        let mut route_plan = Self::build_execution_route_plan_for_load(
            &logical_plan,
            resolved_continuation.route_context(),
            None,
        )?;
        apply_unpaged_top_n_seek_hints(
            &resolved_continuation,
            unpaged_rows_mode,
            top_n_seek_requires_lookahead,
            &mut route_plan,
        );
        let continuation = route_plan.continuation();
        let continuation_capabilities = continuation.capabilities();
        let continuation_applied = continuation_capabilities.applied();
        let continuation_invariants = ScalarRouteContinuationInvariantProjection::new(
            continuation_capabilities.strict_advance_required_when_applied(),
            continuation.effective_offset(),
        );
        resolved_continuation
            .debug_assert_route_continuation_invariants(&logical_plan, continuation_invariants);
        let direction = route_plan.direction();
        let continuation_bindings = resolved_continuation.bindings(direction);
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(&logical_plan.access, direction, continuation_applied));
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&logical_plan);
        let execution_started_at = start_execution_timer();

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
            let execution_time_micros = elapsed_execution_micros(execution_started_at);

            Ok(Self::finalize_execution(
                page,
                metrics,
                &mut span,
                &mut execution_trace,
                execution_time_micros,
            ))
        })();

        result.map(|page| (page, execution_trace))
    }
}
