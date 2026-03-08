use crate::{
    db::{
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPlan,
            ExecutionPreparation, ExecutionTrace, LoadCursorInput,
            ResolvedScalarContinuationContext, ScalarRouteContinuationInvariantProjection,
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
use std::time::Instant;

// Strategy selected once for unpaged scalar execution hinting so the route-plan
// mutation phase applies one mechanical outcome.
enum UnpagedLoadHintStrategy {
    None,
    TopNSeekWindow { fetch: usize },
    PreserveSecondaryOrder,
}

impl UnpagedLoadHintStrategy {
    fn resolve(
        resolved_continuation: &ResolvedScalarContinuationContext,
        unpaged_rows_mode: bool,
        top_n_seek_requires_lookahead: bool,
        route_plan: &ExecutionPlan,
    ) -> Self {
        if !unpaged_rows_mode || resolved_continuation.cursor_boundary().is_some() {
            return Self::None;
        }

        if let Some(top_n_seek_spec) = route_plan.top_n_seek_spec() {
            if !route_plan.shape().is_streaming() || !route_plan.streaming_access_shape_safe() {
                return Self::None;
            }

            let fetch = if top_n_seek_spec.fetch() == 0 {
                0
            } else if !top_n_seek_requires_lookahead {
                let Some(fetch) = route_plan.continuation().window().fetch_count_for(false) else {
                    return Self::None;
                };

                fetch
            } else {
                // Deduplicating lookup shapes need one extra lookahead row to
                // preserve parity after key normalization before windowing.
                top_n_seek_spec.fetch()
            };

            return Self::TopNSeekWindow { fetch };
        }

        if route_plan.secondary_fast_path_eligible()
            && route_plan.scan_hints.physical_fetch_hint.is_none()
        {
            return Self::PreserveSecondaryOrder;
        }

        Self::None
    }

    fn apply(self, route_plan: &mut ExecutionPlan) {
        match self {
            Self::None => {}
            Self::TopNSeekWindow { fetch } => {
                route_plan.scan_hints.physical_fetch_hint = Some(fetch);
                route_plan.scan_hints.load_scan_budget_hint = Some(fetch);
            }
            Self::PreserveSecondaryOrder => {
                route_plan.scan_hints.physical_fetch_hint = Some(usize::MAX);
            }
        }
    }
}

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
        unpaged_rows_mode: bool,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        let top_n_seek_requires_lookahead = logical_plan
            .access_strategy()
            .as_path()
            .map(|path| path.capabilities())
            .is_some_and(|capabilities| capabilities.requires_top_n_seek_lookahead());
        let mut route_plan = Self::build_execution_route_plan_for_load(
            &logical_plan,
            resolved_continuation.route_context(),
            None,
        )?;
        Self::apply_unpaged_top_n_seek_hints(
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
        let execution_started_at = Instant::now();

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
            let execution_time_micros =
                u64::try_from(execution_started_at.elapsed().as_micros()).unwrap_or(u64::MAX);

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

    // Unpaged `execute()` does not need continuation lookahead rows. For
    // route-eligible top-N seek windows, constrain both access probe and load
    // scan-budget hints to the keep-count window (without continuation +1).
    fn apply_unpaged_top_n_seek_hints(
        resolved_continuation: &ResolvedScalarContinuationContext,
        unpaged_rows_mode: bool,
        top_n_seek_requires_lookahead: bool,
        route_plan: &mut ExecutionPlan,
    ) {
        let strategy = UnpagedLoadHintStrategy::resolve(
            resolved_continuation,
            unpaged_rows_mode,
            top_n_seek_requires_lookahead,
            route_plan,
        );

        strategy.apply(route_plan);
    }
}
