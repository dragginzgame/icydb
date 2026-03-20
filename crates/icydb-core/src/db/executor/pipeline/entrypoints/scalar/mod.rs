//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint orchestration and trace wiring.
//! Does not own: planner semantic ownership or grouped-runtime execution policy.
//! Boundary: executes scalar planned routes through load kernels and continuation inputs.

mod hints;
mod surface;

use crate::{
    db::{
        access::single_path_capabilities,
        cursor::PlannedCursor,
        direction::Direction,
        executor::{
            AccessStreamBindings, ContinuationEngine, ExecutablePlan, ExecutionKernel,
            ExecutionPlan, ExecutionPreparation, ExecutionTrace, ResolvedScalarContinuationContext,
            ScalarRouteContinuationInvariantProjection, StructuralStoreResolver,
            pipeline::contracts::{
                ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntime,
                ExecutionRuntimeAdapter, LoadExecutor, StructuralCursorPage,
            },
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            plan_metrics::record_plan_metrics,
            validate_executor_plan,
        },
        executor::{Context, aggregate::PreparedAggregateStreamingInputs},
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::pipeline::entrypoints::scalar::hints::apply_unpaged_top_n_seek_hints;

///
/// ScalarExecutionStage
///
/// ScalarExecutionStage is the structural scalar-load runtime contract built
/// once at the typed boundary.
/// It keeps scalar route planning, execution preparation, and continuation
/// inputs together so the shared scalar loop no longer depends on
/// `LoadExecutor<E>` or `ExecutablePlan<E>`.
///

struct ScalarExecutionStage<'a> {
    runtime: &'a dyn ExecutionRuntime,
    plan: AccessPlannedQuery,
    execution_preparation: ExecutionPreparation,
    route_plan: ExecutionPlan,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    resolved_continuation: ResolvedScalarContinuationContext,
    unpaged_rows_mode: bool,
    debug: bool,
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
    page: StructuralCursorPage,
    metrics: ExecutionOutcomeMetrics,
    trace: Option<ExecutionTrace>,
    execution_time_micros: u64,
}

///
/// PreparedScalarMaterializedBoundary
///
/// PreparedScalarMaterializedBoundary is the neutral typed boundary payload for
/// non-aggregate scalar materialized terminal families.
/// It owns the typed context, structural logical plan, and lowered specs
/// needed to execute structural scalar materialization without reusing
/// `ExecutablePlan<E>` as the internal working contract.
///

pub(in crate::db::executor) struct PreparedScalarMaterializedBoundary<
    'ctx,
    E: EntityKind + EntityValue,
> {
    pub(in crate::db::executor) ctx: Context<'ctx, E>,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) store_resolver: StructuralStoreResolver<'ctx>,
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery,
    pub(in crate::db::executor) index_prefix_specs:
        Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
}

impl<E> PreparedScalarMaterializedBoundary<'_, E>
where
    E: EntityKind + EntityValue,
{
    /// Borrow scalar row-consistency policy for boundary-owned row reads.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        crate::db::executor::traversal::row_read_consistency_for_plan(&self.logical_plan)
    }

    /// Borrow scalar ORDER BY contract at the non-aggregate scalar boundary.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.logical_plan.scalar_plan().order.as_ref()
    }

    /// Borrow scalar pagination contract at the non-aggregate scalar boundary.
    #[must_use]
    pub(in crate::db::executor) const fn page_spec(&self) -> Option<&PageSpec> {
        self.logical_plan.scalar_plan().page.as_ref()
    }

    /// Return whether the boundary still has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.logical_plan.scalar_plan().predicate.is_some()
    }

    /// Return whether the boundary still enables scalar DISTINCT semantics.
    #[must_use]
    pub(in crate::db::executor) const fn is_distinct(&self) -> bool {
        self.logical_plan.scalar_plan().distinct
    }

    /// Return whether predicate and DISTINCT gates are both clear.
    #[must_use]
    pub(in crate::db::executor) const fn has_no_predicate_or_distinct(&self) -> bool {
        !self.has_predicate() && !self.is_distinct()
    }

    /// Return unordered or primary-key-only direction when non-aggregate
    /// terminal routing can use that contract.
    #[must_use]
    pub(in crate::db::executor) fn unordered_or_primary_key_order_direction(
        &self,
    ) -> Option<Direction> {
        let Some(order) = self.order_spec() else {
            return Some(Direction::Asc);
        };

        order
            .primary_key_only_direction(E::MODEL.primary_key.name)
            .map(|direction| match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            })
    }
}

// Execute one scalar route through the canonical monomorphic scalar spine.
fn execute_scalar_execution_stage(
    stage: ScalarExecutionStage<'_>,
) -> Result<ScalarPathExecution, InternalError> {
    let ScalarExecutionStage {
        runtime,
        plan,
        execution_preparation,
        mut route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        debug,
    } = stage;

    // Phase 1: apply structural route hints derived from the scalar load plan.
    let top_n_seek_requires_lookahead = plan
        .access_strategy()
        .as_path()
        .map(single_path_capabilities)
        .is_some_and(|capabilities| capabilities.requires_top_n_seek_lookahead());
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

    // Phase 3: build canonical execution inputs and materialize the scalar route.
    let continuation_bindings = resolved_continuation.bindings(direction);
    let execution_inputs = ExecutionInputs::new(
        runtime,
        &plan,
        AccessStreamBindings {
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            continuation: resolved_continuation.access_scan_input(direction),
        },
        &execution_preparation,
    );
    record_plan_metrics(&plan.access);
    let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
        &execution_inputs,
        &route_plan,
        continuation_bindings,
        IndexCompilePolicy::ConservativeSubset,
    )?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);
    let (page, metrics) = materialized.into_page_and_metrics();

    Ok(ScalarPathExecution {
        page,
        metrics,
        trace: execution_trace.take(),
        execution_time_micros,
    })
}

// Execute one fully materialized scalar rows path from already-resolved typed
// boundary inputs without re-entering the generic `execute(plan)` wrapper.
fn execute_scalar_materialized_rows_boundary<E>(
    executor: &LoadExecutor<E>,
    ctx: Context<'_, E>,
    logical_plan: AccessPlannedQuery,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
) -> Result<StructuralCursorPage, InternalError>
where
    E: EntityKind + EntityValue,
{
    let continuation_contract = logical_plan.continuation_contract(E::PATH).ok_or_else(|| {
        crate::db::error::query_executor_invariant(
            "scalar materialized rows path requires load-mode continuation contract",
        )
    })?;
    let resolved_continuation = ContinuationEngine::resolve_scalar_context(
        PlannedCursor::none(),
        continuation_contract.continuation_signature(),
    );

    // Phase 1: resolve typed execution authority once at the boundary.
    let structural_access = logical_plan.access.clone();
    let runtime = ExecutionRuntimeAdapter::new(&ctx, &structural_access)?;
    let execution_preparation = ExecutionPreparation::from_plan(
        E::MODEL,
        &logical_plan,
        runtime.slot_map().map(<[usize]>::to_vec),
    );
    let mut route_plan = LoadExecutor::<E>::build_execution_route_plan_for_load(
        &logical_plan,
        resolved_continuation.route_context(),
        None,
    )?;

    // Phase 2: clear bounded scan hints before entering the shared scalar
    // runtime so materialized callers observe full row budgets.
    route_plan.scan_hints.physical_fetch_hint = None;
    route_plan.scan_hints.load_scan_budget_hint = None;

    let mut span = Span::<E>::new(ExecKind::Load);
    let ScalarPathExecution {
        page,
        metrics,
        mut trace,
        execution_time_micros,
    } = execute_scalar_execution_stage(ScalarExecutionStage {
        runtime: &runtime,
        plan: logical_plan,
        execution_preparation,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode: false,
        debug: executor.debug,
    })?;

    let page = LoadExecutor::<E>::finalize_structural_page(
        page,
        metrics,
        &mut span,
        &mut trace,
        execution_time_micros,
    );

    Ok(page)
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Consume one typed scalar plan into the neutral non-aggregate
    // materialized-terminal boundary payload.
    pub(in crate::db::executor) fn prepare_scalar_materialized_boundary(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<PreparedScalarMaterializedBoundary<'_, E>, InternalError> {
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_plan();

        validate_executor_plan::<E>(&logical_plan)?;
        let ctx = self.db.recovered_context::<E>()?;
        let store = ctx.structural_store()?;
        let store_resolver = self.db.structural_store_resolver();

        Ok(PreparedScalarMaterializedBoundary {
            ctx,
            store,
            store_resolver,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
        })
    }

    // Scalar execution spine:
    // 1) resolve typed boundary inputs once
    // 2) build one structural scalar execution stage
    // 3) execute the shared scalar runtime
    // 4) finalize typed page + observability
    pub(in crate::db::executor) fn execute_scalar_path(
        &self,
        plan: ExecutablePlan<E>,
        resolved_continuation: ResolvedScalarContinuationContext,
        unpaged_rows_mode: bool,
    ) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_plan();

        // Phase 1: resolve all typed execution authority once at the boundary.
        validate_executor_plan::<E>(&logical_plan)?;
        let ctx = self.db.recovered_context::<E>()?;
        let structural_access = logical_plan.access.clone();
        let runtime = ExecutionRuntimeAdapter::new(&ctx, &structural_access)?;
        let execution_preparation = ExecutionPreparation::from_plan(
            E::MODEL,
            &logical_plan,
            runtime.slot_map().map(<[usize]>::to_vec),
        );
        let route_plan = Self::build_execution_route_plan_for_load(
            &logical_plan,
            resolved_continuation.route_context(),
            None,
        )?;

        // Phase 2: hand off to the structural scalar execution stage.
        let mut span = Span::<E>::new(ExecKind::Load);
        let ScalarPathExecution {
            page,
            metrics,
            mut trace,
            execution_time_micros,
        } = execute_scalar_execution_stage(ScalarExecutionStage {
            runtime: &runtime,
            plan: logical_plan,
            execution_preparation,
            route_plan,
            index_prefix_specs,
            index_range_specs,
            resolved_continuation,
            unpaged_rows_mode,
            debug: self.debug,
        })?;

        // Phase 3: finalize observability before the final typed surface projection.
        let page = Self::finalize_structural_page(
            page,
            metrics,
            &mut span,
            &mut trace,
            execution_time_micros,
        );

        Ok((page, trace))
    }

    // Materialize one scalar page structurally from one already-prepared
    // aggregate/load stage without forcing typed entity reconstruction.
    pub(in crate::db::executor) fn execute_scalar_materialized_page_stage(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_, E>,
    ) -> Result<StructuralCursorPage, InternalError> {
        execute_scalar_materialized_rows_boundary(
            self,
            prepared.ctx,
            prepared.logical_plan,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
        )
    }

    // Materialize one scalar page structurally from the neutral non-aggregate
    // prepared boundary without forcing typed entity response assembly.
    pub(in crate::db::executor) fn execute_scalar_materialized_page_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_, E>,
    ) -> Result<StructuralCursorPage, InternalError> {
        execute_scalar_materialized_rows_boundary(
            self,
            prepared.ctx,
            prepared.logical_plan,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
        )
    }
}
