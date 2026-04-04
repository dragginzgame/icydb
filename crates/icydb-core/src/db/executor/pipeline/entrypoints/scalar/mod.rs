//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint orchestration and trace wiring.
//! Does not own: planner semantic ownership or grouped-runtime execution policy.
//! Boundary: executes scalar planned routes through load kernels and continuation inputs.

mod hints;
mod surface;

use crate::{
    db::{
        Db,
        access::single_path_capabilities,
        cursor::PlannedCursor,
        direction::Direction,
        executor::aggregate::PreparedAggregateStreamingInputs,
        executor::{
            AccessStreamBindings, ContinuationEngine, EntityAuthority, ExecutionKernel,
            ExecutionPlan, ExecutionPreparation, ExecutionTrace, ExecutorPlanError,
            PreparedLoadPlan, ResolvedScalarContinuationContext,
            ScalarRouteContinuationInvariantProjection, StoreResolver, TraversalRuntime,
            pipeline::contracts::{
                ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntime,
                ExecutionRuntimeAdapter, LoadExecutor, StructuralCursorPage,
            },
            pipeline::runtime::finalize_structural_page_for_path,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            plan_metrics::record_plan_metrics,
            preparation::slot_map_for_model_plan,
            validate_executor_plan_for_authority,
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, OrderDirection, OrderSpec, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
};

use crate::db::executor::pipeline::entrypoints::scalar::hints::apply_unpaged_top_n_seek_hints;

// Keep SQL-only projection preservation decisions explicit without adding more
// free-floating bool flags to the shared scalar runtime bundles.
#[derive(Clone, Copy)]
enum ScalarProjectionRuntimeMode {
    SharedValidation,
    SqlImmediateMaterialization,
}

impl ScalarProjectionRuntimeMode {
    const fn validate_projection(self) -> bool {
        matches!(self, Self::SharedValidation)
    }

    const fn retain_slot_rows(self) -> bool {
        matches!(self, Self::SqlImmediateMaterialization)
    }
}

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
    plan: &'a AccessPlannedQuery,
    execution_preparation: ExecutionPreparation,
    route_plan: ExecutionPlan,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    resolved_continuation: ResolvedScalarContinuationContext,
    unpaged_rows_mode: bool,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
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
/// PreparedScalarRouteRuntime
///
/// PreparedScalarRouteRuntime is the generic-free scalar runtime bundle emitted
/// once the typed boundary resolves store authority, route planning, lowered
/// specs, and continuation inputs.
/// Kernel dispatch consumes this bundle directly so the scalar lane no longer
/// carries `LoadExecutor<E>` or `ExecutablePlan<E>` behind a runtime adapter.
///

pub(in crate::db::executor) struct PreparedScalarRouteRuntime {
    store: StoreHandle,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    route_plan: ExecutionPlan,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    resolved_continuation: ResolvedScalarContinuationContext,
    unpaged_rows_mode: bool,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
    debug: bool,
}

///
/// PreparedScalarMaterializedBoundary
///
/// PreparedScalarMaterializedBoundary is the neutral typed boundary payload for
/// non-aggregate scalar materialized terminal families.
/// It owns structural runtime authority, logical plan state, and lowered specs
/// needed to execute structural scalar materialization without reusing
/// `ExecutablePlan<E>` as the internal working contract.
///

pub(in crate::db::executor) struct PreparedScalarMaterializedBoundary<'ctx> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) store: StoreHandle,
    pub(in crate::db::executor) store_resolver: StoreResolver<'ctx>,
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery,
    pub(in crate::db::executor) index_prefix_specs:
        Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
}

impl PreparedScalarMaterializedBoundary<'_> {
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
            .primary_key_only_direction(self.authority.model().primary_key.name)
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
        projection_runtime_mode,
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
    resolved_continuation.debug_assert_route_continuation_invariants(plan, continuation_invariants);
    let direction = route_plan.direction();
    let mut execution_trace =
        debug.then(|| ExecutionTrace::new(&plan.access, direction, continuation_applied));
    let execution_started_at = start_execution_timer();

    // Phase 3: build canonical execution inputs and materialize the scalar route.
    let continuation_bindings = resolved_continuation.bindings(direction);
    let execution_inputs = ExecutionInputs::new(
        runtime,
        plan,
        AccessStreamBindings {
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            continuation: resolved_continuation.access_scan_input(direction),
        },
        &execution_preparation,
        projection_runtime_mode.validate_projection(),
        projection_runtime_mode.retain_slot_rows(),
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

// Execute one prepared scalar runtime bundle through the canonical monomorphic
// scalar spine without re-entering typed executor state.
fn execute_prepared_scalar_path_execution(
    prepared: PreparedScalarRouteRuntime,
) -> Result<ScalarPathExecution, InternalError> {
    let PreparedScalarRouteRuntime {
        store,
        authority,
        plan,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        projection_runtime_mode,
        debug,
    } = prepared;
    let slot_map = slot_map_for_model_plan(authority.model(), &plan);
    let execution_preparation =
        ExecutionPreparation::from_runtime_plan(authority.model(), &plan, slot_map);
    let runtime = ExecutionRuntimeAdapter::from_runtime_parts(
        &plan.access,
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority.model(),
    );

    execute_scalar_execution_stage(ScalarExecutionStage {
        runtime: &runtime,
        plan: &plan,
        execution_preparation,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        projection_runtime_mode,
        debug,
    })
}

/// Execute one prepared scalar runtime bundle and finalize the structural page.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    let entity_path = prepared.authority.entity_path();
    let ScalarPathExecution {
        page,
        metrics,
        mut trace,
        execution_time_micros,
    } = execute_prepared_scalar_path_execution(prepared)?;
    let page = finalize_structural_page_for_path(
        entity_path,
        page,
        metrics,
        &mut trace,
        execution_time_micros,
    );

    Ok((page, trace))
}

// Prepare one scalar runtime bundle once per canister instead of once per
// entity type. This keeps the scalar route-preparation spine shared across all
// entities that live inside the same canister.
fn prepare_scalar_route_runtime_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    resolved_continuation: ResolvedScalarContinuationContext,
    unpaged_rows_mode: bool,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let authority = plan.authority();
    let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
    let index_range_specs = plan.index_range_specs()?.to_vec();
    let logical_plan = plan.into_plan();

    // Phase 1: resolve structural store authority once at the canister
    // boundary.
    validate_executor_plan_for_authority(authority, &logical_plan)?;
    let store = db.recovered_store(authority.store_path())?;
    let route_plan = crate::db::executor::route::build_execution_route_plan_for_load_with_model(
        authority.model(),
        &logical_plan,
        resolved_continuation.route_context(),
        None,
    )?;

    // Phase 2: hand off the generic-free runtime bundle to scalar kernel
    // dispatch.
    Ok(PreparedScalarRouteRuntime {
        store,
        authority,
        plan: logical_plan,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
        debug,
    })
}

// Prepare one initial scalar runtime bundle for unpaged canister rows without
// retaining the resumed load-route builder.
fn prepare_initial_scalar_route_runtime_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let continuation_signature = plan.continuation_signature_for_runtime()?;
    let authority = plan.authority();
    let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
    let index_range_specs = plan.index_range_specs()?.to_vec();
    let logical_plan = plan.into_plan();

    // Phase 1: resolve structural store authority once at the canister
    // boundary.
    validate_executor_plan_for_authority(authority, &logical_plan)?;
    let store = db.recovered_store(authority.store_path())?;
    let route_plan =
        crate::db::executor::route::build_initial_execution_route_plan_for_load_with_model(
            authority.model(),
            &logical_plan,
            None,
        )?;

    // Phase 2: keep the unpaged rows lane on the fixed initial continuation contract.
    Ok(PreparedScalarRouteRuntime {
        store,
        authority,
        plan: logical_plan,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation: ContinuationEngine::resolve_scalar_context(
            PlannedCursor::none(),
            continuation_signature,
        ),
        unpaged_rows_mode: true,
        projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
        debug,
    })
}

// Execute one unpaged scalar rows path once per canister and return the
// structural page at the typed boundary.
pub(in crate::db::executor) fn execute_prepared_scalar_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: build one dedicated initial scalar runtime bundle for the
    // query-only canister rows surface.
    let prepared = prepare_initial_scalar_route_runtime_for_canister(db, debug, plan)?;

    // Phase 2: execute the shared scalar runtime and return the structural page.
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

/// Execute one initial scalar rows path directly from one structural load plan.
///
/// This SQL-only helper avoids rebuilding the broader prepared-load wrapper
/// when the canister query surface already has a fixed initial continuation.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_initial_scalar_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    let continuation_contract = plan
        .continuation_contract(authority.entity_path())
        .ok_or_else(|| {
            ExecutorPlanError::continuation_contract_requires_load_plan().into_internal_error()
        })?;
    let index_prefix_specs =
        crate::db::access::lower_index_prefix_specs(authority.entity_tag(), &plan.access).map_err(
            |_| ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error(),
        )?;
    let index_range_specs =
        crate::db::access::lower_index_range_specs(authority.entity_tag(), &plan.access).map_err(
            |_| ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error(),
        )?;

    // Phase 1: resolve structural store authority once at the canister
    // boundary.
    validate_executor_plan_for_authority(authority, &plan)?;
    let store = db.recovered_store(authority.store_path())?;
    let route_plan =
        crate::db::executor::route::build_initial_execution_route_plan_for_load_with_model(
            authority.model(),
            &plan,
            None,
        )?;

    // Phase 2: execute the shared scalar runtime on the fixed initial continuation contract.
    let prepared = PreparedScalarRouteRuntime {
        store,
        authority,
        plan,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation: ContinuationEngine::resolve_scalar_context(
            PlannedCursor::none(),
            continuation_contract.continuation_signature(),
        ),
        unpaged_rows_mode: true,
        // SQL projection materialization evaluates the projection immediately
        // after row load, so it should keep decoded slots instead of paying
        // the shared validation pass and then rebuilding structural readers.
        projection_runtime_mode: ScalarProjectionRuntimeMode::SqlImmediateMaterialization,
        debug,
    };
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

// Execute one fully materialized scalar rows path from already-resolved typed
// boundary inputs without re-entering the generic `execute(plan)` wrapper.
fn execute_scalar_materialized_rows_boundary<E>(
    executor: &LoadExecutor<E>,
    store: StoreHandle,
    authority: EntityAuthority,
    logical_plan: AccessPlannedQuery,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
) -> Result<StructuralCursorPage, InternalError>
where
    E: EntityKind + EntityValue,
{
    let continuation_contract = logical_plan
        .continuation_contract(authority.entity_path())
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar materialized rows path requires load-mode continuation contract",
            )
        })?;
    let resolved_continuation = ContinuationEngine::resolve_scalar_context(
        PlannedCursor::none(),
        continuation_contract.continuation_signature(),
    );

    // Phase 1: resolve typed execution authority once at the boundary.
    let slot_map = slot_map_for_model_plan(authority.model(), &logical_plan);
    let execution_preparation =
        ExecutionPreparation::from_runtime_plan(authority.model(), &logical_plan, slot_map);
    let runtime = ExecutionRuntimeAdapter::from_runtime_parts(
        &logical_plan.access,
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority.model(),
    );
    let mut route_plan =
        crate::db::executor::route::build_initial_execution_route_plan_for_load_with_model(
            authority.model(),
            &logical_plan,
            None,
        )?;

    // Phase 2: clear bounded scan hints before entering the shared scalar
    // runtime so materialized callers observe full row budgets.
    route_plan.scan_hints.physical_fetch_hint = None;
    route_plan.scan_hints.load_scan_budget_hint = None;

    let ScalarPathExecution {
        page,
        metrics,
        mut trace,
        execution_time_micros,
    } = execute_scalar_execution_stage(ScalarExecutionStage {
        runtime: &runtime,
        plan: &logical_plan,
        execution_preparation,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode: false,
        projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
        debug: executor.debug,
    })?;

    let page = finalize_structural_page_for_path(
        authority.entity_path(),
        page,
        metrics,
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
        plan: PreparedLoadPlan,
    ) -> Result<PreparedScalarMaterializedBoundary<'_>, InternalError> {
        let authority = plan.authority();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_plan();

        validate_executor_plan_for_authority(authority, &logical_plan)?;
        let store = self.db.recovered_store(authority.store_path())?;
        let store_resolver = self.db.store_resolver();

        Ok(PreparedScalarMaterializedBoundary {
            authority,
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
    pub(in crate::db::executor) fn prepare_scalar_route_runtime(
        &self,
        plan: PreparedLoadPlan,
        resolved_continuation: ResolvedScalarContinuationContext,
        unpaged_rows_mode: bool,
    ) -> Result<PreparedScalarRouteRuntime, InternalError> {
        prepare_scalar_route_runtime_for_canister(
            &self.db,
            self.debug,
            plan,
            resolved_continuation,
            unpaged_rows_mode,
        )
    }

    // Materialize one scalar page structurally from one already-prepared
    // aggregate/load stage without forcing typed entity reconstruction.
    pub(in crate::db::executor) fn execute_scalar_materialized_page_stage(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
    ) -> Result<StructuralCursorPage, InternalError> {
        execute_scalar_materialized_rows_boundary(
            self,
            prepared.store,
            prepared.authority,
            prepared.logical_plan,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
        )
    }

    // Materialize one scalar page structurally from the neutral non-aggregate
    // prepared boundary without forcing typed entity response assembly.
    pub(in crate::db::executor) fn execute_scalar_materialized_page_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_>,
    ) -> Result<StructuralCursorPage, InternalError> {
        execute_scalar_materialized_rows_boundary(
            self,
            prepared.store,
            prepared.authority,
            prepared.logical_plan,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
        )
    }
}
