//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint orchestration and trace wiring.
//! Does not own: planner semantic ownership or grouped-runtime execution policy.
//! Boundary: executes scalar planned routes through load kernels and continuation inputs.

mod hints;

use std::sync::Arc;

use crate::{
    db::{
        Db, PersistedRow,
        access::single_path_capabilities,
        cursor::PlannedCursor,
        data::decode_data_rows_into_cursor_page,
        executor::aggregate::PreparedAggregateStreamingInputs,
        executor::{
            AccessStreamBindings, EntityAuthority, ExecutionKernel, ExecutionPlan,
            ExecutionPreparation, ExecutionTrace, ExecutorPlanError, LoadCursorInput,
            PreparedLoadPlan, ScalarContinuationContext, StoreResolver, TraversalRuntime,
            pipeline::contracts::{
                CursorEmissionMode, CursorPage, ExecutionInputs, ExecutionOutcomeMetrics,
                ExecutionRuntimeAdapter, LoadExecutor, MaterializedExecutionPayload,
                PreparedExecutionProjection, ProjectionMaterializationMode, StructuralCursorPage,
            },
            pipeline::entrypoints::{LoadSurfaceMode, LoadTracingMode},
            pipeline::orchestrator::LoadExecutionSurface,
            pipeline::runtime::finalize_structural_page_for_path,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            plan_metrics::record_plan_metrics,
            planning::{
                preparation::slot_map_for_model_plan,
                route::{RoutePlanRequest, build_execution_route_plan},
            },
            validate_executor_plan_for_authority,
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, OrderSpec, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
};

use crate::db::executor::pipeline::entrypoints::scalar::hints::apply_unpaged_top_n_seek_hints;
#[cfg(feature = "diagnostics")]
use crate::db::executor::terminal::with_direct_data_row_phase_attribution;

type ScalarProjectionRuntimeMode = ProjectionMaterializationMode;

// Shared scalar runtime output tuple:
// 1) final materialized payload
// 2) path-outcome observability metrics
// 3) optional execution trace
// 4) elapsed execution time for finalization
type ScalarPathExecution = (
    MaterializedExecutionPayload,
    ExecutionOutcomeMetrics,
    Option<ExecutionTrace>,
    u64,
);

///
/// ScalarExecutePhaseAttribution
///
/// ScalarExecutePhaseAttribution records the internal scalar-load execute split
/// after a prepared plan has already crossed the session compile boundary.
/// It isolates the monomorphic runtime materialization spine from the final
/// structural page assembly step so perf tooling can see whether the remaining
/// floor lives in runtime traversal or page finalization.
///

#[cfg(feature = "diagnostics")]
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct ScalarExecutePhaseAttribution {
    pub(in crate::db) runtime_local_instructions: u64,
    pub(in crate::db) finalize_local_instructions: u64,
    pub(in crate::db) direct_data_row_scan_local_instructions: u64,
    pub(in crate::db) direct_data_row_key_stream_local_instructions: u64,
    pub(in crate::db) direct_data_row_row_read_local_instructions: u64,
    pub(in crate::db) direct_data_row_key_encode_local_instructions: u64,
    pub(in crate::db) direct_data_row_store_get_local_instructions: u64,
    pub(in crate::db) direct_data_row_order_window_local_instructions: u64,
    pub(in crate::db) direct_data_row_page_window_local_instructions: u64,
}

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_scalar_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
fn measure_scalar_execute_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_scalar_local_instruction_counter();
    let result = run();
    let delta = read_scalar_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

///
/// PreparedScalarRouteRuntime
///
/// PreparedScalarRouteRuntime is the generic-free scalar runtime bundle emitted
/// once the typed boundary resolves store authority, route planning, lowered
/// specs, and continuation inputs.
/// Kernel dispatch consumes this bundle directly so the scalar lane no longer
/// carries `LoadExecutor<E>` or `PreparedExecutionPlan<E>` behind a runtime adapter.
///

pub(in crate::db::executor) struct PreparedScalarRouteRuntime {
    store: StoreHandle,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    route_plan: ExecutionPlan,
    execution_preparation: ExecutionPreparation,
    prepared_projection: PreparedExecutionProjection,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    resolved_continuation: ScalarContinuationContext,
    unpaged_rows_mode: bool,
    cursor_emission: CursorEmissionMode,
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
/// `PreparedExecutionPlan<E>` as the internal working contract.
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

    /// Return whether the boundary still has a residual filter.
    #[must_use]
    pub(in crate::db::executor) fn has_predicate(&self) -> bool {
        self.logical_plan.has_residual_filter_expr()
            || self.logical_plan.has_residual_filter_predicate()
    }
}

///
/// ScalarRoutePlanFamily
///
/// ScalarRoutePlanFamily selects whether one scalar prepared runtime should
/// derive an initial route plan or retain a resumed continuation-aware route
/// plan during shared preparation.
/// Scalar entrypoint families use this to keep route-plan selection on one
/// helper instead of rebuilding authority/store setup in parallel flows.
///

enum ScalarRoutePlanFamily {
    Initial,
    Resumed,
}

///
/// ScalarPreparedRuntimeOptions
///
/// ScalarPreparedRuntimeOptions records the per-entrypoint knobs that still
/// vary after a caller has already resolved structural authority, logical
/// plan ownership, and lowered index specs.
/// The shared scalar preparation helper consumes this once so initial,
/// resumed, retained-slot, and materialized entrypoints all follow one build
/// path.
///

struct ScalarPreparedRuntimeOptions {
    resolved_continuation: ScalarContinuationContext,
    unpaged_rows_mode: bool,
    cursor_emission: CursorEmissionMode,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
    route_plan_family: ScalarRoutePlanFamily,
    suppress_route_scan_hints: bool,
}

// Build the shared scalar runtime bundle once after the caller has already
// resolved the store, route plan, continuation policy, and output mode for
// this scalar execution family.
#[expect(clippy::too_many_arguments)]
fn build_prepared_scalar_route_runtime(
    store: StoreHandle,
    authority: EntityAuthority,
    prepared_projection_validation: Option<
        Arc<crate::db::executor::projection::PreparedProjectionShape>,
    >,
    prepared_retained_slot_layout: Option<crate::db::executor::RetainedSlotLayout>,
    plan: AccessPlannedQuery,
    route_plan: ExecutionPlan,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    resolved_continuation: ScalarContinuationContext,
    unpaged_rows_mode: bool,
    cursor_emission: CursorEmissionMode,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
    debug: bool,
) -> PreparedScalarRouteRuntime {
    let slot_map = slot_map_for_model_plan(&plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&plan, slot_map);
    let prepared_projection = PreparedExecutionProjection::compile(
        authority,
        &plan,
        prepared_projection_validation,
        prepared_retained_slot_layout,
        projection_runtime_mode,
        cursor_emission,
    );

    PreparedScalarRouteRuntime {
        store,
        authority,
        plan,
        route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        debug,
    }
}

// Prepare one scalar runtime bundle after the caller has already resolved the
// structural inputs that stay constant across initial, resumed, retained-slot,
// and materialized scalar entrypoint families.
#[expect(clippy::too_many_arguments)]
fn prepare_scalar_route_runtime_from_parts<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    prepared_projection_validation: Option<
        Arc<crate::db::executor::projection::PreparedProjectionShape>,
    >,
    prepared_retained_slot_layout: Option<crate::db::executor::RetainedSlotLayout>,
    logical_plan: AccessPlannedQuery,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
    options: ScalarPreparedRuntimeOptions,
) -> Result<PreparedScalarRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let ScalarPreparedRuntimeOptions {
        resolved_continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        route_plan_family,
        suppress_route_scan_hints,
    } = options;

    // Phase 1: resolve structural store authority and derive the route plan.
    validate_executor_plan_for_authority(authority, &logical_plan)?;
    let store = db.recovered_store(authority.store_path())?;
    let mut route_plan = match route_plan_family {
        ScalarRoutePlanFamily::Initial => build_execution_route_plan(
            &logical_plan,
            RoutePlanRequest::Load {
                continuation: &ScalarContinuationContext::initial(),
                probe_fetch_hint: None,
                authority: Some(authority),
                load_terminal_fast_path: None,
            },
        )?,
        ScalarRoutePlanFamily::Resumed => build_execution_route_plan(
            &logical_plan,
            RoutePlanRequest::Load {
                continuation: &resolved_continuation,
                probe_fetch_hint: None,
                authority: Some(authority),
                load_terminal_fast_path: None,
            },
        )?,
    };

    // Phase 2: apply any route-local hint adjustments required by the caller.
    if suppress_route_scan_hints {
        route_plan.scan_hints.physical_fetch_hint = None;
        route_plan.scan_hints.load_scan_budget_hint = None;
    }

    // Phase 3: hand off one canonical prepared runtime bundle to scalar execution.
    Ok(build_prepared_scalar_route_runtime(
        store,
        authority,
        prepared_projection_validation,
        prepared_retained_slot_layout,
        logical_plan,
        route_plan,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        debug,
    ))
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one traced paged scalar load and materialize traced page output.
    pub(in crate::db::executor) fn execute_load_scalar_page_with_trace(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load_surface(
            plan,
            cursor,
            LoadSurfaceMode::scalar_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_scalar_traced_surface(surface)
    }

    // Project one traced paged scalar load surface and classify shape mismatches.
    fn expect_scalar_traced_surface(
        surface: LoadExecutionSurface,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        match surface {
            LoadExecutionSurface::ScalarPageWithTrace(page, trace) => {
                let (data_rows, next_cursor) = page.into_parts();

                Ok((
                    decode_data_rows_into_cursor_page::<E>(data_rows, next_cursor)?,
                    trace,
                ))
            }
            LoadExecutionSurface::GroupedPageWithTrace(..) => {
                Err(InternalError::query_executor_invariant(
                    "scalar traced entrypoint must produce scalar traced page surface",
                ))
            }
        }
    }
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
        mut route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        debug,
    } = prepared;
    let runtime = ExecutionRuntimeAdapter::from_scalar_runtime_parts(
        &plan.access,
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority,
    );

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
    let continuation_applied = continuation.applied();
    resolved_continuation.debug_assert_route_continuation_invariants(&plan, continuation);
    let direction = route_plan.direction();
    let mut execution_trace =
        debug.then(|| ExecutionTrace::new(&plan.access, direction, continuation_applied));
    let execution_started_at = start_execution_timer();

    // Phase 3: build canonical execution inputs and materialize the scalar route.
    let execution_inputs = ExecutionInputs::new_prepared(
        &runtime,
        &plan,
        AccessStreamBindings {
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            continuation: resolved_continuation.access_scan_input(direction),
        },
        &execution_preparation,
        projection_runtime_mode,
        prepared_projection,
        cursor_emission.enabled(),
    );
    record_plan_metrics(&plan.access);
    let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
        &execution_inputs,
        &route_plan,
        &resolved_continuation,
        IndexCompilePolicy::ConservativeSubset,
    )?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);
    let (payload, metrics) = materialized.into_payload_and_metrics();

    Ok((
        payload,
        metrics,
        execution_trace.take(),
        execution_time_micros,
    ))
}

// Finalize one scalar runtime tuple when the payload must be a structural page.
fn finalize_scalar_structural_path_execution(
    entity_path: &'static str,
    execution: ScalarPathExecution,
) -> (StructuralCursorPage, Option<ExecutionTrace>) {
    let (payload, metrics, mut trace, execution_time_micros) = execution;
    let page = payload;
    let page = finalize_structural_page_for_path(
        entity_path,
        page,
        metrics,
        &mut trace,
        execution_time_micros,
    );

    (page, trace)
}

// Execute one prepared scalar runtime bundle and finalize the shared
// structural page boundary in the common non-attributed path.
fn execute_prepared_scalar_structural_page(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    let entity_path = prepared.authority.entity_path();

    Ok(finalize_scalar_structural_path_execution(
        entity_path,
        execute_prepared_scalar_path_execution(prepared)?,
    ))
}

/// Execute one prepared scalar runtime bundle and finalize the structural page.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    execute_prepared_scalar_structural_page(prepared)
}

/// Execute one prepared scalar runtime bundle while reporting the internal
/// runtime/finalize split for perf-only attribution surfaces.
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime_with_phase_attribution(
    prepared: PreparedScalarRouteRuntime,
) -> Result<
    (
        StructuralCursorPage,
        Option<ExecutionTrace>,
        ScalarExecutePhaseAttribution,
    ),
    InternalError,
> {
    let entity_path = prepared.authority.entity_path();

    // Phase 1: run the monomorphic scalar runtime spine.
    let ((runtime_local_instructions, execution), direct_data_row_phase_attribution) =
        with_direct_data_row_phase_attribution(|| {
            measure_scalar_execute_phase(|| execute_prepared_scalar_path_execution(prepared))
        });
    let execution = execution?;

    // Phase 2: finalize the structural page and observability payload.
    let (finalize_local_instructions, finalized) = measure_scalar_execute_phase(|| {
        Ok::<(StructuralCursorPage, Option<ExecutionTrace>), InternalError>(
            finalize_scalar_structural_path_execution(entity_path, execution),
        )
    });
    let (page, trace) = finalized?;

    Ok((
        page,
        trace,
        ScalarExecutePhaseAttribution {
            runtime_local_instructions,
            finalize_local_instructions,
            direct_data_row_scan_local_instructions: direct_data_row_phase_attribution
                .scan_local_instructions,
            direct_data_row_key_stream_local_instructions: direct_data_row_phase_attribution
                .key_stream_local_instructions,
            direct_data_row_row_read_local_instructions: direct_data_row_phase_attribution
                .row_read_local_instructions,
            direct_data_row_key_encode_local_instructions: direct_data_row_phase_attribution
                .key_encode_local_instructions,
            direct_data_row_store_get_local_instructions: direct_data_row_phase_attribution
                .store_get_local_instructions,
            direct_data_row_order_window_local_instructions: direct_data_row_phase_attribution
                .order_window_local_instructions,
            direct_data_row_page_window_local_instructions: direct_data_row_phase_attribution
                .page_window_local_instructions,
        },
    ))
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
    let continuation_signature = plan.continuation_signature_for_runtime()?;
    let prepared = plan.into_scalar_runtime_parts(
        ScalarProjectionRuntimeMode::None,
        CursorEmissionMode::Suppress,
    )?;
    let prepared = prepare_scalar_route_runtime_from_parts(
        db,
        debug,
        prepared.authority,
        prepared.prepared_projection_shape,
        prepared.retained_slot_layout,
        prepared.plan,
        prepared.index_prefix_specs,
        prepared.index_range_specs,
        ScalarPreparedRuntimeOptions {
            resolved_continuation: ScalarContinuationContext::for_runtime(
                PlannedCursor::none(),
                continuation_signature,
            ),
            unpaged_rows_mode: true,
            cursor_emission: CursorEmissionMode::Suppress,
            projection_runtime_mode: ScalarProjectionRuntimeMode::None,
            route_plan_family: ScalarRoutePlanFamily::Initial,
            suppress_route_scan_hints: false,
        },
    )?;

    // Phase 2: execute the shared scalar runtime and return the structural page.
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

/// Execute one unpaged scalar rows path once per canister while reporting the
/// internal runtime/finalize split for perf-only fluent attribution.
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) fn execute_prepared_scalar_rows_for_canister_with_phase_attribution<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
) -> Result<(StructuralCursorPage, ScalarExecutePhaseAttribution), InternalError>
where
    C: CanisterKind,
{
    // Phase 1: build one dedicated initial scalar runtime bundle for the
    // query-only canister rows surface.
    let continuation_signature = plan.continuation_signature_for_runtime()?;
    let prepared = plan.into_scalar_runtime_parts(
        ScalarProjectionRuntimeMode::None,
        CursorEmissionMode::Suppress,
    )?;
    let prepared = prepare_scalar_route_runtime_from_parts(
        db,
        debug,
        prepared.authority,
        prepared.prepared_projection_shape,
        prepared.retained_slot_layout,
        prepared.plan,
        prepared.index_prefix_specs,
        prepared.index_range_specs,
        ScalarPreparedRuntimeOptions {
            resolved_continuation: ScalarContinuationContext::for_runtime(
                PlannedCursor::none(),
                continuation_signature,
            ),
            unpaged_rows_mode: true,
            cursor_emission: CursorEmissionMode::Suppress,
            projection_runtime_mode: ScalarProjectionRuntimeMode::None,
            route_plan_family: ScalarRoutePlanFamily::Initial,
            suppress_route_scan_hints: false,
        },
    )?;

    // Phase 2: execute the shared scalar runtime and return the structural page.
    let (page, _, phase_attribution) =
        execute_prepared_scalar_route_runtime_with_phase_attribution(prepared)?;

    Ok((page, phase_attribution))
}

/// Execute one retained-slot initial scalar rows path directly from one
/// structural load plan.
///
/// This helper avoids rebuilding the broader prepared-load wrapper when an
/// outer structural consumer already has a fixed initial continuation.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_initial_scalar_retained_slot_page_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    let continuation_contract = plan
        .planned_continuation_contract(authority.entity_path())
        .ok_or_else(|| {
            ExecutorPlanError::continuation_contract_requires_load_plan().into_internal_error()
        })?;
    let lowered_access = crate::db::access::lower_access(authority.entity_tag(), &plan.access)
        .map_err(|err| match err {
            crate::db::access::LoweredAccessError::IndexPrefix(_) => {
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            }
            crate::db::access::LoweredAccessError::IndexRange(_) => {
                ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error()
            }
        })?;
    let (_, index_prefix_specs, index_range_specs) = lowered_access.into_parts();

    // Phase 1: prepare the shared scalar runtime on the fixed initial continuation contract.
    let prepared = prepare_scalar_route_runtime_from_parts(
        db,
        debug,
        authority,
        None,
        None,
        plan,
        index_prefix_specs,
        index_range_specs,
        ScalarPreparedRuntimeOptions {
            resolved_continuation: ScalarContinuationContext::for_runtime(
                PlannedCursor::none(),
                continuation_contract.continuation_signature(),
            ),
            unpaged_rows_mode: true,
            cursor_emission: CursorEmissionMode::Suppress,
            projection_runtime_mode: ScalarProjectionRuntimeMode::RetainSlotRows,
            route_plan_family: ScalarRoutePlanFamily::Initial,
            suppress_route_scan_hints: false,
        },
    )?;
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

// Execute one fully materialized scalar rows path from already-resolved typed
// boundary inputs without re-entering the generic `execute(plan)` wrapper.
fn execute_scalar_materialized_rows_boundary<E>(
    executor: &LoadExecutor<E>,
    authority: EntityAuthority,
    logical_plan: AccessPlannedQuery,
    index_prefix_specs: Vec<crate::db::executor::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::executor::LoweredIndexRangeSpec>,
) -> Result<StructuralCursorPage, InternalError>
where
    E: EntityKind + EntityValue,
{
    let continuation_contract = logical_plan
        .planned_continuation_contract(authority.entity_path())
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "scalar materialized rows path requires load-mode continuation contract",
            )
        })?;
    let resolved_continuation = ScalarContinuationContext::for_runtime(
        PlannedCursor::none(),
        continuation_contract.continuation_signature(),
    );

    // Phase 1: execute the shared scalar runtime through the same prepared
    // route bundle used by the other scalar entrypoint families.
    let prepared = prepare_scalar_route_runtime_from_parts(
        &executor.db,
        executor.debug,
        authority,
        None,
        None,
        logical_plan,
        index_prefix_specs,
        index_range_specs,
        ScalarPreparedRuntimeOptions {
            resolved_continuation,
            unpaged_rows_mode: false,
            cursor_emission: CursorEmissionMode::Suppress,
            projection_runtime_mode: ScalarProjectionRuntimeMode::None,
            route_plan_family: ScalarRoutePlanFamily::Initial,
            suppress_route_scan_hints: true,
        },
    )?;
    let (page, _) = execute_prepared_scalar_structural_page(prepared)?;

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
        let prepared = plan.into_access_plan_parts()?;

        validate_executor_plan_for_authority(prepared.authority, &prepared.plan)?;
        let store = self.db.recovered_store(prepared.authority.store_path())?;
        let store_resolver = self.db.store_resolver();

        Ok(PreparedScalarMaterializedBoundary {
            authority: prepared.authority,
            store,
            store_resolver,
            logical_plan: prepared.plan,
            index_prefix_specs: prepared.index_prefix_specs,
            index_range_specs: prepared.index_range_specs,
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
        resolved_continuation: ScalarContinuationContext,
        unpaged_rows_mode: bool,
    ) -> Result<PreparedScalarRouteRuntime, InternalError> {
        let prepared = plan.into_scalar_runtime_parts(
            ScalarProjectionRuntimeMode::SharedValidation,
            CursorEmissionMode::Emit,
        )?;

        prepare_scalar_route_runtime_from_parts(
            &self.db,
            self.debug,
            prepared.authority,
            prepared.prepared_projection_shape,
            prepared.retained_slot_layout,
            prepared.plan,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
            ScalarPreparedRuntimeOptions {
                resolved_continuation,
                unpaged_rows_mode,
                cursor_emission: CursorEmissionMode::Emit,
                projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
                route_plan_family: ScalarRoutePlanFamily::Resumed,
                suppress_route_scan_hints: false,
            },
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
            prepared.authority,
            prepared.logical_plan,
            prepared.index_prefix_specs,
            prepared.index_range_specs,
        )
    }
}
