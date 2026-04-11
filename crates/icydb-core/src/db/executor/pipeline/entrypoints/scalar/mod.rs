//! Module: executor::pipeline::entrypoints::scalar
//! Responsibility: scalar load execution entrypoint orchestration and trace wiring.
//! Does not own: planner semantic ownership or grouped-runtime execution policy.
//! Boundary: executes scalar planned routes through load kernels and continuation inputs.

mod hints;

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
            aggregate::runtime::finalize_path_outcome_for_path,
            pipeline::contracts::{
                CoveringComponentScanState, CursorPage, ExecutionInputs, ExecutionOutcomeMetrics,
                ExecutionOutputOptions, ExecutionRuntimeAdapter, LoadExecutor,
                MaterializedExecutionPayload, PreparedExecutionProjection,
                ProjectionMaterializationMode, StructuralCursorPage,
            },
            pipeline::entrypoints::{LoadSurfaceMode, LoadTracingMode},
            pipeline::orchestrator::LoadExecutionSurface,
            pipeline::runtime::finalize_structural_page_for_path,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            plan_metrics::record_plan_metrics,
            planning::{
                preparation::slot_map_for_model_plan,
                route::{
                    build_execution_route_plan_for_load,
                    build_initial_execution_route_plan_for_load,
                },
            },
            validate_executor_plan_for_authority,
        },
        index::IndexCompilePolicy,
        predicate::MissingRowPolicy,
        query::plan::{AccessPlannedQuery, OrderSpec, PageSpec},
        registry::StoreHandle,
    },
    error::InternalError,
    metrics::sink::{ExecKind, PathSpan},
    traits::{CanisterKind, EntityKind, EntityValue},
    value::Value,
};

use crate::db::executor::pipeline::entrypoints::scalar::hints::apply_unpaged_top_n_seek_hints;

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
    projection_runtime_mode: ScalarProjectionRuntimeMode,
    fuse_immediate_sql_terminal: bool,
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

    /// Return whether the boundary still has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) fn has_predicate(&self) -> bool {
        self.logical_plan.has_residual_predicate()
    }
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
        projection_runtime_mode,
        fuse_immediate_sql_terminal,
        debug,
    } = prepared;
    let runtime = ExecutionRuntimeAdapter::from_scalar_runtime_parts(
        &plan.access,
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority,
        CoveringComponentScanState {
            entity_tag: authority.entity_tag(),
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
        },
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
        ExecutionOutputOptions::new(
            projection_runtime_mode.emit_cursor(),
            fuse_immediate_sql_terminal,
        ),
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
    structural_payload_error: &'static str,
    execution: ScalarPathExecution,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    let (payload, metrics, mut trace, execution_time_micros) = execution;
    let MaterializedExecutionPayload::StructuralPage(page) = payload else {
        return Err(InternalError::query_executor_invariant(
            structural_payload_error,
        ));
    };
    let page = finalize_structural_page_for_path(
        entity_path,
        page,
        metrics,
        &mut trace,
        execution_time_micros,
    );

    Ok((page, trace))
}

/// Execute one prepared scalar runtime bundle and finalize the structural page.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    let entity_path = prepared.authority.entity_path();
    finalize_scalar_structural_path_execution(
        entity_path,
        "shared scalar route runtime must finalize one structural cursor page",
        execute_prepared_scalar_path_execution(prepared)?,
    )
}

// Prepare one scalar runtime bundle once per canister instead of once per
// entity type. This keeps the scalar route-preparation spine shared across all
// entities that live inside the same canister.
fn prepare_scalar_route_runtime_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    resolved_continuation: ScalarContinuationContext,
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
    let route_plan = build_execution_route_plan_for_load(
        authority,
        &logical_plan,
        &resolved_continuation,
        None,
    )?;
    let slot_map = slot_map_for_model_plan(&logical_plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&logical_plan, slot_map);
    let prepared_projection = PreparedExecutionProjection::compile(
        authority,
        &logical_plan,
        execution_preparation.compiled_predicate(),
        ScalarProjectionRuntimeMode::SharedValidation,
        route_plan.load_terminal_fast_path(),
    )?;

    // Phase 2: hand off the generic-free runtime bundle to scalar kernel
    // dispatch.
    Ok(PreparedScalarRouteRuntime {
        store,
        authority,
        plan: logical_plan,
        route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode,
        projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
        fuse_immediate_sql_terminal: false,
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
    let route_plan = build_initial_execution_route_plan_for_load(authority, &logical_plan, None)?;
    let slot_map = slot_map_for_model_plan(&logical_plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&logical_plan, slot_map);
    let prepared_projection = PreparedExecutionProjection::compile(
        authority,
        &logical_plan,
        execution_preparation.compiled_predicate(),
        ScalarProjectionRuntimeMode::SharedValidation,
        route_plan.load_terminal_fast_path(),
    )?;

    // Phase 2: keep the unpaged rows lane on the fixed initial continuation contract.
    Ok(PreparedScalarRouteRuntime {
        store,
        authority,
        plan: logical_plan,
        route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation: ScalarContinuationContext::for_runtime(
            PlannedCursor::none(),
            continuation_signature,
        ),
        unpaged_rows_mode: true,
        projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
        fuse_immediate_sql_terminal: false,
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
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub(in crate::db) fn execute_initial_scalar_sql_projection_page_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
) -> Result<StructuralCursorPage, InternalError>
where
    C: CanisterKind,
{
    let continuation_contract = plan
        .planned_continuation_contract(authority.entity_path())
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
    let route_plan = build_initial_execution_route_plan_for_load(authority, &plan, None)?;
    let slot_map = slot_map_for_model_plan(&plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&plan, slot_map);
    let prepared_projection = PreparedExecutionProjection::compile(
        authority,
        &plan,
        execution_preparation.compiled_predicate(),
        projection_runtime_mode,
        route_plan.load_terminal_fast_path(),
    )?;

    // Phase 2: execute the shared scalar runtime on the fixed initial continuation contract.
    let prepared = PreparedScalarRouteRuntime {
        store,
        authority,
        plan,
        route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation: ScalarContinuationContext::for_runtime(
            PlannedCursor::none(),
            continuation_contract.continuation_signature(),
        ),
        unpaged_rows_mode: true,
        // SQL projection entrypoints choose their terminal materialization
        // contract up front so the shared scalar runtime can stay structural.
        projection_runtime_mode,
        fuse_immediate_sql_terminal: false,
        debug,
    };
    let (page, _) = execute_prepared_scalar_route_runtime(prepared)?;

    Ok(page)
}

#[cfg(feature = "sql")]
fn finalize_immediate_sql_terminal_for_path(
    entity_path: &'static str,
    row_count: usize,
    metrics: ExecutionOutcomeMetrics,
    execution_trace: &mut Option<ExecutionTrace>,
    execution_time_micros: u64,
) {
    finalize_path_outcome_for_path(
        entity_path,
        execution_trace,
        metrics,
        false,
        execution_time_micros,
    );
    let mut span = PathSpan::new(ExecKind::Load, entity_path);
    span.set_rows(u64::try_from(row_count).unwrap_or(u64::MAX));
}

#[cfg(feature = "sql")]
fn execute_initial_scalar_sql_projection_payload_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    projection_runtime_mode: ScalarProjectionRuntimeMode,
) -> Result<MaterializedExecutionPayload, InternalError>
where
    C: CanisterKind,
{
    let continuation_contract = plan
        .planned_continuation_contract(authority.entity_path())
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

    // Phase 1: resolve structural store authority and execution preparation once.
    validate_executor_plan_for_authority(authority, &plan)?;
    let store = db.recovered_store(authority.store_path())?;
    let route_plan = build_initial_execution_route_plan_for_load(authority, &plan, None)?;
    let slot_map = slot_map_for_model_plan(&plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&plan, slot_map);
    let prepared_projection = PreparedExecutionProjection::compile(
        authority,
        &plan,
        execution_preparation.compiled_predicate(),
        projection_runtime_mode,
        route_plan.load_terminal_fast_path(),
    )?;

    // Phase 2: execute the shared scalar runtime with direct terminal fusion enabled.
    let prepared = PreparedScalarRouteRuntime {
        store,
        authority,
        plan,
        route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation: ScalarContinuationContext::for_runtime(
            PlannedCursor::none(),
            continuation_contract.continuation_signature(),
        ),
        unpaged_rows_mode: true,
        projection_runtime_mode,
        fuse_immediate_sql_terminal: true,
        debug,
    };
    let entity_path = prepared.authority.entity_path();
    let (payload, metrics, mut trace, execution_time_micros) =
        execute_prepared_scalar_path_execution(prepared)?;
    let row_count = match &payload {
        MaterializedExecutionPayload::StructuralPage(page) => page.row_count(),
        MaterializedExecutionPayload::SqlProjectedRows(rows) => rows.len(),
        MaterializedExecutionPayload::SqlRenderedRows(rows) => rows.len(),
    };
    finalize_immediate_sql_terminal_for_path(
        entity_path,
        row_count,
        metrics,
        &mut trace,
        execution_time_micros,
    );

    Ok(payload)
}

#[cfg(feature = "sql")]
pub(in crate::db) fn execute_initial_scalar_sql_projection_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<Vec<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    match execute_initial_scalar_sql_projection_payload_for_canister(
        db,
        debug,
        authority,
        plan,
        ScalarProjectionRuntimeMode::SqlImmediateMaterialization,
    )? {
        MaterializedExecutionPayload::SqlProjectedRows(rows) => Ok(rows),
        MaterializedExecutionPayload::StructuralPage(_)
        | MaterializedExecutionPayload::SqlRenderedRows(_) => {
            Err(InternalError::query_executor_invariant(
                "immediate SQL projection value path did not return projected rows",
            ))
        }
    }
}

#[cfg(feature = "sql")]
pub(in crate::db) fn execute_initial_scalar_sql_projection_text_rows_for_canister<C>(
    db: &Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<Vec<Vec<String>>, InternalError>
where
    C: CanisterKind,
{
    match execute_initial_scalar_sql_projection_payload_for_canister(
        db,
        debug,
        authority,
        plan,
        ScalarProjectionRuntimeMode::SqlImmediateRenderedDispatch,
    )? {
        MaterializedExecutionPayload::SqlRenderedRows(rows) => Ok(rows),
        MaterializedExecutionPayload::StructuralPage(_)
        | MaterializedExecutionPayload::SqlProjectedRows(_) => {
            Err(InternalError::query_executor_invariant(
                "immediate SQL projection text path did not return rendered rows",
            ))
        }
    }
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

    // Phase 1: resolve structural execution preparation once at the boundary.
    let slot_map = slot_map_for_model_plan(&logical_plan);
    let execution_preparation = ExecutionPreparation::from_runtime_plan(&logical_plan, slot_map);
    let mut route_plan =
        build_initial_execution_route_plan_for_load(authority, &logical_plan, None)?;

    // Phase 2: shared materialized scalar boundaries suppress routed scan
    // hints so route-owned ordered streaming contracts cannot leak back in as
    // executor-local materialized shortcuts.
    route_plan.scan_hints.physical_fetch_hint = None;
    route_plan.scan_hints.load_scan_budget_hint = None;
    let prepared_projection = PreparedExecutionProjection::compile(
        authority,
        &logical_plan,
        execution_preparation.compiled_predicate(),
        ScalarProjectionRuntimeMode::SharedValidation,
        route_plan.load_terminal_fast_path(),
    )?;

    // Phase 3: execute the shared scalar runtime through the same prepared
    // route bundle used by the other scalar entrypoint families.
    let prepared = PreparedScalarRouteRuntime {
        store,
        authority,
        plan: logical_plan,
        route_plan,
        execution_preparation,
        prepared_projection,
        index_prefix_specs,
        index_range_specs,
        resolved_continuation,
        unpaged_rows_mode: false,
        projection_runtime_mode: ScalarProjectionRuntimeMode::SharedValidation,
        fuse_immediate_sql_terminal: false,
        debug: executor.debug,
    };
    let (page, _) = finalize_scalar_structural_path_execution(
        authority.entity_path(),
        "shared scalar materialized boundary must emit one structural page",
        execute_prepared_scalar_path_execution(prepared)?,
    )?;

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
        resolved_continuation: ScalarContinuationContext,
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
